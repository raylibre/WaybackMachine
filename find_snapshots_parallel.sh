#!/bin/bash

DOMAIN="$1"
TARGET_DATE="$2"
MAX_PARALLEL="${3:-8}"  # Количество параллельных процессов

if [ -z "$DOMAIN" ] || [ -z "$TARGET_DATE" ]; then
    echo "Использование: $0 DOMAIN TARGET_DATE [MAX_PARALLEL]"
    echo "Пример: $0 ba.org.ua 20191115 8"
    echo "TARGET_DATE в формате YYYYMMDD"
    echo "MAX_PARALLEL - количество параллельных процессов (по умолчанию: 8)"
    exit 1
fi

# Проверяем формат даты
if ! [[ "$TARGET_DATE" =~ ^[0-9]{8}$ ]]; then
    echo "❌ Неверный формат даты. Используйте YYYYMMDD"
    exit 1
fi

# Проверяем наличие мастер-списка
MASTER_LIST="${DOMAIN}_master_list.json"
if [ ! -f "$MASTER_LIST" ]; then
    echo "❌ Мастер-список не найден: $MASTER_LIST"
    echo "Сначала выполните: ./create_master_list.sh $DOMAIN"
    exit 1
fi

echo "⚡ ПАРАЛЛЕЛЬНЫЙ поиск снапшотов для $DOMAIN на дату $TARGET_DATE"
echo "=============================================================="
echo "🔥 Параллельных процессов: $MAX_PARALLEL"

# Функция для вычисления даты (кроссплатформенная)
calculate_date() {
    local base_date="$1"
    local days_offset="$2"

    local year="${base_date:0:4}"
    local month="${base_date:4:2}"
    local day="${base_date:6:2}"
    local formatted_date="$year-$month-$day"

    if [[ "$OSTYPE" == "darwin"* ]]; then
        if command -v gdate >/dev/null 2>&1; then
            gdate -d "$formatted_date $days_offset days" +%Y%m%d
        else
            python3 -c "
import datetime
base = datetime.datetime.strptime('$formatted_date', '%Y-%m-%d')
result = base + datetime.timedelta(days=$days_offset)
print(result.strftime('%Y%m%d'))
"
        fi
    else
        date -d "$formatted_date $days_offset days" +%Y%m%d
    fi
}

# Вычисляем временное окно
FROM_DATE=$(calculate_date "$TARGET_DATE" "-90")
TO_DATE=$(calculate_date "$TARGET_DATE" "90")

echo "📅 Временное окно: $FROM_DATE - $TO_DATE"

# Создаем временные директории
TEMP_DIR="temp_parallel_$$"
mkdir -p "$TEMP_DIR"

echo "🧠 Разбиваем мастер-список на batch'и для параллельной обработки..."

# Разбиваем мастер-список на части
total_urls=$(jq length "$MASTER_LIST")
urls_per_batch=$(( (total_urls + MAX_PARALLEL - 1) / MAX_PARALLEL ))

echo "📊 Всего URL: $total_urls"
echo "📦 URL в батче: $urls_per_batch"

# Создаем batch файлы
for i in $(seq 0 $((MAX_PARALLEL - 1))); do
    start_index=$((i * urls_per_batch))
    jq ".[$start_index:$((start_index + urls_per_batch))]" "$MASTER_LIST" > "$TEMP_DIR/batch_$i.json"
done

# Функция для обработки одного batch'а
process_batch() {
    local batch_id="$1"
    local batch_file="$TEMP_DIR/batch_$batch_id.json"
    local result_file="$TEMP_DIR/result_$batch_id.json"

    if [ ! -s "$batch_file" ]; then
        echo "[]" > "$result_file"
        return
    fi

    echo "🚀 Batch $batch_id: старт обработки"

    # Создаем список всех URL из batch'а для одного CDX запроса
    local urls_list
    urls_list=$(jq -r '.[].original' "$batch_file" | head -20 | tr '\n' ' ')  # Ограничиваем 20 URL на запрос

    if [ -z "$urls_list" ]; then
        echo "[]" > "$result_file"
        return
    fi

    # Формируем URL паттерн для множественного поиска
    local url_pattern
    if [ $(jq length "$batch_file") -eq 1 ]; then
        # Один URL
        url_pattern=$(jq -r '.[0].original' "$batch_file")
    else
        # Множественные URL - используем домен + wildcards
        url_pattern="${DOMAIN}/*"
    fi

    # CDX запрос для batch'а
    local cdx_response
    cdx_response=$(curl -s "http://web.archive.org/cdx/search/cdx?url=${url_pattern}&from=$FROM_DATE&to=$TO_DATE&output=json&fl=timestamp,original,statuscode,mimetype,length&filter=statuscode:200&filter=mimetype:text/html&limit=10000" 2>/dev/null)

    if [ -z "$cdx_response" ] || [ "$cdx_response" = "[]" ]; then
        echo "[]" > "$result_file"
        echo "⚠️  Batch $batch_id: нет данных"
        return
    fi

    # Python обработка batch'а
    python3 << EOF
import json
import sys

try:
    # Загружаем данные batch'а
    with open("$batch_file", "r") as f:
        batch_urls = json.load(f)

    if not batch_urls:
        with open("$result_file", "w") as f:
            json.dump([], f)
        sys.exit(0)

    # Создаем set URL из batch'а для быстрого поиска
    batch_url_set = {item['original'] for item in batch_urls}

    # Парсим CDX ответ
    cdx_data = json.loads('$cdx_response')

    if len(cdx_data) <= 1:
        with open("$result_file", "w") as f:
            json.dump([], f)
        sys.exit(0)

    # Индексируем по URL
    target_timestamp = int("${TARGET_DATE}000000")
    url_best_snapshot = {}

    for row in cdx_data[1:]:  # Пропускаем заголовок
        if len(row) >= 5:
            try:
                timestamp, original, statuscode, mimetype, size = row[:5]

                # Проверяем что URL из нашего batch'а
                if original not in batch_url_set:
                    continue

                snap_timestamp = int(timestamp)
                time_diff = abs(snap_timestamp - target_timestamp)

                if original not in url_best_snapshot or time_diff < url_best_snapshot[original]['time_diff']:
                    url_best_snapshot[original] = {
                        'timestamp': timestamp,
                        'original': original,
                        'statuscode': statuscode,
                        'size': int(size) if size.isdigit() else 0,
                        'time_diff': time_diff,
                        'days_diff': time_diff // 1000000
                    }
            except (ValueError, IndexError):
                continue

    # Формируем результат
    results = []
    for url_data in url_best_snapshot.values():
        snapshot = {
            'archive_url': f"https://web.archive.org/web/{url_data['timestamp']}/{url_data['original']}",
            'timestamp': url_data['timestamp'],
            'original_url': url_data['original'],
            'statuscode': url_data['statuscode'],
            'size': url_data['size'],
            'days_diff': url_data['days_diff']
        }
        results.append(snapshot)

    # Сохраняем результат
    with open("$result_file", "w") as f:
        json.dump(results, f, indent=2)

    print(f"✅ Batch $batch_id: найдено {len(results)} снапшотов")

except Exception as e:
    print(f"❌ Batch $batch_id: ошибка - {e}")
    with open("$result_file", "w") as f:
        json.dump([], f)
EOF

    echo "✅ Batch $batch_id: завершен"
}

# Экспортируем функцию для использования в xargs
export -f process_batch
export TEMP_DIR FROM_DATE TO_DATE TARGET_DATE DOMAIN

echo ""
echo "🔥 Запускаем параллельную обработку ($MAX_PARALLEL процессов)..."

# Запускаем параллельную обработку
seq 0 $((MAX_PARALLEL - 1)) | xargs -n 1 -P "$MAX_PARALLEL" -I {} bash -c 'process_batch {}'

echo ""
echo "🔗 Объединяем результаты..."

# Объединяем все результаты
SNAPSHOT_FILE="${DOMAIN}_snapshots_${TARGET_DATE}.json"

python3 << EOF
import json
import glob

all_snapshots = []

# Читаем все result файлы
for result_file in glob.glob("$TEMP_DIR/result_*.json"):
    try:
        with open(result_file, "r") as f:
            batch_results = json.load(f)
            all_snapshots.extend(batch_results)
    except:
        continue

# Удаляем дубликаты по original_url
seen_urls = set()
unique_snapshots = []
for snapshot in all_snapshots:
    url = snapshot['original_url']
    if url not in seen_urls:
        seen_urls.add(url)
        unique_snapshots.append(snapshot)

# Сортируем по близости к дате
unique_snapshots.sort(key=lambda x: x['days_diff'])

# Сохраняем результат
with open("$SNAPSHOT_FILE", "w") as f:
    json.dump(unique_snapshots, f, indent=2, ensure_ascii=False)

print(f"📄 Уникальных снапшотов: {len(unique_snapshots)}")
EOF

# Удаляем временные файлы
rm -rf "$TEMP_DIR"

# Финальная статистика
if [ -f "$SNAPSHOT_FILE" ]; then
    final_count=$(jq length "$SNAPSHOT_FILE" 2>/dev/null || echo "0")

    echo ""
    echo "⚡ ПАРАЛЛЕЛЬНЫЙ ПОИСК ЗАВЕРШЕН!"
    echo "==============================="
    echo "📊 Статистика:"
    echo "  📄 URL в мастер-списке: $total_urls"
    echo "  📸 Найдено снапшотов: $final_count"
    echo "  ✅ Успешность: $(( final_count * 100 / total_urls ))%"

    if [ "$final_count" -gt 0 ]; then
        echo ""
        echo "🎯 ТОП-5 ближайших снапшотов:"
        jq -r '.[:5] | .[] | "  \(.days_diff) дней: \(.original_url)"' "$SNAPSHOT_FILE"
    fi

    echo ""
    echo "📁 Результат: $SNAPSHOT_FILE"
    echo "🚀 Следующий шаг: poetry run wayback-analyzer download-snapshot $DOMAIN --date $TARGET_DATE"
else
    echo "❌ Ошибка создания файла результатов"
    exit 1
fi