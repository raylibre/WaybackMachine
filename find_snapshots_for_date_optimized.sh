#!/bin/bash

DOMAIN="$1"
TARGET_DATE="$2"

if [ -z "$DOMAIN" ] || [ -z "$TARGET_DATE" ]; then
    echo "Использование: $0 DOMAIN TARGET_DATE"
    echo "Пример: $0 ba.org.ua 20191115"
    echo "TARGET_DATE в формате YYYYMMDD"
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

echo "🚀 ОПТИМИЗИРОВАННЫЙ поиск снапшотов для $DOMAIN на дату $TARGET_DATE"
echo "=============================================================="

# Улучшенная функция для вычисления даты (работает на macOS и Linux)
calculate_date() {
    local base_date="$1"
    local days_offset="$2"

    # Преобразуем YYYYMMDD в формат YYYY-MM-DD
    local year="${base_date:0:4}"
    local month="${base_date:4:2}"
    local day="${base_date:6:2}"
    local formatted_date="$year-$month-$day"

    # Проверяем ОС и используем соответствующую команду
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v gdate >/dev/null 2>&1; then
            # GNU date установлен через homebrew
            gdate -d "$formatted_date $days_offset days" +%Y%m%d
        else
            # Используем встроенный date с другим синтаксисом
            date -j -v"${days_offset}d" -f "%Y-%m-%d" "$formatted_date" +%Y%m%d 2>/dev/null || {
                # Fallback: используем Python
                python3 -c "
import datetime
base = datetime.datetime.strptime('$formatted_date', '%Y-%m-%d')
result = base + datetime.timedelta(days=$days_offset)
print(result.strftime('%Y%m%d'))
"
            }
        fi
    else
        # Linux
        date -d "$formatted_date $days_offset days" +%Y%m%d
    fi
}

# Вычисляем временное окно ±90 дней
echo "🔄 Вычисляем временное окно..."
FROM_DATE=$(calculate_date "$TARGET_DATE" "-90")
TO_DATE=$(calculate_date "$TARGET_DATE" "90")

if [ -z "$FROM_DATE" ] || [ -z "$TO_DATE" ]; then
    echo "❌ Ошибка вычисления дат. Проверьте формат целевой даты."
    exit 1
fi

echo "📅 Временное окно: $FROM_DATE - $TO_DATE"

# НОВАЯ СТРАТЕГИЯ: Один большой CDX запрос на весь домен
echo "🎯 СТРАТЕГИЯ: Один большой запрос вместо тысяч маленьких"
echo ""

SNAPSHOT_FILE="${DOMAIN}_snapshots_${TARGET_DATE}.json"
TEMP_CDX_FILE="${DOMAIN}_cdx_${TARGET_DATE}.json"
TEMP_MATCHED_FILE="${DOMAIN}_matched_${TARGET_DATE}.json"

echo "🌐 Шаг 1: Получение ВСЕХ снапшотов домена за период..."
echo "   (Это может занять 30-60 секунд)"

# Один мега-запрос на весь домен в временном окне
curl -s "http://web.archive.org/cdx/search/cdx?url=${DOMAIN}/*&from=$FROM_DATE&to=$TO_DATE&output=json&fl=timestamp,original,statuscode,mimetype,length&filter=statuscode:200&filter=mimetype:text/html&limit=100000" > "$TEMP_CDX_FILE"

# Проверяем успешность запроса
if [ ! -s "$TEMP_CDX_FILE" ] || ! jq empty "$TEMP_CDX_FILE" 2>/dev/null; then
    echo "❌ Ошибка получения данных от CDX API"
    rm -f "$TEMP_CDX_FILE"
    exit 1
fi

# Получаем количество найденных снапшотов
CDX_COUNT=$(($(jq length "$TEMP_CDX_FILE") - 1))
echo "✅ Получено снапшотов из архива: $CDX_COUNT"

if [ "$CDX_COUNT" -le 0 ]; then
    echo "❌ Снапшотов не найдено в указанном временном окне"
    rm -f "$TEMP_CDX_FILE"
    exit 1
fi

echo ""
echo "🧠 Шаг 2: Умное сопоставление с мастер-списком..."

# Python скрипт для эффективного сопоставления
python3 << EOF
import json
import sys
from datetime import datetime

# Загружаем данные
print("📖 Читаем мастер-список...")
with open("$MASTER_LIST", "r") as f:
    master_list = json.load(f)

print("📖 Читаем CDX данные...")
with open("$TEMP_CDX_FILE", "r") as f:
    cdx_data = json.load(f)

if len(cdx_data) <= 1:
    print("❌ Нет данных в CDX файле")
    sys.exit(1)

# Создаем индекс CDX данных по URL для быстрого поиска
print("🔍 Индексируем CDX данные...")
cdx_index = {}
target_timestamp = int("${TARGET_DATE}000000")

# Обрабатываем CDX данные (пропускаем заголовок)
for row in cdx_data[1:]:
    if len(row) >= 5:
        try:
            timestamp = row[0]
            original = row[1]
            statuscode = row[2]
            mimetype = row[3]
            size = int(row[4]) if row[4] and row[4].isdigit() else 0

            # Вычисляем разность во времени
            snap_timestamp = int(timestamp)
            time_diff = abs(snap_timestamp - target_timestamp)

            # Для каждого URL сохраняем только ближайший снапшот
            if original not in cdx_index or time_diff < cdx_index[original]['time_diff']:
                cdx_index[original] = {
                    'timestamp': timestamp,
                    'original': original,
                    'statuscode': statuscode,
                    'mimetype': mimetype,
                    'size': size,
                    'time_diff': time_diff,
                    'days_diff': time_diff // 1000000  # Примерное количество дней
                }
        except (ValueError, IndexError):
            continue  # Пропускаем некорректные записи

print(f"📊 Уникальных URL в CDX: {len(cdx_index)}")

# Сопоставляем с мастер-списком
print("🎯 Сопоставляем с мастер-списком...")
matched_snapshots = []

for master_item in master_list:
    original_url = master_item['original']

    if original_url in cdx_index:
        cdx_item = cdx_index[original_url]

        snapshot = {
            'archive_url': f"https://web.archive.org/web/{cdx_item['timestamp']}/{original_url}",
            'timestamp': cdx_item['timestamp'],
            'original_url': original_url,
            'statuscode': cdx_item['statuscode'],
            'size': cdx_item['size'],
            'days_diff': cdx_item['days_diff']
        }

        matched_snapshots.append(snapshot)

print(f"✅ Найдено совпадений: {len(matched_snapshots)}")

# Сортируем по близости к целевой дате
matched_snapshots.sort(key=lambda x: x['days_diff'])

# Сохраняем результат
with open("$SNAPSHOT_FILE", "w") as f:
    json.dump(matched_snapshots, f, indent=2, ensure_ascii=False)

print(f"💾 Результат сохранен в: $SNAPSHOT_FILE")
EOF

# Проверяем результат Python скрипта
if [ $? -ne 0 ]; then
    echo "❌ Ошибка при сопоставлении данных"
    exit 1
fi

# Удаляем временные файлы
rm -f "$TEMP_CDX_FILE" "$TEMP_MATCHED_FILE"

# Финальная статистика
if [ -f "$SNAPSHOT_FILE" ]; then
    final_count=$(jq length "$SNAPSHOT_FILE" 2>/dev/null || echo "0")
    master_count=$(jq length "$MASTER_LIST" 2>/dev/null || echo "0")

    echo ""
    echo "🎉 ОПТИМИЗИРОВАННЫЙ ПОИСК ЗАВЕРШЕН!"
    echo "======================================"
    echo "📊 Статистика:"
    echo "  📄 URL в мастер-списке: $master_count"
    echo "  📸 Найдено снапшотов: $final_count"

    if [ "$master_count" -gt 0 ]; then
        success_rate=$(( final_count * 100 / master_count ))
        echo "  ✅ Успешность: $success_rate%"
    fi

    if [ "$final_count" -gt 0 ]; then
        echo ""
        echo "🎯 ТОП-5 ближайших снапшотов:"
        jq -r '.[:5] | .[] | "  \(.days_diff) дней: \(.original_url)"' "$SNAPSHOT_FILE"

        echo ""
        echo "📈 Распределение по отклонению от целевой даты:"
        jq -r 'group_by(.days_diff) | map({days: .[0].days_diff, count: length}) | sort_by(.days) | .[:8] | .[] | "  \(.days) дней: \(.count) снапшотов"' "$SNAPSHOT_FILE"
    fi

    echo ""
    echo "📁 Результат: $SNAPSHOT_FILE"
    echo ""
    echo "🚀 Следующий шаг:"
    echo "poetry run wayback-analyzer download-snapshot $DOMAIN --date $TARGET_DATE"
else
    echo "❌ Файл результатов не был создан"
    exit 1
fi