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

echo "🕐 Поиск снапшотов для $DOMAIN на дату $TARGET_DATE"
echo "=============================================================="

# Функция для вычисления даты ± дни
calculate_date() {
    local base_date="$1"
    local days_offset="$2"

    # Преобразуем YYYYMMDD в формат YYYY-MM-DD для date
    local formatted_date="${base_date:0:4}-${base_date:4:2}-${base_date:6:2}"

    # Вычисляем новую дату
    if command -v gdate >/dev/null 2>&1; then
        # macOS (GNU date via homebrew)
        gdate -d "$formatted_date $days_offset days" +%Y%m%d
    else
        # Linux
        date -d "$formatted_date $days_offset days" +%Y%m%d
    fi
}

# Вычисляем временное окно ±90 дней
FROM_DATE=$(calculate_date "$TARGET_DATE" "-90")
TO_DATE=$(calculate_date "$TARGET_DATE" "90")

echo "📅 Временное окно: $FROM_DATE - $TO_DATE"

# Функция для поиска ближайшего снапшота
find_closest_snapshot() {
    local url="$1"
    local target_timestamp="$2"

    # Делаем CDX запрос для URL в временном окне
    local cdx_response
    cdx_response=$(curl -s "http://web.archive.org/cdx/search/cdx?url=$(printf '%s' "$url" | sed 's/ /%20/g')&from=$FROM_DATE&to=$TO_DATE&output=json&fl=timestamp,original,statuscode,mimetype,length&filter=statuscode:200&filter=mimetype:text/html" 2>/dev/null)

    if [ -z "$cdx_response" ] || [ "$cdx_response" = "[]" ]; then
        return 1
    fi

    # Обрабатываем ответ и находим ближайший снапшот
    echo "$cdx_response" | jq -r --arg target "$target_timestamp" '
    if length > 1 then
      .[1:] | map({
        timestamp: .[0],
        original: .[1],
        statuscode: .[2],
        mimetype: .[3],
        size: (.[4] | tonumber? // 0),
        diff: (.[0] | tonumber) - ($target | tonumber) | if . < 0 then -. else . end
      }) | sort_by(.diff) | .[0] | {
        archive_url: ("https://web.archive.org/web/" + .timestamp + "/" + .original),
        timestamp: .timestamp,
        original_url: .original,
        statuscode: .statuscode,
        size: .size,
        days_diff: (.diff / 1000000 | floor)
      }
    else
      empty
    end'
}

# Читаем мастер-список и обрабатываем каждый URL
SNAPSHOT_FILE="${DOMAIN}_snapshots_${TARGET_DATE}.json"
TEMP_FILE="${SNAPSHOT_FILE}.tmp"

echo "🔍 Поиск снапшотов для URL из мастер-списка..."

# Создаем временный файл для накопления результатов
echo "[]" > "$TEMP_FILE"

total_urls=$(jq length "$MASTER_LIST")
current_url=0
found_snapshots=0

echo "📊 Обрабатываю $total_urls URL..."

# Обрабатываем каждый URL из мастер-списка
jq -r '.[].original' "$MASTER_LIST" | while read -r url; do
    current_url=$((current_url + 1))

    # Показываем прогресс каждые 50 URL
    if [ $((current_url % 50)) -eq 0 ] || [ "$current_url" -eq 1 ]; then
        echo "  📄 Обработано: $current_url/$total_urls"
    fi

    # Ищем ближайший снапшот
    snapshot_info=$(find_closest_snapshot "$url" "${TARGET_DATE}000000")

    if [ -n "$snapshot_info" ] && [ "$snapshot_info" != "null" ]; then
        # Добавляем найденный снапшот к результатам
        jq --argjson new_snapshot "$snapshot_info" '. += [$new_snapshot]' "$TEMP_FILE" > "${TEMP_FILE}.new" && mv "${TEMP_FILE}.new" "$TEMP_FILE"
        found_snapshots=$((found_snapshots + 1))
    fi

    # Небольшая задержка для соблюдения rate limit
    sleep 0.5
done

# Перемещаем временный файл в финальный
mv "$TEMP_FILE" "$SNAPSHOT_FILE"

# Подсчитываем финальную статистику
final_count=$(jq length "$SNAPSHOT_FILE" 2>/dev/null || echo "0")

echo ""
echo "✅ ПОИСК СНАПШОТОВ ЗАВЕРШЕН!"
echo "=============================="
echo "📊 Статистика:"
echo "  Обработано URL: $total_urls"
echo "  Найдено снапшотов: $final_count"
echo "  Успешность: $(( final_count * 100 / total_urls ))%"

if [ "$final_count" -gt 0 ]; then
    echo ""
    echo "🎯 Примеры найденных снапшотов:"
    jq -r '.[:5] | .[] | "  \(.days_diff) дней: \(.original_url)"' "$SNAPSHOT_FILE"

    echo ""
    echo "📈 Распределение по отклонению от целевой даты:"
    jq -r 'group_by(.days_diff) | map({days: .[0].days_diff, count: length}) | sort_by(.days) | .[] | "  \(.days) дней: \(.count) снапшотов"' "$SNAPSHOT_FILE" | head -10
fi

echo ""
echo "📁 Результат сохранен в: $SNAPSHOT_FILE"
echo ""
echo "🚀 Следующий шаг: скачивание контента"
echo "poetry run wayback-analyzer download-snapshot $DOMAIN --date $TARGET_DATE"