#!/bin/bash
DOMAIN="$1"
MIN_SIZE="${2:-5000}"

if [ -z "$DOMAIN" ]; then
    echo "Использование: $0 DOMAIN [MIN_SIZE]"
    echo "Пример: $0 ba.org.ua 5000"
    exit 1
fi

echo "🚀 Создание мастер-списка URL для $DOMAIN (Python дедупликация)"
echo "=============================================================="

# Шаг 1: Получение всех HTML URL
echo "🔍 Шаг 1: Получение всех HTML URL..."
curl -s "http://web.archive.org/cdx/search/cdx?url=${DOMAIN}/*&output=json&fl=original,timestamp,statuscode,mimetype,length&filter=mimetype:text/html&filter=statuscode:200&limit=50000" > ${DOMAIN}_raw.json

raw_count=$(($(jq length ${DOMAIN}_raw.json) - 1))
echo "✅ Найдено HTML записей: $raw_count"

# Шаг 2: Только базовая фильтрация через jq
echo ""
echo "🧹 Шаг 2: Базовая фильтрация..."

jq -r --argjson min_size "$MIN_SIZE" '
if length > 1 then
  .[1:] | map({
    original: .[0],
    timestamp: .[1],
    statuscode: .[2],
    mimetype: .[3],
    size: (.[4] | tonumber? // 0)
  }) | map(select(
    (.size > $min_size) and
    (.original | test("/wp-admin|/admin|/login|/auth|/dashboard|/api/|/rest/|/json/|/xml/|/ajax|/css/|/js/|/images/|/img/|/assets/|/static/|/media/|/404|/error|/maintenance|/503|robots\\.txt|sitemap\\.xml|\\.well-known/|/search|/tag/|/category/|/archive|\\?page=|\\?p=|/page/|\\?offset=|/feed|/rss|/atom|/trackback|/xmlrpc|/embed/?$|\\?amp=1|\\?utm_|/amp/?$|/print/?$|\\?preview=|\\.(css|js|png|jpg|jpeg|gif|ico|pdf|zip|doc|docx|txt|xml)$|/wp-content|/wp-includes|/wp-json|/node_modules|/vendor|/.git|/debug|/test|/tmp|/temp") | not)
  ))
else
  []
end
' ${DOMAIN}_raw.json > ${DOMAIN}_filtered.json

filtered_count=$(jq length ${DOMAIN}_filtered.json)
echo "✅ После фильтрации: $filtered_count"

# Шаг 3: Python дедупликация
echo ""
echo "🐍 Шаг 3: Python дедупликация..."

python3 dedupe_urls.py ${DOMAIN}_filtered.json ${DOMAIN}_master_list.json

final_count=$(jq length ${DOMAIN}_master_list.json 2>/dev/null || echo "0")

echo ""
echo "✅ МАСТЕР-СПИСОК СОЗДАН!"
echo "========================"
echo "📊 Финальная статистика:"
echo "  Всего найдено записей: $raw_count"
echo "  После базовой фильтрации: $filtered_count"
echo "  Финальный мастер-список: $final_count"

if [ "$final_count" -gt 0 ]; then
    echo ""
    echo "🎯 ТОП-10 URL по приоритету:"
    jq -r '.[] | "\(.priority_score | floor)\t\(.original)"' ${DOMAIN}_master_list.json | head -10 | while IFS=$'\t' read score url; do
        echo "  $score: $url"
    done
fi

echo ""
echo "📁 Файлы:"
echo "  ${DOMAIN}_master_list.json - финальный мастер-список"