#!/usr/bin/env python3
import json
import sys
from urllib.parse import urlparse
import re

def normalize_url(url):
    """Нормализует URL для сравнения"""
    # http -> https
    normalized = url.replace('http://', 'https://')
    # Убираем :80
    normalized = normalized.replace(':80/', '/')
    # Убираем trailing slash (кроме главной)
    if normalized.endswith('/') and normalized.count('/') > 3:
        normalized = normalized[:-1]
    return normalized

def content_normalize_url(url):
    """Нормализует URL для контентной группировки"""
    # Убираем номерные суффиксы
    normalized = re.sub(r'-\d+/?$', '', url)
    # Убираем query параметры
    normalized = normalized.split('?')[0]
    # Убираем якорные ссылки
    normalized = normalized.split('#')[0]
    return normalized

def deduplicate_urls(input_file, output_file):
    """Дедуплицирует URL в два этапа"""

    print(f"🔍 Читаем файл: {input_file}")

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")
        return

    if not data:
        print("❌ Файл пустой")
        return

    print(f"📊 Исходных записей: {len(data)}")

    # Этап 1: Протокольная дедупликация
    protocol_dedup = {}
    for item in data:
        url = item['original']
        normalized = normalize_url(url)

        if normalized not in protocol_dedup:
            protocol_dedup[normalized] = item
        else:
            # Выбираем лучший вариант
            current = protocol_dedup[normalized]

            # Приоритеты: HTTPS > HTTP, больший размер, свежее время
            if (url.startswith('https://') and not current['original'].startswith('https://')) or \
                    (url.startswith('https://') == current['original'].startswith('https://') and
                     item['size'] > current['size']):
                protocol_dedup[normalized] = item

    stage1_result = list(protocol_dedup.values())
    print(f"✅ После протокольной дедупликации: {len(stage1_result)}")

    # Этап 2: Контентная дедупликация
    content_dedup = {}
    for item in stage1_result:
        url = item['original']
        content_key = content_normalize_url(url)

        if content_key not in content_dedup:
            content_dedup[content_key] = item
        else:
            current = content_dedup[content_key]

            # Приоритеты: без номера > с номером, больший размер
            has_number = bool(re.search(r'-\d+/?$', url))
            current_has_number = bool(re.search(r'-\d+/?$', current['original']))

            if (not has_number and current_has_number) or \
                    (has_number == current_has_number and item['size'] > current['size']):
                content_dedup[content_key] = item

    final_result = list(content_dedup.values())
    print(f"✅ После контентной дедупликации: {len(final_result)}")

    # Добавляем priority_score
    for item in final_result:
        url = item['original']
        path_depth = len([p for p in url.split('/')[3:] if p])
        size_score = item['size'] / 1000
        depth_score = max(0, 50 - path_depth * 5)
        https_bonus = 5 if url.startswith('https://') else 0
        encoding_penalty = url.count('%')

        item['priority_score'] = size_score + depth_score + https_bonus - encoding_penalty

    # Сортируем по приоритету
    final_result.sort(key=lambda x: x['priority_score'], reverse=True)

    # Сохраняем результат
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(final_result, f, indent=2, ensure_ascii=False)
        print(f"💾 Результат сохранен в: {output_file}")
    except Exception as e:
        print(f"❌ Ошибка записи файла: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Использование: python3 dedupe_urls.py input_file output_file")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    deduplicate_urls(input_file, output_file)