#!/bin/bash

# 🎯 Полный workflow создания исторического снапшота сайта
# Использование: ./create_full_snapshot.sh DOMAIN DATE [MIN_SIZE]

DOMAIN="$1"
DATE="$2"
MIN_SIZE="${3:-5000}"

# Проверка аргументов
if [ -z "$DOMAIN" ] || [ -z "$DATE" ]; then
    echo "🎯 Полный workflow создания исторического снапшота сайта"
    echo "=============================================================="
    echo ""
    echo "Использование: $0 DOMAIN DATE [MIN_SIZE]"
    echo ""
    echo "Параметры:"
    echo "  DOMAIN   - доменное имя (например: sluga-narodu.com)"
    echo "  DATE     - целевая дата в формате YYYYMMDD (например: 20191115)"
    echo "  MIN_SIZE - минимальный размер страницы в байтах (по умолчанию: 5000)"
    echo ""
    echo "Примеры:"
    echo "  $0 sluga-narodu.com 20191115"
    echo "  $0 ba.org.ua 20200315 10000"
    echo ""
    echo "Результат: Полный снапшот в папке snapshots/DOMAIN/DATE/"
    exit 1
fi

# Проверка формата даты
if ! [[ "$DATE" =~ ^[0-9]{8}$ ]]; then
    echo "❌ Неверный формат даты. Используйте YYYYMMDD"
    echo "Пример: 20191115 (15 ноября 2019)"
    exit 1
fi

# Проверка зависимостей
if ! command -v curl >/dev/null 2>&1; then
    echo "❌ curl не найден. Установите curl для продолжения."
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "❌ jq не найден. Установите jq для продолжения."
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo "❌ python3 не найден. Установите Python 3 для продолжения."
    exit 1
fi

echo "🎯 СОЗДАНИЕ ПОЛНОГО СНАПШОТА САЙТА"
echo "=============================================================="
echo "🌐 Домен: $DOMAIN"
echo "📅 Целевая дата: $DATE"
echo "📏 Минимальный размер: $MIN_SIZE байт"
echo ""

START_TIME=$(date +%s)

# ====================================================================
# ЭТАП 1: Создание мастер-списка URL
# ====================================================================

echo "🔍 ЭТАП 1/3: Создание мастер-списка URL"
echo "----------------------------------------------------------------------"

if [ -f "${DOMAIN}_master_list.json" ]; then
    echo "✅ Мастер-список уже существует: ${DOMAIN}_master_list.json"
    MASTER_COUNT=$(jq length "${DOMAIN}_master_list.json" 2>/dev/null || echo "0")
    echo "📊 URL в мастер-списке: $MASTER_COUNT"

    read -p "🤔 Пересоздать мастер-список? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🔄 Пересоздаем мастер-список..."
        ./create_master_list.sh "$DOMAIN" "$MIN_SIZE"
    fi
else
    echo "🚀 Создаем мастер-список URL..."
    if ! ./create_master_list.sh "$DOMAIN" "$MIN_SIZE"; then
        echo "❌ Ошибка при создании мастер-списка"
        exit 1
    fi
fi

# Проверяем результат
if [ ! -f "${DOMAIN}_master_list.json" ]; then
    echo "❌ Мастер-список не был создан"
    exit 1
fi

MASTER_COUNT=$(jq length "${DOMAIN}_master_list.json")
echo "✅ Этап 1 завершен. URL в мастер-списке: $MASTER_COUNT"
echo ""

# ====================================================================
# ЭТАП 2: Поиск снапшотов для целевой даты
# ====================================================================

echo "📅 ЭТАП 2/3: Поиск снапшотов на дату $DATE"
echo "----------------------------------------------------------------------"

SNAPSHOTS_FILE="${DOMAIN}_snapshots_${DATE}.json"

if [ -f "$SNAPSHOTS_FILE" ]; then
    echo "✅ Файл снапшотов уже существует: $SNAPSHOTS_FILE"
    SNAPSHOTS_COUNT=$(jq length "$SNAPSHOTS_FILE" 2>/dev/null || echo "0")
    echo "📊 Найденных снапшотов: $SNAPSHOTS_COUNT"

    read -p "🤔 Пересоздать список снапшотов? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🔄 Ищем снапшоты заново..."
        ./find_snapshots_for_date.sh "$DOMAIN" "$DATE"
    fi
else
    # Выбираем оптимальную стратегию поиска
    if [ "$MASTER_COUNT" -gt 1000 ]; then
        echo "🚀 Большой сайт ($MASTER_COUNT URL) - используем ОПТИМИЗИРОВАННЫЙ поиск..."
        if ! ./find_snapshots_for_date_optimized.sh "$DOMAIN" "$DATE"; then
            echo "❌ Ошибка при оптимизированном поиске снапшотов"
            exit 1
        fi
    else
        echo "🚀 Обычный размер сайта - используем стандартный поиск..."
        if ! ./find_snapshots_for_date.sh "$DOMAIN" "$DATE"; then
            echo "❌ Ошибка при поиске снапшотов"
            echo "💡 Попробуйте: ./find_snapshots_for_date_optimized.sh $DOMAIN $DATE"
            exit 1
        fi
    fi
fi

# Проверяем результат
if [ ! -f "$SNAPSHOTS_FILE" ]; then
    echo "❌ Файл снапшотов не был создан"
    exit 1
fi

SNAPSHOTS_COUNT=$(jq length "$SNAPSHOTS_FILE")
if [ "$SNAPSHOTS_COUNT" -eq 0 ]; then
    echo "❌ Снапшотов не найдено для даты $DATE"
    echo "💡 Попробуйте другую дату в пределах ±90 дней"
    exit 1
fi

echo "✅ Этап 2 завершен. Найдено снапшотов: $SNAPSHOTS_COUNT"
echo ""

# ====================================================================
# ЭТАП 3: Массовое скачивание
# ====================================================================

echo "⬇️  ЭТАП 3/3: Массовое скачивание контента"
echo "----------------------------------------------------------------------"

SNAPSHOT_DIR="snapshots/${DOMAIN}/${DATE}"

if [ -d "$SNAPSHOT_DIR" ] && [ "$(ls -A "$SNAPSHOT_DIR" 2>/dev/null | wc -l)" -gt 0 ]; then
    echo "✅ Директория снапшота уже существует: $SNAPSHOT_DIR"
    EXISTING_FILES=$(find "$SNAPSHOT_DIR" -name "*.html" | wc -l)
    echo "📊 Уже скачано файлов: $EXISTING_FILES"

    read -p "🤔 Продолжить скачивание (resume mode)? (Y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "⏭️  Пропускаем этап скачивания"
    else
        echo "🔄 Продолжаем скачивание в resume режиме..."
        poetry run wayback-analyzer download-snapshot "$DOMAIN" --date "$DATE" --resume
    fi
else
    echo "🚀 Начинаем массовое скачивание..."
    poetry run wayback-analyzer download-snapshot "$DOMAIN" --date "$DATE"
fi

# ====================================================================
# ФИНАЛЬНАЯ СТАТИСТИКА
# ====================================================================

END_TIME=$(date +%s)
TOTAL_DURATION=$((END_TIME - START_TIME))
TOTAL_MINUTES=$((TOTAL_DURATION / 60))

echo ""
echo "🎉 ПОЛНЫЙ СНАПШОТ СОЗДАН!"
echo "=============================================================="
echo "📊 Финальная статистика:"
echo "  🌐 Домен: $DOMAIN"
echo "  📅 Дата: $DATE"
echo "  📄 URL в мастер-списке: $MASTER_COUNT"
echo "  📸 Найдено снапшотов: $SNAPSHOTS_COUNT"

if [ -d "$SNAPSHOT_DIR" ]; then
    DOWNLOADED_FILES=$(find "$SNAPSHOT_DIR" -name "*.html" 2>/dev/null | wc -l)
    TOTAL_SIZE=$(du -sh "$SNAPSHOT_DIR" 2>/dev/null | cut -f1)
    echo "  ⬇️  Скачано файлов: $DOWNLOADED_FILES"
    echo "  💾 Общий размер: $TOTAL_SIZE"
    echo "  📁 Сохранено в: $SNAPSHOT_DIR"

    # Показываем примеры файлов
    echo ""
    echo "📋 Примеры скачанных файлов:"
    find "$SNAPSHOT_DIR" -name "*.html" | head -5 | while read file; do
        echo "  📄 $(basename "$file")"
    done

    if [ -f "$SNAPSHOT_DIR/snapshot_manifest.json" ]; then
        echo ""
        echo "📊 Детальная статистика в: $SNAPSHOT_DIR/snapshot_manifest.json"
    fi
fi

echo "  ⏱️  Общее время: $TOTAL_MINUTES минут"
echo ""

# Рекомендации по дальнейшему использованию
echo "🚀 Что делать дальше:"
echo "  1. Анализировать контент: poetry run wayback-analyzer analyze-content $DOMAIN --date $DATE"
echo "  2. Сравнить с другими датами: ./create_full_snapshot.sh $DOMAIN ДРУГАЯ_ДАТА"
echo "  3. Изучить файлы в: $SNAPSHOT_DIR"
echo ""
echo "✅ Готово! Полный исторический снапшот сайта $DOMAIN на $DATE создан."