# src/wayback_analyzer/cli/political_scraper.py
import click
from datetime import date, datetime
from pathlib import Path
import asyncio
import logging

from ..models.political_events import UKRAINE_POLITICAL_EVENTS, EventType
from ..core.snapshot_finder import PoliticalSnapshotFinder
from ..core.site_crawler import ArchiveSiteCrawler
from ..core.content_downloader import MassContentDownloader
from ..core.storage_manager import StorageManager
from ..utils.rate_limiter import RateLimiter

@click.group()
def political():
    """Инструменты для анализа украинских политических партий."""
    pass

@political.command()
@click.argument('site_url')
@click.option('--event-dates', '-e', multiple=True, help='Конкретные даты для анализа (YYYY-MM-DD)')
@click.option('--event-types', '-t', multiple=True,
              type=click.Choice(['election', 'war', 'covid', 'legislation', 'international']),
              help='Типы событий для анализа')
@click.option('--days-before', default=30, help='Дней до события для поиска снапшотов')
@click.option('--days-after', default=30, help='Дней после события для поиска снапшотов')
@click.option('--output-dir', default='./political_data', help='Директория для сохранения данных')
@click.option('--max-depth', default=2, help='Максимальная глубина обхода сайта')
@click.option('--rate-limit', default=1.0, help='Задержка между запросами (секунды)')
def scrape_political_site(site_url, event_dates, event_types, days_before, days_after,
                          output_dir, max_depth, rate_limit):
    """Собрать данные политического сайта для ключевых событий."""

    # Настройка логирования
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    click.echo(f"🇺🇦 Анализ политического сайта: {site_url}")

    # Инициализация компонентов
    rate_limiter = RateLimiter(delay=rate_limit)
    storage_manager = StorageManager(base_path=Path(output_dir))
    snapshot_finder = PoliticalSnapshotFinder()
    site_crawler = ArchiveSiteCrawler(rate_limiter, max_depth=max_depth)
    content_downloader = MassContentDownloader(storage_manager)

    # Определяем события для анализа
    events_to_analyze = []

    if event_dates:
        # Пользовательские даты
        for date_str in event_dates:
            try:
                event_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                events_to_analyze.append({
                    'name': f'Custom_Event_{date_str}',
                    'date': event_date,
                    'type': 'custom'
                })
            except ValueError:
                click.echo(f"❌ Неверный формат даты: {date_str}")
                return
    else:
        # Предопределенные события
        filter_types = set(event_types) if event_types else None

        for event in UKRAINE_POLITICAL_EVENTS:
            if filter_types is None or event.event_type.value in filter_types:
                events_to_analyze.append(event)

    if not events_to_analyze:
        click.echo("❌ Не выбрано ни одного события для анализа")
        return

    click.echo(f"📅 Будет проанализировано {len(events_to_analyze)} событий")

    # Анализируем каждое событие
    for event in events_to_analyze:
        click.echo(f"\n🔍 Анализ события: {event.name if hasattr(event, 'name') else event['name']}")

        # Находим снапшоты
        snapshots = snapshot_finder.find_event_snapshots(
            site_url, event, days_before, days_after
        ) if hasattr(event, 'name') else snapshot_finder.find_event_snapshots(
            site_url, event, days_before, days_after
        )

        total_snapshots = len(snapshots['before_event']) + len(snapshots['after_event'])
        click.echo(f"  📸 Найдено снапшотов: {total_snapshots}")

        if total_snapshots == 0:
            click.echo("  ⚠️  Снапшотов не найдено, пропускаем")
            continue

        # Обходим структуру каждого снапшота
        all_pages = []

        for period, snapshot_list in [('before', snapshots['before_event']),
                                      ('after', snapshots['after_event'])]:

            for snapshot in snapshot_list[:3]:  # Ограничиваем для примера
                click.echo(f"    🕷️  Обход снапшота {snapshot['timestamp']} ({period})")

                domain = site_url.replace('http://', '').replace('https://', '').split('/')[0]
                pages = site_crawler.discover_site_structure(snapshot['url'], domain)

                # Добавляем метаинформацию к каждой странице
                for page in pages:
                    page['event_period'] = period
                    page['event_name'] = event.name if hasattr(event, 'name') else event['name']
                    page['snapshot_date'] = snapshot['date']

                all_pages.extend(pages)

        click.echo(f"  📄 Найдено страниц для загрузки: {len(all_pages)}")

        # Загружаем весь контент
        if all_pages:
            click.echo(f"  ⬇️  Загрузка контента...")

            site_name = site_url.replace('http://', '').replace('https://', '').replace('/', '_')
            event_name = event.name if hasattr(event, 'name') else event['name']

            loop = asyncio.get_event_loop()
            result = loop.run_until_complete(
                content_downloader.download_all_pages(all_pages, site_name, event_name)
            )

            click.echo(f"  ✅ Загружено: {result['successful']} страниц, ошибок: {result['failed']}")

    click.echo(f"\n🎉 Анализ завершен! Данные сохранены в: {output_dir}")

@political.command()
def list_events():
    """Показать список доступных политических событий."""
    click.echo("📅 Доступные события для анализа:\n")

    for event in UKRAINE_POLITICAL_EVENTS:
        click.echo(f"  {event.date} - {event.name}")
        click.echo(f"    Тип: {event.event_type.value}")
        click.echo(f"    Описание: {event.description}")
        click.echo()

if __name__ == "__main__":
    political()