"""Command-line interface for Wayback Analyzer."""

import click
import logging
import time
from datetime import datetime
from ..core.client import WaybackClient


@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def main(verbose: bool):
    """Wayback Machine Analyzer CLI."""
    if verbose:
        logging.basicConfig(level=logging.DEBUG)


@main.command()
@click.argument('domain')
@click.option('--limit', default=10, help='Maximum snapshots to show')
def snapshots(domain: str, limit: int):
    """Get snapshots for a domain."""
    click.echo(f"Getting snapshots for: {domain}")

    client = WaybackClient()
    snapshots = client.get_snapshots(f"http://{domain}", limit=limit)

    if snapshots:
        click.echo(f"Found {len(snapshots)} snapshots:")
        for snapshot in snapshots:
            click.echo(f"  {snapshot}")
    else:
        click.echo("No snapshots found.")


@main.command()
def test():
    """Test connection to Wayback Machine."""
    client = WaybackClient()
    if client.test_connection():
        click.echo("✅ Connection to Wayback Machine successful!")
    else:
        click.echo("❌ Failed to connect to Wayback Machine")


# ВСЕ ОСТАЛЬНЫЕ КОМАНДЫ ДОБАВЛЯЙТЕ ЗДЕСЬ ПОСЛЕ main
@main.command()
@click.argument('site_url')
@click.option('--target-date', help='Целевая дата в формате YYYY-MM-DD')
@click.option('--output-dir', default='./political_archives', help='Директория для сохранения')
@click.option('--max-pages', default=500, help='Максимальное количество страниц')
@click.option('--max-depth', default=4, help='Максимальная глубина обхода')
@click.option('--rate-limit', default=2.0, help='Задержка между запросами')
def crawl_political_site(site_url, target_date, output_dir, max_pages, max_depth, rate_limit):
    """Полный обход политического сайта."""
    from pathlib import Path
    from ..core.storage_manager import StorageManager
    from ..core.enhanced_crawler import EnhancedSnapshotCrawler
    from ..utils.rate_limiter import RateLimiter

    click.echo(f"🇺🇦 Обход сайта: {site_url}")

    storage_manager = StorageManager(Path(output_dir))
    rate_limiter = RateLimiter(requests_per_second=1.0/rate_limit)

    crawler = EnhancedSnapshotCrawler(
        storage_manager=storage_manager,
        rate_limiter=rate_limiter,
        max_depth=max_depth,
        max_pages=max_pages
    )

    try:
        summary = crawler.crawl_political_site(site_url, target_date)
        click.echo(f"✅ Завершено: {summary['total_pages_found']} страниц")
    except Exception as e:
        click.echo(f"❌ Ошибка: {e}")

@main.command()
@click.argument('domain')
@click.option('--date', required=True, help='Дата в формате YYYYMMDD')
@click.option('--output-dir', default='./snapshots', help='Директория для сохранения снапшотов')
@click.option('--rate-limit', default=2.0, help='Задержка между запросами в секундах')
@click.option('--max-concurrent', default=3, help='Максимальное количество одновременных загрузок')
@click.option('--resume', is_flag=True, help='Продолжить прерванную загрузку')
def download_snapshot(domain, date, output_dir, rate_limit, max_concurrent, resume):
    """Скачать снапшот сайта на конкретную дату из подготовленного списка URL."""
    import json
    import asyncio
    from pathlib import Path
    from ..core.snapshot_downloader import SnapshotDownloader
    from ..core.storage_manager import StorageManager
    from ..utils.rate_limiter import RateLimiter

    # Проверяем формат даты
    if not date.isdigit() or len(date) != 8:
        click.echo("❌ Неверный формат даты. Используйте YYYYMMDD")
        return

    # Формируем имя файла со снапшотами
    snapshots_file = f"{domain}_snapshots_{date}.json"

    if not Path(snapshots_file).exists():
        click.echo(f"❌ Файл снапшотов не найден: {snapshots_file}")
        click.echo(f"Сначала выполните: ./find_snapshots_for_date.sh {domain} {date}")
        return

    click.echo(f"📦 Скачивание снапшота {domain} на дату {date}")
    click.echo("=" * 60)

    # Читаем список снапшотов
    try:
        with open(snapshots_file, 'r', encoding='utf-8') as f:
            snapshots = json.load(f)
    except Exception as e:
        click.echo(f"❌ Ошибка чтения файла снапшотов: {e}")
        return

    if not snapshots:
        click.echo("❌ Список снапшотов пустой")
        return

    click.echo(f"📊 Найдено снапшотов для скачивания: {len(snapshots)}")

    # Настройка компонентов
    storage_manager = StorageManager(Path(output_dir))
    rate_limiter = RateLimiter(requests_per_second=1.0/rate_limit)

    downloader = SnapshotDownloader(
        storage_manager=storage_manager,
        rate_limiter=rate_limiter,
        max_concurrent=max_concurrent,
        resume_mode=resume
    )

    # Запуск асинхронного скачивания
    try:
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(
            downloader.download_snapshot_batch(domain, date, snapshots)
        )

        click.echo(f"\n✅ Скачивание завершено!")
        click.echo(f"📄 Успешно: {result['successful']}")
        click.echo(f"❌ Ошибок: {result['failed']}")
        click.echo(f"⏱️  Время: {result['duration_minutes']:.1f} мин")
        click.echo(f"📁 Сохранено в: {output_dir}/snapshots/{domain}/{date}/")

    except KeyboardInterrupt:
        click.echo("\n⏹️  Скачивание прервано пользователем")
    except Exception as e:
        click.echo(f"\n❌ Ошибка при скачивании: {e}")

if __name__ == "__main__":
    main()