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


if __name__ == "__main__":
    main()