"""Асинхронный загрузчик снапшотов из подготовленного списка URL."""

import asyncio
import aiohttp
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from bs4 import BeautifulSoup

from .storage_manager import StorageManager
from ..utils.rate_limiter import RateLimiter


class SnapshotDownloader:
    """Загрузчик для массового скачивания снапшотов на конкретную дату."""

    def __init__(
            self,
            storage_manager: StorageManager,
            rate_limiter: RateLimiter,
            max_concurrent: int = 3,
            resume_mode: bool = True
    ):
        self.storage_manager = storage_manager
        self.rate_limiter = rate_limiter
        self.max_concurrent = max_concurrent
        self.resume_mode = resume_mode

        self.logger = logging.getLogger(__name__)

        # Статистика
        self.stats = {
            'start_time': None,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_size_mb': 0
        }

        # Список неудачных загрузок для повторных попыток
        self.failed_downloads = []

    async def download_snapshot_batch(
            self,
            domain: str,
            date: str,
            snapshots: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Скачать весь batch снапшотов для домена на конкретную дату.

        Args:
            domain: Доменное имя
            date: Дата в формате YYYYMMDD
            snapshots: Список снапшотов из find_snapshots_for_date.sh

        Returns:
            Статистика скачивания
        """

        self.logger.info(f"🚀 Начинаем скачивание {len(snapshots)} снапшотов для {domain} на {date}")
        self.stats['start_time'] = time.time()

        # Создаем директорию для снапшота
        snapshot_dir = self.storage_manager.base_path / "snapshots" / domain / date
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        # Проверяем resume режим
        if self.resume_mode:
            snapshots = self._filter_existing_pages(snapshots, snapshot_dir)
            self.logger.info(f"📂 Resume режим: осталось скачать {len(snapshots)} страниц")

        if not snapshots:
            self.logger.info("✅ Все страницы уже скачаны")
            return self._build_result_summary(domain, date, 0)

        # Асинхронное скачивание
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            limit_per_host=self.max_concurrent,
            keepalive_timeout=60
        )

        timeout = aiohttp.ClientTimeout(total=60, connect=30)

        async with aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; WaybackAnalyzer/1.0)'}
        ) as session:

            # Создаем семафор для ограничения конкурентности
            semaphore = asyncio.Semaphore(self.max_concurrent)

            # Создаем задачи для всех снапшотов
            tasks = []
            for i, snapshot in enumerate(snapshots):
                task = self._download_single_snapshot(
                    session, semaphore, snapshot, domain, date, i + 1, len(snapshots)
                )
                tasks.append(task)

            # Выполняем все задачи с отображением прогресса
            self.logger.info(f"⬇️  Начинаем параллельное скачивание ({self.max_concurrent} потоков)")

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Обрабатываем результаты
            for result in results:
                if isinstance(result, Exception):
                    self.stats['failed'] += 1
                    self.logger.error(f"❌ Исключение при скачивании: {result}")
                elif result:
                    self.stats['successful'] += 1
                else:
                    self.stats['failed'] += 1

        # Сохраняем манифест снапшота
        await self._save_snapshot_manifest(domain, date, snapshots)

        # Повторные попытки для неудачных загрузок
        if self.failed_downloads:
            self.logger.info(f"🔄 Повторные попытки для {len(self.failed_downloads)} неудачных загрузок")
            await self._retry_failed_downloads(domain, date)

        return self._build_result_summary(domain, date, len(snapshots))

    async def _download_single_snapshot(
            self,
            session: aiohttp.ClientSession,
            semaphore: asyncio.Semaphore,
            snapshot: Dict[str, Any],
            domain: str,
            date: str,
            current: int,
            total: int
    ) -> bool:
        """Скачать один снапшот."""

        async with semaphore:
            # Rate limiting
            await asyncio.sleep(1.0 / (1.0 / self.rate_limiter.delay))

            archive_url = snapshot['archive_url']
            original_url = snapshot['original_url']

            try:
                # Показываем прогресс
                if current % 10 == 0 or current == 1:
                    self.logger.info(f"📄 [{current}/{total}] Скачиваю: {original_url}")

                async with session.get(archive_url) as response:
                    if response.status == 200:
                        content = await response.text()

                        # Извлекаем базовые метаданные
                        metadata = self._extract_page_metadata(content, snapshot)

                        # Сохраняем страницу
                        await self._save_page_to_snapshot_dir(
                            domain, date, original_url, content, metadata
                        )

                        # Обновляем статистику
                        content_size_mb = len(content.encode('utf-8')) / (1024 * 1024)
                        self.stats['total_size_mb'] += content_size_mb

                        return True

                    else:
                        self.logger.warning(f"⚠️  HTTP {response.status} для {archive_url}")
                        self.failed_downloads.append(snapshot)
                        return False

            except asyncio.TimeoutError:
                self.logger.warning(f"⏱️  Таймаут для {archive_url}")
                self.failed_downloads.append(snapshot)
                return False

            except Exception as e:
                self.logger.error(f"❌ Ошибка при скачивании {archive_url}: {e}")
                self.failed_downloads.append(snapshot)
                return False

    def _extract_page_metadata(self, content: str, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        """Извлечь метаданные страницы."""

        try:
            soup = BeautifulSoup(content, 'html.parser')
            title = soup.title.string.strip() if soup.title and soup.title.string else "Без заголовка"
        except:
            title = "Ошибка извлечения заголовка"

        return {
            'archive_url': snapshot['archive_url'],
            'original_url': snapshot['original_url'],
            'timestamp': snapshot['timestamp'],
            'title': title,
            'content_length': len(content),
            'content_length_mb': round(len(content.encode('utf-8')) / (1024 * 1024), 3),
            'statuscode': snapshot.get('statuscode', '200'),
            'size': snapshot.get('size', 0),
            'days_diff': snapshot.get('days_diff', 0),
            'downloaded_at': datetime.now().isoformat(),
            'extracted_links': len(soup.find_all('a', href=True)) if 'soup' in locals() else 0,
            'extracted_images': len(soup.find_all('img')) if 'soup' in locals() else 0
        }

    async def _save_page_to_snapshot_dir(
            self,
            domain: str,
            date: str,
            original_url: str,
            content: str,
            metadata: Dict[str, Any]
    ):
        """Сохранить страницу в директорию снапшота."""

        # Создаем безопасное имя файла
        safe_filename = self._url_to_safe_filename(original_url)

        snapshot_dir = self.storage_manager.base_path / "snapshots" / domain / date

        # Сохраняем HTML
        html_path = snapshot_dir / f"{safe_filename}.html"
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(content)

        # Сохраняем метаданные
        meta_path = snapshot_dir / f"{safe_filename}.html.meta.json"
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

    def _url_to_safe_filename(self, url: str) -> str:
        """Преобразовать URL в безопасное имя файла."""
        import hashlib

        # Убираем протокол
        if '://' in url:
            url = url.split('://', 1)[1]

        # Заменяем специальные символы
        safe_name = url.replace('/', '_').replace('?', '_').replace('&', '_').replace('=', '_')
        safe_name = safe_name.replace(':', '_').replace('#', '_').replace('%', '_')

        # Ограничиваем длину
        if len(safe_name) > 150:
            # Используем hash для очень длинных URL
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            safe_name = safe_name[:140] + f"_{url_hash}"

        return safe_name

    def _filter_existing_pages(self, snapshots: List[Dict], snapshot_dir: Path) -> List[Dict]:
        """Фильтровать уже скачанные страницы для resume режима."""

        filtered = []
        for snapshot in snapshots:
            safe_filename = self._url_to_safe_filename(snapshot['original_url'])
            html_path = snapshot_dir / f"{safe_filename}.html"

            if not html_path.exists():
                filtered.append(snapshot)
            else:
                self.stats['skipped'] += 1

        return filtered

    async def _save_snapshot_manifest(
            self,
            domain: str,
            date: str,
            original_snapshots: List[Dict[str, Any]]
    ):
        """Сохранить манифест снапшота."""

        snapshot_dir = self.storage_manager.base_path / "snapshots" / domain / date
        manifest_path = snapshot_dir / "snapshot_manifest.json"

        manifest = {
            'domain': domain,
            'target_date': date,
            'created_at': datetime.now().isoformat(),
            'total_snapshots': len(original_snapshots),
            'successful_downloads': self.stats['successful'],
            'failed_downloads': self.stats['failed'],
            'skipped_existing': self.stats['skipped'],
            'total_size_mb': round(self.stats['total_size_mb'], 2),
            'duration_seconds': round(time.time() - self.stats['start_time'], 2),
            'snapshots_metadata': original_snapshots[:10]  # Первые 10 для примера
        }

        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

    async def _retry_failed_downloads(self, domain: str, date: str):
        """Повторные попытки для неудачных загрузок."""

        if not self.failed_downloads:
            return

        retry_snapshots = self.failed_downloads.copy()
        self.failed_downloads.clear()

        # Увеличиваем задержки для повторных попыток
        original_delay = self.rate_limiter.delay
        self.rate_limiter.delay *= 2  # Удваиваем задержку

        try:
            result = await self.download_snapshot_batch(domain, date, retry_snapshots)
            self.logger.info(f"🔄 Повторные попытки: {result['successful']} успешно")
        finally:
            self.rate_limiter.delay = original_delay

    def _build_result_summary(self, domain: str, date: str, total_attempted: int) -> Dict[str, Any]:
        """Построить итоговую сводку результатов."""

        duration = time.time() - self.stats['start_time']

        return {
            'domain': domain,
            'date': date,
            'total_attempted': total_attempted,
            'successful': self.stats['successful'],
            'failed': self.stats['failed'],
            'skipped': self.stats['skipped'],
            'duration_seconds': round(duration, 2),
            'duration_minutes': round(duration / 60, 2),
            'total_size_mb': round(self.stats['total_size_mb'], 2),
            'pages_per_minute': round(self.stats['successful'] / (duration / 60), 1) if duration > 0 else 0,
            'average_page_size_kb': round((self.stats['total_size_mb'] * 1024) / max(self.stats['successful'], 1), 1)
        }