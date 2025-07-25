# src/wayback_analyzer/core/content_downloader.py
import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from typing import List, Dict
import logging
from ..utils.retry_handler import RetryHandler
from ..core.storage_manager import StorageManager

class MassContentDownloader:
    def __init__(self, storage_manager: StorageManager, max_concurrent: int = 5):
        self.storage_manager = storage_manager
        self.max_concurrent = max_concurrent
        self.retry_handler = RetryHandler(max_retries=3, backoff_factor=2)
        self.session = None

    async def download_all_pages(self, pages: List[Dict], site_name: str, event_name: str):
        """Массово загрузить все страницы сайта."""

        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            self.session = session

            # Создаем семафор для ограничения конкурентности
            semaphore = asyncio.Semaphore(self.max_concurrent)

            # Создаем задачи для всех страниц
            tasks = []
            for page in pages:
                task = self._download_single_page(semaphore, page, site_name, event_name)
                tasks.append(task)

            # Выполняем все задачи
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Подсчитываем результаты
            successful = sum(1 for r in results if r is True)
            failed = len(results) - successful

            logging.info(f"Загрузка завершена: {successful} успешно, {failed} ошибок")

            return {'successful': successful, 'failed': failed}

    async def _download_single_page(self, semaphore: asyncio.Semaphore, page: Dict, site_name: str, event_name: str):
        """Загрузить одну страницу с повторными попытками."""

        async with semaphore:
            return await self.retry_handler.execute_with_retry(
                self._fetch_and_save_page, page, site_name, event_name
            )

    async def _fetch_and_save_page(self, page: Dict, site_name: str, event_name: str) -> bool:
        """Загрузить и сохранить HTML страницы."""

        try:
            # Небольшая задержка между запросами
            await asyncio.sleep(0.5)

            async with self.session.get(page['archive_url']) as response:
                if response.status == 200:
                    content = await response.text()

                    # Сохраняем контент
                    file_path = self.storage_manager.save_page_content(
                        site_name=site_name,
                        event_name=event_name,
                        page_info=page,
                        content=content
                    )

                    logging.debug(f"Сохранено: {file_path}")
                    return True
                else:
                    logging.warning(f"HTTP {response.status} для {page['archive_url']}")
                    return False

        except Exception as e:
            logging.error(f"Ошибка загрузки {page['archive_url']}: {e}")
            raise