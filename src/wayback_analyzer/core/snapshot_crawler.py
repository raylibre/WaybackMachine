"""Краулер для извлечения всего контента архивного снапшота."""

import requests
import logging
from bs4 import BeautifulSoup
from typing import Set, List, Dict, Optional
from urllib.parse import urljoin, urlparse
import time

from .storage_manager import StorageManager
from ..utils.rate_limiter import RateLimiter
from ..utils.url_helper import ArchiveUrlHelper


class SnapshotCrawler:
    """Краулер для рекурсивного обхода архивного снапшота сайта."""

    def __init__(
            self,
            storage_manager: StorageManager,
            rate_limiter: RateLimiter,
            max_depth: int = 3,
            max_pages: int = 100
    ):
        """
        Args:
            storage_manager: Менеджер хранения
            rate_limiter: Rate limiter для запросов
            max_depth: Максимальная глубина обхода
            max_pages: Максимальное количество страниц
        """
        self.storage_manager = storage_manager
        self.rate_limiter = rate_limiter
        self.max_depth = max_depth
        self.max_pages = max_pages

        self.visited_urls: Set[str] = set()
        self.found_pages: List[Dict] = []
        self.failed_urls: List[str] = []

        self.logger = logging.getLogger(__name__)

        # Настройка requests session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; UkrainePoliticalAnalyzer/1.0; +https://github.com/your/repo)'
        })

    def crawl_snapshot(self, start_archive_url: str) -> Dict:
        """
        Полный обход архивного снапшота сайта.

        Args:
            start_archive_url: Стартовая архивная ссылка

        Returns:
            Словарь с результатами обхода
        """
        self.logger.info(f"Начинаем обход снапшота: {start_archive_url}")

        # Получаем базовый домен
        target_domain = ArchiveUrlHelper.get_domain(start_archive_url)
        if not target_domain:
            raise ValueError(f"Cannot extract domain from: {start_archive_url}")

        self.logger.info(f"Целевой домен: {target_domain}")

        # Сбрасываем состояние
        self.visited_urls.clear()
        self.found_pages.clear()
        self.failed_urls.clear()

        # Начинаем рекурсивный обход
        start_time = time.time()
        self._crawl_recursive(start_archive_url, target_domain, depth=0)
        end_time = time.time()

        # Создаем сводку
        summary = {
            'start_url': start_archive_url,
            'target_domain': target_domain,
            'total_pages_found': len(self.found_pages),
            'total_pages_failed': len(self.failed_urls),
            'crawl_duration_seconds': round(end_time - start_time, 2),
            'max_depth_reached': max((page.get('depth', 0) for page in self.found_pages), default=0),
            'pages': self.found_pages,
            'failed_urls': self.failed_urls
        }

        # Сохраняем сводку
        self.storage_manager.save_snapshot_summary(start_archive_url, summary)

        self.logger.info(f"Обход завершен: {len(self.found_pages)} страниц, {len(self.failed_urls)} ошибок")

        return summary

    def _crawl_recursive(self, archive_url: str, target_domain: str, depth: int):
        """Рекурсивный обход страниц."""

        # Проверяем ограничения
        if (depth > self.max_depth or
                len(self.found_pages) >= self.max_pages or
                archive_url in self.visited_urls):
            return

        self.visited_urls.add(archive_url)
        self.logger.debug(f"Обрабатываем (глубина {depth}): {archive_url}")

        try:
            # Соблюдаем rate limiting
            self.rate_limiter.wait_if_needed()

            # Проверяем, не сохранена ли уже страница
            if self.storage_manager.page_exists(archive_url):
                self.logger.debug(f"Страница уже существует, пропускаем: {archive_url}")
                return

            # Загружаем страницу
            response = self.session.get(archive_url, timeout=30)

            if response.status_code != 200:
                self.logger.warning(f"HTTP {response.status_code} для {archive_url}")
                self.failed_urls.append(archive_url)
                return

            # Парсим HTML
            soup = BeautifulSoup(response.text, 'html.parser')

            # Извлекаем информацию о странице
            timestamp, original_url = ArchiveUrlHelper.extract_timestamp_and_original(archive_url)

            page_info = {
                'archive_url': archive_url,
                'original_url': original_url,
                'timestamp': timestamp,
                'title': soup.title.string.strip() if soup.title and soup.title.string else '',
                'content_length': len(response.text),
                'depth': depth,
                'links_found': 0,
                'images_found': 0,
                'processed_at': time.time()
            }

            # Подсчитываем ссылки и изображения
            links = soup.find_all('a', href=True)
            images = soup.find_all('img', src=True)
            page_info['links_found'] = len(links)
            page_info['images_found'] = len(images)

            # Сохраняем контент
            file_path = self.storage_manager.save_page_content(
                archive_url=archive_url,
                content=response.text,
                metadata=page_info
            )

            page_info['saved_to'] = str(file_path)
            self.found_pages.append(page_info)

            self.logger.info(f"Сохранено ({len(self.found_pages)}/{self.max_pages}): {page_info['title'][:50]}...")

            # Ищем ссылки на другие страницы того же домена
            internal_links = self._extract_internal_links(soup, archive_url, target_domain)

            # Рекурсивно обходим найденные ссылки
            for link_url in internal_links:
                if len(self.found_pages) < self.max_pages:
                    self._crawl_recursive(link_url, target_domain, depth + 1)
                else:
                    break

        except requests.RequestException as e:
            self.logger.error(f"Ошибка запроса для {archive_url}: {e}")
            self.failed_urls.append(archive_url)

        except Exception as e:
            self.logger.error(f"Неожиданная ошибка для {archive_url}: {e}")
            self.failed_urls.append(archive_url)

    def _extract_internal_links(self, soup: BeautifulSoup, base_archive_url: str, target_domain: str) -> List[str]:
        """Извлечь внутренние ссылки сайта из HTML."""

        internal_links = []
        timestamp, _ = ArchiveUrlHelper.extract_timestamp_and_original(base_archive_url)

        links = soup.find_all('a', href=True)

        for link in links:
            href = link['href'].strip()
            if not href or href.startswith('#'):
                continue

            archive_link = None

            # Относительная ссылка
            if href.startswith('/'):
                archive_link = ArchiveUrlHelper.convert_relative_to_archive(href, base_archive_url)

            # Абсолютная ссылка того же домена
            elif target_domain in href:
                if ArchiveUrlHelper.is_archive_url(href):
                    # Уже архивная ссылка
                    archive_link = href
                else:
                    # Обычная ссылка, преобразуем в архивную
                    archive_link = ArchiveUrlHelper.build_archive_url(timestamp, href)

            # Проверяем что ссылка валидна и принадлежит нашему домену
            if (archive_link and
                    ArchiveUrlHelper.is_same_domain(archive_link, base_archive_url) and
                    archive_link not in self.visited_urls):

                internal_links.append(archive_link)

        self.logger.debug(f"Найдено внутренних ссылок: {len(internal_links)}")
        return internal_links