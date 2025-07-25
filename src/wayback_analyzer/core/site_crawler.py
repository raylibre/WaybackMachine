# src/wayback_analyzer/core/site_crawler.py
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import Set, List, Dict
import time
from ..utils.rate_limiter import RateLimiter

class ArchiveSiteCrawler:
    def __init__(self, rate_limiter: RateLimiter, max_depth: int = 3):
        self.rate_limiter = rate_limiter
        self.max_depth = max_depth
        self.visited_urls: Set[str] = set()
        self.found_pages: List[Dict] = []

    def discover_site_structure(self, archive_url: str, base_domain: str) -> List[Dict]:
        """Рекурсивно обнаружить все страницы сайта в архиве."""

        self._crawl_recursive(archive_url, base_domain, depth=0)
        return self.found_pages

    def _crawl_recursive(self, archive_url: str, base_domain: str, depth: int):
        """Рекурсивный обход архивной версии сайта."""

        if depth > self.max_depth or archive_url in self.visited_urls:
            return

        self.visited_urls.add(archive_url)

        try:
            # Соблюдаем rate limiting
            self.rate_limiter.wait_if_needed()

            response = requests.get(archive_url, timeout=30)
            if response.status_code != 200:
                return

            # Парсим HTML
            soup = BeautifulSoup(response.content, 'html.parser')

            # Извлекаем оригинальный URL из архивной ссылки
            original_url = self._extract_original_url(archive_url)

            # Сохраняем информацию о странице
            page_info = {
                'archive_url': archive_url,
                'original_url': original_url,
                'title': soup.title.string if soup.title else '',
                'content_length': len(response.content),
                'depth': depth,
                'timestamp': self._extract_timestamp(archive_url)
            }
            self.found_pages.append(page_info)

            # Находим все ссылки на другие страницы того же домена
            links = soup.find_all('a', href=True)
            for link in links:
                href = link['href']

                # Преобразуем относительные ссылки в абсолютные архивные
                if href.startswith('/'):
                    # Это относительная ссылка
                    archive_base = self._get_archive_base(archive_url)
                    full_archive_url = archive_base + href
                elif base_domain in href and 'web.archive.org' in href:
                    # Это уже архивная ссылка
                    full_archive_url = href
                elif base_domain in href:
                    # Обычная ссылка, нужно превратить в архивную
                    timestamp = self._extract_timestamp(archive_url)
                    full_archive_url = f"https://web.archive.org/web/{timestamp}/{href}"
                else:
                    continue  # Внешняя ссылка, пропускаем

                # Рекурсивно обходим найденные страницы
                self._crawl_recursive(full_archive_url, base_domain, depth + 1)

        except Exception as e:
            print(f"Ошибка при обходе {archive_url}: {e}")

    def _extract_original_url(self, archive_url: str) -> str:
        """Извлечь оригинальный URL из архивной ссылки."""
        # web.archive.org/web/20220224120000/https://example.com
        parts = archive_url.split('/')
        if len(parts) >= 6 and 'web.archive.org' in archive_url:
            return '/'.join(parts[6:])
        return archive_url

    def _extract_timestamp(self, archive_url: str) -> str:
        """Извлечь timestamp из архивной ссылки."""
        parts = archive_url.split('/')
        if len(parts) >= 5 and 'web.archive.org' in archive_url:
            return parts[5]
        return ''

    def _get_archive_base(self, archive_url: str) -> str:
        """Получить базовую часть архивной ссылки."""
        # Возвращает https://web.archive.org/web/20220224120000
        parts = archive_url.split('/')
        if len(parts) >= 6:
            return '/'.join(parts[:6])
        return ''