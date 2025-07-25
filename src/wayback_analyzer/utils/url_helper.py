"""Помощник для работы с URL архива."""

import re
from urllib.parse import urljoin, urlparse
from typing import Optional, Tuple


class ArchiveUrlHelper:
    """Помощник для работы с URL Wayback Machine."""

    @staticmethod
    def is_archive_url(url: str) -> bool:
        """Проверить, является ли URL архивным."""
        return 'web.archive.org' in url and '/web/' in url

    @staticmethod
    def extract_timestamp_and_original(archive_url: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Извлечь timestamp и оригинальный URL из архивной ссылки.

        Returns:
            Tuple[timestamp, original_url] или (None, None) если не архивная ссылка
        """
        if not ArchiveUrlHelper.is_archive_url(archive_url):
            return None, None

        # Паттерн: https://web.archive.org/web/20201231013900/https://sluga-narodu.com/
        pattern = r'web\.archive\.org/web/(\d{14})/(.+)'
        match = re.search(pattern, archive_url)

        if match:
            timestamp = match.group(1)
            original_url = match.group(2)
            # Добавляем протокол если его нет
            if not original_url.startswith(('http://', 'https://')):
                original_url = 'https://' + original_url
            return timestamp, original_url

        return None, None

    @staticmethod
    def build_archive_url(timestamp: str, original_url: str) -> str:
        """Построить архивную ссылку из timestamp и оригинального URL."""
        # Убираем протокол из original_url если есть
        clean_url = original_url
        if clean_url.startswith(('http://', 'https://')):
            clean_url = clean_url.split('://', 1)[1] if '://' in clean_url else clean_url

        return f"https://web.archive.org/web/{timestamp}/{original_url}"

    @staticmethod
    def convert_relative_to_archive(relative_url: str, base_archive_url: str) -> Optional[str]:
        """
        Преобразовать относительную ссылку в архивную.

        Args:
            relative_url: Относительная ссылка (/about, ../news, etc.)
            base_archive_url: Базовая архивная ссылка

        Returns:
            Полная архивная ссылка или None
        """
        timestamp, original_base = ArchiveUrlHelper.extract_timestamp_and_original(base_archive_url)
        if not timestamp or not original_base:
            return None

        # Строим полный оригинальный URL
        full_original = urljoin(original_base, relative_url)

        # Преобразуем в архивную ссылку
        return ArchiveUrlHelper.build_archive_url(timestamp, full_original)

    @staticmethod
    def get_domain(url: str) -> Optional[str]:
        """Получить домен из URL."""
        try:
            if ArchiveUrlHelper.is_archive_url(url):
                _, original_url = ArchiveUrlHelper.extract_timestamp_and_original(url)
                if original_url:
                    url = original_url

            parsed = urlparse(url)
            return parsed.netloc.lower()
        except:
            return None

    @staticmethod
    def is_same_domain(url1: str, url2: str) -> bool:
        """Проверить, принадлежат ли URL одному домену."""
        domain1 = ArchiveUrlHelper.get_domain(url1)
        domain2 = ArchiveUrlHelper.get_domain(url2)

        return domain1 is not None and domain1 == domain2