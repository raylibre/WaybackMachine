"""Менеджер для структурированного хранения архивных данных."""

import os
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import hashlib


class StorageManager:
    """Управляет сохранением архивных данных в файловую систему."""

    def __init__(self, base_path: Path = Path("./archive_data")):
        """
        Args:
            base_path: Базовая директория для сохранения данных
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

        # Создаем основные поддиректории
        (self.base_path / "snapshots").mkdir(exist_ok=True)
        (self.base_path / "metadata").mkdir(exist_ok=True)
        (self.base_path / "logs").mkdir(exist_ok=True)

    def save_page_content(
            self,
            archive_url: str,
            content: str,
            metadata: Optional[Dict[str, Any]] = None
    ) -> Path:
        """
        Сохранить контент страницы.

        Args:
            archive_url: URL архивной страницы
            content: HTML контент
            metadata: Дополнительные метаданные

        Returns:
            Путь к сохраненному файлу
        """
        from ..utils.url_helper import ArchiveUrlHelper

        timestamp, original_url = ArchiveUrlHelper.extract_timestamp_and_original(archive_url)
        if not timestamp or not original_url:
            raise ValueError(f"Invalid archive URL: {archive_url}")

        # Создаем структуру директорий
        domain = ArchiveUrlHelper.get_domain(original_url)
        if not domain:
            raise ValueError(f"Cannot extract domain from: {original_url}")

        # Создаем безопасное имя для домена
        safe_domain = domain.replace('.', '_').replace('-', '_')

        # Структура: snapshots/domain/timestamp/
        snapshot_dir = self.base_path / "snapshots" / safe_domain / timestamp
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        # Создаем имя файла из URL
        file_name = self._url_to_filename(original_url)
        file_path = snapshot_dir / file_name

        # Сохраняем HTML контент
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        # Сохраняем метаданные
        if metadata is None:
            metadata = {}

        metadata.update({
            'archive_url': archive_url,
            'original_url': original_url,
            'timestamp': timestamp,
            'domain': domain,
            'saved_at': datetime.now().isoformat(),
            'content_length': len(content),
            'file_path': str(file_path.relative_to(self.base_path))
        })

        metadata_path = snapshot_dir / f"{file_name}.meta.json"
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        return file_path

    def save_snapshot_summary(
            self,
            archive_url: str,
            summary_data: Dict[str, Any]
    ) -> Path:
        """Сохранить сводку по снапшоту."""
        from ..utils.url_helper import ArchiveUrlHelper

        timestamp, original_url = ArchiveUrlHelper.extract_timestamp_and_original(archive_url)
        domain = ArchiveUrlHelper.get_domain(original_url)
        safe_domain = domain.replace('.', '_').replace('-', '_')

        summary_dir = self.base_path / "metadata" / safe_domain
        summary_dir.mkdir(parents=True, exist_ok=True)

        summary_path = summary_dir / f"snapshot_{timestamp}_summary.json"

        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)

        return summary_path

    def get_snapshot_path(self, archive_url: str) -> Path:
        """Получить путь к директории снапшота."""
        from ..utils.url_helper import ArchiveUrlHelper

        timestamp, original_url = ArchiveUrlHelper.extract_timestamp_and_original(archive_url)
        domain = ArchiveUrlHelper.get_domain(original_url)
        safe_domain = domain.replace('.', '_').replace('-', '_')

        return self.base_path / "snapshots" / safe_domain / timestamp

    def page_exists(self, archive_url: str) -> bool:
        """Проверить, существует ли уже сохраненная страница."""
        from ..utils.url_helper import ArchiveUrlHelper

        timestamp, original_url = ArchiveUrlHelper.extract_timestamp_and_original(archive_url)
        if not timestamp or not original_url:
            return False

        domain = ArchiveUrlHelper.get_domain(original_url)
        safe_domain = domain.replace('.', '_').replace('-', '_')

        file_name = self._url_to_filename(original_url)
        file_path = self.base_path / "snapshots" / safe_domain / timestamp / file_name

        return file_path.exists()

    def _url_to_filename(self, url: str) -> str:
        """Преобразовать URL в безопасное имя файла."""
        # Убираем протокол
        if '://' in url:
            url = url.split('://', 1)[1]

        # Заменяем специальные символы
        filename = url.replace('/', '_').replace('?', '_').replace('&', '_').replace('=', '_')

        # Ограничиваем длину и добавляем расширение
        if len(filename) > 200:
            # Используем hash для очень длинных URL
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            filename = filename[:180] + f"_{url_hash}"

        # Добавляем расширение если его нет
        if not filename.endswith('.html'):
            filename += '.html'

        return filename