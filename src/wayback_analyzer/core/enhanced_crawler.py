"""Улучшенный краулер для массового извлечения контента."""

import requests
import logging
import json
import time
from bs4 import BeautifulSoup
from typing import Set, List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse
from pathlib import Path
from datetime import datetime, timedelta

from .storage_manager import StorageManager
from ..utils.rate_limiter import RateLimiter
from ..utils.url_helper import ArchiveUrlHelper


class EnhancedSnapshotCrawler:
    """Улучшенный краулер для больших политических сайтов."""

    def __init__(
            self,
            storage_manager: StorageManager,
            rate_limiter: RateLimiter,
            max_depth: int = 4,
            max_pages: int = 500,
            resume_mode: bool = True
    ):
        self.storage_manager = storage_manager
        self.rate_limiter = rate_limiter
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.resume_mode = resume_mode

        self.visited_urls: Set[str] = set()
        self.found_pages: List[Dict] = []
        self.failed_urls: List[Dict] = []
        self.skipped_urls: List[str] = []

        self.logger = logging.getLogger(__name__)

        # Расширенная статистика
        self.stats = {
            'start_time': None,
            'pages_processed': 0,
            'pages_skipped': 0,
            'pages_failed': 0,
            'total_size_mb': 0,
            'avg_page_size': 0
        }

        # Настройка requests session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; UkrainePoliticalAnalyzer/1.0; +https://github.com/your/repo)'
        })

        # Приоритетные разделы для политических сайтов
        self.priority_paths = [
            '/about', '/про-партію', '/програма', '/program',
            '/news', '/новини', '/blog', '/блог',
            '/deputies', '/депутати', '/candidates', '/кандидати',
            '/contacts', '/контакти', '/join', '/приєднатися'
        ]

        # Исключаемые пути
        self.exclude_paths = [
            '/wp-admin', '/admin', '/login',
            '/search', '/пошук', '/404',
            '.pdf', '.doc', '.zip', '.jpg', '.png', '.gif'
        ]

    def crawl_political_site(
            self,
            site_url: str,
            target_date: str = None,
            callback=None
    ) -> Dict[str, Any]:
        """
        Полный обход политического сайта с оптимизацией.

        Args:
            site_url: URL сайта (например, https://sluga-narodu.com)
            target_date: Целевая дата в формате YYYY-MM-DD (опционально)
            callback: Функция для обновления прогресса
        """

        self.logger.info(f"🚀 Начинаем обход политического сайта: {site_url}")
        self.stats['start_time'] = time.time()

        # Находим подходящий снапшот
        archive_url = self._find_best_snapshot(site_url, target_date)
        if not archive_url:
            raise ValueError(f"Не найден архивный снапшот для {site_url}")

        timestamp, original_url = ArchiveUrlHelper.extract_timestamp_and_original(archive_url)
        target_domain = ArchiveUrlHelper.get_domain(original_url)

        self.logger.info(f"📅 Найден снапшот: {timestamp}")
        self.logger.info(f"🌐 Целевой домен: {target_domain}")

        # Проверяем resume режим
        if self.resume_mode:
            self._load_previous_state(archive_url)

        # Сбрасываем состояние если не resume
        if not self.resume_mode:
            self.visited_urls.clear()
            self.found_pages.clear()
            self.failed_urls.clear()

        # Начинаем рекурсивный обход
        self._crawl_with_priority(archive_url, target_domain, callback)

        # Финальная статистика
        end_time = time.time()
        duration = end_time - self.stats['start_time']

        summary = {
            'start_url': archive_url,
            'original_url': site_url,
            'timestamp': timestamp,
            'target_domain': target_domain,
            'total_pages_found': len(self.found_pages),
            'total_pages_failed': len(self.failed_urls),
            'total_pages_skipped': len(self.skipped_urls),
            'crawl_duration_seconds': round(duration, 2),
            'crawl_duration_minutes': round(duration / 60, 2),
            'max_depth_reached': max((page.get('depth', 0) for page in self.found_pages), default=0),
            'avg_page_size_kb': round(self.stats['avg_page_size'] / 1024, 2) if self.stats['avg_page_size'] else 0,
            'total_size_mb': round(self.stats['total_size_mb'], 2),
            'pages_per_minute': round(len(self.found_pages) / (duration / 60), 2) if duration > 0 else 0,
            'pages': self.found_pages,
            'failed_urls': self.failed_urls,
            'skipped_urls': self.skipped_urls[:50]  # Ограничиваем для JSON
        }

        # Сохраняем сводку и состояние
        self.storage_manager.save_snapshot_summary(archive_url, summary)
        self._save_crawler_state(archive_url, summary)

        self.logger.info(f"✅ Обход завершен:")
        self.logger.info(f"   📄 Страниц: {len(self.found_pages)}")
        self.logger.info(f"   ❌ Ошибок: {len(self.failed_urls)}")
        self.logger.info(f"   ⏱️  Время: {summary['crawl_duration_minutes']:.1f} мин")
        self.logger.info(f"   💾 Размер: {summary['total_size_mb']:.1f} MB")

        return summary

    def _find_best_snapshot(self, site_url: str, target_date: str = None) -> Optional[str]:
        """Найти лучший доступный снапшот для сайта."""
        from waybackpy import WaybackMachineCDXServerAPI

        try:
            cdx_api = WaybackMachineCDXServerAPI(site_url, self.session.headers['User-Agent'])

            if target_date:
                # Ищем снапшоты рядом с целевой датой
                target_dt = datetime.strptime(target_date, '%Y-%m-%d')

                # Ищем в окне ±30 дней
                best_snapshot = None
                min_diff = float('inf')

                for snapshot in cdx_api.snapshots():
                    try:
                        snapshot_dt = datetime.strptime(snapshot.timestamp, "%Y%m%d%H%M%S")
                        diff = abs((snapshot_dt - target_dt).days)

                        if diff < min_diff and snapshot.statuscode == '200':
                            min_diff = diff
                            best_snapshot = snapshot

                        # Если нашли точное совпадение по дню, берем его
                        if diff == 0:
                            break

                    except (ValueError, AttributeError):
                        continue

                if best_snapshot:
                    self.logger.info(f"📅 Найден снапшот на расстоянии {min_diff} дней от целевой даты")
                    return best_snapshot.archive_url

            else:
                # Берем последний доступный снапшот
                for snapshot in cdx_api.snapshots():
                    if snapshot.statuscode == '200':
                        return snapshot.archive_url

            return None

        except Exception as e:
            self.logger.error(f"Ошибка при поиске снапшота: {e}")
            return None

    def _crawl_with_priority(self, start_url: str, target_domain: str, callback=None):
        """Обход с приоритизацией важных разделов."""

        # Сначала обходим приоритетные разделы
        priority_urls = []
        timestamp, base_url = ArchiveUrlHelper.extract_timestamp_and_original(start_url)

        for priority_path in self.priority_paths:
            priority_url = ArchiveUrlHelper.build_archive_url(timestamp, base_url + priority_path)
            priority_urls.append(priority_url)

        # Добавляем главную страницу в начало
        all_urls = [start_url] + priority_urls

        # Обходим приоритетные URL
        for url in all_urls:
            if len(self.found_pages) >= self.max_pages:
                break

            if url not in self.visited_urls:
                self._crawl_recursive(url, target_domain, depth=0, callback=callback)

        # Если еще есть место, продолжаем обычный обход
        if len(self.found_pages) < self.max_pages:
            self._crawl_recursive(start_url, target_domain, depth=0, callback=callback)

    def _crawl_recursive(self, archive_url: str, target_domain: str, depth: int, callback=None):
        """Рекурсивный обход с улучшенной обработкой ошибок."""

        # Проверяем ограничения
        if (depth > self.max_depth or
                len(self.found_pages) >= self.max_pages or
                archive_url in self.visited_urls):
            return

        self.visited_urls.add(archive_url)

        # Проверяем исключаемые пути
        if any(exclude in archive_url.lower() for exclude in self.exclude_paths):
            self.skipped_urls.append(archive_url)
            self.logger.debug(f"⏭️ Пропущен (исключен): {archive_url}")
            return

        # Обновляем прогресс
        if callback:
            callback(len(self.found_pages), self.max_pages)

        self.logger.debug(f"🔍 Обрабатываем (глубина {depth}): {archive_url}")

        try:
            # Соблюдаем rate limiting
            self.rate_limiter.wait_if_needed()

            # Проверяем, не сохранена ли уже страница (resume)
            if self.resume_mode and self.storage_manager.page_exists(archive_url):
                self.logger.debug(f"✅ Страница уже существует: {archive_url}")
                self.stats['pages_skipped'] += 1
                return

            # Загружаем страницу
            response = self.session.get(archive_url, timeout=30)

            if response.status_code == 404:
                self.logger.debug(f"🚫 Страница не найдена в архиве: {archive_url}")
                self.failed_urls.append({
                    'url': archive_url,
                    'error': 'Not found in archive',
                    'status_code': 404,
                    'depth': depth
                })
                return

            if response.status_code != 200:
                self.logger.warning(f"⚠️ HTTP {response.status_code} для {archive_url}")
                self.failed_urls.append({
                    'url': archive_url,
                    'error': f'HTTP {response.status_code}',
                    'status_code': response.status_code,
                    'depth': depth
                })
                return

            # Обрабатываем успешную страницу
            self._process_successful_page(archive_url, response, target_domain, depth)

            # Ищем ссылки для дальнейшего обхода
            if depth < self.max_depth and len(self.found_pages) < self.max_pages:
                internal_links = self._extract_internal_links_optimized(
                    response.text, archive_url, target_domain
                )

                # Рекурсивно обходим найденные ссылки
                for link_url in internal_links:
                    if len(self.found_pages) >= self.max_pages:
                        break
                    self._crawl_recursive(link_url, target_domain, depth + 1, callback)

        except requests.RequestException as e:
            self.logger.error(f"❌ Ошибка запроса для {archive_url}: {e}")
            self.failed_urls.append({
                'url': archive_url,
                'error': str(e),
                'error_type': 'RequestException',
                'depth': depth
            })

        except Exception as e:
            self.logger.error(f"❌ Неожиданная ошибка для {archive_url}: {e}")
            self.failed_urls.append({
                'url': archive_url,
                'error': str(e),
                'error_type': 'UnexpectedException',
                'depth': depth
            })

    def _process_successful_page(self, archive_url: str, response, target_domain: str, depth: int):
        """Обработать успешно загруженную страницу."""

        # Парсим только для извлечения метаданных
        soup = BeautifulSoup(response.text, 'html.parser')

        # Извлекаем базовую информацию
        timestamp, original_url = ArchiveUrlHelper.extract_timestamp_and_original(archive_url)

        # Собираем метаданные
        page_info = {
            'archive_url': archive_url,
            'original_url': original_url,
            'timestamp': timestamp,
            'title': self._extract_title_safe(soup),
            'content_length': len(response.text),
            'content_length_mb': round(len(response.text) / (1024 * 1024), 3),
            'depth': depth,
            'links_found': len(soup.find_all('a', href=True)),
            'images_found': len(soup.find_all('img')),
            'processed_at': time.time(),
            'response_time_ms': round(response.elapsed.total_seconds() * 1000, 2)
        }

        # Обновляем статистику
        self.stats['pages_processed'] += 1
        self.stats['total_size_mb'] += page_info['content_length_mb']
        self.stats['avg_page_size'] = (
                (self.stats['avg_page_size'] * (self.stats['pages_processed'] - 1) +
                 len(response.text)) / self.stats['pages_processed']
        )

        # Сохраняем контент
        file_path = self.storage_manager.save_page_content(
            archive_url=archive_url,
            content=response.text,
            metadata=page_info
        )

        page_info['saved_to'] = str(file_path)
        self.found_pages.append(page_info)

        self.logger.info(
            f"✅ [{len(self.found_pages)}/{self.max_pages}] "
            f"{page_info['title'][:50]}... "
            f"({page_info['content_length_mb']} MB)"
        )

    def _extract_title_safe(self, soup: BeautifulSoup) -> str:
        """Безопасно извлечь заголовок страницы."""
        try:
            if soup.title and soup.title.string:
                return soup.title.string.strip()

            h1_tag = soup.find('h1')
            if h1_tag and h1_tag.get_text():
                return h1_tag.get_text().strip()

            return "Без заголовка"
        except:
            return "Ошибка извлечения заголовка"

    def _extract_internal_links_optimized(
            self,
            html_content: str,
            base_archive_url: str,
            target_domain: str
    ) -> List[str]:
        """Оптимизированное извлечение внутренних ссылок."""

        try:
            # Парсим только для ссылок
            soup = BeautifulSoup(html_content, 'html.parser')
            timestamp, _ = ArchiveUrlHelper.extract_timestamp_and_original(base_archive_url)

            internal_links = []
            links = soup.find_all('a', href=True)

            for link in links:
                href = link['href'].strip()
                if not href or href.startswith('#'):
                    continue

                # Быстрая проверка на исключаемые пути
                if any(exclude in href.lower() for exclude in self.exclude_paths):
                    continue

                archive_link = None

                # Относительная ссылка
                if href.startswith('/'):
                    archive_link = ArchiveUrlHelper.convert_relative_to_archive(href, base_archive_url)

                # Абсолютная ссылка того же домена
                elif target_domain in href.lower():
                    if ArchiveUrlHelper.is_archive_url(href):
                        archive_link = href
                    else:
                        archive_link = ArchiveUrlHelper.build_archive_url(timestamp, href)

                # Добавляем валидную ссылку
                if (archive_link and
                        archive_link not in self.visited_urls and
                        ArchiveUrlHelper.is_same_domain(archive_link, base_archive_url)):

                    internal_links.append(archive_link)

            # Удаляем дубликаты и сортируем по приоритету
            unique_links = list(set(internal_links))

            # Приоритизируем важные разделы
            priority_links = []
            regular_links = []

            for link in unique_links:
                is_priority = any(priority in link.lower() for priority in self.priority_paths)
                if is_priority:
                    priority_links.append(link)
                else:
                    regular_links.append(link)

            return priority_links + regular_links

        except Exception as e:
            self.logger.error(f"Ошибка извлечения ссылок: {e}")
            return []

    def _load_previous_state(self, archive_url: str):
        """Загрузить предыдущее состояние для resume."""
        try:
            snapshot_path = self.storage_manager.get_snapshot_path(archive_url)
            state_file = snapshot_path / 'crawler_state.json'

            if state_file.exists():
                with open(state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)

                self.visited_urls = set(state.get('visited_urls', []))
                self.found_pages = state.get('found_pages', [])
                self.failed_urls = state.get('failed_urls', [])

                self.logger.info(f"🔄 Восстановлено состояние: {len(self.visited_urls)} посещенных URL")

        except Exception as e:
            self.logger.warning(f"Не удалось загрузить предыдущее состояние: {e}")

    def _save_crawler_state(self, archive_url: str, summary: Dict[str, Any]):
        """Сохранить состояние краулера."""
        try:
            snapshot_path = self.storage_manager.get_snapshot_path(archive_url)
            state_file = snapshot_path / 'crawler_state.json'

            state = {
                'visited_urls': list(self.visited_urls),
                'found_pages': self.found_pages,
                'failed_urls': self.failed_urls,
                'summary': summary,
                'saved_at': datetime.now().isoformat()
            }

            with open(state_file, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)

        except Exception as e:
            self.logger.warning(f"Не удалось сохранить состояние: {e}")