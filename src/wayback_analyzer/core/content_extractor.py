"""Экстрактор контента из сохраненных HTML страниц."""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup, Comment
from dataclasses import dataclass
from datetime import datetime

from .storage_manager import StorageManager


@dataclass
class ExtractedContent:
    """Структура извлеченного контента."""

    # Базовая информация
    url: str
    title: str
    timestamp: str

    # Основной контент
    main_text: str
    headings: Dict[str, List[str]]  # h1, h2, h3 и т.д.
    paragraphs: List[str]

    # Навигация и ссылки
    menu_links: List[Dict[str, str]]
    internal_links: List[Dict[str, str]]
    external_links: List[Dict[str, str]]

    # Политический контент
    political_keywords: List[str]
    quotes: List[str]
    promises: List[str]

    # Метаданные
    word_count: int
    language: str
    extraction_date: str


class PoliticalContentExtractor:
    """Экстрактор политического контента из HTML."""

    def __init__(self):
        self.political_keywords = {
            # Общие политические термины
            'общие': [
                'реформа', 'реформи', 'зміни', 'програма', 'платформа',
                'ідеологія', 'принципи', 'цінності', 'мета', 'завдання'
            ],

            # Обещания и планы
            'обещания': [
                'обіцяємо', 'зобов\'язуємося', 'плануємо', 'будемо',
                'реалізуємо', 'впровадимо', 'забезпечимо', 'створимо',
                'побудуємо', 'досягнемо', 'змінимо'
            ],

            # Критика и оппозиция
            'критика': [
                'корупція', 'олігархи', 'стара влада', 'система',
                'бездіяльність', 'неефективність', 'провал', 'криза'
            ],

            # Ключевые темы для Украины
            'темы': [
                'децентралізація', 'євроінтеграція', 'нато', 'безпека',
                'економіка', 'освіта', 'медицина', 'пенсії', 'зарплати',
                'армія', 'війна', 'мир', 'територіальна цілісність'
            ]
        }

        # Паттерны для поиска цитат и обещаний
        self.quote_patterns = [
            r'"([^"]{20,200})"',  # Текст в кавычках
            r'«([^»]{20,200})»',  # Текст в украинских кавычках
        ]

        self.promise_patterns = [
            r'((?:ми|партія|слуга народу).{0,50}(?:обіцяємо|зобов\'язуємося|плануємо|будемо).{10,200})',
            r'((?:наша мета|наше завдання|ми досягнемо).{10,150})',
        ]

    def extract_from_html_file(self, file_path: Path) -> ExtractedContent:
        """Извлечь контент из HTML файла."""

        # Читаем HTML файл
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()

        # Читаем метаданные если есть
        meta_path = file_path.with_suffix('.html.meta.json')
        metadata = {}
        if meta_path.exists():
            with open(meta_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)

        return self.extract_from_html(html_content, metadata)

    def extract_from_html(self, html_content: str, metadata: Dict = None) -> ExtractedContent:
        """Извлечь контент из HTML строки."""

        if metadata is None:
            metadata = {}

        soup = BeautifulSoup(html_content, 'html.parser')

        # Удаляем скрипты и стили
        for element in soup(['script', 'style', 'nav', 'footer', 'aside']):
            element.decompose()

        # Удаляем комментарии
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        # Базовая информация
        title = self._extract_title(soup)
        main_text = self._extract_main_text(soup)

        # Структурированный контент
        headings = self._extract_headings(soup)
        paragraphs = self._extract_paragraphs(soup)

        # Ссылки
        menu_links = self._extract_menu_links(soup)
        internal_links, external_links = self._extract_links(soup)

        # Политический анализ
        political_keywords = self._find_political_keywords(main_text)
        quotes = self._extract_quotes(main_text)
        promises = self._extract_promises(main_text)

        return ExtractedContent(
            url=metadata.get('original_url', ''),
            title=title,
            timestamp=metadata.get('timestamp', ''),
            main_text=main_text,
            headings=headings,
            paragraphs=paragraphs,
            menu_links=menu_links,
            internal_links=internal_links,
            external_links=external_links,
            political_keywords=political_keywords,
            quotes=quotes,
            promises=promises,
            word_count=len(main_text.split()),
            language='uk',
            extraction_date=datetime.now().isoformat()
        )

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Извлечь заголовок страницы."""
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text().strip()

        h1_tag = soup.find('h1')
        if h1_tag:
            return h1_tag.get_text().strip()

        return "Без заголовка"

    def _extract_main_text(self, soup: BeautifulSoup) -> str:
        """Извлечь основной текст страницы."""

        # Ищем основной контент в специальных тегах
        main_selectors = [
            'main', 'article', '.main', '.content',
            '.post-content', '.entry-content', '#content'
        ]

        main_content = None
        for selector in main_selectors:
            main_content = soup.select_one(selector)
            if main_content:
                break

        if not main_content:
            main_content = soup.find('body')

        if main_content:
            # Очищаем от ненужных элементов
            for unwanted in main_content.find_all(['nav', 'footer', 'aside', 'header']):
                unwanted.decompose()

            text = main_content.get_text(separator=' ', strip=True)
            # Очищаем множественные пробелы
            text = re.sub(r'\s+', ' ', text)
            return text.strip()

        return soup.get_text(separator=' ', strip=True)

    def _extract_headings(self, soup: BeautifulSoup) -> Dict[str, List[str]]:
        """Извлечь заголовки по уровням."""
        headings = {}

        for level in range(1, 7):  # h1-h6
            tag_name = f'h{level}'
            tags = soup.find_all(tag_name)
            if tags:
                headings[tag_name] = [tag.get_text().strip() for tag in tags if tag.get_text().strip()]

        return headings

    def _extract_paragraphs(self, soup: BeautifulSoup) -> List[str]:
        """Извлечь абзацы текста."""
        paragraphs = soup.find_all('p')

        result = []
        for p in paragraphs:
            text = p.get_text().strip()
            if len(text) > 20:  # Игнорируем слишком короткие абзацы
                result.append(text)

        return result

    def _extract_menu_links(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Извлечь ссылки из меню навигации."""
        menu_links = []

        # Ищем навигационные меню
        nav_selectors = [
            'nav', '.menu', '.navigation', '.nav',
            '.primary-menu', '.main-menu', '.header-menu'
        ]

        for selector in nav_selectors:
            nav_elements = soup.select(selector)
            for nav in nav_elements:
                links = nav.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    text = link.get_text().strip()
                    if text and href:
                        menu_links.append({
                            'text': text,
                            'url': href,
                            'type': 'menu'
                        })

        return menu_links

    def _extract_links(self, soup: BeautifulSoup) -> tuple[List[Dict[str, str]], List[Dict[str, str]]]:
        """Извлечь внутренние и внешние ссылки."""
        all_links = soup.find_all('a', href=True)

        internal_links = []
        external_links = []

        for link in all_links:
            href = link.get('href', '')
            text = link.get_text().strip()

            if not text or not href:
                continue

            link_data = {
                'text': text,
                'url': href
            }

            # Определяем тип ссылки
            if (href.startswith('/') or
                    'sluga-narodu.com' in href or
                    href.startswith('#')):
                internal_links.append(link_data)
            elif href.startswith(('http://', 'https://')):
                external_links.append(link_data)

        return internal_links, external_links

    def _find_political_keywords(self, text: str) -> List[str]:
        """Найти политические ключевые слова в тексте."""
        found_keywords = []
        text_lower = text.lower()

        for category, keywords in self.political_keywords.items():
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    found_keywords.append(f"{category}: {keyword}")

        return found_keywords

    def _extract_quotes(self, text: str) -> List[str]:
        """Извлечь цитаты из текста."""
        quotes = []

        for pattern in self.quote_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.DOTALL)
            for match in matches:
                quote = match.group(1).strip()
                if len(quote) > 20:
                    quotes.append(quote)

        return quotes

    def _extract_promises(self, text: str) -> List[str]:
        """Извлечь обещания и заявления из текста."""
        promises = []

        for pattern in self.promise_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.DOTALL)
            for match in matches:
                promise = match.group(1).strip()
                if len(promise) > 20:
                    promises.append(promise)

        return promises


class ContentAnalyzer:
    """Анализатор извлеченного контента."""

    def __init__(self, storage_manager: StorageManager):
        self.storage_manager = storage_manager
        self.extractor = PoliticalContentExtractor()

    def analyze_snapshot_folder(self, snapshot_path: Path) -> Dict[str, Any]:
        """Анализировать все HTML файлы в папке снапшота."""

        html_files = list(snapshot_path.glob('*.html'))

        if not html_files:
            return {'error': 'No HTML files found'}

        extracted_pages = []
        all_keywords = []
        all_quotes = []
        all_promises = []
        total_words = 0

        for html_file in html_files:
            try:
                content = self.extractor.extract_from_html_file(html_file)

                extracted_pages.append({
                    'file': html_file.name,
                    'title': content.title,
                    'url': content.url,
                    'word_count': content.word_count,
                    'headings_count': sum(len(headings) for headings in content.headings.values()),
                    'links_count': len(content.internal_links) + len(content.external_links),
                    'keywords_found': len(content.political_keywords),
                    'quotes_found': len(content.quotes),
                    'promises_found': len(content.promises)
                })

                all_keywords.extend(content.political_keywords)
                all_quotes.extend(content.quotes)
                all_promises.extend(content.promises)
                total_words += content.word_count

            except Exception as e:
                print(f"Ошибка при анализе {html_file}: {e}")
                continue

        # Создаем сводку
        analysis_summary = {
            'snapshot_path': str(snapshot_path),
            'total_pages': len(extracted_pages),
            'total_words': total_words,
            'total_keywords': len(all_keywords),
            'total_quotes': len(all_quotes),
            'total_promises': len(all_promises),
            'pages': extracted_pages,
            'top_keywords': self._get_top_items(all_keywords, 10),
            'analysis_date': datetime.now().isoformat()
        }

        return analysis_summary

    def extract_detailed_content(self, snapshot_path: Path) -> Dict[str, ExtractedContent]:
        """Извлечь детальный контент всех страниц снапшота."""

        html_files = list(snapshot_path.glob('*.html'))
        detailed_content = {}

        for html_file in html_files:
            try:
                content = self.extractor.extract_from_html_file(html_file)
                detailed_content[html_file.name] = content
            except Exception as e:
                print(f"Ошибка при извлечении контента из {html_file}: {e}")

        return detailed_content

    def _get_top_items(self, items: List[str], limit: int = 10) -> List[Dict[str, Any]]:
        """Получить топ самых частых элементов."""
        from collections import Counter

        counter = Counter(items)
        return [
            {'item': item, 'count': count}
            for item, count in counter.most_common(limit)
        ]

    def save_analysis_results(
            self,
            analysis_results: Dict[str, Any],
            output_path: Path
    ) -> Path:
        """Сохранить результаты анализа в JSON файл."""

        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, indent=2, ensure_ascii=False)

        return output_path