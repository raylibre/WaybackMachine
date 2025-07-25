"""Модели политических событий для анализа украинских партий."""

from datetime import date, datetime, timedelta
from enum import Enum
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from pydantic import BaseModel


class EventType(Enum):
    """Типы политических событий."""
    ELECTION = "election"
    WAR = "war"
    COVID = "covid"
    LEGISLATION = "legislation"
    INTERNATIONAL = "international"
    REVOLUTION = "revolution"
    ECONOMIC = "economic"


class EventImportance(Enum):
    """Важность события для анализа."""
    CRITICAL = "critical"  # Критически важные события
    HIGH = "high"         # Высокая важность
    MEDIUM = "medium"     # Средняя важность
    LOW = "low"          # Низкая важность


@dataclass
class PoliticalEvent:
    """Модель политического события."""
    name: str
    date: date
    event_type: EventType
    description: str
    importance: EventImportance = EventImportance.MEDIUM
    analysis_window_days_before: int = 30
    analysis_window_days_after: int = 30
    tags: List[str] = field(default_factory=list)

    @property
    def date_range(self) -> Tuple[date, date]:
        """Получить диапазон дат для анализа."""
        start_date = self.date - timedelta(days=self.analysis_window_days_before)
        end_date = self.date + timedelta(days=self.analysis_window_days_after)
        return (start_date, end_date)

    @property
    def slug(self) -> str:
        """Получить slug для файловой системы."""
        return self.name.lower().replace(' ', '_').replace('(', '').replace(')', '')

    def is_in_range(self, check_date: date, buffer_days: int = 0) -> bool:
        """Проверить, попадает ли дата в диапазон события."""
        start_date = self.date - timedelta(days=self.analysis_window_days_before + buffer_days)
        end_date = self.date + timedelta(days=self.analysis_window_days_after + buffer_days)
        return start_date <= check_date <= end_date


# Предопределенные политические события для Украины
UKRAINE_POLITICAL_EVENTS = [
    # ===== ВЫБОРЫ =====
    PoliticalEvent(
        name="Президентские выборы 2019 (1 тур)",
        date=date(2019, 3, 31),
        event_type=EventType.ELECTION,
        description="Первый тур президентских выборов в Украине",
        importance=EventImportance.CRITICAL,
        analysis_window_days_before=45,
        analysis_window_days_after=21,
        tags=["выборы", "президент", "зеленский", "порошенко", "тимошенко"]
    ),

    PoliticalEvent(
        name="Президентские выборы 2019 (2 тур)",
        date=date(2019, 4, 21),
        event_type=EventType.ELECTION,
        description="Второй тур президентских выборов, победа В. Зеленского",
        importance=EventImportance.CRITICAL,
        analysis_window_days_before=21,
        analysis_window_days_after=45,
        tags=["выборы", "президент", "зеленский", "порошенко", "победа"]
    ),

    PoliticalEvent(
        name="Парламентские выборы 2019",
        date=date(2019, 7, 21),
        event_type=EventType.ELECTION,
        description="Досрочные выборы в Верховную Раду IX созыва",
        importance=EventImportance.CRITICAL,
        analysis_window_days_before=45,
        analysis_window_days_after=30,
        tags=["выборы", "рада", "парламент", "слуга_народа"]
    ),

    PoliticalEvent(
        name="Местные выборы 2020",
        date=date(2020, 10, 25),
        event_type=EventType.ELECTION,
        description="Местные выборы во время пандемии COVID-19",
        importance=EventImportance.HIGH,
        analysis_window_days_before=30,
        analysis_window_days_after=21,
        tags=["выборы", "местные", "covid", "мэры", "советы"]
    ),

    # ===== ВОЙНА И КОНФЛИКТЫ =====
    PoliticalEvent(
        name="Аннексия Крыма",
        date=date(2014, 3, 18),
        event_type=EventType.WAR,
        description="Подписание договора о принятии Крыма в состав РФ",
        importance=EventImportance.CRITICAL,
        analysis_window_days_before=30,
        analysis_window_days_after=60,
        tags=["крым", "аннексия", "россия", "референдум", "оккупация"]
    ),

    PoliticalEvent(
        name="Начало конфликта на Донбассе",
        date=date(2014, 4, 6),
        event_type=EventType.WAR,
        description="Захват административных зданий в Донецке и Луганске",
        importance=EventImportance.CRITICAL,
        analysis_window_days_before=21,
        analysis_window_days_after=60,
        tags=["донбасс", "ато", "сепаратисты", "донецк", "луганск"]
    ),

    PoliticalEvent(
        name="Полномасштабное вторжение России",
        date=date(2022, 2, 24),
        event_type=EventType.WAR,
        description="Начало полномасштабной войны России против Украины",
        importance=EventImportance.CRITICAL,
        analysis_window_days_before=60,
        analysis_window_days_after=90,
        tags=["война", "вторжение", "россия", "путин", "защита"]
    ),

    # ===== COVID-19 =====
    PoliticalEvent(
        name="Первый случай COVID-19 в Украине",
        date=date(2020, 3, 3),
        event_type=EventType.COVID,
        description="Подтверждение первого случая коронавируса в Украине",
        importance=EventImportance.HIGH,
        analysis_window_days_before=14,
        analysis_window_days_after=30,
        tags=["covid", "пандемия", "первый_случай", "здравоохранение"]
    ),

    PoliticalEvent(
        name="Введение карантина COVID-19",
        date=date(2020, 3, 17),
        event_type=EventType.COVID,
        description="Введение общенационального карантина",
        importance=EventImportance.HIGH,
        analysis_window_days_before=14,
        analysis_window_days_after=45,
        tags=["covid", "карантин", "локдаун", "ограничения"]
    ),

    PoliticalEvent(
        name="Пик COVID-19 (осень 2020)",
        date=date(2020, 11, 15),
        event_type=EventType.COVID,
        description="Пик заболеваемости коронавирусом осенью 2020",
        importance=EventImportance.MEDIUM,
        analysis_window_days_before=30,
        analysis_window_days_after=30,
        tags=["covid", "пик", "больницы", "вакцинация"]
    ),

    # ===== РЕВОЛЮЦИИ И ПРОТЕСТЫ =====
    PoliticalEvent(
        name="Начало Революции Достоинства",
        date=date(2013, 11, 21),
        event_type=EventType.REVOLUTION,
        description="Начало протестов на Майдане после отказа от Ассоциации с ЕС",
        importance=EventImportance.CRITICAL,
        analysis_window_days_before=21,
        analysis_window_days_after=120,
        tags=["майдан", "революция", "евромайдан", "янукович", "ес"]
    ),

    PoliticalEvent(
        name="Завершение Революции Достоинства",
        date=date(2014, 2, 23),
        event_type=EventType.REVOLUTION,
        description="Бегство Януковича, завершение Революции Достоинства",
        importance=EventImportance.CRITICAL,
        analysis_window_days_before=30,
        analysis_window_days_after=45,
        tags=["майдан", "янукович", "бегство", "временное_правительство"]
    ),

    # ===== МЕЖДУНАРОДНЫЕ СОБЫТИЯ =====
    PoliticalEvent(
        name="Безвизовый режим с ЕС",
        date=date(2017, 6, 11),
        event_type=EventType.INTERNATIONAL,
        description="Вступление в силу безвизового режима с ЕС",
        importance=EventImportance.HIGH,
        analysis_window_days_before=30,
        analysis_window_days_after=30,
        tags=["безвиз", "ес", "европа", "интеграция", "свобода"]
    ),

    # ===== ЗАКОНОДАТЕЛЬСТВО =====
    PoliticalEvent(
        name="Закон о языке",
        date=date(2019, 4, 25),
        event_type=EventType.LEGISLATION,
        description="Принятие закона об обеспечении функционирования украинского языка",
        importance=EventImportance.HIGH,
        analysis_window_days_before=45,
        analysis_window_days_after=30,
        tags=["язык", "украинский", "закон", "идентичность"]
    ),

    PoliticalEvent(
        name="Закон об олигархах",
        date=date(2021, 9, 23),
        event_type=EventType.LEGISLATION,
        description="Принятие закона о предотвращении угроз национальной безопасности",
        importance=EventImportance.HIGH,
        analysis_window_days_before=30,
        analysis_window_days_after=30,
        tags=["олигархи", "деолигархизация", "реестр", "влияние"]
    ),

    # ===== ЭКОНОМИЧЕСКИЕ СОБЫТИЯ =====
    PoliticalEvent(
        name="Дефолт по еврооблигациям 2015",
        date=date(2015, 12, 18),
        event_type=EventType.ECONOMIC,
        description="Объявление моратория на выплаты по еврооблигациям",
        importance=EventImportance.HIGH,
        analysis_window_days_before=30,
        analysis_window_days_after=60,
        tags=["дефолт", "долг", "экономика", "мвф", "реструктуризация"]
    ),

    PoliticalEvent(
        name="Подписание меморандума с МВФ 2020",
        date=date(2020, 6, 9),
        event_type=EventType.ECONOMIC,
        description="Подписание программы stand-by с МВФ на $5 млрд",
        importance=EventImportance.MEDIUM,
        analysis_window_days_before=21,
        analysis_window_days_after=30,
        tags=["мвф", "кредит", "реформы", "экономика", "стабилизация"]
    )
]


class EventFilter(BaseModel):
    """Фильтр для поиска событий."""
    event_types: Optional[List[EventType]] = None
    importance_levels: Optional[List[EventImportance]] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    tags: Optional[List[str]] = None

    class Config:
        use_enum_values = True


class PoliticalEventManager:
    """Менеджер для работы с политическими событиями."""

    def __init__(self, events: List[PoliticalEvent] = None):
        self.events = events or UKRAINE_POLITICAL_EVENTS

    def get_events_by_filter(self, event_filter: EventFilter) -> List[PoliticalEvent]:
        """Получить события по фильтру."""
        filtered_events = self.events

        # Фильтр по типам событий
        if event_filter.event_types:
            filtered_events = [
                e for e in filtered_events
                if e.event_type in event_filter.event_types
            ]

        # Фильтр по важности
        if event_filter.importance_levels:
            filtered_events = [
                e for e in filtered_events
                if e.importance in event_filter.importance_levels
            ]

        # Фильтр по дате от
        if event_filter.date_from:
            filtered_events = [
                e for e in filtered_events
                if e.date >= event_filter.date_from
            ]

        # Фильтр по дате до
        if event_filter.date_to:
            filtered_events = [
                e for e in filtered_events
                if e.date <= event_filter.date_to
            ]

        # Фильтр по тегам
        if event_filter.tags:
            filtered_events = [
                e for e in filtered_events
                if any(tag in e.tags for tag in event_filter.tags)
            ]

        return sorted(filtered_events, key=lambda x: x.date)

    def get_events_by_type(self, event_type: EventType) -> List[PoliticalEvent]:
        """Получить события по типу."""
        return [e for e in self.events if e.event_type == event_type]

    def get_critical_events(self) -> List[PoliticalEvent]:
        """Получить критически важные события."""
        return [e for e in self.events if e.importance == EventImportance.CRITICAL]

    def get_events_in_date_range(self, start_date: date, end_date: date) -> List[PoliticalEvent]:
        """Получить события в диапазоне дат."""
        return [
            e for e in self.events
            if start_date <= e.date <= end_date
        ]

    def find_events_near_date(self, target_date: date, days_window: int = 30) -> List[PoliticalEvent]:
        """Найти события рядом с указанной датой."""
        start_date = target_date - timedelta(days=days_window)
        end_date = target_date + timedelta(days=days_window)
        return self.get_events_in_date_range(start_date, end_date)

    def get_events_by_year(self, year: int) -> Dict[EventType, List[PoliticalEvent]]:
        """Получить события по году, сгруппированные по типам."""
        year_events = [e for e in self.events if e.date.year == year]

        grouped = {}
        for event in year_events:
            if event.event_type not in grouped:
                grouped[event.event_type] = []
            grouped[event.event_type].append(event)

        return grouped

    def get_timeline_summary(self) -> Dict[int, int]:
        """Получить сводку по годам (количество событий)."""
        timeline = {}
        for event in self.events:
            year = event.date.year
            timeline[year] = timeline.get(year, 0) + 1

        return dict(sorted(timeline.items()))

    def add_custom_event(self, event: PoliticalEvent) -> None:
        """Добавить пользовательское событие."""
        self.events.append(event)
        self.events.sort(key=lambda x: x.date)

    def export_events_summary(self) -> Dict:
        """Экспортировать сводку событий."""
        return {
            'total_events': len(self.events),
            'by_type': {
                event_type.value: len(self.get_events_by_type(event_type))
                for event_type in EventType
            },
            'by_importance': {
                importance.value: len([e for e in self.events if e.importance == importance])
                for importance in EventImportance
            },
            'timeline': self.get_timeline_summary(),
            'date_range': {
                'earliest': min(e.date for e in self.events).isoformat(),
                'latest': max(e.date for e in self.events).isoformat()
            }
        }


# Создаем глобальный экземпляр менеджера
ukraine_events = PoliticalEventManager()