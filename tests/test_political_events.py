"""Тесты для моделей политических событий."""

import pytest
from datetime import date, timedelta
from wayback_analyzer.models import (
    PoliticalEvent,
    EventType,
    EventImportance,
    EventFilter,
    PoliticalEventManager,
    UKRAINE_POLITICAL_EVENTS,
    ukraine_events
)


def test_political_event_creation():
    """Тест создания политического события."""
    event = PoliticalEvent(
        name="Тестовое событие",
        date=date(2020, 1, 1),
        event_type=EventType.ELECTION,
        description="Описание тестового события"
    )

    assert event.name == "Тестовое событие"
    assert event.date == date(2020, 1, 1)
    assert event.event_type == EventType.ELECTION
    assert event.importance == EventImportance.MEDIUM  # по умолчанию


def test_political_event_date_range():
    """Тест расчета диапазона дат."""
    event = PoliticalEvent(
        name="Тест",
        date=date(2020, 6, 15),
        event_type=EventType.COVID,
        description="Тест",
        analysis_window_days_before=10,
        analysis_window_days_after=20
    )

    start_date, end_date = event.date_range

    assert start_date == date(2020, 6, 5)  # 15 - 10 дней
    assert end_date == date(2020, 7, 5)    # 15 + 20 дней


def test_political_event_slug():
    """Тест генерации slug."""
    event = PoliticalEvent(
        name="Президентские выборы 2019 (1 тур)",
        date=date(2019, 3, 31),
        event_type=EventType.ELECTION,
        description="Тест"
    )

    assert event.slug == "президентские_выборы_2019_1_тур"


def test_political_event_is_in_range():
    """Тест проверки попадания даты в диапазон."""
    event = PoliticalEvent(
        name="Тест",
        date=date(2020, 6, 15),
        event_type=EventType.COVID,
        description="Тест",
        analysis_window_days_before=10,
        analysis_window_days_after=10
    )

    # Дата в диапазоне
    assert event.is_in_range(date(2020, 6, 10)) == True

    # Дата за пределами диапазона
    assert event.is_in_range(date(2020, 6, 1)) == False
    assert event.is_in_range(date(2020, 7, 1)) == False


def test_ukraine_events_loaded():
    """Тест загрузки украинских событий."""
    assert len(UKRAINE_POLITICAL_EVENTS) > 0

    # Проверяем что есть события разных типов
    event_types = {event.event_type for event in UKRAINE_POLITICAL_EVENTS}
    assert EventType.ELECTION in event_types
    assert EventType.WAR in event_types
    assert EventType.COVID in event_types


def test_event_manager_filter_by_type():
    """Тест фильтрации событий по типу."""
    manager = PoliticalEventManager()

    # Получаем только выборы
    elections = manager.get_events_by_type(EventType.ELECTION)

    assert len(elections) > 0
    assert all(event.event_type == EventType.ELECTION for event in elections)


def test_event_manager_critical_events():
    """Тест получения критических событий."""
    manager = PoliticalEventManager()

    critical_events = manager.get_critical_events()

    assert len(critical_events) > 0
    assert all(event.importance == EventImportance.CRITICAL for event in critical_events)


def test_event_manager_date_range():
    """Тест поиска событий в диапазоне дат."""
    manager = PoliticalEventManager()

    # События 2019 года
    events_2019 = manager.get_events_in_date_range(
        date(2019, 1, 1),
        date(2019, 12, 31)
    )

    assert len(events_2019) > 0
    assert all(event.date.year == 2019 for event in events_2019)


def test_event_manager_near_date():
    """Тест поиска событий рядом с датой."""
    manager = PoliticalEventManager()

    # События рядом с началом войны
    near_war = manager.find_events_near_date(date(2022, 2, 24), days_window=60)

    assert len(near_war) > 0


def test_event_filter():
    """Тест фильтрации событий."""
    manager = PoliticalEventManager()

    # Фильтр по типу и важности
    event_filter = EventFilter(
        event_types=[EventType.ELECTION],
        importance_levels=[EventImportance.CRITICAL]
    )

    filtered_events = manager.get_events_by_filter(event_filter)

    assert len(filtered_events) > 0
    assert all(
        event.event_type == EventType.ELECTION and
        event.importance == EventImportance.CRITICAL
        for event in filtered_events
    )


def test_global_ukraine_events():
    """Тест глобального экземпляра событий."""
    assert ukraine_events is not None
    assert len(ukraine_events.events) > 0

    # Можем получить критические события
    critical = ukraine_events.get_critical_events()
    assert len(critical) > 0


def test_export_summary():
    """Тест экспорта сводки."""
    manager = PoliticalEventManager()
    summary = manager.export_events_summary()

    assert 'total_events' in summary
    assert 'by_type' in summary
    assert 'by_importance' in summary
    assert 'timeline' in summary
    assert 'date_range' in summary

    assert summary['total_events'] > 0