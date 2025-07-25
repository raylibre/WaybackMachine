"""Модели данных для Wayback Analyzer."""

from .political_events import (
    PoliticalEvent,
    EventType,
    EventImportance,
    EventFilter,
    PoliticalEventManager,
    UKRAINE_POLITICAL_EVENTS,
    ukraine_events
)

__all__ = [
    "PoliticalEvent",
    "EventType",
    "EventImportance",
    "EventFilter",
    "PoliticalEventManager",
    "UKRAINE_POLITICAL_EVENTS",
    "ukraine_events"
]