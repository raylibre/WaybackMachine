# src/wayback_analyzer/core/snapshot_finder.py
from datetime import date, timedelta
from typing import List, Optional, Dict
from waybackpy import WaybackMachineCDXServerAPI
from ..models.political_events import PoliticalEvent
from ..utils.date_matcher import find_closest_snapshots

class PoliticalSnapshotFinder:
    def __init__(self, user_agent: str = "UkrainePoliticalAnalyzer/1.0"):
        self.user_agent = user_agent

    def find_event_snapshots(
            self,
            site_url: str,
            event: PoliticalEvent,
            days_before: int = 30,
            days_after: int = 30
    ) -> Dict[str, List]:
        """Найти снапшоты до и после политического события."""

        # Определяем временные окна
        start_date = event.date - timedelta(days=days_before)
        end_date = event.date + timedelta(days=days_after)

        # Ищем снапшоты в указанном диапазоне
        cdx_api = WaybackMachineCDXServerAPI(site_url, self.user_agent)

        snapshots = {
            'before_event': [],
            'after_event': [],
            'event_metadata': {
                'event_name': event.name,
                'event_date': event.date.isoformat(),
                'event_type': event.event_type.value
            }
        }

        try:
            for snapshot in cdx_api.snapshots():
                snapshot_date = datetime.strptime(snapshot.timestamp, "%Y%m%d%H%M%S").date()

                if start_date <= snapshot_date <= event.date:
                    snapshots['before_event'].append({
                        'url': snapshot.archive_url,
                        'timestamp': snapshot.timestamp,
                        'date': snapshot_date.isoformat(),
                        'original_url': snapshot.original,
                        'status_code': snapshot.statuscode,
                        'days_to_event': (event.date - snapshot_date).days
                    })
                elif event.date < snapshot_date <= end_date:
                    snapshots['after_event'].append({
                        'url': snapshot.archive_url,
                        'timestamp': snapshot.timestamp,
                        'date': snapshot_date.isoformat(),
                        'original_url': snapshot.original,
                        'status_code': snapshot.statuscode,
                        'days_from_event': (snapshot_date - event.date).days
                    })

        except Exception as e:
            print(f"Ошибка при поиске снапшотов: {e}")

        # Сортируем по близости к событию
        snapshots['before_event'].sort(key=lambda x: x['days_to_event'])
        snapshots['after_event'].sort(key=lambda x: x['days_from_event'])

        return snapshots