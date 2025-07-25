"""Main client for interacting with Wayback Machine."""

import logging
from typing import List
from waybackpy import WaybackMachineCDXServerAPI

logger = logging.getLogger(__name__)


class WaybackClient:
    """Advanced Wayback Machine client."""

    def __init__(self, user_agent: str = "WaybackAnalyzer/1.0"):
        self.user_agent = user_agent

    def get_snapshots(self, url: str, limit: int = 10) -> List[str]:
        """Get archived snapshots for a URL."""
        try:
            cdx_api = WaybackMachineCDXServerAPI(url, self.user_agent)
            snapshots = []

            for snapshot in cdx_api.snapshots():
                snapshots.append(snapshot.archive_url)
                if len(snapshots) >= limit:
                    break

            return snapshots

        except Exception as e:
            logger.error(f"Failed to get snapshots for {url}: {e}")
            return []

    def test_connection(self) -> bool:
        """Test connection to Wayback Machine."""
        try:
            test_snapshots = self.get_snapshots("http://example.com", limit=1)
            return len(test_snapshots) > 0
        except:
            return False