"""Basic tests for WaybackClient."""

import pytest
from wayback_analyzer.core.client import WaybackClient


def test_wayback_client_creation():
    """Test WaybackClient can be created."""
    client = WaybackClient()
    assert client.user_agent == "WaybackAnalyzer/1.0"


def test_wayback_client_custom_user_agent():
    """Test WaybackClient with custom user agent."""
    client = WaybackClient(user_agent="TestBot/1.0")
    assert client.user_agent == "TestBot/1.0"