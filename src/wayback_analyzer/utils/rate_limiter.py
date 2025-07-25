"""Rate limiter для контроля скорости запросов к архиву."""

import time
import logging
from typing import Optional


class RateLimiter:
    """Контролирует скорость запросов к Wayback Machine."""

    def __init__(self, requests_per_second: float = 0.5, burst_limit: int = 3):
        """
        Args:
            requests_per_second: Максимальное количество запросов в секунду
            burst_limit: Максимальное количество запросов в burst режиме
        """
        self.delay = 1.0 / requests_per_second
        self.burst_limit = burst_limit
        self.last_request_time = 0.0
        self.burst_count = 0
        self.burst_start_time = 0.0

        self.logger = logging.getLogger(__name__)

    def wait_if_needed(self) -> None:
        """Ожидать если необходимо соблюсти rate limit."""
        current_time = time.time()

        # Сброс burst счетчика если прошло много времени
        if current_time - self.burst_start_time > 10.0:
            self.burst_count = 0
            self.burst_start_time = current_time

        # Проверка burst лимита
        if self.burst_count >= self.burst_limit:
            wait_time = self.delay * 2  # Двойная задержка при превышении burst
            self.logger.debug(f"Burst limit reached, waiting {wait_time:.2f}s")
            time.sleep(wait_time)
            self.burst_count = 0
            self.burst_start_time = current_time

        # Обычная задержка
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.delay:
            wait_time = self.delay - time_since_last
            self.logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)

        self.last_request_time = time.time()
        self.burst_count += 1

    def reset(self) -> None:
        """Сбросить счетчики rate limiter."""
        self.last_request_time = 0.0
        self.burst_count = 0
        self.burst_start_time = 0.0