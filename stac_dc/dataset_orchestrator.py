import threading
import time
import logging

from datetime import datetime, timezone, timedelta

from .dataset_worker import DatasetWorker

from env import env

RETRY_DELAYS = [600, 1800, 3600, 7200, 14400, 28800]


class DatasetOrchestrator:
    def __init__(
            self,
            worker: DatasetWorker,
            max_retries: int = 5,
            logger=logging.getLogger(env.get_app__name())
    ):
        self._worker: DatasetWorker = worker
        self._max_retries: int = max_retries
        self._logger: logging.Logger = logger
        self._lock: threading.Lock = threading.Lock()

    def get_worker(self) -> DatasetWorker:
        return self._worker

    def execute(self, **kwargs) -> None:
        old_thread_name = threading.current_thread().name
        threading.current_thread().name = (
            f"{self.get_worker().__class__.__name__}_{self.get_worker().get_aoi().__name__} ({old_thread_name})"
        )

        try:
            self._run_scheduled(**kwargs)

        finally:
            threading.current_thread().name = old_thread_name

    def _run_scheduled(self, hour: int = 9, minute: int = 0, **kwargs) -> None:
        while True:
            if self._lock.acquire(blocking=False):
                try:
                    self._run_once(**kwargs)
                finally:
                    self._lock.release()

            else:
                self._logger.warning(f"Worker is already running, skipping this scheduled run.")

            now = datetime.now(timezone.utc)

            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)

            delay = (next_run - now).total_seconds()
            self._logger.info(f"Scheduled run at {next_run} {next_run.tzinfo}")
            time.sleep(delay)

    def _run_once(self, **kwargs):
        self._worker.reset_run_attempt()

        while self._worker.get_run_attempt() < self._max_retries:
            try:
                self._worker.get_run_attempt()
                self._logger.info(f"Starting attempt #{self._worker.get_run_attempt()}")
                self._worker.run(**kwargs)
                self._logger.info(f"Finished successfully")

                return

            except Exception as e:
                self._logger.error(f"Error on attempt #{self._worker.get_run_attempt()}: {e}", exc_info=True)

                if self._worker.get_run_attempt() >= self._max_retries:
                    self._report_error(attempt=self._worker.get_run_attempt(), exiting=True)
                    return

                else:
                    delay = self._get_retry_delay(self._worker.get_run_attempt())
                    self._report_error(attempt=self._worker.get_run_attempt(), delay=delay, exiting=False)
                    time.sleep(delay)

    @staticmethod
    def _get_retry_delay(attempt: int):
        retry_delays = RETRY_DELAYS
        return retry_delays[min(attempt - 1, len(retry_delays) - 1)]

    def _report_error(self, attempt, delay: int = -1, exiting: bool = False):
        if exiting:
            self._logger.error(f"Error occurred! Reached max retries ({attempt}), stopping")
            # todo: send email/slack...

        else:
            self._logger.warning(f"Error occurred! Attempt #{attempt}, will retry in {delay // 60} minutes")
            # todo: send email/slack...
