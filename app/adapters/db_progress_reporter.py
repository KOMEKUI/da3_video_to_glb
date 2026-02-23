import time

from app.application.ports import JobRepositoryPort, ProgressReporterPort


class DbProgressReporter(ProgressReporterPort):
    """DBへ jobs.progress_percent を反映するアダプターです。"""

    def __init__(self, job_repository: JobRepositoryPort, job_id: str, min_interval_sec: float = 2.0) -> None:
        self._job_repository = job_repository
        self._job_id = job_id
        self._min_interval_sec = min_interval_sec
        self._last_update_at = 0.0

    def report_phase(self, phase: str, message: str) -> None:
        """フェーズ通知時は進捗更新しません（ログは別途job_logsで扱う）。"""

    def report_progress(self, current: int, total: int, message: str) -> None:
        safe_total = total if total > 0 else 1
        percent = int((float(current) / float(safe_total)) * 100.0)

        now = time.time()
        if (now - self._last_update_at) < self._min_interval_sec and current < safe_total:
            return

        self._job_repository.update_progress(
            job_id=self._job_id,
            progress_percent=percent,
        )
        self._last_update_at = now