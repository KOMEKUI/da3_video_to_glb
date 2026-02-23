from typing import Iterable

from app.application.ports import ProgressReporterPort


class CompositeProgressReporter(ProgressReporterPort):
    """複数の進捗通知先へ中継するアダプターです。"""

    def __init__(self, reporters: Iterable[ProgressReporterPort]) -> None:
        self._reporters = list(reporters)

    def report_phase(self, phase: str, message: str) -> None:
        for reporter in self._reporters:
            reporter.report_phase(phase, message)

    def report_progress(self, current: int, total: int, message: str) -> None:
        for reporter in self._reporters:
            reporter.report_progress(current, total, message)