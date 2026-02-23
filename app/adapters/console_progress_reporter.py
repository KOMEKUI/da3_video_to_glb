from app.application.ports import ProgressReporterPort


class ConsoleProgressReporter(ProgressReporterPort):
    """コンソール出力で進捗表示するアダプターです。"""

    def report_phase(self, phase: str, message: str) -> None:
        """フェーズ情報を出力します。"""

        print(f"[PHASE] {phase}: {message}")

    def report_progress(self, current: int, total: int, message: str) -> None:
        """進捗を出力します。"""

        safe_total = total if total > 0 else 1
        percent = (current / safe_total) * 100.0
        print(f"[PROGRESS] {percent:.1f}% ({current}/{safe_total}) {message}")