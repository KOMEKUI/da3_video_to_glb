from pathlib import Path
from typing import Optional, Protocol, Sequence

from app.domain.job_models import JobAttemptInfo, VideoJob, WorkerInfo
from app.domain.models import FrameExtractionResult, GlbExportResult


class ProgressReporterPort(Protocol):
    """進捗通知のポートです。"""

    def report_phase(self, phase: str, message: str) -> None:
        """フェーズ開始・更新を通知します。"""

    def report_progress(self, current: int, total: int, message: str) -> None:
        """進捗率を通知します。"""


class FrameExtractorPort(Protocol):
    """動画をフレーム列へ変換するポートです。"""

    def extract_frames(
        self,
        input_video_path: Path,
        frames_dir: Path,
        fps: float,
        progress_reporter: ProgressReporterPort,
    ) -> FrameExtractionResult:
        """動画からフレームを抽出します。"""


class FileGatewayPort(Protocol):
    """ローカルファイル操作のポートです。"""

    def ensure_dir(self, path: Path) -> None:
        """ディレクトリを作成します。"""

    def list_frame_images(self, frames_dir: Path) -> Sequence[Path]:
        """フレーム画像一覧を取得します。"""

    def find_latest_glb(self, output_dir: Path) -> Optional[Path]:
        """出力ディレクトリからGLBを1件特定します。"""

    def remove_dir(self, path: Path) -> None:
        """ディレクトリを削除します。"""


class Da3InferencePort(Protocol):
    """DA3推論のポートです。"""

    def export_glb_from_images(
        self,
        image_paths: Sequence[Path],
        output_dir: Path,
        model_id: str,
        progress_reporter: ProgressReporterPort,
    ) -> GlbExportResult:
        """画像列からGLBを出力します。"""


class JobRepositoryPort(Protocol):
    """ジョブ永続化のポートです。"""

    def upsert_worker_heartbeat(
        self,
        worker_key: str,
        display_name: str,
        status: str,
        ip_address: Optional[str],
        tags_json_text: str,
        capacity_json_text: str,
    ) -> WorkerInfo:
        """ワーカー情報を登録または更新し、worker情報を返します。"""

    def fetch_next_queued_job(self, worker_key: str) -> Optional[VideoJob]:
        """実行対象ジョブを1件取得します。"""

    def start_job_attempt(self, job_id: str, worker_id: str) -> JobAttemptInfo:
        """job_attempts を開始し、jobs を running に更新します。"""

    def update_progress(
        self,
        job_id: str,
        progress_percent: int,
    ) -> None:
        """jobs.progress_percent を更新します。"""

    def add_job_log(
        self,
        job_id: str,
        attempt_id: Optional[str],
        level: str,
        message: Optional[str],
        object_key: Optional[str],
    ) -> None:
        """job_logs を追加します。"""

    def add_artifact(
        self,
        job_id: str,
        artifact_type: str,
        object_key: str,
        content_type: Optional[str],
        size_bytes: Optional[int],
    ) -> None:
        """artifacts を追加します。"""

    def mark_job_succeeded(
        self,
        job_id: str,
        attempt_id: str,
    ) -> None:
        """jobs と job_attempts を成功状態に更新します。"""

    def mark_job_failed(
        self,
        job_id: str,
        attempt_id: Optional[str],
        error_code: Optional[str],
        error_message: str,
        exit_code: Optional[int],
    ) -> None:
        """jobs と job_attempts を失敗状態に更新します。"""


class ObjectStoragePort(Protocol):
    """オブジェクトストレージのポートです。"""

    def download_file(self, bucket: str, key: str, local_path: Path) -> None:
        """オブジェクトをローカルへダウンロードします。"""

    def upload_file(
        self,
        local_path: Path,
        bucket: str,
        key: str,
        content_type: Optional[str] = None,
    ) -> None:
        """ローカルファイルをアップロードします。"""