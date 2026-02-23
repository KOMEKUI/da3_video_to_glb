from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class VideoJob:
    """動画変換ジョブを表します。"""

    job_id: str
    input_object_key: str
    output_prefix: str
    fps: float
    model_id: str


@dataclass(frozen=True)
class WorkerInfo:
    """GPUワーカー情報を表します。"""

    worker_id: str
    worker_key: str
    display_name: str


@dataclass(frozen=True)
class JobAttemptInfo:
    """ジョブ試行情報を表します。"""

    attempt_id: str
    attempt_no: int


@dataclass(frozen=True)
class UploadedJobResult:
    """ジョブ出力のアップロード結果を表します。"""

    glb_object_key: str
    frame_count: int
    log_object_key: Optional[str]