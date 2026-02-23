from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class VideoToGlbRequest:
    """動画からGLBを生成する要求を表します。"""

    input_video_path: Path
    output_dir: Path
    fps: float = 2.0
    keep_frames: bool = True
    model_id: str = "depth-anything/da3nested-giant-large"


@dataclass(frozen=True)
class FrameExtractionResult:
    """フレーム抽出結果を表します。"""

    frames_dir: Path
    frame_count: int


@dataclass(frozen=True)
class GlbExportResult:
    """GLB出力結果を表します。"""

    output_dir: Path
    glb_path: Optional[Path]
    frame_count: int