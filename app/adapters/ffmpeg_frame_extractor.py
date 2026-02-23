import shutil
import subprocess
from pathlib import Path

from app.application.ports import FrameExtractorPort, ProgressReporterPort
from app.domain.models import FrameExtractionResult


class FfmpegFrameExtractor(FrameExtractorPort):
    """ffmpegを使って動画をフレーム画像へ変換するアダプターです。"""

    def extract_frames(
        self,
        input_video_path: Path,
        frames_dir: Path,
        fps: float,
        progress_reporter: ProgressReporterPort,
    ) -> FrameExtractionResult:
        """ffmpegでフレーム抽出を実行します。"""

        progress_reporter.report_phase(
            "extract_frames",
            f"ffmpegでフレーム抽出します。fps={fps}",
        )

        ffmpeg_path = shutil.which("ffmpeg")
        if ffmpeg_path is None:
            raise RuntimeError(
                "ffmpeg コマンドが見つかりません。"
                " ffmpeg をインストールして PATH を通すか、"
                "コード内で ffmpeg.exe のフルパスを指定してください。"
            )

        output_pattern = frames_dir / "frame_%06d.png"
        command = [
            ffmpeg_path,
            "-y",
            "-i",
            str(input_video_path),
            "-vf",
            f"fps={fps}",
            str(output_pattern),
        ]

        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
        )

        if completed.returncode != 0:
            raise RuntimeError(
                "ffmpeg によるフレーム抽出に失敗しました。\n"
                f"stdout:\n{completed.stdout}\n\nstderr:\n{completed.stderr}"
            )

        frame_count = len(sorted(frames_dir.glob("*.png")))
        progress_reporter.report_progress(
            current=frame_count,
            total=frame_count if frame_count > 0 else 1,
            message=f"フレーム抽出完了: {frame_count} 枚",
        )

        return FrameExtractionResult(
            frames_dir=frames_dir,
            frame_count=frame_count,
        )