from pathlib import Path

from app.application.ports import (
    Da3InferencePort,
    FileGatewayPort,
    FrameExtractorPort,
    ProgressReporterPort,
)
from app.domain.models import GlbExportResult, VideoToGlbRequest


class ConvertVideoToGlbUseCase:
    """動画をDA3でGLBへ変換するユースケースです。"""

    def __init__(
        self,
        frame_extractor: FrameExtractorPort,
        file_gateway: FileGatewayPort,
        da3_inference: Da3InferencePort,
        progress_reporter: ProgressReporterPort,
    ) -> None:
        self._frame_extractor = frame_extractor
        self._file_gateway = file_gateway
        self._da3_inference = da3_inference
        self._progress_reporter = progress_reporter

    def execute(self, request: VideoToGlbRequest) -> GlbExportResult:
        """動画からGLBを生成します。"""

        self._validate_request(request)

        self._progress_reporter.report_phase("prepare", "出力ディレクトリを準備します。")
        self._file_gateway.ensure_dir(request.output_dir)

        frames_dir = request.output_dir / "frames"
        self._file_gateway.ensure_dir(frames_dir)

        extraction = self._frame_extractor.extract_frames(
            input_video_path=request.input_video_path,
            frames_dir=frames_dir,
            fps=request.fps,
            progress_reporter=self._progress_reporter,
        )

        image_paths = self._file_gateway.list_frame_images(extraction.frames_dir)
        if len(image_paths) == 0:
            raise RuntimeError("フレーム抽出結果が0件でした。ffmpegの設定や入力動画を確認してください。")

        self._progress_reporter.report_phase(
            "infer",
            f"DA3推論を開始します。frames={len(image_paths)} model={request.model_id}",
        )

        result = self._da3_inference.export_glb_from_images(
            image_paths=image_paths,
            output_dir=request.output_dir,
            model_id=request.model_id,
            progress_reporter=self._progress_reporter,
        )

        if request.keep_frames == False:
            try:
                self._progress_reporter.report_phase("cleanup", "中間フレームを削除します。")
                self._file_gateway.remove_dir(extraction.frames_dir)
            except Exception as ex:
                # cleanup失敗は非致命として扱う
                self._progress_reporter.report_phase("cleanup_warn", "中間フレーム削除をスキップします: {0}".format(ex))

        self._progress_reporter.report_phase("done", "GLB変換が完了しました。")
        return result

    def _validate_request(self, request: VideoToGlbRequest) -> None:
        """入力値を検証します。"""

        if request.input_video_path.exists() == False:
            raise FileNotFoundError(f"入力動画が見つかりません: {request.input_video_path}")

        if request.fps <= 0:
            raise ValueError("fps は 0 より大きい値を指定してください。")