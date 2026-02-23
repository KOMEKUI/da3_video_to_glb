from pathlib import Path

from app.application.ports import (
    FileGatewayPort,
    JobRepositoryPort,
    ObjectStoragePort,
    ProgressReporterPort,
)
from app.application.use_cases import ConvertVideoToGlbUseCase
from app.domain.job_models import VideoJob, WorkerInfo
from app.domain.models import VideoToGlbRequest


class RunSingleJobUseCase:
    """DBとストレージを使って1件のジョブを実行するユースケースです。"""

    def __init__(
        self,
        job_repository: JobRepositoryPort,
        object_storage: ObjectStoragePort,
        file_gateway: FileGatewayPort,
        convert_video_to_glb_use_case: ConvertVideoToGlbUseCase,
        progress_reporter: ProgressReporterPort,
        input_bucket: str,
        output_bucket: str,
        keep_frames_for_debug: bool = False,
    ) -> None:
        self._job_repository = job_repository
        self._object_storage = object_storage
        self._file_gateway = file_gateway
        self._convert_video_to_glb_use_case = convert_video_to_glb_use_case
        self._progress_reporter = progress_reporter
        self._input_bucket = input_bucket
        self._output_bucket = output_bucket
        self._keep_frames_for_debug = keep_frames_for_debug

    def execute(self, job: VideoJob, worker: WorkerInfo) -> None:
        """ジョブを1件実行します。"""

        work_dir = Path("work") / job.job_id
        input_dir = work_dir / "input"
        output_dir = work_dir / "output"

        self._file_gateway.ensure_dir(input_dir)
        self._file_gateway.ensure_dir(output_dir)

        local_video_path = input_dir / "source.mp4"
        attempt_info = None

        try:
            attempt_info = self._job_repository.start_job_attempt(job_id=job.job_id, worker_id=worker.worker_id)

            self._progress_reporter.report_phase("download", "入力動画をストレージから取得します。")
            self._object_storage.download_file(self._input_bucket, job.input_object_key, local_video_path)

            convert_request = VideoToGlbRequest(
                input_video_path=local_video_path,
                output_dir=output_dir,
                fps=job.fps,
                keep_frames=self._keep_frames_for_debug,
                model_id=job.model_id,
            )

            convert_result = self._convert_video_to_glb_use_case.execute(convert_request)

            if convert_result.glb_path is None:
                raise RuntimeError("GLB出力に失敗しました。出力ファイルが見つかりません。")

            glb_object_key = "{0}/result.glb".format(job.output_prefix.rstrip("/"))

            self._progress_reporter.report_phase("upload", "GLBをストレージへアップロードします。")
            self._object_storage.upload_file(
                local_path=convert_result.glb_path,
                bucket=self._output_bucket,
                key=glb_object_key,
                content_type="model/gltf-binary",
            )

            glb_size = convert_result.glb_path.stat().st_size if convert_result.glb_path.exists() else None

            self._job_repository.add_artifact(
                job_id=job.job_id,
                artifact_type="glb",
                object_key=glb_object_key,
                content_type="model/gltf-binary",
                size_bytes=glb_size,
            )

            self._job_repository.mark_job_succeeded(
                job_id=job.job_id,
                attempt_id=attempt_info.attempt_id,
            )

        except Exception as ex:
            self._job_repository.mark_job_failed(
                job_id=job.job_id,
                attempt_id=attempt_info.attempt_id if attempt_info is not None else None,
                error_code="worker_runtime_error",
                error_message=str(ex),
                exit_code=1,
            )
            raise