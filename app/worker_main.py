import json
import os
import socket
import threading
import time
from typing import Optional

from app.adapters.composite_progress_reporter import CompositeProgressReporter
from app.adapters.console_progress_reporter import ConsoleProgressReporter
from app.adapters.da3_pytorch_inference import Da3PyTorchInferenceAdapter
from app.adapters.db_progress_reporter import DbProgressReporter
from app.adapters.ffmpeg_frame_extractor import FfmpegFrameExtractor
from app.adapters.local_file_gateway import LocalFileGateway
from app.adapters.minio_object_storage import MinioObjectStorageAdapter
from app.adapters.postgres_job_repository import PostgresJobRepositoryAdapter
from app.application.job_runner_use_cases import RunSingleJobUseCase
from app.application.use_cases import ConvertVideoToGlbUseCase


class WorkerState:
    """ワーカーの共有状態を保持します。"""

    def __init__(self, initial_status: str) -> None:
        self._lock = threading.Lock()
        self._status = initial_status
        self._current_job_id = None

    def set_status(self, status: str, current_job_id: str = None) -> None:
        with self._lock:
            self._status = status
            self._current_job_id = current_job_id

    def get_snapshot(self):
        with self._lock:
            return self._status, self._current_job_id


def main() -> int:
    """ワーカーのエントリポイントです。"""

    postgres_dsn = _get_required_env("POSTGRES_DSN")
    minio_endpoint = _get_required_env("MINIO_ENDPOINT")
    minio_access_key = _get_required_env("MINIO_ACCESS_KEY")
    minio_secret_key = _get_required_env("MINIO_SECRET_KEY")
    minio_secure = _get_env_bool("MINIO_SECURE", False)

    input_bucket = _get_required_env("JOB_INPUT_BUCKET")
    output_bucket = _get_required_env("JOB_OUTPUT_BUCKET")

    worker_key = os.getenv("WORKER_KEY")
    if worker_key is None or worker_key.strip() == "":
        worker_key = socket.gethostname()

    worker_display_name = os.getenv("WORKER_DISPLAY_NAME", worker_key)
    worker_ip = os.getenv("WORKER_IP_ADDRESS")

    tags_json_text = os.getenv("WORKER_TAGS_JSON", "{}")
    capacity_json_text = os.getenv("WORKER_CAPACITY_JSON", "{}")

    # JSON妥当性チェック
    json.loads(tags_json_text)
    json.loads(capacity_json_text)

    idle_sleep_sec = _get_env_float("IDLE_SLEEP_SEC", 2.0)
    heartbeat_interval_sec = 2.0  # 要件固定
    keep_frames_for_debug = _get_env_bool("KEEP_FRAMES_FOR_DEBUG", False)

    job_repository = PostgresJobRepositoryAdapter(dsn=postgres_dsn)
    object_storage = MinioObjectStorageAdapter(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure,
    )
    file_gateway = LocalFileGateway()

    state = WorkerState(initial_status="online")
    stop_event = threading.Event()

    heartbeat_thread = threading.Thread(
        target=_heartbeat_loop,
        args=(
            stop_event,
            job_repository,
            worker_key,
            worker_display_name,
            worker_ip,
            tags_json_text,
            capacity_json_text,
            state,
            heartbeat_interval_sec,
        ),
        daemon=True,
    )
    heartbeat_thread.start()

    try:
        while True:
            worker_status, _ = state.get_snapshot()
            if worker_status != "online":
                state.set_status("online", None)

            # 待機中にも fetch を繰り返す
            job = job_repository.fetch_next_queued_job(worker_key=worker_key)
            if job is None:
                time.sleep(idle_sleep_sec)
                continue

            # 実行中は「忙しい」扱いとして draining にする
            state.set_status("draining", job.job_id)

            # 実行中ジョブごとに ProgressReporter を作る（job_id が必要なため）
            console_reporter = ConsoleProgressReporter()
            db_reporter = DbProgressReporter(
                job_repository=job_repository,
                job_id=job.job_id,
                min_interval_sec=2.0,
            )
            progress_reporter = CompositeProgressReporter([console_reporter, db_reporter])

            convert_use_case = ConvertVideoToGlbUseCase(
                frame_extractor=FfmpegFrameExtractor(),
                file_gateway=file_gateway,
                da3_inference=Da3PyTorchInferenceAdapter(),
                progress_reporter=progress_reporter,
            )

            run_job_use_case = RunSingleJobUseCase(
                job_repository=job_repository,
                object_storage=object_storage,
                file_gateway=file_gateway,
                convert_video_to_glb_use_case=convert_use_case,
                progress_reporter=progress_reporter,
                input_bucket=input_bucket,
                output_bucket=output_bucket,
                keep_frames_for_debug=keep_frames_for_debug,
            )

            try:
                worker = job_repository.upsert_worker_heartbeat(
                    worker_key=worker_key,
                    display_name=worker_display_name,
                    status="draining",
                    ip_address=worker_ip,
                    tags_json_text=tags_json_text,
                    capacity_json_text=capacity_json_text,
                )

                print("[INFO] start job_id={0} worker_key={1}".format(job.job_id, worker.worker_key))
                run_job_use_case.execute(job=job, worker=worker)
                print("[INFO] completed job_id={0}".format(job.job_id))

            except Exception as ex:
                print("[ERROR] job failed job_id={0} error={1}".format(job.job_id, ex))

            finally:
                # ジョブ終了後は待機状態に戻す
                state.set_status("online", None)

    except KeyboardInterrupt:
        print("[INFO] stopping worker...")

    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=3.0)

        # 終了時に offline 更新したい場合（任意）
        try:
            job_repository.upsert_worker_heartbeat(
                worker_key=worker_key,
                display_name=worker_display_name,
                status="offline",
                ip_address=worker_ip,
                tags_json_text=tags_json_text,
                capacity_json_text=capacity_json_text,
            )
        except Exception as ex:
            print("[WARN] failed to set offline heartbeat: {0}".format(ex))

    return 0


def _heartbeat_loop(
    stop_event: threading.Event,
    job_repository,
    worker_key: str,
    worker_display_name: str,
    worker_ip: Optional[str],
    tags_json_text: str,
    capacity_json_text: str,
    state: WorkerState,
    interval_sec: float,
) -> None:
    """2秒ごとに gpu_workers を更新します。"""

    while stop_event.is_set() == False:
        try:
            status, current_job_id = state.get_snapshot()

            # current_job_id は DB列に無いので display_name に軽く反映してもよい
            # （不要なら worker_display_name のままでOK）
            if current_job_id is None:
                display_name = worker_display_name
            else:
                display_name = "{0} (busy)".format(worker_display_name)

            job_repository.upsert_worker_heartbeat(
                worker_key=worker_key,
                display_name=display_name,
                status=status,
                ip_address=worker_ip,
                tags_json_text=tags_json_text,
                capacity_json_text=capacity_json_text,
            )
        except Exception as ex:
            print("[WARN] heartbeat failed: {0}".format(ex))

        stop_event.wait(interval_sec)


def _get_required_env(name: str) -> str:
    """必須環境変数を取得します。"""

    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise RuntimeError("environment variable is required: {0}".format(name))

    return value


def _get_env_bool(name: str, default_value: bool) -> bool:
    """真偽値環境変数を取得します。"""

    value = os.getenv(name)
    if value is None:
        return default_value

    normalized = value.strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True

    if normalized in ("0", "false", "no", "off"):
        return False

    raise RuntimeError("invalid bool environment variable: {0}={1}".format(name, value))


def _get_env_float(name: str, default_value: float) -> float:
    """浮動小数の環境変数を取得します。"""

    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default_value

    return float(value)


if __name__ == "__main__":
    raise SystemExit(main())