import json
from typing import Optional

import psycopg

from app.application.ports import JobRepositoryPort
from app.domain.job_models import JobAttemptInfo, VideoJob, WorkerInfo


class PostgresJobRepositoryAdapter(JobRepositoryPort):
    """PostgreSQLを利用するジョブリポジトリアダプターです。"""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def upsert_worker_heartbeat(
        self,
        worker_key: str,
        display_name: str,
        status: str,
        ip_address: Optional[str],
        tags_json_text: str,
        capacity_json_text: str,
    ) -> WorkerInfo:
        """gpu_workers をUPSERTし、worker情報を返します。"""

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gpu_workers (
                        worker_key,
                        display_name,
                        status,
                        last_heartbeat_at,
                        ip_address,
                        tags_json,
                        capacity_json,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, NOW(), %s, %s::jsonb, %s::jsonb, NOW(), NOW()
                    )
                    ON CONFLICT (worker_key)
                    DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        status = EXCLUDED.status,
                        last_heartbeat_at = NOW(),
                        ip_address = EXCLUDED.ip_address,
                        tags_json = EXCLUDED.tags_json,
                        capacity_json = EXCLUDED.capacity_json,
                        updated_at = NOW()
                    RETURNING id::text, worker_key, display_name
                    """,
                    (
                        worker_key,
                        display_name,
                        status,
                        ip_address,
                        tags_json_text,
                        capacity_json_text,
                    ),
                )
                row = cur.fetchone()
            conn.commit()

        return WorkerInfo(
            worker_id=str(row[0]),
            worker_key=str(row[1]),
            display_name=str(row[2]),
        )

    def fetch_next_queued_job(self, worker_key: str) -> Optional[VideoJob]:
        """queuedジョブを1件取得します（ロック付き）。"""

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        j.id::text,
                        j.input_object_key,
                        j.output_prefix,
                        COALESCE((j.params_json->>'fps')::double precision, 2.0) AS fps,
                        COALESCE(j.params_json->>'modelId', 'depth-anything/da3nested-giant-large') AS model_id
                    FROM jobs j
                    WHERE j.status = 'queued'
                    ORDER BY j.priority DESC, j.created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
            conn.commit()

        if row is None:
            return None

        return VideoJob(
            job_id=str(row[0]),
            input_object_key=str(row[1]),
            output_prefix=str(row[2]),
            fps=float(row[3]),
            model_id=str(row[4]),
        )

    def start_job_attempt(self, job_id: str, worker_id: str) -> JobAttemptInfo:
        """job_attempts開始 + jobsをrunningへ更新します。"""

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(attempt_no), 0) + 1
                    FROM job_attempts
                    WHERE job_id = %s::uuid
                    """,
                    (job_id,),
                )
                next_attempt_no = int(cur.fetchone()[0])

                cur.execute(
                    """
                    INSERT INTO job_attempts (
                        job_id,
                        attempt_no,
                        worker_id,
                        status,
                        started_at
                    )
                    VALUES (
                        %s::uuid, %s, %s::uuid, 'running', NOW()
                    )
                    RETURNING id::text, attempt_no
                    """,
                    (job_id, next_attempt_no, worker_id),
                )
                attempt_row = cur.fetchone()

                cur.execute(
                    """
                    UPDATE jobs
                    SET
                        status = 'running',
                        progress_percent = 0,
                        error_code = NULL,
                        error_message = NULL,
                        started_at = COALESCE(started_at, NOW()),
                        finished_at = NULL
                    WHERE id = %s::uuid
                    """,
                    (job_id,),
                )
            conn.commit()

        return JobAttemptInfo(
            attempt_id=str(attempt_row[0]),
            attempt_no=int(attempt_row[1]),
        )

    def update_progress(
        self,
        job_id: str,
        progress_percent: int,
    ) -> None:
        """jobs.progress_percent を更新します。"""

        safe_percent = progress_percent
        if safe_percent < 0:
            safe_percent = 0
        if safe_percent > 100:
            safe_percent = 100

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE jobs
                    SET progress_percent = %s
                    WHERE id = %s::uuid
                    """,
                    (safe_percent, job_id),
                )
            conn.commit()

    def add_job_log(
        self,
        job_id: str,
        attempt_id: Optional[str],
        level: str,
        message: Optional[str],
        object_key: Optional[str],
    ) -> None:
        """job_logs を追加します。"""

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO job_logs (
                        job_id,
                        attempt_id,
                        level,
                        message,
                        object_key,
                        created_at
                    )
                    VALUES (
                        %s::uuid,
                        %s::uuid,
                        %s,
                        %s,
                        %s,
                        NOW()
                    )
                    """,
                    (job_id, attempt_id, level, message, object_key),
                )
            conn.commit()

    def add_artifact(
        self,
        job_id: str,
        artifact_type: str,
        object_key: str,
        content_type: Optional[str],
        size_bytes: Optional[int],
    ) -> None:
        """artifacts を追加します。"""

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO artifacts (
                        job_id,
                        type,
                        object_key,
                        content_type,
                        size_bytes,
                        created_at
                    )
                    VALUES (
                        %s::uuid,
                        %s,
                        %s,
                        %s,
                        %s,
                        NOW()
                    )
                    """,
                    (job_id, artifact_type, object_key, content_type, size_bytes),
                )
            conn.commit()

    def mark_job_succeeded(
        self,
        job_id: str,
        attempt_id: str,
    ) -> None:
        """jobs と job_attempts を成功状態へ更新します。"""

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE job_attempts
                    SET
                        status = 'succeeded',
                        finished_at = NOW(),
                        exit_code = 0,
                        error_message = NULL
                    WHERE id = %s::uuid
                    """,
                    (attempt_id,),
                )

                cur.execute(
                    """
                    UPDATE jobs
                    SET
                        status = 'succeeded',
                        progress_percent = 100,
                        error_code = NULL,
                        error_message = NULL,
                        finished_at = NOW()
                    WHERE id = %s::uuid
                    """,
                    (job_id,),
                )
            conn.commit()

    def mark_job_failed(
        self,
        job_id: str,
        attempt_id: Optional[str],
        error_code: Optional[str],
        error_message: str,
        exit_code: Optional[int],
    ) -> None:
        """jobs と job_attempts を失敗状態へ更新します。"""

        truncated_message = error_message[:4000]

        with psycopg.connect(self._dsn) as conn:
            with conn.cursor() as cur:
                if attempt_id is not None:
                    cur.execute(
                        """
                        UPDATE job_attempts
                        SET
                            status = 'failed',
                            finished_at = NOW(),
                            exit_code = %s,
                            error_message = %s
                        WHERE id = %s::uuid
                        """,
                        (exit_code, truncated_message, attempt_id),
                    )

                cur.execute(
                    """
                    UPDATE jobs
                    SET
                        status = 'failed',
                        error_code = %s,
                        error_message = %s,
                        finished_at = NOW()
                    WHERE id = %s::uuid
                    """,
                    (error_code, truncated_message, job_id),
                )
            conn.commit()