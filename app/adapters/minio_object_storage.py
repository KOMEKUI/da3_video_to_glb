from pathlib import Path
from typing import Optional

from minio import Minio

from app.application.ports import ObjectStoragePort


class MinioObjectStorageAdapter(ObjectStoragePort):
    """MinIOを利用するオブジェクトストレージアダプターです。"""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool,
    ) -> None:
        self._client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def download_file(self, bucket: str, key: str, local_path: Path) -> None:
        """オブジェクトをローカルへダウンロードします。"""

        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._client.fget_object(bucket_name=bucket, object_name=key, file_path=str(local_path))

    def upload_file(
        self,
        local_path: Path,
        bucket: str,
        key: str,
        content_type: Optional[str] = None,
    ) -> None:
        """ローカルファイルをアップロードします。"""

        self._ensure_bucket_exists(bucket)

        if content_type is None:
            self._client.fput_object(
                bucket_name=bucket,
                object_name=key,
                file_path=str(local_path),
            )
            return

        self._client.fput_object(
            bucket_name=bucket,
            object_name=key,
            file_path=str(local_path),
            content_type=content_type,
        )

    def _ensure_bucket_exists(self, bucket: str) -> None:
        """バケットの存在を確認し、なければ作成します。"""

        exists = self._client.bucket_exists(bucket)
        if exists == False:
            self._client.make_bucket(bucket)