import shutil
from pathlib import Path
from typing import Sequence, Optional

from app.application.ports import FileGatewayPort


class LocalFileGateway(FileGatewayPort):
    """ローカルファイル操作のアダプターです。"""

    def ensure_dir(self, path: Path) -> None:
        """ディレクトリを作成します。"""

        path.mkdir(parents=True, exist_ok=True)

    def list_frame_images(self, frames_dir: Path) -> Sequence[Path]:
        """フレーム画像一覧を取得します。"""

        patterns = ("*.png", "*.jpg", "*.jpeg", "*.webp")
        images: list[Path] = []

        for pattern in patterns:
            images.extend(frames_dir.glob(pattern))

        return sorted(images)

    def find_latest_glb(self, output_dir: Path) -> Optional[Path]:
        """GLBファイルを新しい順に1件返します。"""

        glb_files = sorted(output_dir.glob("*.glb"), key=lambda x: x.stat().st_mtime, reverse=True)
        if len(glb_files) == 0:
            return None

        return glb_files[0]

    def remove_dir(self, path: Path) -> None:
        """ディレクトリを削除します。"""

        if path.exists():
            shutil.rmtree(path)