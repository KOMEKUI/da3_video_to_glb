from pathlib import Path
from typing import Sequence, Optional

from app.application.ports import Da3InferencePort, ProgressReporterPort
from app.domain.models import GlbExportResult


class Da3PyTorchInferenceAdapter(Da3InferencePort):
    """Depth Anything 3 (PyTorch) を用いて画像列からGLBを出力するアダプターです。"""

    def export_glb_from_images(
        self,
        image_paths: Sequence[Path],
        output_dir: Path,
        model_id: str,
        progress_reporter: ProgressReporterPort,
    ) -> GlbExportResult:
        """DA3のPyTorch APIを使ってGLBを書き出します。"""

        progress_reporter.report_phase("load_model", f"モデルを読み込みます: {model_id}")

        import torch
        from depth_anything_3.api import DepthAnything3

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        model = DepthAnything3.from_pretrained(model_id)
        model = model.to(device=device)

        progress_reporter.report_phase(
            "infer",
            f"推論実行中（画像列をまとめて処理）: {len(image_paths)} 枚",
        )
        progress_reporter.report_progress(0, len(image_paths), "DA3推論開始")

        # DA3のREADME例に合わせて画像パス配列をそのまま渡す
        # export_format="glb" を指定すると output_dir にGLBが出力される
        prediction = model.inference(
            [str(p) for p in image_paths],
            export_dir=str(output_dir),
            export_format="glb",
        )

        progress_reporter.report_progress(len(image_paths), len(image_paths), "DA3推論完了・GLB出力完了")

        glb_path = self._find_exported_glb(output_dir)
        return GlbExportResult(
            output_dir=output_dir,
            glb_path=glb_path,
            frame_count=len(image_paths),
        )

    def _find_exported_glb(self, output_dir: Path) -> Optional[Path]:
        """出力されたGLBを探索します。"""

        glb_files = sorted(output_dir.glob("*.glb"), key=lambda x: x.stat().st_mtime, reverse=True)
        if len(glb_files) == 0:
            return None

        return glb_files[0]