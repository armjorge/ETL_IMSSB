from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pandas as pd

from modules.config import ConfigManager
from modules.datasets import find_dataset, normalize_datasets, to_source_entry, validate_prefix
from modules.helpers import HELPERS
from modules.s3_client import S3Client
from modules.source_validation import validate_source


@dataclass
class ExtractResult:
    source: str
    ok: bool
    message: str
    local_path: str | None = None
    s3_uri: str | None = None
    rows: int = 0


@dataclass
class ExtractReport:
    results: list[ExtractResult] = field(default_factory=list)

    @property
    def all_ok(self) -> bool:
        return bool(self.results) and all(r.ok for r in self.results)


def _clean_text(value) -> str:
    """Minimal cleanup: stringify and strip quote characters that break CSVs."""
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value)
    if text.lower() == "nan":
        return ""
    return text.replace('"', "").replace("'", "")


def clean_dataframe_as_text(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [_clean_text(c) for c in cleaned.columns]
    for col in cleaned.columns:
        cleaned[col] = cleaned[col].map(_clean_text)
    return cleaned


class XlsxExtractor:
    """Take clean pipe-separated snapshots from configured dataset schemas."""

    def __init__(self, config: dict | None = None, project_root: str | Path | None = None):
        self.config_manager = ConfigManager(project_root)
        self.config = config if config is not None else self.config_manager.load()
        self.helpers = HELPERS()
        s3_cfg = self.config_manager.get_s3_config(self.config)
        self.s3 = S3Client(
            bucket=s3_cfg["bucket"],
            root_prefix=s3_cfg["root_prefix"],
            region=s3_cfg["region"],
        )
        self.main_path = self.config_manager.resolve_main_path(self.config)
        self.project_root = self.config_manager.project_root

    @staticmethod
    def _timestamp() -> str:
        # dd-mm-yyyy hh mm  (spaces, as requested)
        return datetime.now().strftime("%d-%m-%Y %H %M")

    def _dataset_output_path(self, prefix: str) -> Path:
        filename = f"{prefix} {self._timestamp()}.csv"
        out_dir = self.main_path / "imssb_files" / prefix
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / filename

    def _write_clean_pipe_csv(self, df: pd.DataFrame, out_path: Path) -> Path:
        cleaned = clean_dataframe_as_text(df)
        cleaned.to_csv(
            out_path,
            sep="|",
            index=False,
            encoding="utf-8",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            lineterminator="\n",
        )
        return out_path

    def snapshot_dataset(self, dataset_id: str, upload: bool = True) -> ExtractResult:
        datasets = normalize_datasets(self.config.get("datasets"))
        dataset = find_dataset(datasets, dataset_id)
        if not dataset:
            return ExtractResult(
                source=dataset_id,
                ok=False,
                message=f"Dataset `{dataset_id}` not found in config",
            )

        dataset_name = dataset.get("dataset_name") or dataset_id
        prefix = (dataset.get("prefix") or "").strip()
        prefix_ok, prefix_msg = validate_prefix(prefix)
        if not prefix_ok:
            return ExtractResult(source=dataset_name, ok=False, message=prefix_msg)

        entry = to_source_entry(dataset)
        resolved = ConfigManager.resolve_existing_path(entry.get("file_path"), self.project_root)
        if resolved:
            entry["file_path"] = str(resolved)

        validation = validate_source(dataset_name, entry, self.project_root)
        if not validation.ok:
            return ExtractResult(
                source=dataset_name,
                ok=False,
                message=validation.message,
            )

        try:
            df = self.helpers.load_and_concat({dataset_id: entry})
            if df is None or df.empty:
                return ExtractResult(
                    source=dataset_name,
                    ok=False,
                    message="Snapshot produced an empty result",
                )

            local_path = self._write_clean_pipe_csv(df, self._dataset_output_path(prefix))
            s3_uri = None
            if upload:
                if not self.s3.bucket_exists():
                    return ExtractResult(
                        source=dataset_name,
                        ok=False,
                        message=(
                            f"CSV saved locally but S3 bucket '{self.s3.bucket}' "
                            "does not exist"
                        ),
                        local_path=str(local_path),
                        rows=len(df),
                    )
                s3_uri = self.s3.upload_file(local_path, prefix)

            return ExtractResult(
                source=dataset_name,
                ok=True,
                message="Snapshot saved" + (" and uploaded" if s3_uri else ""),
                local_path=str(local_path),
                s3_uri=s3_uri,
                rows=len(df),
            )
        except Exception as exc:
            return ExtractResult(
                source=dataset_name,
                ok=False,
                message=f"Snapshot failed: {exc}",
            )

    def extract_xlsx_sources(self, upload: bool = True) -> ExtractReport:
        report = ExtractReport()
        for dataset in normalize_datasets(self.config.get("datasets")):
            report.results.append(self.snapshot_dataset(dataset["id"], upload=upload))
        if not report.results:
            report.results.append(
                ExtractResult(source="datasets", ok=False, message="No datasets configured")
            )
        return report
