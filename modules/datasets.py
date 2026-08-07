from __future__ import annotations

import re
import uuid
from copy import deepcopy

PREFIX_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def sanitize_prefix(value: str) -> str:
    text = (value or "").strip().lower().replace(" ", "_")
    text = re.sub(r"[^a-z0-9_-]", "", text)
    text = re.sub(r"_+", "_", text).strip("_-")
    return text


def validate_prefix(prefix: str) -> tuple[bool, str]:
    cleaned = (prefix or "").strip()
    if not cleaned:
        return False, "Prefix is required (used as subfolder name)."
    if cleaned != cleaned.lower() or " " in cleaned or not PREFIX_PATTERN.match(cleaned):
        suggestion = sanitize_prefix(cleaned)
        return False, (
            "Prefix must be a simple subfolder name: lowercase letters, numbers, "
            f"`_` or `-` only (no spaces/special chars). Suggested: `{suggestion or 'payments'}`"
        )
    return True, "Prefix OK — files go under `{MAIN_PATH}/imssb_files/{prefix}/`."


def new_dataset_id() -> str:
    return uuid.uuid4().hex[:12]


def to_source_entry(dataset: dict) -> dict:
    """Shape expected by validation + load_and_concat."""
    columns = dataset.get("columns") or dataset.get("rows") or []
    return {
        "file_path": dataset.get("file_path", ""),
        "sheet": dataset.get("sheet", ""),
        "rows": list(columns),
    }


def normalize_datasets(datasets) -> list[dict]:
    """
    Normalize datasets to a list of:
      {id, dataset_name, prefix, file_path, sheet, columns}
    Accepts legacy dict keyed by slug.
    """
    if not datasets:
        return []

    if isinstance(datasets, list):
        normalized = []
        for item in datasets:
            if not isinstance(item, dict):
                continue
            ds = {
                "id": item.get("id") or new_dataset_id(),
                "dataset_name": item.get("dataset_name") or item.get("name") or "dataset",
                "prefix": item.get("prefix") or "",
                "file_path": item.get("file_path") or "",
                "sheet": item.get("sheet") or "",
                "columns": list(item.get("columns") or item.get("rows") or []),
            }
            normalized.append(ds)
        return normalized

    if isinstance(datasets, dict):
        normalized = []
        for slug, item in datasets.items():
            item = item or {}
            normalized.append(
                {
                    "id": item.get("id") or slug or new_dataset_id(),
                    "dataset_name": item.get("dataset_name") or slug,
                    "prefix": item.get("prefix") or "",
                    "file_path": item.get("file_path") or "",
                    "sheet": item.get("sheet") or "",
                    "columns": list(item.get("columns") or item.get("rows") or []),
                }
            )
        return normalized

    return []


def find_dataset(datasets: list[dict], dataset_id: str) -> dict | None:
    for item in datasets:
        if item.get("id") == dataset_id:
            return item
    return None


def migrate_legacy_sections(config: dict) -> dict:
    """Ensure config['datasets'] is a list; migrate legacy sections / dict shape."""
    cfg = deepcopy(config)
    existing = normalize_datasets(cfg.get("datasets"))

    if existing:
        cfg["datasets"] = existing
        return cfg

    datasets: list[dict] = []
    legacy_map = (
        ("PAQS_INSABI", "invoicing"),
        ("PAGOS_PAQ", "payments"),
    )
    for section_key, default_prefix in legacy_map:
        section = cfg.get(section_key) or {}
        if not isinstance(section, dict):
            continue
        for name, entry in section.items():
            entry = entry or {}
            datasets.append(
                {
                    "id": new_dataset_id(),
                    "dataset_name": name,
                    "prefix": default_prefix,
                    "file_path": entry.get("file_path", ""),
                    "sheet": entry.get("sheet", ""),
                    "columns": list(entry.get("rows") or entry.get("columns") or []),
                }
            )

    cfg["datasets"] = datasets
    return cfg
