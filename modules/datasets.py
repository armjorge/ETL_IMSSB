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


def validate_folder(folder: str) -> tuple[bool, str]:
    cleaned = (folder or "").strip()
    if not cleaned:
        return False, "Upload folder is required (used as subfolder name)."
    if cleaned != cleaned.lower() or " " in cleaned or not PREFIX_PATTERN.match(cleaned):
        suggestion = sanitize_prefix(cleaned)
        return False, (
            "Upload folder must be a simple subfolder name: lowercase letters, numbers, "
            f"`_` or `-` only (no spaces/special chars). Suggested: `{suggestion or 'payments'}`"
        )
    return True, "Folder OK — files go under `{MAIN_PATH}/imssb_files/{folder}/`."


def validate_optional_prefix(prefix: str) -> tuple[bool, str]:
    """Optional filename tag; empty/null is allowed."""
    cleaned = (prefix or "").strip()
    if not cleaned:
        return True, "No filename prefix (optional)."
    if cleaned != cleaned.lower() or " " in cleaned or not PREFIX_PATTERN.match(cleaned):
        suggestion = sanitize_prefix(cleaned)
        return False, (
            "Prefix must be lowercase letters, numbers, `_` or `-` only "
            f"(or leave empty). Suggested: `{suggestion or 'fantasmas'}`"
        )
    return True, "Prefix OK — included in snapshot filename."


def validate_prefix(prefix: str) -> tuple[bool, str]:
    """Back-compat alias: historically `prefix` meant the upload folder."""
    return validate_folder(prefix)


def resolve_folder_and_prefix(item: dict) -> tuple[str, str]:
    """
    folder = S3/local upload subfolder (required).
    prefix = optional filename tag (may be empty).

    Legacy configs only had `prefix` meaning the folder — migrate that when
    `folder` is absent.
    """
    if not isinstance(item, dict):
        return "", ""
    if "folder" in item:
        folder = (item.get("folder") or "").strip()
        prefix = (item.get("prefix") or "").strip()
        return folder, prefix
    # Legacy: prefix was the upload folder; no filename tag.
    return (item.get("prefix") or "").strip(), ""


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
      {id, dataset_name, folder, prefix, file_path, sheet, columns}
    `folder` = upload subfolder; `prefix` = optional filename tag.
    Accepts legacy dict keyed by slug, and legacy `prefix`-as-folder.
    """
    if not datasets:
        return []

    if isinstance(datasets, list):
        normalized = []
        for item in datasets:
            if not isinstance(item, dict):
                continue
            folder, prefix = resolve_folder_and_prefix(item)
            ds = {
                "id": item.get("id") or new_dataset_id(),
                "dataset_name": item.get("dataset_name") or item.get("name") or "dataset",
                "folder": folder,
                "prefix": prefix,
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
            folder, prefix = resolve_folder_and_prefix(item)
            normalized.append(
                {
                    "id": item.get("id") or slug or new_dataset_id(),
                    "dataset_name": item.get("dataset_name") or slug,
                    "folder": folder,
                    "prefix": prefix,
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
    for section_key, default_folder in legacy_map:
        section = cfg.get(section_key) or {}
        if not isinstance(section, dict):
            continue
        for name, entry in section.items():
            entry = entry or {}
            datasets.append(
                {
                    "id": new_dataset_id(),
                    "dataset_name": name,
                    "folder": default_folder,
                    "prefix": "",
                    "file_path": entry.get("file_path", ""),
                    "sheet": entry.get("sheet", ""),
                    "columns": list(entry.get("rows") or entry.get("columns") or []),
                }
            )

    cfg["datasets"] = datasets
    return cfg
