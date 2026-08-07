from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import streamlit as st

from modules.config import ConfigManager
from modules.credentials import CredentialStore
from modules.datasets import (
    new_dataset_id,
    normalize_datasets,
    sanitize_prefix,
    to_source_entry,
    validate_prefix,
)
from modules.s3_client import S3Client
from modules.source_validation import SourceValidation, validate_source
from modules.web_automation_driver import WebAutomationDriver
from modules.web_extract import WebExtractResult, WebExtractor
from modules.xlsx_extract import ExtractReport, ExtractResult, XlsxExtractor

PROJECT_ROOT = Path(__file__).resolve().parent


def _init_state(cfg_manager: ConfigManager) -> None:
    if "config" not in st.session_state:
        st.session_state.config = cfg_manager.ensure_config()
        st.session_state.secrets_revealed = False
    elif "config_path" not in st.session_state or st.session_state.config_path != str(
        cfg_manager.config_path
    ):
        if cfg_manager.config_path.exists():
            st.session_state.config = cfg_manager.load()
            st.session_state.secrets_revealed = False
    # Always keep datasets as a list with stable ids
    st.session_state.config["datasets"] = normalize_datasets(
        st.session_state.config.get("datasets")
    )
    st.session_state.config_path = str(cfg_manager.config_path)
    if not st.session_state.get("secrets_revealed"):
        _reveal_secrets_in_session(cfg_manager)
        st.session_state.secrets_revealed = True
        # Reset credential widgets so they pick up decrypted values
        for system in ("CAMUNDA", "SAGI"):
            for field in ("url", "user", "password"):
                key = f"{system}::{field}"
                if key in st.session_state:
                    del st.session_state[key]
    if "extract_report" not in st.session_state:
        st.session_state.extract_report = None
    if "last_snapshot" not in st.session_state:
        st.session_state.last_snapshot = None
    if "last_snapshots" not in st.session_state:
        st.session_state.last_snapshots = None
    if "last_web_extract" not in st.session_state:
        st.session_state.last_web_extract = None


def _widget_key(dataset_id: str, field: str) -> str:
    return f"dataset::{dataset_id}::{field}"


def _seed_dataset_widgets(dataset: dict) -> None:
    """Initialize widget keys once per dataset id (before widgets are created)."""
    ds_id = dataset["id"]
    defaults = {
        "name": dataset.get("dataset_name") or "",
        "prefix": dataset.get("prefix") or "",
        "path": dataset.get("file_path") or "",
        "sheet": dataset.get("sheet") or "",
        "columns": "\n".join(dataset.get("columns") or []),
    }
    for field, value in defaults.items():
        key = _widget_key(ds_id, field)
        if key not in st.session_state:
            st.session_state[key] = value


def _apply_pending_path(dataset_id: str) -> None:
    """Apply queued path changes before the path widget is instantiated."""
    pending = st.session_state.setdefault("pending_dataset_paths", {})
    if dataset_id in pending:
        st.session_state[_widget_key(dataset_id, "path")] = pending.pop(dataset_id)


def _queue_path(dataset_id: str, path: str) -> None:
    st.session_state.setdefault("pending_dataset_paths", {})[dataset_id] = path


def _read_dataset_from_widgets(dataset_id: str) -> dict:
    columns_raw = st.session_state.get(_widget_key(dataset_id, "columns"), "")
    columns = [c.strip() for c in str(columns_raw).splitlines() if c.strip()]
    prefix = sanitize_prefix(st.session_state.get(_widget_key(dataset_id, "prefix"), ""))
    path = st.session_state.get(_widget_key(dataset_id, "path"), "")
    resolved = ConfigManager.resolve_existing_path(path, PROJECT_ROOT)
    return {
        "id": dataset_id,
        "dataset_name": (st.session_state.get(_widget_key(dataset_id, "name"), "") or "").strip(),
        "prefix": prefix,
        "file_path": str(resolved) if resolved else ConfigManager.normalize_path(path),
        "sheet": (st.session_state.get(_widget_key(dataset_id, "sheet"), "") or "").strip(),
        "columns": columns,
    }


def _upsert_dataset(dataset: dict) -> None:
    datasets = normalize_datasets(st.session_state.config.get("datasets"))
    found = False
    for i, item in enumerate(datasets):
        if item.get("id") == dataset["id"]:
            datasets[i] = dataset
            found = True
            break
    if not found:
        datasets.append(dataset)
    st.session_state.config["datasets"] = datasets


def _delete_dataset(dataset_id: str) -> None:
    datasets = [
        d
        for d in normalize_datasets(st.session_state.config.get("datasets"))
        if d.get("id") != dataset_id
    ]
    st.session_state.config["datasets"] = datasets
    # Drop widget state for this id so it cannot leak into a new dataset
    prefix = f"dataset::{dataset_id}::"
    for key in list(st.session_state.keys()):
        if isinstance(key, str) and key.startswith(prefix):
            del st.session_state[key]


def _credential_store(cfg_manager: ConfigManager) -> CredentialStore:
    return CredentialStore(cfg_manager.imssb_dir)


def _reveal_secrets_in_session(cfg_manager: ConfigManager) -> None:
    store = _credential_store(cfg_manager)
    for system in ("CAMUNDA", "SAGI"):
        creds = st.session_state.config.get(system) or {}
        st.session_state.config[system] = store.reveal_credentials(creds)


def _save_config(cfg_manager: ConfigManager, *, quiet: bool = False) -> Path:
    """Persist config with CAMUNDA/SAGI user+password lightly encrypted on disk."""
    to_disk = deepcopy(st.session_state.config)
    store = _credential_store(cfg_manager)
    for system in ("CAMUNDA", "SAGI"):
        if system in to_disk:
            to_disk[system] = store.protect_credentials(to_disk.get(system) or {})
    path = cfg_manager.save(to_disk)
    st.session_state.config_path = str(path)
    if not quiet:
        st.success(f"Saved `{path}`")
    return path


def _source_badge(validation: SourceValidation, prefix_ok: bool = True) -> str:
    if validation.ok and prefix_ok:
        return "🟢 ready"
    if not prefix_ok:
        return "🔴 prefix"
    if not validation.file_ok:
        return "🔴 file"
    if not validation.sheet_ok:
        return "🔴 sheet"
    return "🔴 columns"


def _render_source_validation(validation: SourceValidation) -> None:
    st.markdown("**Validation**")
    st.write(
        f"{'🟢' if validation.file_ok else '🔴'} File  ·  "
        f"{'🟢' if validation.sheet_ok else '🔴'} Sheet  ·  "
        f"{'🟢' if validation.columns_ok else '🔴'} Columns"
    )
    if validation.available_sheets and not validation.sheet_ok:
        st.caption("Available sheets: " + ", ".join(validation.available_sheets))
    if validation.columns:
        for col in validation.columns:
            st.write(f"{'🟢' if col.ok else '🔴'} `{col.name}`")
    if validation.ok:
        st.success(validation.message)
    else:
        st.error(validation.message)
        if validation.available_columns and not validation.columns_ok:
            st.caption("Columns found on sheet: " + ", ".join(validation.available_columns))


def _show_extract_result(result: ExtractResult) -> None:
    if result.ok:
        st.success(f"**{result.source}**: {result.message} ({result.rows} rows)")
    else:
        st.error(f"**{result.source}**: {result.message}")
    if result.local_path:
        st.write(f"Local: `{result.local_path}`")
    if result.s3_uri:
        st.write(f"S3: `{result.s3_uri}`")


def _show_web_extract_result(result: WebExtractResult) -> None:
    if result.ok:
        st.success(f"**{result.source}**: {result.message}")
    else:
        st.error(f"**{result.source}**: {result.message}")
    for path in result.local_files:
        st.write(f"Local: `{path}`")
    for uri in result.s3_uris:
        st.write(f"S3: `{uri}`")


def _run_web_extract(source: str, upload: bool) -> WebExtractResult:
    """Sync creds from widgets, run browser session, return True/False via result.ok."""
    _sync_credentials_from_widgets()
    extractor = WebExtractor(config=st.session_state.config, project_root=PROJECT_ROOT)
    if source == "camunda":
        return extractor.extract_camunda(upload=upload)
    return extractor.extract_sagi(upload=upload)


def _render_path_diagnostics(file_path: str, key_prefix: str) -> str | None:
    diagnosis = ConfigManager.diagnose_path(file_path, project_root=PROJECT_ROOT)
    if diagnosis["ok"]:
        st.success(diagnosis["message"])
        return None
    st.error(diagnosis["message"])
    if diagnosis.get("suggestion"):
        st.info(f"Suggested path: `{diagnosis['suggestion']}`")
        if st.button("Use suggested path", key=f"{key_prefix}_use_suggestion"):
            return diagnosis["suggestion"]
    return None


def _pick_or_upload_file(
    key_prefix: str, cfg_manager: ConfigManager, current_path: str = ""
) -> str | None:
    st.markdown("**Choose Excel file**")
    pick_tab, upload_tab = st.tabs(["Browse project files", "Upload from computer"])
    selected: str | None = None
    upload_dir = cfg_manager.imssb_dir
    upload_dir.mkdir(parents=True, exist_ok=True)

    with pick_tab:
        excel_files = cfg_manager.list_excel_files()
        if not excel_files:
            st.warning(
                f"No `.xlsx` / `.xls` found under `{upload_dir}`. "
                "Point to a synced cloud folder file, or upload one."
            )
        else:
            labels = [str(p) for p in excel_files]
            try:
                index = labels.index(
                    str(ConfigManager.resolve_existing_path(current_path, PROJECT_ROOT) or "")
                )
            except ValueError:
                index = 0
            choice = st.selectbox(
                "Excel files on this machine (Linux paths)",
                options=labels,
                index=index if labels else 0,
                key=f"{key_prefix}_browse",
            )
            if st.button("Use selected file", key=f"{key_prefix}_use_browse"):
                selected = choice

    with upload_tab:
        uploaded = st.file_uploader(
            "Open file picker",
            type=["xlsx", "xls"],
            key=f"{key_prefix}_uploader",
        )
        if uploaded is not None:
            dest = upload_dir / uploaded.name
            if st.button(f"Save and use `{uploaded.name}`", key=f"{key_prefix}_save_upload"):
                dest.write_bytes(uploaded.getvalue())
                selected = str(dest.resolve())
                st.success(f"Saved to `{dest}`")

    return selected


def _render_datasets(cfg_manager: ConfigManager) -> None:
    st.subheader("Dataset schemas")
    st.info(
        "**Cloud folders:** these sources are usually Excel files that change regularly "
        "(Dropbox / Drive / OneDrive synced paths). Keep the path pointed at the live file "
        "in the synced folder — each **Take snapshot** captures the current sheet/columns "
        "as a clean pipe CSV (no heavy transforms)."
    )

    datasets = normalize_datasets(st.session_state.config.get("datasets"))
    st.session_state.config["datasets"] = datasets

    with st.expander("➕ Create dataset schema", expanded=not datasets):
        with st.form("create_dataset_form", clear_on_submit=True):
            new_name = st.text_input(
                "Dataset name",
                placeholder="payments 2025-2026",
                help="Editable label stored in config.yml (not a technical key).",
            )
            new_prefix = st.text_input(
                "Prefix (subfolder)",
                placeholder="payments",
                help=(
                    "Subfolder under `{MAIN_PATH}/imssb_files/{prefix}/` and S3. "
                    "Lowercase letters, numbers, `_` or `-` only."
                ),
            )
            new_path = st.text_input("XLSX file path (Linux / synced cloud path)")
            new_sheet = st.text_input("Sheet")
            new_columns = st.text_area("Columns (one per line)", height=120)
            created = st.form_submit_button("Create dataset")
            if created:
                prefix = sanitize_prefix(new_prefix) or sanitize_prefix(new_name)
                ok, msg = validate_prefix(prefix)
                if not new_name.strip():
                    st.error("Dataset name is required")
                elif not ok:
                    st.error(msg)
                else:
                    columns = [c.strip() for c in new_columns.splitlines() if c.strip()]
                    resolved = ConfigManager.resolve_existing_path(new_path, PROJECT_ROOT)
                    ds_id = new_dataset_id()
                    dataset = {
                        "id": ds_id,
                        "dataset_name": new_name.strip(),
                        "prefix": prefix,
                        "file_path": (
                            str(resolved) if resolved else ConfigManager.normalize_path(new_path)
                        ),
                        "sheet": new_sheet.strip(),
                        "columns": columns,
                    }
                    _upsert_dataset(dataset)
                    _seed_dataset_widgets(dataset)
                    _save_config(cfg_manager, quiet=True)
                    st.success(f"Created `{dataset['dataset_name']}` (prefix `{prefix}`)")
                    st.rerun()

    if not datasets:
        st.warning("No datasets yet. Create one above.")
        return

    upload_to_s3 = st.checkbox("Upload snapshot to S3", value=True, key="datasets_upload_s3")
    ready_batch: list[dict] = []

    for dataset in datasets:
        ds_id = dataset["id"]
        _apply_pending_path(ds_id)
        _seed_dataset_widgets(dataset)

        # Read current widget values for validation/title (does not write other datasets)
        live = _read_dataset_from_widgets(ds_id)
        name = live["dataset_name"] or dataset.get("dataset_name") or ds_id
        prefix_ok, _ = validate_prefix(live["prefix"])
        validation = validate_source(name, to_source_entry(live), PROJECT_ROOT)

        with st.expander(
            f"{name}  ·  `{live['prefix'] or dataset.get('prefix')}/`  — "
            f"{_source_badge(validation, prefix_ok)}",
            expanded=False,
        ):
            st.caption(f"Internal id: `{ds_id}` (stable; renaming the dataset does not change it)")

            st.text_input("Dataset name", key=_widget_key(ds_id, "name"))
            st.text_input(
                "Prefix (subfolder)",
                key=_widget_key(ds_id, "prefix"),
                help="Subfolder under imssb_files/ and S3. No spaces or special characters.",
            )

            live_prefix = sanitize_prefix(st.session_state.get(_widget_key(ds_id, "prefix"), ""))
            live_ok, live_msg = validate_prefix(live_prefix)
            if live_ok:
                st.caption(
                    f"🟢 Snapshots → `{{MAIN_PATH}}/imssb_files/{live_prefix}/` and "
                    f"`s3://…/{{root_prefix}}/{live_prefix}/`"
                )
            else:
                st.error(live_msg)

            picked = _pick_or_upload_file(
                _widget_key(ds_id, "picker"),
                cfg_manager,
                st.session_state.get(_widget_key(ds_id, "path"), ""),
            )
            if picked:
                # Queue path update for next run (path widget may already exist below)
                _queue_path(ds_id, picked)
                updated = _read_dataset_from_widgets(ds_id)
                updated["file_path"] = picked
                _upsert_dataset(updated)
                _save_config(cfg_manager, quiet=True)
                st.rerun()

            st.text_input(
                "XLSX file path",
                key=_widget_key(ds_id, "path"),
                help="Prefer a synced cloud folder path that updates over time.",
            )
            suggested_path = _render_path_diagnostics(
                st.session_state.get(_widget_key(ds_id, "path"), ""),
                _widget_key(ds_id, "diag"),
            )
            if suggested_path:
                _queue_path(ds_id, suggested_path)
                updated = _read_dataset_from_widgets(ds_id)
                updated["file_path"] = suggested_path
                _upsert_dataset(updated)
                _save_config(cfg_manager, quiet=True)
                st.rerun()

            st.text_input("Sheet", key=_widget_key(ds_id, "sheet"))
            st.text_area("Columns (one per line)", key=_widget_key(ds_id, "columns"), height=140)

            # Re-read after widgets rendered (Streamlit updates session_state during render)
            live = _read_dataset_from_widgets(ds_id)
            live_validation = validate_source(
                live["dataset_name"] or ds_id, to_source_entry(live), PROJECT_ROOT
            )
            _render_source_validation(live_validation)
            live_ready = live_validation.ok and validate_prefix(live["prefix"])[0]
            if live_ready:
                ready_batch.append(live)

            st.caption(
                f"Snapshot file: `{live['prefix'] or 'prefix'} dd-mm-yyyy hh mm.csv` "
                "(pipe `|`, all text)"
            )

            col_save, col_snap, col_del = st.columns(3)
            with col_save:
                if st.button("Save dataset", key=_widget_key(ds_id, "save")):
                    updated = _read_dataset_from_widgets(ds_id)
                    ok, msg = validate_prefix(updated["prefix"])
                    if not updated["dataset_name"]:
                        st.error("Dataset name is required")
                    elif not ok:
                        st.error(msg)
                    else:
                        _upsert_dataset(updated)
                        _save_config(cfg_manager)
                        st.rerun()
            with col_snap:
                if st.button(
                    "Take snapshot",
                    type="primary",
                    disabled=not live_ready,
                    key=_widget_key(ds_id, "snapshot"),
                ):
                    updated = _read_dataset_from_widgets(ds_id)
                    _upsert_dataset(updated)
                    _save_config(cfg_manager, quiet=True)
                    extractor = XlsxExtractor(
                        config=deepcopy(st.session_state.config),
                        project_root=PROJECT_ROOT,
                    )
                    with st.spinner(f"Snapshotting `{updated['dataset_name']}`..."):
                        result = extractor.snapshot_dataset(ds_id, upload=upload_to_s3)
                    st.session_state.last_snapshot = result
                    st.session_state.last_snapshots = None
                    st.rerun()
            with col_del:
                if st.button("Delete", key=_widget_key(ds_id, "delete")):
                    _delete_dataset(ds_id)
                    _save_config(cfg_manager, quiet=True)
                    st.rerun()

            if not live_ready:
                st.info("Fix prefix / file / sheet / columns (all green) to enable **Take snapshot**.")

    st.divider()
    st.subheader("Batch snapshot")
    ready_count = len(ready_batch)
    total_count = len(datasets)
    if ready_count == total_count and ready_count > 0:
        st.success(f"All {ready_count} dataset(s) are green and ready.")
    elif ready_count > 0:
        st.warning(f"{ready_count} of {total_count} dataset(s) ready. Others will be skipped.")
    else:
        st.info("No green datasets yet. Fix validation above to unlock batch snapshot.")

    if st.button(
        f"Snapshot all ready ({ready_count})",
        type="primary",
        disabled=ready_count == 0,
        key="snapshot_all_ready",
    ):
        # Persist every ready dataset from current widget values, in list order
        for item in ready_batch:
            _upsert_dataset(item)
        _save_config(cfg_manager, quiet=True)

        extractor = XlsxExtractor(
            config=deepcopy(st.session_state.config),
            project_root=PROJECT_ROOT,
        )
        report = ExtractReport()
        with st.spinner(f"Snapshotting {ready_count} dataset(s) in order..."):
            for item in ready_batch:
                report.results.append(
                    extractor.snapshot_dataset(item["id"], upload=upload_to_s3)
                )
        st.session_state.last_snapshots = report
        st.session_state.last_snapshot = report.results[-1] if report.results else None
        st.rerun()

    if st.session_state.last_snapshots is not None:
        st.divider()
        st.subheader("Last batch results")
        for result in st.session_state.last_snapshots.results:
            _show_extract_result(result)
    elif st.session_state.last_snapshot is not None:
        st.divider()
        st.subheader("Last snapshot")
        _show_extract_result(st.session_state.last_snapshot)


def _web_driver_root(cfg_manager: ConfigManager) -> Path:
    return cfg_manager.imssb_dir / "web_driver"


def _render_chromium_status(cfg_manager: ConfigManager) -> None:
    st.subheader("Chromium / ChromeDriver")
    st.caption(
        f"Portable browser files live under `{_web_driver_root(cfg_manager)}` "
        "(used by Camunda and SAGI automation)."
    )
    probe = WebAutomationDriver(
        downloads_path=cfg_manager.imssb_dir / "downloads",
        web_driver_root=_web_driver_root(cfg_manager),
    )
    platforms = probe.status()["available_cft_platforms"]
    default_plat = probe.default_cft_platform()
    try:
        default_idx = platforms.index(default_plat)
    except ValueError:
        default_idx = 0

    cft_platform = st.selectbox(
        "Chrome for Testing package to install",
        options=platforms,
        index=default_idx,
        key="cft_platform_select",
        help=(
            "Official CfT zips: linux64, mac-arm64, mac-x64, win32, win64. "
            "There is no linux-arm64 build — on ARM Linux we default to linux64."
        ),
    )

    driver = WebAutomationDriver(
        downloads_path=cfg_manager.imssb_dir / "downloads",
        web_driver_root=_web_driver_root(cfg_manager),
        cft_platform=cft_platform,
    )
    status = driver.status(cft_platform)
    if status["ready"]:
        st.markdown("🟢 **Chromium ready**")
    elif status.get("chrome_found") and status.get("chromedriver_found"):
        st.markdown("🔴 **Chromium installed but not runnable on this host**")
    else:
        st.markdown("⬜ **Chromium not found**")

    st.write(f"Host: `{status['host_platform']}` · CfT package: `{status['cft_platform']}`")
    chrome_mark = (
        "🟢"
        if status.get("chrome_runs")
        else ("🟡" if status.get("chrome_found") else "⬜")
    )
    driver_mark = (
        "🟢"
        if status.get("chromedriver_runs")
        else ("🟡" if status.get("chromedriver_found") else "⬜")
    )
    st.write(f"{chrome_mark} Chrome: `{status['chrome_path']}`")
    st.write(f"{driver_mark} ChromeDriver: `{status['chromedriver_path']}`")
    if status.get("note"):
        if status["host_platform"] == "linux-arm64":
            st.warning(status["note"])
        else:
            st.info(status["note"])

    if st.button("Download / install Chromium", key="install_chromium"):
        with st.spinner(f"Downloading Chrome for Testing ({cft_platform})..."):
            result = driver.install_chrome(cft_platform)
        if result.get("ok"):
            st.success(result.get("message", "Installed"))
        else:
            st.error(result.get("message", "Install failed"))
        st.rerun()


def _render_credentials(system_key: str, title: str) -> None:
    st.subheader(title)
    creds = st.session_state.config.setdefault(
        system_key, {"url": "", "user": "", "password": ""}
    )
    if "url" not in creds:
        st.session_state.config[system_key] = {"url": "", "user": "", "password": ""}
        creds = st.session_state.config[system_key]

    # Seed widget state once so edits don't fight encrypted disk values
    for field in ("url", "user", "password"):
        key = f"{system_key}::{field}"
        if key not in st.session_state:
            st.session_state[key] = creds.get(field, "") or ""

    st.text_input("URL", key=f"{system_key}::url")
    st.text_input("User", key=f"{system_key}::user")
    st.text_input("Password", type="password", key=f"{system_key}::password")
    st.caption("User and password are stored obfuscated (`enc:v1:…`) in config.yml.")


def _sync_credentials_from_widgets() -> None:
    for system_key in ("CAMUNDA", "SAGI"):
        st.session_state.config[system_key] = {
            "url": st.session_state.get(f"{system_key}::url", ""),
            "user": st.session_state.get(f"{system_key}::user", ""),
            "password": st.session_state.get(f"{system_key}::password", ""),
        }


def main() -> None:
    st.set_page_config(page_title="ETL IMSSB Config", layout="wide")
    st.title("ETL IMSSB — Config & Extract")
    st.caption("Create dataset schemas from live Excel files, snapshot to pipe CSV, upload to S3.")

    cfg_manager = ConfigManager(PROJECT_ROOT)
    _init_state(cfg_manager)
    st.caption(f"Durable config: `{cfg_manager.config_path}`")

    tab_paths, tab_s3, tab_datasets, tab_creds = st.tabs(
        ["Paths", "S3", "Datasets", "Camunda / SAGI"]
    )

    with tab_paths:
        st.subheader("Working folder")
        main_path = st.text_input(
            "MAIN_PATH",
            value=st.session_state.config.get("MAIN_PATH", "."),
            help=(
                "Root working folder. Config: `{MAIN_PATH}/imssb_files/config.yml`. "
                "Snapshots: `{MAIN_PATH}/imssb_files/{prefix}/`."
            ),
        )
        st.session_state.config["MAIN_PATH"] = main_path
        resolved = cfg_manager.resolve_main_path(st.session_state.config)
        st.write(f"Resolved MAIN_PATH: `{resolved}`")
        st.write(f"Config file: `{resolved / 'imssb_files' / 'config.yml'}`")
        if st.button("Save paths"):
            _save_config(cfg_manager)
            cfg_manager.set_main_path(main_path)
            st.session_state.config_path = str(cfg_manager.config_path)
            st.rerun()

    with tab_s3:
        st.subheader("S3 configuration")
        s3_cfg = st.session_state.config.setdefault(
            "s3",
            {"bucket": "so3-data", "root_prefix": "imss_bienestar", "region": "us-east-1"},
        )
        s3_cfg["bucket"] = st.text_input("Bucket name", value=s3_cfg.get("bucket", "so3-data"))
        s3_cfg["root_prefix"] = st.text_input(
            "Root prefix", value=s3_cfg.get("root_prefix", "imss_bienestar")
        )
        s3_cfg["region"] = st.text_input("Region", value=s3_cfg.get("region", "us-east-1"))

        client = S3Client(
            bucket=s3_cfg["bucket"],
            root_prefix=s3_cfg["root_prefix"],
            region=s3_cfg["region"],
        )
        try:
            status = client.status()
            if status.get("error"):
                st.warning(f"AWS credential/client issue: {status['error']}")
            if status["exists"]:
                st.success(f"Bucket reachable: `{status['uri']}`")
            else:
                st.error(
                    f"Bucket not found or not accessible: `{s3_cfg['bucket']}` "
                    f"(region `{s3_cfg['region']}`)."
                )
        except Exception as exc:
            st.error(f"Could not check S3 status: {exc}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Save S3 settings"):
                _save_config(cfg_manager)
        with col2:
            if st.button("Create bucket if missing"):
                try:
                    result = client.ensure_bucket()
                    _save_config(cfg_manager)
                    if result["created"]:
                        st.success(f"Created bucket and prefixes at `{result['uri']}`")
                    else:
                        st.success(f"Bucket already existed; ensured prefixes at `{result['uri']}`")
                except Exception as exc:
                    st.error(f"Could not create/ensure bucket: {exc}")

        st.info(
            "Preferred IaC: `cd infra && terraform init && terraform plan && terraform apply`. "
            "Dataset snapshots upload under `s3://{bucket}/{root_prefix}/{prefix}/`."
        )

    with tab_datasets:
        _render_datasets(cfg_manager)

    with tab_creds:
        _render_chromium_status(cfg_manager)
        st.divider()
        _render_credentials("CAMUNDA", "Camunda")
        st.divider()
        _render_credentials("SAGI", "SAGI")
        if st.button("Save credentials", type="primary"):
            _sync_credentials_from_widgets()
            _save_config(cfg_manager)
            st.info("Credentials saved to config.yml (user/password obfuscated).")

        st.divider()
        st.subheader("Web extract")
        st.caption(
            "Opens portable Chrome, runs the legacy Camunda/SAGI navigation, "
            "collects downloads as-is under `imssb_files/{camunda|sagi}/`, "
            "and uploads to S3. The button blocks until the session returns True/False."
        )
        upload_web = st.checkbox("Upload to S3 after download", value=True, key="web_upload_s3")
        col_c, col_s = st.columns(2)
        with col_c:
            if st.button("Extract Camunda", key="extract_camunda", type="primary"):
                with st.spinner(
                    "Camunda: Chrome open — filter/export in the browser; "
                    "waiting for downloads to finish…"
                ):
                    st.session_state.last_web_extract = _run_web_extract(
                        "camunda", upload=upload_web
                    )
        with col_s:
            if st.button("Extract SAGI", key="extract_sagi", type="primary"):
                with st.spinner("SAGI: Chrome open — waiting for export to finish…"):
                    st.session_state.last_web_extract = _run_web_extract(
                        "sagi", upload=upload_web
                    )

        if st.session_state.last_web_extract:
            _show_web_extract_result(st.session_state.last_web_extract)


if __name__ == "__main__":
    main()
