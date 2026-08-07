from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from modules.config import ConfigManager
from modules.credentials import CredentialStore
from modules.nav_steps import build_camunda_actions, build_sagi_actions
from modules.orders_management import orders_management
from modules.s3_client import S3Client
from modules.web_automation_driver import WebAutomationDriver

VALID_EXTENSIONS = {".csv", ".xlsx", ".xls"}


@dataclass
class WebExtractResult:
    source: str
    ok: bool
    message: str
    local_files: list[str] = field(default_factory=list)
    s3_uris: list[str] = field(default_factory=list)


class WebExtractor:
    """
    Run Camunda / SAGI browser sessions, collect downloaded files as-is,
    and upload them under s3://{bucket}/{root}/{camunda|sagi}/.
    """

    def __init__(self, config: dict | None = None, project_root: str | Path | None = None):
        self.config_manager = ConfigManager(project_root)
        self.config = config if config is not None else self.config_manager.load()
        self.main_path = self.config_manager.resolve_main_path(self.config)
        self.imssb_dir = self.main_path / "imssb_files"
        self.web_driver_root = self.imssb_dir / "web_driver"
        self.creds = CredentialStore(self.imssb_dir)
        s3_cfg = self.config_manager.get_s3_config(self.config)
        self.s3 = S3Client(
            bucket=s3_cfg["bucket"],
            root_prefix=s3_cfg["root_prefix"],
            region=s3_cfg["region"],
        )

    def _system_creds(self, system: str) -> dict:
        raw = self.config.get(system) or {}
        return self.creds.reveal_credentials(raw)

    def _download_dir(self, prefix: str) -> Path:
        path = self.imssb_dir / prefix / "downloads"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _staging_dir(self, prefix: str) -> Path:
        path = self.imssb_dir / prefix
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _collect_files(self, download_dir: Path, staging_dir: Path, prefix: str) -> list[Path]:
        """
        Move completed downloads into staging with a light timestamp rename.
        No content transformation — files kept as downloaded.
        """
        stamp = datetime.now().strftime("%d-%m-%Y %H %M")
        collected: list[Path] = []
        for src in sorted(download_dir.iterdir()):
            if not src.is_file():
                continue
            if src.suffix.lower() not in VALID_EXTENSIONS:
                continue
            if src.name.endswith((".crdownload", ".tmp", ".part")):
                continue
            dest_name = f"{prefix} {stamp} {src.name}"
            dest = staging_dir / dest_name
            # Avoid overwrite if re-run same minute
            if dest.exists():
                dest = staging_dir / f"{prefix} {stamp} {src.stem}_{src.stat().st_size}{src.suffix}"
            shutil.move(str(src), str(dest))
            collected.append(dest)
            print(f"📦 Staged: {dest}")
        return collected

    def _upload_files(self, files: list[Path], prefix: str, upload: bool) -> list[str]:
        if not upload:
            return []
        if not self.s3.bucket_exists():
            raise RuntimeError(f"S3 bucket '{self.s3.bucket}' not reachable")
        uris = []
        for path in files:
            uris.append(self.s3.upload_file(path, prefix))
            print(f"☁️  Uploaded: {uris[-1]}")
        return uris

    def _clear_download_dir(self, download_dir: Path) -> None:
        for path in download_dir.iterdir():
            if path.is_file():
                try:
                    path.unlink()
                except OSError as exc:
                    print(f"⚠️ Could not remove leftover {path.name}: {exc}")

    def _run_session(self, *, prefix: str, actions_key: str, actions: dict, upload: bool) -> WebExtractResult:
        download_dir = self._download_dir(prefix)
        staging_dir = self._staging_dir(prefix)
        working_folder = str(self.imssb_dir)
        self._clear_download_dir(download_dir)

        web_driver = WebAutomationDriver(
            downloads_path=download_dir,
            web_driver_root=self.web_driver_root,
        )
        data_access = {actions_key: actions}
        manager = orders_management(working_folder, web_driver, data_access)

        print(f"\n🚀 Starting {prefix} download session → {download_dir}")
        ok = manager.execute_download_session(str(download_dir), actions_key)
        if not ok:
            return WebExtractResult(
                source=prefix,
                ok=False,
                message=f"{prefix} automation did not finish successfully (False)",
            )

        files = self._collect_files(download_dir, staging_dir, prefix)
        if not files:
            return WebExtractResult(
                source=prefix,
                ok=False,
                message=(
                    f"{prefix} session returned True but no download files were found in "
                    f"{download_dir}"
                ),
            )

        try:
            uris = self._upload_files(files, prefix, upload=upload)
        except Exception as exc:
            return WebExtractResult(
                source=prefix,
                ok=False,
                message=f"Files saved locally but S3 upload failed: {exc}",
                local_files=[str(p) for p in files],
            )

        return WebExtractResult(
            source=prefix,
            ok=True,
            message=(
                f"{prefix} extract OK — {len(files)} file(s)"
                + (" uploaded" if uris else " saved locally")
            ),
            local_files=[str(p) for p in files],
            s3_uris=uris,
        )

    def extract_camunda(self, upload: bool = True) -> WebExtractResult:
        creds = self._system_creds("CAMUNDA")
        url = (creds.get("url") or "").strip()
        user = (creds.get("user") or "").strip()
        password = creds.get("password") or ""
        if not url or not user or not password:
            return WebExtractResult(
                source="camunda",
                ok=False,
                message="CAMUNDA url/user/password incomplete in config",
            )
        actions = build_camunda_actions(url, user, password)
        return self._run_session(
            prefix="camunda",
            actions_key="CAMUNDA",
            actions=actions,
            upload=upload,
        )

    def extract_sagi(self, upload: bool = True) -> WebExtractResult:
        creds = self._system_creds("SAGI")
        url = (creds.get("url") or "").strip()
        user = (creds.get("user") or "").strip()
        password = creds.get("password") or ""
        if not url or not user or not password:
            return WebExtractResult(
                source="sagi",
                ok=False,
                message="SAGI url/user/password incomplete in config",
            )
        actions = build_sagi_actions(url, user, password)
        return self._run_session(
            prefix="sagi",
            actions_key="SAGI",
            actions=actions,
            upload=upload,
        )
