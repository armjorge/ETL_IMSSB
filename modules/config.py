import os
from copy import deepcopy
from pathlib import Path

import yaml

DEFAULT_CONFIG = {
    "MAIN_PATH": ".",
    "s3": {
        "bucket": "so3-data",
        "root_prefix": "imss_bienestar",
        "region": "us-east-1",
    },
    "datasets": [],
    "PAQS_INSABI": {},
    "PAGOS_PAQ": {},
    "CAMUNDA": {
        "url": "",
        "user": "",
        "password": "",
    },
    "SAGI": {
        "url": "",
        "user": "",
        "password": "",
    },
}

SOURCE_FOLDERS = [
    "camunda",
    "sagi",
    "invoicing",
    "payments",
    "banking",
    "institution_status",
]

CONFIG_FILENAME = "config.yml"


class ConfigManager:
    """Load/save durable config at {MAIN_PATH}/imssb_files/config.yml."""

    def __init__(self, project_root: str | Path | None = None, main_path: str | Path | None = None):
        candidate = Path(project_root or Path.cwd()).resolve()
        if (candidate / "config.example.yaml").exists() or (candidate / "app.py").exists():
            self.project_root = candidate
        elif (candidate.parent / "config.example.yaml").exists() or (candidate.parent / "app.py").exists():
            self.project_root = candidate.parent
        else:
            self.project_root = candidate

        self.example_path = self.project_root / "config.example.yaml"
        self.legacy_root_config = self.project_root / "config.yaml"

        self._load_simple_env()
        bootstrap_main = main_path or os.getenv("MAIN_PATH") or "."
        self.main_path = self._resolve_main_path_value(bootstrap_main)
        self.imssb_dir = self.main_path / "imssb_files"
        self.config_path = self.imssb_dir / CONFIG_FILENAME

    def _load_simple_env(self) -> None:
        """Load KEY=VALUE lines from .env; ignore legacy YAML content safely."""
        env_path = self.project_root / ".env"
        if not env_path.exists():
            return
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, _, value = stripped.partition("=")
            key = key.strip()
            if not key or any(ch.isspace() for ch in key) or ":" in key:
                continue
            value = value.strip().strip("'").strip('"')
            os.environ.setdefault(key, value)

    def _resolve_main_path_value(self, main_path: str | Path) -> Path:
        path = Path(str(main_path or ".")).expanduser()
        if not path.is_absolute():
            path = (self.project_root / path).resolve()
        return path

    def set_main_path(self, main_path: str | Path) -> Path:
        """Update MAIN_PATH and the config.yml location under imssb_files/."""
        self.main_path = self._resolve_main_path_value(main_path)
        self.imssb_dir = self.main_path / "imssb_files"
        self.config_path = self.imssb_dir / CONFIG_FILENAME
        return self.main_path

    def ensure_config(self) -> dict:
        """Create config.yml if missing (migrate legacy root config.yaml when present)."""
        self.imssb_dir.mkdir(parents=True, exist_ok=True)

        if not self.config_path.exists():
            if self.legacy_root_config.exists():
                data = yaml.safe_load(self.legacy_root_config.read_text(encoding="utf-8")) or {}
                merged = self._merge_defaults(data)
                merged["MAIN_PATH"] = self._relative_or_abs_main_path()
                self.save(merged)
            elif self.example_path.exists():
                data = yaml.safe_load(self.example_path.read_text(encoding="utf-8")) or {}
                merged = self._merge_defaults(data)
                merged["MAIN_PATH"] = self._relative_or_abs_main_path()
                self.save(merged)
            else:
                data = deepcopy(DEFAULT_CONFIG)
                data["MAIN_PATH"] = self._relative_or_abs_main_path()
                self.save(data)

        return self.load()

    def _relative_or_abs_main_path(self) -> str:
        try:
            return str(self.main_path.relative_to(self.project_root))
        except ValueError:
            return str(self.main_path)

    def load(self) -> dict:
        if not self.config_path.exists():
            return deepcopy(DEFAULT_CONFIG)

        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        merged = self._merge_defaults(data)
        from modules.datasets import migrate_legacy_sections

        migrated = migrate_legacy_sections(merged)
        # Persist when shape changed (dict→list, legacy→datasets, missing ids)
        if migrated.get("datasets") != merged.get("datasets"):
            self.save(migrated)
        merged = migrated

        if merged.get("MAIN_PATH"):
            self.set_main_path(merged["MAIN_PATH"])
        return merged

    def save(self, data: dict) -> Path:
        # If MAIN_PATH changed in data, relocate config.yml accordingly
        if data.get("MAIN_PATH"):
            self.set_main_path(data["MAIN_PATH"])
            data = dict(data)
            data["MAIN_PATH"] = self._relative_or_abs_main_path()

        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(
                data,
                f,
                default_flow_style=False,
                allow_unicode=True,
                sort_keys=False,
            )
        return self.config_path

    def resolve_main_path(self, data: dict | None = None) -> Path:
        if data is not None and data.get("MAIN_PATH"):
            return self._resolve_main_path_value(data["MAIN_PATH"])
        return self.main_path

    def get_s3_config(self, data: dict | None = None) -> dict:
        cfg = data if data is not None else self.load()
        s3 = cfg.get("s3") or {}
        return {
            "bucket": s3.get("bucket") or DEFAULT_CONFIG["s3"]["bucket"],
            "root_prefix": (s3.get("root_prefix") or DEFAULT_CONFIG["s3"]["root_prefix"]).strip("/"),
            "region": s3.get("region") or DEFAULT_CONFIG["s3"]["region"],
        }

    @staticmethod
    def normalize_path(file_path: str | None) -> str:
        """Normalize user-entered paths (OrbStack mac mounts, relative paths, etc.)."""
        if not file_path:
            return ""
        raw = str(file_path).strip().strip('"').strip("'")
        if not raw:
            return ""

        # macOS OrbStack path into the Linux VM home:
        # /Users/<mac>/OrbStack/ubuntu/home/<linux-user>/... -> /home/<linux-user>/...
        marker = "/OrbStack/ubuntu/home/"
        if marker in raw:
            raw = "/home/" + raw.split(marker, 1)[1]

        path = Path(raw).expanduser()
        return str(path)

    @classmethod
    def resolve_existing_path(cls, file_path: str | None, project_root: str | Path | None = None) -> Path | None:
        """Return a Path that exists as a file, trying normalizations and project-relative joins."""
        candidates: list[Path] = []
        normalized = cls.normalize_path(file_path)
        if normalized:
            candidates.append(Path(normalized))
            if not Path(normalized).is_absolute() and project_root is not None:
                candidates.append(Path(project_root) / normalized)

        if file_path:
            raw = str(file_path).strip().strip('"').strip("'")
            if raw and raw != normalized:
                candidates.append(Path(raw).expanduser())

        seen: set[str] = set()
        for candidate in candidates:
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            if candidate.is_file():
                return candidate.resolve()
        return None

    @classmethod
    def file_exists(cls, file_path: str | None, project_root: str | Path | None = None) -> bool:
        return cls.resolve_existing_path(file_path, project_root=project_root) is not None

    @classmethod
    def diagnose_path(cls, file_path: str | None, project_root: str | Path | None = None) -> dict:
        """Explain why a configured path is missing / how to fix it."""
        if not file_path or not str(file_path).strip():
            return {
                "ok": False,
                "normalized": "",
                "reason": "empty",
                "message": "No file path set.",
                "suggestion": None,
            }

        normalized = cls.normalize_path(file_path)
        resolved = cls.resolve_existing_path(file_path, project_root=project_root)
        if resolved is not None:
            return {
                "ok": True,
                "normalized": str(resolved),
                "reason": "found",
                "message": f"File found: `{resolved}`",
                "suggestion": None,
            }

        path = Path(normalized)
        parent = path.parent
        suggestion = None

        raw = str(file_path).strip()
        if "/OrbStack/ubuntu/home/" in raw and normalized != raw:
            suggestion = normalized

        if path.exists() and path.is_dir():
            xlsx_here = sorted(path.glob("*.xlsx")) + sorted(path.glob("*.xls"))
            names = ", ".join(p.name for p in xlsx_here[:8]) or "(none)"
            return {
                "ok": False,
                "normalized": normalized,
                "reason": "is_directory",
                "message": (
                    f"Path exists but is a **directory**, not a file: `{path}`. "
                    f"Excel files here: {names}"
                ),
                "suggestion": str(xlsx_here[0]) if xlsx_here else suggestion,
            }

        if not parent.exists():
            return {
                "ok": False,
                "normalized": normalized,
                "reason": "parent_missing",
                "message": (
                    f"Parent folder does not exist: `{parent}`. "
                    "Streamlit runs inside the Linux environment — use a Linux path "
                    "(e.g. `/home/armjorge/...`), not a macOS `/Users/.../OrbStack/...` path."
                ),
                "suggestion": suggestion,
            }

        siblings = sorted(parent.glob("*.xlsx")) + sorted(parent.glob("*.xls"))
        sibling_names = ", ".join(p.name for p in siblings[:8]) or "(no .xlsx/.xls in parent)"
        return {
            "ok": False,
            "normalized": normalized,
            "reason": "file_missing",
            "message": (
                f"File not found: `{path}`. "
                f"Parent exists (`{parent}`). Nearby Excel files: {sibling_names}"
            ),
            "suggestion": (
                str(siblings[0])
                if siblings and path.name not in {p.name for p in siblings}
                else suggestion
            ),
        }

    def list_excel_files(self, search_roots: list[str | Path] | None = None) -> list[Path]:
        """List .xlsx/.xls under imssb_files and MAIN_PATH for the file picker."""
        roots = search_roots or [
            self.imssb_dir,
            self.main_path,
            self.project_root / "imssb_files",
        ]
        found: list[Path] = []
        seen: set[str] = set()
        for root in roots:
            root_path = Path(root)
            if not root_path.exists():
                continue
            for pattern in ("*.xlsx", "*.xls"):
                for path in root_path.rglob(pattern):
                    if any(
                        part in {".git", ".venv", ".tools", "infra", "__pycache__"}
                        for part in path.parts
                    ):
                        continue
                    key = str(path.resolve())
                    if key in seen:
                        continue
                    seen.add(key)
                    found.append(path.resolve())
        return sorted(found)

    def _merge_defaults(self, data: dict) -> dict:
        merged = deepcopy(DEFAULT_CONFIG)
        for key, value in data.items():
            if key == "s3" and isinstance(value, dict):
                merged["s3"].update(value)
            elif key in ("CAMUNDA", "SAGI") and isinstance(value, dict):
                if "url" in value or "user" in value or "password" in value:
                    merged[key].update({k: value.get(k, "") for k in ("url", "user", "password")})
                else:
                    merged[key] = self._coerce_legacy_credentials(key, value)
            else:
                merged[key] = value
        return merged

    @staticmethod
    def _coerce_legacy_credentials(system: str, value: dict) -> dict:
        """Best-effort parse of old CAMUNDA/SAGI selenium step lists into url/user/password."""
        url = next(iter(value.keys()), "")
        user = ""
        password = ""
        steps = value.get(url) or []
        send_keys = [s for s in steps if isinstance(s, dict) and s.get("type") == "send_keys"]
        if system == "SAGI" and len(send_keys) >= 2:
            user = send_keys[0].get("value") or user
            password = send_keys[1].get("value") or password
        if system == "CAMUNDA" and len(send_keys) >= 2:
            user = send_keys[0].get("value") or user
            password = send_keys[1].get("value") or password
        return {"url": url, "user": user, "password": password}

    # --- Legacy compatibility for main.py ---
    def yaml_creation(self, working_folder):
        """Legacy entrypoint used by main.py; prefers imssb_files/config.yml when present."""
        if self.config_path.exists():
            data = self.load()
            print(f"✅ Archivo YAML cargado correctamente: {self.config_path}")
            return data

        legacy_path = Path(working_folder) / "config.yaml"
        if legacy_path.exists():
            with open(legacy_path, "r", encoding="utf-8") as f:
                data_access = yaml.safe_load(f)
            print(f"✅ Archivo YAML cargado correctamente: {legacy_path.name}")
            return data_access

        print("No se localizó un yaml válido, vamos a crear uno")
        os.makedirs(working_folder, exist_ok=True)
        platforms = ["imss", "prei"]
        fields = ["url", "user", "password", "actions"]
        lines = []
        for platform in platforms:
            for field in fields:
                lines.append(f"{platform}_{field}: ''")
            lines.append("")
        with open(legacy_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print("Generamos el YAML para que captures información, vuelve a correr la script para abrirlo.")
        return None
