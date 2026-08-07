from __future__ import annotations

import json
import os
import platform
import shutil
import stat
import subprocess
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

CFT_LAST_KNOWN_GOOD = (
    "https://googlechromelabs.github.io/chrome-for-testing/"
    "last-known-good-versions-with-downloads.json"
)

# Platforms published by Chrome for Testing (no linux-arm64).
CFT_PLATFORMS = (
    "linux64",
    "mac-arm64",
    "mac-x64",
    "win64",
    "win32",
)


class WebAutomationDriver:
    """
    Chrome for Testing + ChromeDriver under:
      {web_driver_root}/  (default: {MAIN_PATH}/imssb_files/web_driver)
    """

    def __init__(
        self,
        downloads_path,
        web_driver_root: str | Path | None = None,
        cft_platform: str | None = None,
    ):
        self.downloads_path = Path(downloads_path)
        self.system = platform.system()
        self.machine = platform.machine().lower()
        self.host_platform = self._detect_host_platform()
        self.cft_platform = cft_platform or self.default_cft_platform()
        if self.cft_platform not in CFT_PLATFORMS:
            raise ValueError(
                f"Unsupported CfT platform '{self.cft_platform}'. "
                f"Choose one of: {', '.join(CFT_PLATFORMS)}"
            )
        if web_driver_root is None:
            web_driver_root = Path.home() / "imssb_files" / "web_driver"
        self.web_driver_root = Path(web_driver_root)
        self.web_driver_root.mkdir(parents=True, exist_ok=True)
        self._validate_downloads_path()
        self._set_chrome_paths(self.cft_platform)

    def _validate_downloads_path(self):
        self.downloads_path.mkdir(parents=True, exist_ok=True)
        if not self.downloads_path.is_dir():
            raise NotADirectoryError(f"Downloads path is not a directory: {self.downloads_path}")

    def _detect_host_platform(self) -> str:
        if self.system == "Windows":
            return "win64"
        if self.system == "Darwin":
            return "mac-arm64" if self.machine in {"arm64", "aarch64"} else "mac-x64"
        if self.system == "Linux":
            if self.machine in {"arm64", "aarch64"}:
                return "linux-arm64"
            return "linux64"
        raise OSError(f"Unsupported OS: {self.system}")

    def default_cft_platform(self) -> str:
        """Map host → CfT zip. linux-arm64 falls back to linux64 (x86_64)."""
        if self.host_platform == "linux-arm64":
            return "linux64"
        if self.host_platform in CFT_PLATFORMS:
            return self.host_platform
        return "linux64"

    def _set_chrome_paths(self, cft_platform: str) -> None:
        root = self.web_driver_root
        self.cft_platform = cft_platform

        if cft_platform == "win64":
            self.chrome_binary_path = root / "chrome-win64" / "chrome.exe"
            self.chromedriver_path = root / "chromedriver-win64" / "chromedriver.exe"
        elif cft_platform == "win32":
            self.chrome_binary_path = root / "chrome-win32" / "chrome.exe"
            self.chromedriver_path = root / "chromedriver-win32" / "chromedriver.exe"
        elif cft_platform == "mac-arm64":
            self.chrome_binary_path = (
                root
                / "chrome-mac-arm64"
                / "Google Chrome for Testing.app"
                / "Contents"
                / "MacOS"
                / "Google Chrome for Testing"
            )
            self.chromedriver_path = root / "chromedriver-mac-arm64" / "chromedriver"
        elif cft_platform == "mac-x64":
            self.chrome_binary_path = (
                root
                / "chrome-mac-x64"
                / "Google Chrome for Testing.app"
                / "Contents"
                / "MacOS"
                / "Google Chrome for Testing"
            )
            self.chromedriver_path = root / "chromedriver-mac-x64" / "chromedriver"
        else:  # linux64
            self.chrome_binary_path = root / "chrome-linux64" / "chrome"
            self.chromedriver_path = root / "chromedriver-linux64" / "chromedriver"

        # Optional system Chromium fallback when CfT binaries are missing
        if not self.chrome_binary_path.exists():
            for candidate in (
                Path("/usr/bin/chromium"),
                Path("/usr/bin/chromium-browser"),
                Path("/usr/bin/google-chrome"),
                Path("/usr/bin/google-chrome-stable"),
            ):
                if candidate.exists():
                    self.chrome_binary_path = candidate
                    break

    def detect_installed_cft_platform(self) -> str | None:
        """Return the first CfT platform that looks installed under web_driver_root."""
        for key in CFT_PLATFORMS:
            chrome, driver = self._paths_for(key)
            if chrome.exists() and driver.exists():
                return key
        return None

    def _paths_for(self, cft_platform: str) -> tuple[Path, Path]:
        root = self.web_driver_root
        if cft_platform == "win64":
            return root / "chrome-win64" / "chrome.exe", root / "chromedriver-win64" / "chromedriver.exe"
        if cft_platform == "win32":
            return root / "chrome-win32" / "chrome.exe", root / "chromedriver-win32" / "chromedriver.exe"
        if cft_platform == "mac-arm64":
            chrome = (
                root
                / "chrome-mac-arm64"
                / "Google Chrome for Testing.app"
                / "Contents"
                / "MacOS"
                / "Google Chrome for Testing"
            )
            return chrome, root / "chromedriver-mac-arm64" / "chromedriver"
        if cft_platform == "mac-x64":
            chrome = (
                root
                / "chrome-mac-x64"
                / "Google Chrome for Testing.app"
                / "Contents"
                / "MacOS"
                / "Google Chrome for Testing"
            )
            return chrome, root / "chromedriver-mac-x64" / "chromedriver"
        return root / "chrome-linux64" / "chrome", root / "chromedriver-linux64" / "chromedriver"

    def _binary_runs(self, path: Path) -> bool:
        """True if the binary can at least start on this host (catches arch mismatch)."""
        if not path.exists() or not path.is_file():
            return False
        try:
            proc = subprocess.run(
                [str(path), "--version"],
                capture_output=True,
                text=True,
                timeout=15,
                env={**os.environ, "QT_QPA_PLATFORM": "offscreen"},
            )
            # Exit 0 with version text, or some Chrome builds still print version on nonzero
            out = (proc.stdout or "") + (proc.stderr or "")
            if "OrbStack ERROR" in out or "Dynamic loader not found" in out:
                return False
            if "cannot execute" in out.lower() or "Exec format error" in out:
                return False
            return proc.returncode == 0 or "Chrome" in out or "Chromium" in out
        except (OSError, subprocess.TimeoutExpired):
            return False

    def status(self, cft_platform: str | None = None) -> dict:
        """Return installation status for UI (green/gray)."""
        key = cft_platform or self.cft_platform
        chrome_path, driver_path = self._paths_for(key)
        # Prefer CfT paths for status; ignore system chrome unless CfT missing
        chrome_ok = chrome_path.exists()
        driver_ok = driver_path.exists()
        system_chrome = None
        if not chrome_ok:
            for candidate in (
                Path("/usr/bin/chromium"),
                Path("/usr/bin/chromium-browser"),
                Path("/usr/bin/google-chrome"),
                Path("/usr/bin/google-chrome-stable"),
            ):
                if candidate.exists():
                    system_chrome = candidate
                    chrome_ok = True
                    chrome_path = candidate
                    break

        chrome_runs = self._binary_runs(chrome_path) if chrome_ok else False
        driver_runs = self._binary_runs(driver_path) if driver_ok else False
        # System Chrome without a matching chromedriver still isn't ready for Selenium
        ready = bool(chrome_ok and driver_ok and chrome_runs and driver_runs)

        note = ""
        if self.host_platform == "linux-arm64" and key == "linux64":
            note = (
                "Host is **linux-arm64** (OrbStack/Ubuntu ARM). Chrome for Testing has "
                "**no linux-arm64** build — only **linux64** (x86_64), which cannot run here "
                "(chromedriver exit 255 / missing `/lib64/ld-linux-x86-64.so.2`). "
                "Run Streamlit on **macOS** and install the **mac-arm64** package instead."
            )
        elif chrome_ok and driver_ok and not ready:
            note = (
                f"Binaries for `{key}` are present but do not execute on this host "
                f"(`{self.host_platform}`). Install a matching CfT package "
                f"(e.g. **mac-arm64** on Apple Silicon)."
            )
        elif not ready:
            note = f"Missing Chrome for Testing package for `{key}` under `{self.web_driver_root}`."

        return {
            "ready": ready,
            "host_platform": self.host_platform,
            "cft_platform": key,
            "platform": key,  # back-compat for UI
            "web_driver_root": str(self.web_driver_root),
            "chrome_path": str(chrome_path),
            "chrome_found": chrome_ok,
            "chrome_runs": chrome_runs,
            "chromedriver_path": str(driver_path),
            "chromedriver_found": driver_ok,
            "chromedriver_runs": driver_runs,
            "note": note,
            "available_cft_platforms": list(CFT_PLATFORMS),
        }

    def _check_chrome_installation(self) -> bool:
        status = self.status()
        if status["chrome_found"]:
            print(f"✅ Chrome binary found at: {status['chrome_path']}")
        else:
            print(f"❌ Chrome binary not found at: {status['chrome_path']}")
        if status["chromedriver_found"]:
            print(f"✅ ChromeDriver found at: {status['chromedriver_path']}")
        else:
            print(f"❌ ChromeDriver not found at: {status['chromedriver_path']}")
        return status["ready"]

    def _http_get_json(self, url: str) -> dict:
        req = Request(url, headers={"User-Agent": "ETL_IMSSB/1.0"})
        with urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _http_download(self, url: str, dest: Path) -> Path:
        dest.parent.mkdir(parents=True, exist_ok=True)
        req = Request(url, headers={"User-Agent": "ETL_IMSSB/1.0"})
        with urlopen(req, timeout=600) as resp, open(dest, "wb") as out:
            shutil.copyfileobj(resp, out)
        return dest

    def _cft_download_urls(self, platform_key: str) -> tuple[str, str, str]:
        payload = self._http_get_json(CFT_LAST_KNOWN_GOOD)
        channel = payload["channels"]["Stable"]
        version = channel["version"]
        downloads = channel["downloads"]
        try:
            chrome_url = next(
                item["url"]
                for item in downloads.get("chrome", [])
                if item["platform"] == platform_key
            )
            driver_url = next(
                item["url"]
                for item in downloads.get("chromedriver", [])
                if item["platform"] == platform_key
            )
        except StopIteration as exc:
            raise RuntimeError(
                f"No Chrome for Testing download for platform '{platform_key}'. "
                f"Available chrome platforms: "
                f"{sorted({i['platform'] for i in downloads.get('chrome', [])})}"
            ) from exc
        return version, chrome_url, driver_url

    def _unzip(self, zip_path: Path, target_dir: Path) -> None:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(target_dir)

    def _make_executable(self, path: Path) -> None:
        if path.exists() and self.system != "Windows":
            path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def install_chrome(self, cft_platform: str | None = None) -> dict:
        """Download and install Chrome for Testing + ChromeDriver into web_driver_root."""
        self.web_driver_root.mkdir(parents=True, exist_ok=True)
        key = cft_platform or self.cft_platform
        if key not in CFT_PLATFORMS:
            return {
                "ok": False,
                "message": (
                    f"Platform `{key}` is not published by Chrome for Testing. "
                    f"Use one of: {', '.join(CFT_PLATFORMS)}"
                ),
                "status": self.status(self.default_cft_platform()),
            }

        version, chrome_url, driver_url = self._cft_download_urls(key)
        tmp = self.web_driver_root / "_downloads"
        tmp.mkdir(parents=True, exist_ok=True)
        chrome_zip = tmp / Path(chrome_url).name
        driver_zip = tmp / Path(driver_url).name

        print(f"Downloading Chrome for Testing {version} ({key})...")
        print(f"  {chrome_url}")
        self._http_download(chrome_url, chrome_zip)
        print(f"Downloading ChromeDriver {version} ({key})...")
        print(f"  {driver_url}")
        self._http_download(driver_url, driver_zip)

        print(f"Extracting into {self.web_driver_root}...")
        self._unzip(chrome_zip, self.web_driver_root)
        self._unzip(driver_zip, self.web_driver_root)

        self._set_chrome_paths(key)
        chrome_path, driver_path = self._paths_for(key)
        self._make_executable(driver_path)
        self._make_executable(chrome_path)

        status = self.status(key)
        ready = status["chrome_found"] and status["chromedriver_found"]
        msg = (
            f"Installed Chrome for Testing {version} ({key}) under {self.web_driver_root}"
            if ready
            else "Download finished but binaries were not found at expected paths."
        )
        if self.host_platform == "linux-arm64" and key == "linux64" and ready:
            msg += (
                " Note: host is ARM; linux64 is x86_64 and may need qemu/amd64 support to run."
            )
        return {"ok": ready, "message": msg, "version": version, "status": status}

    # Back-compat aliases
    def _install_chrome_macos(self):
        return self.install_chrome("mac-arm64" if self.machine in {"arm64", "aarch64"} else "mac-x64")

    def _install_chrome_windows(self):
        return self.install_chrome("win64")

    def ensure_chrome_installed(self):
        installed = self.detect_installed_cft_platform()
        if installed:
            self._set_chrome_paths(installed)
        if self._check_chrome_installation():
            return True
        # Don't keep re-downloading x86 linux64 on ARM — it will never run here.
        if self.host_platform == "linux-arm64":
            print(
                "❌ Host is linux-arm64; Chrome for Testing has no ARM Linux build. "
                "Run on macOS with CfT package mac-arm64."
            )
            return False
        print("Chrome or ChromeDriver not found. Installing...")
        result = self.install_chrome()
        return bool(result.get("ok"))

    def create_driver(self, custom_downloads_path=None):
        if not self.ensure_chrome_installed():
            raise RuntimeError("Failed to install or locate Chrome components")

        status = self.status(self.cft_platform)
        if not status["ready"]:
            raise RuntimeError(
                status.get("note")
                or (
                    f"Chrome/ChromeDriver for `{self.cft_platform}` are not runnable "
                    f"on host `{self.host_platform}`."
                )
            )

        downloads_path = Path(custom_downloads_path) if custom_downloads_path else self.downloads_path
        downloads_path.mkdir(parents=True, exist_ok=True)

        chrome_options = Options()
        chrome_options.binary_location = str(self.chrome_binary_path)

        prefs = {
            "download.default_directory": str(downloads_path.absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-component-update")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")

        try:
            if self.chromedriver_path.exists():
                service = Service(str(self.chromedriver_path))
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                driver = webdriver.Chrome(options=chrome_options)
            print(f"✅ Chrome driver created successfully with downloads path: {downloads_path}")
            return driver
        except Exception as e:
            print(f"❌ Failed to initialize Chrome driver: {e}")
            raise

    def get_downloads_path(self):
        return self.downloads_path

    def list_downloaded_files(self):
        return list(self.downloads_path.iterdir())
