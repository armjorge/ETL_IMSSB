from __future__ import annotations

import base64
import hashlib
import os
import secrets
from pathlib import Path

PREFIX = "enc:v1:"


class CredentialStore:
    """
    Lightweight reversible obfuscation for reference credentials in config.yml.

    Not a substitute for a vault — just keeps plaintext passwords out of the YAML.
    Key lives next to config at {imssb_dir}/.secret_key (gitignored with imssb_files/).
    """

    def __init__(self, imssb_dir: str | Path):
        self.imssb_dir = Path(imssb_dir)
        self.imssb_dir.mkdir(parents=True, exist_ok=True)
        self.key_path = self.imssb_dir / ".secret_key"
        self._key = self._load_or_create_key()

    def _load_or_create_key(self) -> bytes:
        if self.key_path.exists():
            return self.key_path.read_bytes()
        key = secrets.token_bytes(32)
        self.key_path.write_bytes(key)
        try:
            os.chmod(self.key_path, 0o600)
        except OSError:
            pass
        return key

    def _stream(self, length: int) -> bytes:
        out = bytearray()
        counter = 0
        while len(out) < length:
            out.extend(hashlib.sha256(self._key + counter.to_bytes(4, "big")).digest())
            counter += 1
        return bytes(out[:length])

    def encrypt(self, value: str | None) -> str:
        text = "" if value is None else str(value)
        if not text:
            return ""
        if text.startswith(PREFIX):
            return text
        data = text.encode("utf-8")
        stream = self._stream(len(data))
        xored = bytes(a ^ b for a, b in zip(data, stream))
        return PREFIX + base64.urlsafe_b64encode(xored).decode("ascii")

    def decrypt(self, value: str | None) -> str:
        text = "" if value is None else str(value)
        if not text:
            return ""
        if not text.startswith(PREFIX):
            return text  # legacy plaintext
        raw = base64.urlsafe_b64decode(text[len(PREFIX) :].encode("ascii"))
        stream = self._stream(len(raw))
        return bytes(a ^ b for a, b in zip(raw, stream)).decode("utf-8")

    def protect_credentials(self, creds: dict) -> dict:
        """Return a copy with user/password encrypted for disk."""
        out = dict(creds or {})
        if "user" in out:
            out["user"] = self.encrypt(out.get("user"))
        if "password" in out:
            out["password"] = self.encrypt(out.get("password"))
        return out

    def reveal_credentials(self, creds: dict) -> dict:
        """Return a copy with user/password decrypted for the UI / automation."""
        out = dict(creds or {})
        if "user" in out:
            out["user"] = self.decrypt(out.get("user"))
        if "password" in out:
            out["password"] = self.decrypt(out.get("password"))
        return out
