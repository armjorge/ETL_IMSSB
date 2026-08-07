from __future__ import annotations

from pathlib import Path

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from modules.config import SOURCE_FOLDERS


class S3Client:
    """Thin helper around boto3 for bucket create/link and CSV uploads."""

    def __init__(self, bucket: str, root_prefix: str = "imss_bienestar", region: str = "us-east-1"):
        self.bucket = bucket
        self.root_prefix = root_prefix.strip("/")
        self.region = region
        self._session = None
        self._s3 = None

    @property
    def session(self):
        if self._session is None:
            self._session = boto3.session.Session(region_name=self.region)
        return self._session

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = self.session.client("s3")
        return self._s3

    def bucket_exists(self) -> bool:
        try:
            self.s3.head_bucket(Bucket=self.bucket)
            return True
        except (ClientError, BotoCoreError, Exception):
            return False

    def status(self) -> dict:
        error = None
        exists = False
        try:
            exists = self.bucket_exists()
        except Exception as exc:
            error = str(exc)
        result = {
            "bucket": self.bucket,
            "region": self.region,
            "root_prefix": self.root_prefix,
            "exists": exists,
            "uri": f"s3://{self.bucket}/{self.root_prefix}/",
        }
        if error:
            result["error"] = error
        return result

    def ensure_bucket(self, folders: list[str] | None = None) -> dict:
        """Create the bucket if missing and ensure source folder placeholders exist."""
        folders = folders or SOURCE_FOLDERS
        created = False

        if not self.bucket_exists():
            create_kwargs = {"Bucket": self.bucket}
            # us-east-1 must omit LocationConstraint
            if self.region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.region
                }
            self.s3.create_bucket(**create_kwargs)

            self.s3.put_public_access_block(
                Bucket=self.bucket,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
            self.s3.put_bucket_encryption(
                Bucket=self.bucket,
                ServerSideEncryptionConfiguration={
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "AES256"
                            }
                        }
                    ]
                },
            )
            created = True

        for folder in folders:
            key = f"{self.root_prefix}/{folder}/"
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=b"")

        return {
            "created": created,
            "bucket": self.bucket,
            "uri": f"s3://{self.bucket}/{self.root_prefix}/",
            "folders": [f"s3://{self.bucket}/{self.root_prefix}/{f}/" for f in folders],
        }

    def object_key(self, source_folder: str, filename: str) -> str:
        return f"{self.root_prefix}/{source_folder.strip('/')}/{filename}"

    def upload_file(self, local_path: str | Path, source_folder: str, filename: str | None = None) -> str:
        local_path = Path(local_path)
        if not local_path.is_file():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        key = self.object_key(source_folder, filename or local_path.name)
        self.s3.upload_file(str(local_path), self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def list_prefix(self, source_folder: str, max_keys: int = 20) -> list[str]:
        prefix = f"{self.root_prefix}/{source_folder.strip('/')}/"
        response = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix, MaxKeys=max_keys
        )
        return [obj["Key"] for obj in response.get("Contents", []) if not obj["Key"].endswith("/")]
