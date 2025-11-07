from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


class CloudflareR2Storage:
    """Simple helper around the S3 compatible Cloudflare R2 API."""

    def __init__(
        self,
        account_id: str,
        access_key_id: str,
        secret_access_key: str,
        bucket: str,
        *,
        region: str = "auto",
        endpoint: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        endpoint_url = endpoint or f"https://{account_id}.r2.cloudflarestorage.com"
        session = boto3.session.Session()
        self._client = session.client(
            "s3",
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region,
            endpoint_url=endpoint_url,
            config=BotoConfig(connect_timeout=timeout, read_timeout=timeout),
        )
        self._bucket = bucket

    def load_json(self, key: str) -> Dict[str, Any]:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(f"Unable to load remote configuration from {key}: {exc}") from exc
        body = response["Body"].read().decode("utf-8")
        return json.loads(body) if body else {}

    def save_json(self, key: str, data: Dict[str, Any]) -> None:
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True).encode("utf-8")
        try:
            self._client.put_object(Bucket=self._bucket, Key=key, Body=payload)
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(f"Unable to persist remote state to {key}: {exc}") from exc

    def download_file(self, key: str, destination: Path) -> Path:
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            self._client.download_file(self._bucket, key, str(destination))
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(f"Unable to download {key} to {destination}: {exc}") from exc
        return destination

    def upload_file(self, key: str, source: Path) -> None:
        if not source.exists():
            raise FileNotFoundError(source)
        try:
            self._client.upload_file(str(source), self._bucket, key)
        except (ClientError, BotoCoreError) as exc:
            raise RuntimeError(f"Unable to upload {source} to {key}: {exc}") from exc


__all__ = ["CloudflareR2Storage"]
