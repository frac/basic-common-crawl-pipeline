import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from minio import Minio
from minio.error import S3Error

from .logging import log_with_fields, setup_logger


class ObjectStore:
    """MinIO object store client for storing extracted text content."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: bool = False,
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.secure = secure
        self.logger = setup_logger("objectstore")

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """Ensure bucket exists, create if it doesn't."""
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                log_with_fields(
                    self.logger, "info", "Created bucket", bucket=bucket_name
                )
            return True
        except S3Error as e:
            log_with_fields(
                self.logger,
                "error",
                "Failed to ensure bucket exists",
                bucket=bucket_name,
                error=str(e),
            )
            return False

    def store_document(self, bucket_name: str, document_data: Dict[str, Any]) -> bool:
        """Store document data in JSONL format."""
        try:
            self.ensure_bucket_exists(bucket_name)

            # Generate object key based on URL and timestamp
            url_hash = hash(document_data.get("url", "unknown"))
            timestamp = document_data.get(
                "timestamp", datetime.now(timezone.utc).isoformat()
            )
            object_key = f"documents/{timestamp[:10]}/{abs(url_hash)}.jsonl"

            # Convert to JSONL format
            jsonl_data = json.dumps(document_data)

            # Upload to MinIO - convert bytes to file-like object
            data_bytes = jsonl_data.encode("utf-8")
            data_stream = io.BytesIO(data_bytes)

            self.client.put_object(
                bucket_name=bucket_name,
                object_name=object_key,
                data=data_stream,
                length=len(data_bytes),
                content_type="application/jsonl",
            )

            log_with_fields(
                self.logger,
                "debug",
                "Document stored successfully",
                bucket=bucket_name,
                object_key=object_key,
                size_bytes=len(jsonl_data.encode("utf-8")),
            )

            return True

        except S3Error as e:
            log_with_fields(
                self.logger,
                "error",
                "Failed to store document",
                bucket=bucket_name,
                error=str(e),
                url=document_data.get("url", "unknown"),
            )
            return False

    def health_check(self) -> bool:
        """Check if MinIO connection is healthy."""
        try:
            # Try to list buckets as a simple health check
            list(self.client.list_buckets())
            return True
        except Exception as e:
            log_with_fields(
                self.logger, "error", "Object store health check failed", error=str(e)
            )
            return False
