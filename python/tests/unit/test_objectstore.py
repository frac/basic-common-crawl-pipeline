import json
from unittest.mock import MagicMock, patch

import pytest
from minio.error import S3Error

from bccp.objectstore import ObjectStore


@pytest.fixture
def mock_minio_client():
    """Mock MinIO client for testing."""
    with patch("bccp.objectstore.Minio") as mock_minio:
        client_instance = MagicMock()
        mock_minio.return_value = client_instance
        yield client_instance


def test_objectstore_initialization_with_defaults():
    """Test ObjectStore initialization with default values."""
    with patch("bccp.objectstore.Minio") as mock_minio:
        with patch.dict("os.environ", {}, clear=True):
            store = ObjectStore()

            assert store.endpoint == "localhost:9000"
            assert store.access_key == "minioadmin"
            assert store.secret_key == "minioadmin"
            assert store.secure is False

            mock_minio.assert_called_once_with(
                "localhost:9000",
                access_key="minioadmin",
                secret_key="minioadmin",
                secure=False,
            )


def test_objectstore_initialization_with_env_vars():
    """Test ObjectStore initialization with environment variables."""
    env_vars = {
        "MINIO_ENDPOINT": "minio.example.com:9000",
        "MINIO_ACCESS_KEY": "test_access",
        "MINIO_SECRET_KEY": "test_secret",
    }

    with patch("bccp.objectstore.Minio"):
        with patch.dict("os.environ", env_vars):
            store = ObjectStore()

            assert store.endpoint == "minio.example.com:9000"
            assert store.access_key == "test_access"
            assert store.secret_key == "test_secret"


def test_objectstore_initialization_with_params():
    """Test ObjectStore initialization with explicit parameters."""
    with patch("bccp.objectstore.Minio") as mock_minio:
        store = ObjectStore(
            endpoint="custom.minio.com:9000",
            access_key="custom_access",
            secret_key="custom_secret",
            secure=True,
        )

        assert store.endpoint == "custom.minio.com:9000"
        assert store.access_key == "custom_access"
        assert store.secret_key == "custom_secret"
        assert store.secure is True

        mock_minio.assert_called_once_with(
            "custom.minio.com:9000",
            access_key="custom_access",
            secret_key="custom_secret",
            secure=True,
        )


def test_ensure_bucket_exists_bucket_already_exists(mock_minio_client):
    """Test ensure_bucket_exists when bucket already exists."""
    mock_minio_client.bucket_exists.return_value = True

    store = ObjectStore()
    result = store.ensure_bucket_exists("test-bucket")

    assert result is True
    mock_minio_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_minio_client.make_bucket.assert_not_called()


def test_ensure_bucket_exists_creates_new_bucket(mock_minio_client):
    """Test ensure_bucket_exists creates bucket when it doesn't exist."""
    mock_minio_client.bucket_exists.return_value = False

    store = ObjectStore()
    result = store.ensure_bucket_exists("test-bucket")

    assert result is True
    mock_minio_client.bucket_exists.assert_called_once_with("test-bucket")
    mock_minio_client.make_bucket.assert_called_once_with("test-bucket")


def test_ensure_bucket_exists_handles_s3_error(mock_minio_client):
    """Test ensure_bucket_exists handles S3 errors gracefully."""
    mock_minio_client.bucket_exists.side_effect = S3Error(
        "AccessDenied", "Access denied", "resource", "request_id", "host_id", "response"
    )

    store = ObjectStore()
    result = store.ensure_bucket_exists("test-bucket")

    assert result is False
    mock_minio_client.bucket_exists.assert_called_once_with("test-bucket")


def test_store_document_success(mock_minio_client):
    """Test successful document storage."""
    mock_minio_client.bucket_exists.return_value = True

    store = ObjectStore()
    document_data = {
        "url": "http://example.com/test",
        "text": "This is test content",
        "timestamp": "2024-07-22T12:07:56Z",
        "text_length": 20,
    }

    result = store.store_document("test-bucket", document_data)

    assert result is True
    mock_minio_client.put_object.assert_called_once()

    # Verify put_object was called with correct parameters
    call_args = mock_minio_client.put_object.call_args
    assert call_args.kwargs["bucket_name"] == "test-bucket"
    assert call_args.kwargs["content_type"] == "application/jsonl"
    assert "documents/2024-07-22/" in call_args.kwargs["object_name"]
    assert call_args.kwargs["object_name"].endswith(".jsonl")

    # Verify data content
    uploaded_data = call_args.kwargs["data"].getvalue().decode("utf-8")
    parsed_data = json.loads(uploaded_data)
    assert parsed_data == document_data


def test_store_document_bucket_creation(mock_minio_client):
    """Test document storage when bucket needs to be created."""
    mock_minio_client.bucket_exists.return_value = False

    store = ObjectStore()
    document_data = {"url": "http://example.com", "text": "content"}

    result = store.store_document("new-bucket", document_data)

    assert result is True
    mock_minio_client.bucket_exists.assert_called_once_with("new-bucket")
    mock_minio_client.make_bucket.assert_called_once_with("new-bucket")
    mock_minio_client.put_object.assert_called_once()


def test_store_document_handles_s3_error(mock_minio_client):
    """Test document storage handles S3 errors."""
    mock_minio_client.bucket_exists.return_value = True
    mock_minio_client.put_object.side_effect = S3Error(
        "NoSuchBucket",
        "Bucket does not exist",
        "resource",
        "request_id",
        "host_id",
        "response",
    )

    store = ObjectStore()
    document_data = {"url": "http://example.com", "text": "content"}

    result = store.store_document("test-bucket", document_data)

    assert result is False


def test_store_document_object_key_generation(mock_minio_client):
    """Test object key generation for document storage."""
    mock_minio_client.bucket_exists.return_value = True

    store = ObjectStore()
    document_data = {
        "url": "http://example.com/specific-path",
        "text": "content",
        "timestamp": "2024-12-25T10:30:45Z",
    }

    store.store_document("test-bucket", document_data)

    call_args = mock_minio_client.put_object.call_args
    object_name = call_args.kwargs["object_name"]

    # Should use date from timestamp in path
    assert object_name.startswith("documents/2024-12-25/")
    assert object_name.endswith(".jsonl")

    # Should generate consistent hash for same URL
    url_hash = abs(hash("http://example.com/specific-path"))
    assert f"{url_hash}.jsonl" in object_name


def test_store_document_missing_timestamp(mock_minio_client):
    """Test document storage with missing timestamp uses current time."""
    mock_minio_client.bucket_exists.return_value = True

    with patch("bccp.objectstore.datetime") as mock_datetime:
        mock_datetime.now.return_value.isoformat.return_value = "2024-07-22T15:30:45Z"

        store = ObjectStore()
        document_data = {"url": "http://example.com", "text": "content"}

        store.store_document("test-bucket", document_data)

        call_args = mock_minio_client.put_object.call_args
        object_name = call_args.kwargs["object_name"]
        assert object_name.startswith("documents/2024-07-22/")


def test_health_check_success(mock_minio_client):
    """Test successful health check."""
    mock_minio_client.list_buckets.return_value = []

    store = ObjectStore()
    result = store.health_check()

    assert result is True
    mock_minio_client.list_buckets.assert_called_once()


def test_health_check_failure(mock_minio_client):
    """Test health check failure."""
    mock_minio_client.list_buckets.side_effect = Exception("Connection failed")

    store = ObjectStore()
    result = store.health_check()

    assert result is False
    mock_minio_client.list_buckets.assert_called_once()


def test_store_document_with_special_characters(mock_minio_client):
    """Test document storage with special characters in text."""
    mock_minio_client.bucket_exists.return_value = True

    store = ObjectStore()
    document_data = {
        "url": "http://example.com",
        "text": (
            "Special chars: àáâãäåæ 中文 🚀 \"quotes\" 'apostrophes' "
            "\n newlines \t tabs"
        ),
        "timestamp": "2024-07-22T12:07:56Z",
    }

    result = store.store_document("test-bucket", document_data)

    assert result is True

    # Verify the data was properly encoded
    call_args = mock_minio_client.put_object.call_args
    uploaded_data = call_args.kwargs["data"].getvalue().decode("utf-8")
    parsed_data = json.loads(uploaded_data)
    assert parsed_data["text"] == document_data["text"]


def test_store_document_empty_data(mock_minio_client):
    """Test document storage with minimal data."""
    mock_minio_client.bucket_exists.return_value = True

    store = ObjectStore()
    document_data = {}

    result = store.store_document("test-bucket", document_data)

    assert result is True
    mock_minio_client.put_object.assert_called_once()

    call_args = mock_minio_client.put_object.call_args
    uploaded_data = call_args.kwargs["data"].getvalue().decode("utf-8")
    parsed_data = json.loads(uploaded_data)
    assert parsed_data == {}


def test_store_document_large_text(mock_minio_client):
    """Test document storage with large text content."""
    mock_minio_client.bucket_exists.return_value = True

    store = ObjectStore()
    large_text = "A" * 100000  # 100KB of text
    document_data = {
        "url": "http://example.com",
        "text": large_text,
        "text_length": len(large_text),
    }

    result = store.store_document("test-bucket", document_data)

    assert result is True

    call_args = mock_minio_client.put_object.call_args
    uploaded_data = call_args.kwargs["data"].getvalue()
    assert len(uploaded_data) > 100000  # Should be larger due to JSON encoding
