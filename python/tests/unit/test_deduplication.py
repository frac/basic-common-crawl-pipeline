"""Tests for URL deduplication functionality."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from bccp.deduplication import URLDeduplicator


@pytest.fixture
def mock_redis():
    """Mock Redis client for testing."""
    redis_mock = MagicMock()
    redis_mock.ping.return_value = True
    redis_mock.exists.return_value = 0
    redis_mock.set.return_value = True
    redis_mock.get.return_value = None
    redis_mock.keys.return_value = []
    return redis_mock


@pytest.fixture
def deduplicator(mock_redis):
    """URLDeduplicator instance with mocked Redis."""
    with patch("bccp.deduplication.redis.Redis", return_value=mock_redis):
        return URLDeduplicator()


def test_url_normalization(deduplicator):
    """Test URL normalization functionality."""
    # Test basic normalization
    assert deduplicator._normalize_url("HTTP://EXAMPLE.COM/PATH") == "http://example.com/path"
    
    # Test query parameter sorting
    url1 = deduplicator._normalize_url("http://example.com?b=2&a=1")
    url2 = deduplicator._normalize_url("http://example.com?a=1&b=2")
    assert url1 == url2 == "http://example.com?a=1&b=2"
    
    # Test whitespace handling
    assert deduplicator._normalize_url("  http://example.com/  ") == "http://example.com/"


def test_url_hash_generation(deduplicator):
    """Test URL hash generation."""
    url = "http://example.com/test"
    hash1 = deduplicator._url_hash(url)
    hash2 = deduplicator._url_hash(url)
    
    # Same URL should produce same hash
    assert hash1 == hash2
    
    # Hash should be 16 characters
    assert len(hash1) == 16
    
    # Different URLs should produce different hashes
    different_hash = deduplicator._url_hash("http://different.com/test")
    assert hash1 != different_hash


def test_try_reserve_url_success(deduplicator, mock_redis):
    """Test successful URL reservation."""
    mock_redis.set.return_value = True  # Successful reservation
    
    result = deduplicator.try_reserve_url("http://example.com/test", "CC-MAIN-2024-30")
    
    assert result is True
    mock_redis.set.assert_called_once()
    
    # Check that the call was made with correct parameters
    call_args = mock_redis.set.call_args
    assert call_args[1]["nx"] is True  # Should use NX flag
    
    # Check the stored data structure
    stored_data = json.loads(call_args[0][1])
    assert stored_data["status"] == "reserved"
    assert stored_data["crawl_version"] == "CC-MAIN-2024-30"
    assert stored_data["url"] == "http://example.com/test"


def test_try_reserve_url_already_exists(deduplicator, mock_redis):
    """Test URL reservation when URL already exists."""
    mock_redis.set.return_value = None  # Key already exists
    
    result = deduplicator.try_reserve_url("http://example.com/test", "CC-MAIN-2024-30")
    
    assert result is False
    mock_redis.set.assert_called_once()


def test_is_url_processed(deduplicator, mock_redis):
    """Test URL processed check."""
    # URL exists
    mock_redis.exists.return_value = 1
    assert deduplicator.is_url_processed("http://example.com/test") is True
    
    # URL doesn't exist
    mock_redis.exists.return_value = 0
    assert deduplicator.is_url_processed("http://example.com/test") is False


def test_mark_url_processed_success(deduplicator, mock_redis):
    """Test marking URL as processed successfully."""
    # Mock existing reservation data
    existing_data = {
        "url": "http://example.com/test",
        "crawl_version": "CC-MAIN-2024-30",
        "status": "reserved",
        "reserved_at": int(time.time()),
    }
    mock_redis.get.return_value = json.dumps(existing_data)
    
    result = deduplicator.mark_url_processed("http://example.com/test", success=True)
    
    assert result is True
    
    # Check that URL was updated with completed status
    mock_redis.set.assert_called()
    call_args = mock_redis.set.call_args
    updated_data = json.loads(call_args[0][1])
    assert updated_data["status"] == "completed"
    assert "processed_at" in updated_data


def test_mark_url_processed_failure(deduplicator, mock_redis):
    """Test marking URL as failed."""
    # Mock existing reservation data
    existing_data = {
        "url": "http://example.com/test",
        "crawl_version": "CC-MAIN-2024-30",
        "status": "reserved",
        "reserved_at": int(time.time()),
    }
    mock_redis.get.return_value = json.dumps(existing_data)
    
    result = deduplicator.mark_url_processed(
        "http://example.com/test", success=False, error_reason="extraction_failed"
    )
    
    assert result is True
    
    # Check that URL was updated with failed status
    mock_redis.set.assert_called()
    call_args = mock_redis.set.call_args
    updated_data = json.loads(call_args[0][1])
    assert updated_data["status"] == "failed"
    assert updated_data["error_reason"] == "extraction_failed"
    assert "processed_at" in updated_data


def test_mark_url_processed_nonexistent(deduplicator, mock_redis):
    """Test marking non-existent URL as processed."""
    mock_redis.get.return_value = None  # No existing data
    
    result = deduplicator.mark_url_processed("http://example.com/test", success=True)
    
    assert result is False


def test_get_processed_count(deduplicator, mock_redis):
    """Test getting processed URL count."""
    mock_redis.keys.return_value = ["key1", "key2", "key3"]
    
    count = deduplicator.get_processed_count()
    
    assert count == 3
    mock_redis.keys.assert_called_once_with("bccp:urls:*")


def test_get_url_info(deduplicator, mock_redis):
    """Test getting URL information."""
    url_data = {
        "url": "http://example.com/test",
        "crawl_version": "CC-MAIN-2024-30",
        "status": "completed",
        "processed_at": int(time.time()),
    }
    mock_redis.get.return_value = json.dumps(url_data)
    
    result = deduplicator.get_url_info("http://example.com/test")
    
    assert result == url_data


def test_clear_all(deduplicator, mock_redis):
    """Test clearing all processed URLs."""
    mock_redis.keys.return_value = ["key1", "key2"]
    
    result = deduplicator.clear_all()
    
    assert result is True
    mock_redis.keys.assert_called_once_with("bccp:urls:*")
    mock_redis.delete.assert_called_once_with("key1", "key2")


def test_redis_connection_failure():
    """Test handling Redis connection failure."""
    with patch("bccp.deduplication.redis.Redis") as mock_redis_class:
        mock_redis_instance = MagicMock()
        mock_redis_instance.ping.side_effect = Exception("Connection failed")
        mock_redis_class.return_value = mock_redis_instance
        
        with pytest.raises(Exception):
            URLDeduplicator()


def test_redis_operation_error_handling(deduplicator, mock_redis):
    """Test handling Redis operation errors."""
    mock_redis.set.side_effect = Exception("Redis error")
    
    # Should return False on error, not raise exception
    result = deduplicator.try_reserve_url("http://example.com/test", "CC-MAIN-2024-30")
    assert result is False
    
    # Should return False on error for is_url_processed
    mock_redis.exists.side_effect = Exception("Redis error")
    result = deduplicator.is_url_processed("http://example.com/test")
    assert result is False