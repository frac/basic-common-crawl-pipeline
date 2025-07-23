"""Shared pytest fixtures for all test modules."""

from unittest.mock import MagicMock

import pytest


# Common test data fixtures
@pytest.fixture
def sample_batch_item():
    """Single batch item with realistic data."""
    return {
        "surt_url": "example.com/test-page",
        "timestamp": "20240722120756",
        "metadata": {
            "filename": "test.warc.gz",
            "offset": "0",
            "length": "200",
            "url": "http://example.com/test-page",
            "status": "200",
            "languages": ["eng"],
        },
    }


@pytest.fixture
def sample_batch(sample_batch_item):
    """Sample batch data for testing."""
    return [sample_batch_item]


@pytest.fixture
def sample_cdx_data():
    """Sample CDX data for batcher tests."""
    return (
        b'url1 20240722120756 {"url": "http://example1.com/", "status": "200", '
        b'"languages": ["eng"], "filename": "test1.warc.gz"}\n'
        b'url2 20240722120757 {"url": "http://example2.com/", "status": "200", '
        b'"languages": ["eng"], "filename": "test2.warc.gz"}'
    )


@pytest.fixture
def sample_warc_data():
    """Sample WARC data for worker tests."""
    return (
        b"WARC/1.0\r\nWARC-Type: response\r\n"
        b"WARC-Target-URI: http://example.com/\r\n"
        b"WARC-Date: 2024-07-22T12:07:56Z\r\n"
        b"WARC-Record-ID: <urn:uuid:12345>\r\n"
        b"Content-Type: application/http; msgtype=response\r\n"
        b"Content-Length: 100\r\n\r\n"
        b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n"
        b"<html><body><h1>Test Page</h1><p>Sample content for testing.</p></body></html>"
    )


# Mock factories
@pytest.fixture
def mock_downloader(sample_warc_data):
    """Mock downloader with realistic WARC data."""
    downloader = MagicMock()
    downloader.download_and_unzip.return_value = sample_warc_data
    return downloader


@pytest.fixture
def mock_batcher_downloader(sample_cdx_data):
    """Mock downloader for batcher tests with CDX data."""
    downloader = MagicMock()
    downloader.download_and_unzip.return_value = sample_cdx_data
    return downloader


@pytest.fixture
def mock_object_store():
    """Mock object store that succeeds by default."""
    store = MagicMock()
    store.store_document.return_value = True
    store.health_check.return_value = True
    store.ensure_bucket_exists.return_value = True
    return store


@pytest.fixture
def mock_object_store_failure():
    """Mock object store that fails operations."""
    store = MagicMock()
    store.store_document.return_value = False
    store.health_check.return_value = False
    store.ensure_bucket_exists.return_value = False
    return store


@pytest.fixture
def mock_channel():
    """Mock RabbitMQ channel."""
    channel = MagicMock()
    return channel


@pytest.fixture
def mock_method():
    """Mock AMQP method with delivery tag."""
    method = MagicMock()
    method.delivery_tag = "test-delivery-tag-123"
    return method


@pytest.fixture
def mock_warc_record():
    """Mock WARC record for response processing."""
    record = MagicMock()
    record.rec_type = "response"
    record.content_stream.return_value.read.return_value = (
        b"<html><body><h1>Test</h1><p>Content</p></body></html>"
    )
    return record


@pytest.fixture
def mock_warc_record_non_response():
    """Mock WARC record for non-response types."""
    record = MagicMock()
    record.rec_type = "request"
    return record


@pytest.fixture
def mock_index_reader():
    """Mock index reader with test data."""
    reader = MagicMock()
    reader.__iter__ = MagicMock(
        return_value=iter(
            [
                ["test_entry1", "test1.gz", "0", "100", "1"],
                ["test_entry2", "test2.gz", "100", "150", "2"],
            ]
        )
    )
    return reader


@pytest.fixture
def mock_single_index_reader():
    """Mock index reader with single entry - commonly used pattern."""
    reader = MagicMock()
    reader.__iter__ = MagicMock(
        return_value=iter([["test_entry", "test.gz", "0", "100", "1"]])
    )
    return reader


# Message queue mock for integration tests
class MockMessageQueue:
    """Mock message queue for integration testing without RabbitMQ."""

    def __init__(self):
        self.messages = []
        self.acked_messages = []

    def basic_publish(self, exchange, routing_key, body):
        self.messages.append(body)

    def basic_ack(self, delivery_tag):
        self.acked_messages.append(delivery_tag)

    def get_messages(self):
        return self.messages


@pytest.fixture
def mock_message_queue():
    """Mock message queue for integration tests."""
    return MockMessageQueue()


# Metric value getters for cleaner test code
@pytest.fixture
def get_counter_value():
    """Helper to get current counter values."""

    def _get_value(counter):
        return counter._value.get()

    return _get_value


@pytest.fixture
def get_labeled_counter_value():
    """Helper to get labeled counter values."""

    def _get_value(counter, **labels):
        return counter.labels(**labels)._value.get()

    return _get_value
