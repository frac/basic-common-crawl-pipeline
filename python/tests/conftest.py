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
    """Sample WARC data for worker tests with realistic HTML."""
    return (
        b"WARC/1.0\r\nWARC-Type: response\r\n"
        b"WARC-Target-URI: http://example.com/\r\n"
        b"WARC-Date: 2024-07-22T12:07:56Z\r\n"
        b"WARC-Record-ID: <urn:uuid:12345>\r\n"
        b"Content-Type: application/http; msgtype=response\r\n"
        b"Content-Length: 250\r\n\r\n"
        b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n"
        b"<html><head><title>Example Page</title></head>"
        b"<body><h1>Welcome to Example.com</h1>"
        b"<p>This is a sample paragraph with some meaningful content "
        b"that trafilatura can extract.</p>"
        b"<p>Another paragraph with more <em>text content</em> for testing.</p>"
        b"<div class='content'><p>Even more content in a div for testing.</p></div>"
        b"</body></html>"
    )


# Mock factories
@pytest.fixture
def mock_downloader(realistic_warc_data):
    """Mock downloader with realistic WARC data for E2E testing."""
    downloader = MagicMock()
    downloader.download_and_unzip.return_value = realistic_warc_data
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


# Mock method objects
@pytest.fixture
def mock_method_with_tag():
    """Mock AMQP method with a specific delivery tag."""
    method = MagicMock()
    method.delivery_tag = "test-delivery-tag-123"
    return method


# Worker-specific fixtures
@pytest.fixture
def worker_instance(mock_downloader, mock_channel, mock_object_store):
    """Basic worker instance for testing."""
    from bccp.worker import Worker

    return Worker(
        downloader=mock_downloader,
        channel=mock_channel,
        object_store=mock_object_store,
    )


@pytest.fixture
def worker_with_failing_store(mock_downloader, mock_channel, mock_object_store_failure):
    """Worker instance with failing object store."""
    from bccp.worker import Worker

    return Worker(
        downloader=mock_downloader,
        channel=mock_channel,
        object_store=mock_object_store_failure,
    )


# Batcher test helpers
@pytest.fixture
def fake_downloader():
    """Factory for creating fake downloaders with custom data."""
    from bccp.commoncrawl import Downloader

    class FakeDownloader(Downloader):
        def __init__(self, data):
            self.data = data

        def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
            return (
                self.data.encode("utf-8") if isinstance(self.data, str) else self.data
            )

    return FakeDownloader


@pytest.fixture
def channel_spy():
    """Mock channel that tracks messages sent."""
    from bccp.rabbitmq import MessageQueueChannel

    class ChannelSpy(MessageQueueChannel):
        def __init__(self):
            self.num_called = 0
            self.sent_messages = []

        def basic_publish(self, exchange, routing_key, body):
            self.num_called += 1
            self.sent_messages.append(body)

    return ChannelSpy()


# Common test batch data structures
@pytest.fixture
def standard_test_batch():
    """Standard test batch data used across multiple tests."""
    return [
        {
            "surt_url": "example.com/test",
            "timestamp": "20240722120756",
            "metadata": {
                "url": "http://example.com/test",
                "status": "200",
                "languages": ["eng"],
                "filename": "test.warc.gz",
                "offset": "0",
                "length": "200",
            },
        }
    ]


@pytest.fixture
def multi_url_test_batch():
    """Multi-URL test batch for testing batch processing."""
    return [
        {
            "surt_url": "example1.com/",
            "timestamp": "20240722120756",
            "metadata": {
                "url": "http://example1.com/",
                "status": "200",
                "languages": ["eng"],
                "filename": "test1.warc.gz",
                "offset": "0",
                "length": "200",
            },
        },
        {
            "surt_url": "example2.com/",
            "timestamp": "20240722120757",
            "metadata": {
                "url": "http://example2.com/",
                "status": "200",
                "languages": ["eng"],
                "filename": "test2.warc.gz",
                "offset": "200",
                "length": "300",
            },
        },
    ]


# Prometheus metrics helpers
@pytest.fixture
def prometheus_counter_mock():
    """Mock Prometheus counter for testing metrics."""
    counter = MagicMock()
    counter._value.get.return_value = 0
    counter.inc = MagicMock()
    counter.labels = MagicMock(return_value=counter)
    return counter


@pytest.fixture
def mock_all_worker_metrics(monkeypatch):
    """Mock all worker Prometheus metrics."""
    mock_metrics = {}
    metric_names = [
        "batches_processed_counter",
        "documents_processed_counter",
        "text_extracted_counter",
        "download_bytes_counter",
        "batch_processing_time_histogram",
        "warc_records_processed_counter",
        "processing_errors_counter",
        "storage_errors_counter",
        "documents_stored_counter",
        "documents_filtered_counter",
        "document_length_histogram",
        "tokenization_counter",
        "tokenization_errors_counter",
        "token_count_histogram",
    ]

    for name in metric_names:
        mock_metric = MagicMock()
        mock_metric._value.get.return_value = 0
        mock_metric.inc = MagicMock()
        mock_metric.observe = MagicMock()
        mock_metric.labels = MagicMock(return_value=mock_metric)
        mock_metrics[name] = mock_metric
        monkeypatch.setattr(f"bccp.worker.{name}", mock_metric)

    return mock_metrics


@pytest.fixture
def mock_all_batcher_metrics(monkeypatch):
    """Mock all batcher Prometheus metrics."""
    mock_metrics = {}
    metric_names = [
        "cdx_chunks_processed_counter",
        "urls_processed_counter",
        "urls_filtered_counter",
        "batches_published_counter",
        "errors_counter",
    ]

    for name in metric_names:
        mock_metric = MagicMock()
        mock_metric._value.get.return_value = 0
        mock_metric.inc = MagicMock()
        mock_metric.labels = MagicMock(return_value=mock_metric)
        mock_metrics[name] = mock_metric
        monkeypatch.setattr(f"bccp.batcher.{name}", mock_metric)

    return mock_metrics


# Text extraction helpers
@pytest.fixture
def mock_text_extraction():
    """Mock trafilatura text extraction."""
    from unittest.mock import patch

    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = "Extracted text content for testing"
        yield mock_extract


# Tokenization helpers
@pytest.fixture
def mock_tokenizer():
    """Mock AutoTokenizer for testing."""
    from unittest.mock import MagicMock

    tokenizer = MagicMock()
    tokenizer.encode.return_value = [101, 2023, 3793, 102]  # Example token IDs
    tokenizer.name_or_path = "test-tokenizer"
    tokenizer.model_max_length = 512
    return tokenizer


@pytest.fixture
def worker_with_tokenizer(
    mock_downloader, mock_channel, mock_object_store, mock_tokenizer
):
    """Worker instance with tokenizer enabled."""
    from unittest.mock import patch

    from bccp.worker import Worker

    with patch(
        "bccp.worker.AutoTokenizer.from_pretrained", return_value=mock_tokenizer
    ):
        return Worker(
            downloader=mock_downloader,
            channel=mock_channel,
            object_store=mock_object_store,
            tokenizer_name="test-tokenizer",
        )


@pytest.fixture
def worker_with_custom_length_limits(mock_downloader, mock_channel, mock_object_store):
    """Worker instance with custom document length limits for testing."""
    from bccp.worker import Worker

    return Worker(
        downloader=mock_downloader,
        channel=mock_channel,
        object_store=mock_object_store,
        min_doc_length=10,
        max_doc_length=50,
    )


@pytest.fixture
def worker_with_strict_min_length(mock_downloader, mock_channel, mock_object_store):
    """Worker instance with strict minimum length for filtering tests."""
    from bccp.worker import Worker

    return Worker(
        downloader=mock_downloader,
        channel=mock_channel,
        object_store=mock_object_store,
        min_doc_length=100,
        max_doc_length=1000000,
    )


@pytest.fixture
def worker_with_strict_max_length(mock_downloader, mock_channel, mock_object_store):
    """Worker instance with strict maximum length for filtering tests."""
    from bccp.worker import Worker

    return Worker(
        downloader=mock_downloader,
        channel=mock_channel,
        object_store=mock_object_store,
        min_doc_length=10,
        max_doc_length=100,
    )


# Complex test data
@pytest.fixture
def mixed_status_cdx_data():
    """CDX data with mixed status codes for filtering tests."""
    return b"""url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"], "filename": "test1.warc.gz"}  # noqa: E501
url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"], "filename": "test2.warc.gz"}  # noqa: E501
url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"], "filename": "test3.warc.gz"}  # noqa: E501
url4 20240722120759 {"url": "http://example4.com/", "status": "200", "languages": ["eng"], "filename": "test4.warc.gz"}"""  # noqa: E501


@pytest.fixture
def realistic_warc_data():
    """Realistic WARC response data for integration tests with rich HTML content."""
    # Construct the HTML content first to calculate correct Content-Length
    html_content = b"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Test Article - Example News</title>
    <meta name="description" content="A sample article for testing">
</head>
<body>
    <header><h1>Example News Site</h1></header>
    <main>
        <article>
            <h2>Breaking: New Technology Advances</h2>
            <p class="lead">Scientists have made significant breakthroughs in artificial intelligence research this week.</p>  # noqa: E501
            <p>The research team at Example University published their findings in the latest journal. The study shows promising results for future applications in various industries.</p>  # noqa: E501
            <p>According to lead researcher Dr. Jane Smith, "This technology could revolutionize how we process information and solve complex problems."</p>  # noqa: E501
            <p>The implications for healthcare, finance, and education are particularly noteworthy according to industry experts.</p>  # noqa: E501
        </article>
    </main>
    <footer><p>Copyright 2024 Example News</p></footer>
</body>
</html>"""

    # HTTP response headers + HTML content
    http_response = (
        b"""HTTP/1.1 200 OK\r
Content-Type: text/html; charset=utf-8\r
Content-Length: """
        + str(len(html_content)).encode()
        + b"""\r
\r
"""
        + html_content
    )

    # Calculate total WARC content length (HTTP response length)
    content_length = len(http_response)

    # Construct the complete WARC record
    return (
        b"""WARC/1.0\r
WARC-Type: response\r
WARC-Target-URI: http://example.com/test\r
WARC-Date: 2024-07-22T12:07:56Z\r
WARC-Record-ID: <urn:uuid:12345>\r
Content-Type: application/http; msgtype=response\r
Content-Length: """
        + str(content_length).encode()
        + b"""\r
\r
"""
        + http_response
    )


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
