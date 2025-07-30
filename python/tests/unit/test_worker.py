import json
from unittest.mock import MagicMock, patch

from bccp.worker import (
    Worker,
    batches_processed_counter,
    documents_stored_counter,
    storage_errors_counter,
)

# Removed duplicate fixtures - now using ones from conftest.py


@patch("bccp.worker.WARCIterator")
@patch("bccp.worker.trafilatura.extract")
def test_process_batch_ack_and_counter(
    mock_extract,
    mock_warciterator,
    worker_instance,
    sample_batch,
    mock_warc_record,
    mock_channel,
    mock_method_with_tag,
    mock_downloader,
    mock_object_store,
):
    """Test successful batch processing with counter increments."""
    # Use longer text to pass length filter (default min is 500 chars)
    mock_extract.return_value = (
        "x" * 600
    )  # 600 characters to pass default length filter
    mock_warciterator.return_value = [mock_warc_record]

    body = json.dumps(sample_batch).encode()
    before = batches_processed_counter._value.get()

    worker_instance.process_batch(mock_channel, mock_method_with_tag, None, body)

    after = batches_processed_counter._value.get()

    mock_downloader.download_and_unzip.assert_called_once_with("test.warc.gz", 0, 200)
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test-delivery-tag-123")
    assert after == before + 1
    mock_extract.assert_called_once()
    mock_object_store.store_document.assert_called_once()


@patch("bccp.worker.WARCIterator")
def test_process_batch_handles_empty_batch(
    mock_warciterator, worker_instance, mock_channel, mock_method_with_tag
):
    """Test processing of empty batch."""
    mock_warciterator.return_value = []

    body = json.dumps([]).encode()
    before = batches_processed_counter._value.get()

    worker_instance.process_batch(mock_channel, mock_method_with_tag, None, body)

    after = batches_processed_counter._value.get()
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test-delivery-tag-123")
    assert after == before + 1


@patch("bccp.worker.start_http_server")
def test_worker_run(mock_start_http_server):
    mock_channel = MagicMock()
    mock_object_store = MagicMock()
    worker = Worker(channel=mock_channel, object_store=mock_object_store)

    # Mock the start_consuming to raise KeyboardInterrupt (simulating Ctrl+C)
    mock_channel.start_consuming.side_effect = KeyboardInterrupt()

    # The new implementation catches KeyboardInterrupt and breaks gracefully
    worker.run()  # Should not raise an exception

    mock_start_http_server.assert_called_once_with(9001)
    mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
    mock_channel.basic_consume.assert_called_once()
    mock_channel.start_consuming.assert_called_once()
    mock_channel.stop_consuming.assert_called_once()


@patch("bccp.worker.WARCIterator")
@patch("bccp.worker.trafilatura.extract")
def test_process_batch_with_non_response_record(
    mock_extract,
    mock_warciterator,
    worker_instance,
    sample_batch,
    mock_warc_record_non_response,
    mock_channel,
    mock_method,
    mock_downloader,
):
    """Test that non-response WARC records don't trigger text extraction."""
    mock_warciterator.return_value = [mock_warc_record_non_response]

    body = json.dumps(sample_batch).encode()
    worker_instance.process_batch(mock_channel, mock_method, None, body)

    mock_downloader.download_and_unzip.assert_called_once_with("test.warc.gz", 0, 200)
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test-delivery-tag-123")
    mock_extract.assert_not_called()  # Should not extract from non-response records


def test_worker_initialization():
    # Test with custom parameters
    mock_downloader = MagicMock()
    mock_channel = MagicMock()
    mock_object_store = MagicMock()
    worker = Worker(
        downloader=mock_downloader, channel=mock_channel, object_store=mock_object_store
    )
    assert worker.downloader == mock_downloader
    assert worker.channel == mock_channel
    assert worker.object_store == mock_object_store


def test_worker_prometheus_metrics(mock_all_worker_metrics, mock_text_extraction):
    """Test that worker Prometheus metrics are incremented correctly."""
    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/\r\nContent-Length: 50\r\n\r\n<html>test content</html>"""  # noqa: E501

    mock_ch = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = "tag123"

    worker = Worker(
        downloader=mock_downloader, channel=MagicMock(), object_store=MagicMock()
    )

    test_batch = [
        {
            "surt_url": "example.com/",
            "metadata": {
                "filename": "test.warc.gz",
                "offset": "0",
                "length": "100",
            },
        }
    ]

    worker.process_batch(mock_ch, mock_method, None, json.dumps(test_batch).encode())

    # Verify metrics were called using the mocked metrics from fixture
    mock_all_worker_metrics["batches_processed_counter"].inc.assert_called_once()
    mock_all_worker_metrics["documents_processed_counter"].inc.assert_called_once()
    mock_all_worker_metrics["text_extracted_counter"].inc.assert_called_once()
    mock_all_worker_metrics["download_bytes_counter"].inc.assert_called()
    mock_all_worker_metrics["batch_processing_time_histogram"].observe.assert_called()
    mock_all_worker_metrics["warc_records_processed_counter"].labels.assert_called()


def test_worker_error_metrics(mock_all_worker_metrics):
    """Test that worker error metrics are incremented correctly."""
    from unittest.mock import MagicMock

    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.side_effect = Exception("Download failed")

    mock_ch = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = "tag123"

    worker = Worker(downloader=mock_downloader, channel=MagicMock())

    test_batch = [
        {"metadata": {"filename": "test.warc.gz", "offset": "0", "length": "100"}}
    ]

    worker.process_batch(mock_ch, mock_method, None, json.dumps(test_batch).encode())

    # Should still ack the message and record error
    mock_ch.basic_ack.assert_called_once_with(delivery_tag="tag123")
    mock_all_worker_metrics["processing_errors_counter"].labels.assert_called_with(
        error_type="document_processing"
    )


def test_worker_text_extraction_error(mock_all_worker_metrics):
    """Test text extraction error handling."""
    from unittest.mock import MagicMock

    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/\r\nWARC-Date: 2024-07-22T12:07:56Z\r\nWARC-Record-ID: <urn:uuid:12345>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 50\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\ntest content"""  # noqa: E501

    mock_ch = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = "tag123"

    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.side_effect = Exception("Extraction failed")

        worker = Worker(
            downloader=mock_downloader, channel=MagicMock(), object_store=MagicMock()
        )

        test_batch = [
            {"metadata": {"filename": "test.warc.gz", "offset": "0", "length": "100"}}
        ]

        worker.process_batch(
            mock_ch, mock_method, None, json.dumps(test_batch).encode()
        )

        # Document should be processed but text extraction should fail
        mock_all_worker_metrics["documents_processed_counter"].inc.assert_called()
        mock_all_worker_metrics["processing_errors_counter"].labels.assert_called_with(
            error_type="text_extraction"
        )


def test_worker_document_processing_method(
    mock_text_extraction, standard_test_batch, worker_with_custom_length_limits
):
    """Test the _process_document method directly."""
    worker_with_custom_length_limits.downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/\r\nWARC-Date: 2024-07-22T12:07:56Z\r\nWARC-Record-ID: <urn:uuid:12345>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 50\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\ntest content"""  # noqa: E501

    worker = worker_with_custom_length_limits

    test_item = standard_test_batch[0]  # Use the standard test batch fixture

    # Should complete without raising exceptions
    worker._process_document(test_item)

    worker.downloader.download_and_unzip.assert_called_once_with(
        "test.warc.gz", 0, 200  # Updated to match the fixture data
    )
    mock_text_extraction.assert_called_once()
    worker.object_store.store_document.assert_called_once()


@patch("bccp.worker.WARCIterator")
def test_worker_object_store_integration(
    mock_warciterator,
    sample_batch_item,
    mock_warc_record,
    get_counter_value,
    worker_with_custom_length_limits,
):
    """Test worker object store integration with document storage."""
    mock_extract_text = "This is extracted text content"
    mock_warciterator.return_value = [mock_warc_record]

    worker_with_custom_length_limits.downloader.download_and_unzip.return_value = (
        b"warc data"
    )
    worker_with_custom_length_limits.bucket_name = "test-bucket"
    worker = worker_with_custom_length_limits

    initial_stored = get_counter_value(documents_stored_counter)

    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = mock_extract_text

        worker._process_document(sample_batch_item)

        # Verify object store was called with correct data structure
        worker.object_store.store_document.assert_called_once()
        call_args = worker.object_store.store_document.call_args

        assert call_args[0][0] == "test-bucket"  # bucket name
        document_data = call_args[0][1]  # document data

        assert document_data["url"] == "http://example.com/test-page"
        assert document_data["surt_url"] == "example.com/test-page"
        assert document_data["timestamp"] == "20240722120756"
        assert document_data["text"] == mock_extract_text
        assert document_data["text_length"] == len(mock_extract_text)
        assert document_data["filename"] == "test.warc.gz"
        assert "processing_timestamp" in document_data

        # Verify counter was incremented
        final_stored = get_counter_value(documents_stored_counter)
        assert final_stored == initial_stored + 1


@patch("bccp.worker.WARCIterator")
def test_worker_object_store_failure(
    mock_warciterator, worker_with_custom_length_limits
):
    """Test worker handles object store failures gracefully."""
    worker_with_custom_length_limits.downloader.download_and_unzip.return_value = (
        b"warc data"
    )
    worker_with_custom_length_limits.object_store.store_document.return_value = (
        False  # Simulate failure
    )

    # Mock WARC record
    mock_record = MagicMock()
    mock_record.rec_type = "response"
    mock_record.content_stream.return_value.read.return_value = (
        b"<html>test content</html>"
    )
    mock_warciterator.return_value = [mock_record]

    worker = worker_with_custom_length_limits

    test_item = {
        "surt_url": "example.com/page",
        "timestamp": "20240722120756",
        "metadata": {
            "filename": "test.warc.gz",
            "offset": "0",
            "length": "100",
            "url": "http://example.com/page",
        },
    }

    initial_errors = storage_errors_counter.labels(
        error_type="store_document"
    )._value.get()
    initial_stored = documents_stored_counter._value.get()

    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = "Extracted text content"

        # Should not raise exception despite storage failure
        worker._process_document(test_item)

        # Verify error counter was incremented
        final_errors = storage_errors_counter.labels(
            error_type="store_document"
        )._value.get()
        assert final_errors > initial_errors

        # Verify stored counter was NOT incremented
        final_stored = documents_stored_counter._value.get()
        assert final_stored == initial_stored


def test_worker_tokenization_enabled(worker_with_tokenizer, mock_tokenizer):
    """Test worker with tokenization enabled."""
    # Test the tokenization method directly
    test_text = "This is a test sentence for tokenization."
    result = worker_with_tokenizer._tokenize_text(test_text)

    assert result is not None
    assert "tokens" in result
    assert "token_count" in result
    assert "tokenizer_name" in result
    assert result["tokens"] == [101, 2023, 3793, 102]
    assert result["token_count"] == 4
    assert result["tokenizer_name"] == "test-tokenizer"


def test_worker_without_tokenizer(worker_instance):
    """Test worker without tokenization."""
    # Test the tokenization method when no tokenizer is available
    result = worker_instance._tokenize_text("Some text")
    assert result is None


def test_worker_document_length_filtering(worker_with_custom_length_limits):
    """Test document length filtering functionality."""
    from bccp.worker import documents_filtered_counter

    worker = worker_with_custom_length_limits

    # Test valid document length
    valid_text = "This is a valid document with appropriate length."
    assert worker._is_document_length_valid(valid_text) is True

    # Test document too short
    short_text = "Short"
    initial_short_count = documents_filtered_counter.labels(
        filter_reason="too_short"
    )._value.get()
    assert worker._is_document_length_valid(short_text) is False
    final_short_count = documents_filtered_counter.labels(
        filter_reason="too_short"
    )._value.get()
    assert final_short_count > initial_short_count

    # Test document too long
    long_text = "x" * 100  # 100 characters, exceeds max of 50
    initial_long_count = documents_filtered_counter.labels(
        filter_reason="too_long"
    )._value.get()
    assert worker._is_document_length_valid(long_text) is False
    final_long_count = documents_filtered_counter.labels(
        filter_reason="too_long"
    )._value.get()
    assert final_long_count > initial_long_count


@patch("bccp.worker.WARCIterator")
@patch("bccp.worker.trafilatura.extract")
def test_worker_filters_short_documents(
    mock_extract,
    mock_warciterator,
    mock_warc_record,
    sample_batch_item,
    worker_with_strict_min_length,
):
    """Test that worker filters out documents that are too short."""
    # Mock extraction to return short text
    mock_extract.return_value = "Short"  # Only 5 characters
    mock_warciterator.return_value = [mock_warc_record]

    worker = worker_with_strict_min_length
    worker.downloader.download_and_unzip.return_value = b"warc data"

    worker._process_document(sample_batch_item)

    # Document should not be stored due to short length
    worker.object_store.store_document.assert_not_called()


@patch("bccp.worker.WARCIterator")
@patch("bccp.worker.trafilatura.extract")
def test_worker_filters_long_documents(
    mock_extract,
    mock_warciterator,
    mock_warc_record,
    sample_batch_item,
    worker_with_strict_max_length,
):
    """Test that worker filters out documents that are too long."""
    # Mock extraction to return very long text
    mock_extract.return_value = "x" * 1000  # 1000 characters
    mock_warciterator.return_value = [mock_warc_record]

    worker = worker_with_strict_max_length
    worker.downloader.download_and_unzip.return_value = b"warc data"

    worker._process_document(sample_batch_item)

    # Document should not be stored due to long length
    worker.object_store.store_document.assert_not_called()


@patch("bccp.worker.WARCIterator")
@patch("bccp.worker.trafilatura.extract")
def test_worker_processes_valid_length_documents(
    mock_extract,
    mock_warciterator,
    mock_warc_record,
    sample_batch_item,
    worker_with_strict_max_length,
):
    """Test that worker processes documents with valid length."""
    # Mock extraction to return text of valid length
    valid_text = "This is a document with valid length for processing."
    mock_extract.return_value = valid_text
    mock_warciterator.return_value = [mock_warc_record]

    worker = worker_with_strict_max_length
    worker.downloader.download_and_unzip.return_value = b"warc data"

    worker._process_document(sample_batch_item)

    # Document should be stored as it meets length requirements
    worker.object_store.store_document.assert_called_once()

    # Verify the stored document contains the expected text
    call_args = worker.object_store.store_document.call_args
    document_data = call_args[0][1]
    assert document_data["text"] == valid_text
    assert document_data["text_length"] == len(valid_text)
