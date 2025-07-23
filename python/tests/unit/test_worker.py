import json
from unittest.mock import MagicMock, patch

import pytest

from bccp.worker import (
    Worker,
    batches_processed_counter,
    documents_stored_counter,
    storage_errors_counter,
)


@pytest.fixture
def worker(mock_downloader, mock_channel, mock_object_store):
    """Worker instance with mocked dependencies."""
    return Worker(
        downloader=mock_downloader, channel=mock_channel, object_store=mock_object_store
    )


@pytest.fixture
def worker_with_failing_store(mock_downloader, mock_channel, mock_object_store_failure):
    """Worker instance with failing object store."""
    return Worker(
        downloader=mock_downloader,
        channel=mock_channel,
        object_store=mock_object_store_failure,
    )


@patch("bccp.worker.WARCIterator")
@patch("bccp.worker.trafilatura.extract")
def test_process_batch_ack_and_counter(
    mock_extract,
    mock_warciterator,
    worker,
    sample_batch,
    mock_warc_record,
    mock_channel,
    mock_method,
    mock_downloader,
    mock_object_store,
):
    """Test successful batch processing with counter increments."""
    mock_extract.return_value = "extracted text content"
    mock_warciterator.return_value = [mock_warc_record]

    body = json.dumps(sample_batch).encode()
    before = batches_processed_counter._value.get()

    worker.process_batch(mock_channel, mock_method, None, body)

    after = batches_processed_counter._value.get()

    mock_downloader.download_and_unzip.assert_called_once_with("test.warc.gz", 0, 200)
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test-delivery-tag-123")
    assert after == before + 1
    mock_extract.assert_called_once()
    mock_object_store.store_document.assert_called_once()


@patch("bccp.worker.WARCIterator")
def test_process_batch_handles_empty_batch(
    mock_warciterator, worker, mock_channel, mock_method
):
    """Test processing of empty batch."""
    mock_warciterator.return_value = []

    body = json.dumps([]).encode()
    before = batches_processed_counter._value.get()

    worker.process_batch(mock_channel, mock_method, None, body)

    after = batches_processed_counter._value.get()
    mock_channel.basic_ack.assert_called_once_with(delivery_tag="test-delivery-tag-123")
    assert after == before + 1


@patch("bccp.worker.start_http_server")
def test_worker_run(mock_start_http_server):
    mock_channel = MagicMock()
    mock_object_store = MagicMock()
    worker = Worker(channel=mock_channel, object_store=mock_object_store)

    # Mock the start_consuming to prevent infinite loop
    mock_channel.start_consuming.side_effect = KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        worker.run()

    mock_start_http_server.assert_called_once_with(9001)
    mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
    mock_channel.basic_consume.assert_called_once()
    mock_channel.start_consuming.assert_called_once()


@patch("bccp.worker.WARCIterator")
@patch("bccp.worker.trafilatura.extract")
def test_process_batch_with_non_response_record(
    mock_extract,
    mock_warciterator,
    worker,
    sample_batch,
    mock_warc_record_non_response,
    mock_channel,
    mock_method,
    mock_downloader,
):
    """Test that non-response WARC records don't trigger text extraction."""
    mock_warciterator.return_value = [mock_warc_record_non_response]

    body = json.dumps(sample_batch).encode()
    worker.process_batch(mock_channel, mock_method, None, body)

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


def test_worker_prometheus_metrics(monkeypatch):
    """Test that worker Prometheus metrics are incremented correctly."""
    from unittest.mock import MagicMock

    # Mock all the prometheus metrics
    mock_batches_processed = MagicMock()
    mock_documents_processed = MagicMock()
    mock_text_extracted = MagicMock()
    mock_download_bytes = MagicMock()
    mock_processing_time = MagicMock()
    mock_warc_records = MagicMock()

    monkeypatch.setattr("bccp.worker.batches_processed_counter", mock_batches_processed)
    monkeypatch.setattr(
        "bccp.worker.documents_processed_counter", mock_documents_processed
    )
    monkeypatch.setattr("bccp.worker.text_extracted_counter", mock_text_extracted)
    monkeypatch.setattr("bccp.worker.download_bytes_counter", mock_download_bytes)
    monkeypatch.setattr(
        "bccp.worker.batch_processing_time_histogram", mock_processing_time
    )
    monkeypatch.setattr("bccp.worker.warc_records_processed_counter", mock_warc_records)

    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/\r\nContent-Length: 50\r\n\r\n<html>test content</html>"""

    mock_ch = MagicMock()
    mock_method = MagicMock()
    mock_method.delivery_tag = "tag123"

    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = "Extracted text content"

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

        worker.process_batch(
            mock_ch, mock_method, None, json.dumps(test_batch).encode()
        )

        # Verify metrics were called
        mock_batches_processed.inc.assert_called_once()
        mock_documents_processed.inc.assert_called_once()
        mock_text_extracted.inc.assert_called_once()
        mock_download_bytes.inc.assert_called()
        mock_processing_time.observe.assert_called()
        mock_warc_records.labels.assert_called()


def test_worker_error_metrics(monkeypatch):
    """Test that worker error metrics are incremented correctly."""
    from unittest.mock import MagicMock

    mock_error_counter = MagicMock()
    monkeypatch.setattr("bccp.worker.processing_errors_counter", mock_error_counter)

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
    mock_error_counter.labels.assert_called_with(error_type="document_processing")


def test_worker_text_extraction_error(monkeypatch):
    """Test text extraction error handling."""
    from unittest.mock import MagicMock

    mock_error_counter = MagicMock()
    mock_documents_processed = MagicMock()
    monkeypatch.setattr("bccp.worker.processing_errors_counter", mock_error_counter)
    monkeypatch.setattr(
        "bccp.worker.documents_processed_counter", mock_documents_processed
    )

    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/\r\nWARC-Date: 2024-07-22T12:07:56Z\r\nWARC-Record-ID: <urn:uuid:12345>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 50\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\ntest content"""

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
        mock_documents_processed.inc.assert_called()
        mock_error_counter.labels.assert_called_with(error_type="text_extraction")


def test_worker_document_processing_method():
    """Test the _process_document method directly."""
    from unittest.mock import patch

    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/\r\nWARC-Date: 2024-07-22T12:07:56Z\r\nWARC-Record-ID: <urn:uuid:12345>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 50\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\ntest content"""

    mock_object_store = MagicMock()
    mock_object_store.store_document.return_value = True

    worker = Worker(
        downloader=mock_downloader, channel=MagicMock(), object_store=mock_object_store
    )

    test_item = {
        "surt_url": "example.com/test",
        "timestamp": "20240722120756",
        "metadata": {
            "filename": "test.warc.gz",
            "offset": "0",
            "length": "100",
            "url": "http://example.com/test",
        },
    }

    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = "Extracted text"

        # Should complete without raising exceptions
        worker._process_document(test_item)

        mock_downloader.download_and_unzip.assert_called_once_with(
            "test.warc.gz", 0, 100
        )
        mock_extract.assert_called_once()
        mock_object_store.store_document.assert_called_once()


@patch("bccp.worker.WARCIterator")
def test_worker_object_store_integration(
    mock_warciterator, sample_batch_item, mock_warc_record, get_counter_value
):
    """Test worker object store integration with document storage."""
    mock_extract_text = "This is extracted text content"
    mock_warciterator.return_value = [mock_warc_record]

    mock_object_store = MagicMock()
    mock_object_store.store_document.return_value = True

    mock_downloader_local = MagicMock()
    mock_downloader_local.download_and_unzip.return_value = b"warc data"

    worker = Worker(
        downloader=mock_downloader_local,
        channel=MagicMock(),
        object_store=mock_object_store,
        bucket_name="test-bucket",
    )

    initial_stored = get_counter_value(documents_stored_counter)

    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = mock_extract_text

        worker._process_document(sample_batch_item)

        # Verify object store was called with correct data structure
        mock_object_store.store_document.assert_called_once()
        call_args = mock_object_store.store_document.call_args

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
def test_worker_object_store_failure(mock_warciterator):
    """Test worker handles object store failures gracefully."""
    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"warc data"

    mock_object_store = MagicMock()
    mock_object_store.store_document.return_value = False  # Simulate failure

    # Mock WARC record
    mock_record = MagicMock()
    mock_record.rec_type = "response"
    mock_record.content_stream.return_value.read.return_value = (
        b"<html>test content</html>"
    )
    mock_warciterator.return_value = [mock_record]

    worker = Worker(
        downloader=mock_downloader, channel=MagicMock(), object_store=mock_object_store
    )

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
