import json
from unittest.mock import MagicMock, patch

import pytest

from bccp.batcher import Batcher
from bccp.commoncrawl import IndexReader
from bccp.worker import Worker


def test_batcher_worker_pipeline_integration(mock_message_queue, mock_index_reader):
    """Test complete pipeline integration using mocked components."""

    # Shared message queue
    message_queue = mock_message_queue

    # Mock batcher downloader - returns CDX data
    mock_batcher_downloader = MagicMock()
    mock_batcher_downloader.download_and_unzip.return_value = b"""0,100,22,165)/ 20240722120756 {"url": "http://example1.com/", "mime": "text/html", "status": "200", "languages": ["eng"], "digest": "ABC123", "length": "200", "offset": "0", "filename": "test1.warc.gz"}
101,141,199,66)/ 20240722120757 {"url": "http://example2.com/", "mime": "text/html", "status": "200", "languages": ["eng"], "digest": "DEF456", "length": "300", "offset": "200", "filename": "test2.warc.gz"}"""

    # Mock worker downloader - returns WARC data
    mock_worker_downloader = MagicMock()
    mock_worker_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/\r\nWARC-Date: 2024-07-22T12:07:56Z\r\nWARC-Record-ID: <urn:uuid:12345>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 150\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><body><h1>Test Page</h1><p>Sample content for testing.</p></body></html>"""

    index_reader = mock_index_reader

    # Phase 1: Run batcher
    with patch("bccp.batcher.start_http_server"):
        batcher = Batcher(
            cluster_idx_filename="dummy.csv",
            channel=message_queue,
            downloader=mock_batcher_downloader,
            index_reader=index_reader,
        )
        batcher.process_index()

    # Verify batcher published messages
    messages = message_queue.get_messages()
    assert len(messages) == 1, f"Expected 1 batch message, got {len(messages)}"

    batch = json.loads(messages[0])
    assert len(batch) == 4, f"Expected 4 URLs in batch, got {len(batch)}"
    assert batch[0]["surt_url"] == "0,100,22,165)/"
    assert batch[1]["surt_url"] == "101,141,199,66)/"

    # Phase 2: Run worker to process the messages
    with patch("bccp.worker.start_http_server"):
        with patch("bccp.worker.trafilatura.extract") as mock_extract:
            mock_extract.return_value = "Extracted text content from page"

            mock_object_store = MagicMock()
            mock_object_store.store_document.return_value = True

            worker = Worker(
                downloader=mock_worker_downloader,
                channel=message_queue,
                object_store=mock_object_store,
            )

            # Process each message
            for i, message in enumerate(messages):
                mock_method = MagicMock()
                mock_method.delivery_tag = f"tag_{i}"

                worker.process_batch(message_queue, mock_method, None, message.encode())

            # Verify text extraction was called for each document
            assert (
                mock_extract.call_count >= 4
            ), "Text extraction should be called for each document"

    # Verify all messages were acknowledged
    assert len(message_queue.acked_messages) == len(messages)


def test_batcher_filtering_integration(mock_message_queue, mock_single_index_reader):
    """Test batcher filtering logic in isolation."""

    message_queue = mock_message_queue

    # Mock downloader with mixed status codes and languages
    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"], "filename": "test1.warc.gz"}
url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"], "filename": "test2.warc.gz"}
url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"], "filename": "test3.warc.gz"}
url4 20240722120759 {"url": "http://example4.com/", "status": "200", "languages": ["eng"], "filename": "test4.warc.gz"}
url5 20240722120800 {"url": "http://example5.com/", "status": "200", "languages": ["eng"], "filename": "test5.warc.gz"}"""

    index_reader = mock_single_index_reader

    with patch("bccp.batcher.start_http_server"):
        batcher = Batcher(
            cluster_idx_filename="dummy.csv",
            channel=message_queue,
            downloader=mock_downloader,
            index_reader=index_reader,
        )
        batcher.process_index()

    # Verify filtering worked correctly
    messages = message_queue.get_messages()
    assert len(messages) == 1

    batch = json.loads(messages[0])
    # Should only have 3 URLs (url1, url4, url5) - 200 status + English
    assert len(batch) == 3

    valid_urls = [item["surt_url"] for item in batch]
    assert "url1" in valid_urls
    assert "url4" in valid_urls
    assert "url5" in valid_urls
    assert "url2" not in valid_urls  # 404 status
    assert "url3" not in valid_urls  # French language


def test_worker_warc_processing_integration(mock_message_queue):
    """Test worker WARC processing in isolation."""

    message_queue = mock_message_queue

    # Mock downloader that returns realistic WARC data
    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""WARC/1.0\r
WARC-Type: response\r
WARC-Target-URI: http://example.com/\r
WARC-Date: 2024-07-22T12:07:56Z\r
WARC-Record-ID: <urn:uuid:12345>\r
Content-Type: application/http; msgtype=response\r
Content-Length: 150\r
\r
HTTP/1.1 200 OK\r
Content-Type: text/html\r
\r
<html><head><title>Test</title></head><body><h1>Main Title</h1><p>This is some test content for extraction.</p></body></html>"""

    test_batch = [
        {
            "surt_url": "example.com/",
            "timestamp": "20240722120756",
            "metadata": {
                "url": "http://example.com/",
                "status": "200",
                "languages": ["eng"],
                "filename": "test.warc.gz",
                "offset": "0",
                "length": "500",
            },
        }
    ]

    with patch("bccp.worker.start_http_server"):
        with patch("bccp.worker.trafilatura.extract") as mock_extract:
            mock_extract.return_value = (
                "Main Title\nThis is some test content for extraction."
            )

            mock_object_store = MagicMock()
            mock_object_store.store_document.return_value = True

            worker = Worker(
                downloader=mock_downloader,
                channel=message_queue,
                object_store=mock_object_store,
            )

            mock_method = MagicMock()
            mock_method.delivery_tag = "test_tag"

            # Process the batch
            worker.process_batch(
                message_queue, mock_method, None, json.dumps(test_batch).encode()
            )

            # Verify downloader was called with correct parameters
            mock_downloader.download_and_unzip.assert_called_once_with(
                "test.warc.gz", 0, 500
            )

            # Verify text extraction was attempted
            mock_extract.assert_called()

            # Verify message was acknowledged
            assert "test_tag" in message_queue.acked_messages


def test_batch_size_handling_integration(mock_message_queue, mock_single_index_reader):
    """Test that batching works correctly with different batch sizes."""

    message_queue = mock_message_queue

    # Create enough data for multiple batches (using small batch size)
    urls_data = []
    for i in range(5):
        urls_data.append(
            f'url{i} 20240722120756 {{"url": "http://example{i}.com/", "status": "200", "languages": ["eng"], "filename": "test{i}.warc.gz"}}'
        )

    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = "\n".join(urls_data).encode()

    index_reader = mock_single_index_reader

    with patch("bccp.batcher.start_http_server"):
        # Use small batch size to force multiple batches
        batcher = Batcher(
            cluster_idx_filename="dummy.csv",
            channel=message_queue,
            downloader=mock_downloader,
            index_reader=index_reader,
        )
        # Override batch size for testing
        original_batch_size = batcher.__class__.__dict__.get("BATCH_SIZE", 50)

        # Manually set smaller batch size by modifying the constant in the process
        with patch("bccp.batcher.BATCH_SIZE", 2):
            batcher.process_index()

    # Should have created multiple batches due to small batch size
    messages = message_queue.get_messages()
    assert len(messages) >= 2, f"Expected multiple batches, got {len(messages)}"

    # Verify total URLs across all batches
    total_urls = 0
    for message in messages:
        batch = json.loads(message)
        total_urls += len(batch)
        # Each batch should have at most 2 URLs (except possibly the last)
        assert len(batch) <= 2

    assert total_urls == 5, f"Expected 5 total URLs, got {total_urls}"


def test_error_handling_integration(mock_message_queue, mock_single_index_reader):
    """Test error handling in the pipeline - resilient processing."""

    message_queue = mock_message_queue

    # Mock downloader that raises an exception for CDX chunks
    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.side_effect = Exception("Network error")

    index_reader = mock_single_index_reader

    with patch("bccp.batcher.start_http_server"):
        with patch("bccp.batcher.errors_counter") as mock_error_counter:
            batcher = Batcher(
                cluster_idx_filename="dummy.csv",
                channel=message_queue,
                downloader=mock_downloader,
                index_reader=index_reader,
            )

            # Should complete processing despite CDX chunk errors
            batcher.process_index()

            # Verify error was recorded in metrics
            mock_error_counter.labels.assert_called_with(
                error_type="cdx_chunk_processing"
            )

    # No messages should have been published due to error
    assert len(message_queue.get_messages()) == 0


def test_prometheus_metrics_integration(mock_message_queue, mock_single_index_reader):
    """Test that Prometheus metrics are correctly incremented during processing."""
    from bccp.batcher import (
        batches_published_counter,
        cdx_chunks_processed_counter,
        urls_filtered_counter,
        urls_processed_counter,
    )

    message_queue = mock_message_queue

    # Get initial metric values
    initial_batches = batches_published_counter._value.get()
    initial_processed = urls_processed_counter._value.get()
    initial_chunks = cdx_chunks_processed_counter._value.get()

    # Mock downloader with mixed data
    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"]}
url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"]}
url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"]}
url4 20240722120759 {"url": "http://example4.com/", "status": "200", "languages": ["eng"]}"""

    index_reader = mock_single_index_reader

    with patch("bccp.batcher.start_http_server"):
        batcher = Batcher(
            cluster_idx_filename="dummy.csv",
            channel=message_queue,
            downloader=mock_downloader,
            index_reader=index_reader,
        )
        batcher.process_index()

    # Verify metrics were incremented
    final_batches = batches_published_counter._value.get()
    final_processed = urls_processed_counter._value.get()
    final_chunks = cdx_chunks_processed_counter._value.get()

    # Check that metrics increased
    assert final_batches > initial_batches, "Batches counter should have increased"
    assert (
        final_processed > initial_processed
    ), "URLs processed counter should have increased"
    assert final_chunks > initial_chunks, "CDX chunks counter should have increased"

    # Should process 4 URLs total
    assert (
        final_processed - initial_processed == 4
    ), f"Expected 4 URLs processed, got {final_processed - initial_processed}"

    # Should process 1 CDX chunk
    assert (
        final_chunks - initial_chunks == 1
    ), f"Expected 1 CDX chunk, got {final_chunks - initial_chunks}"

    # Should publish 1 batch (2 valid URLs: url1 and url4)
    assert (
        final_batches - initial_batches == 1
    ), f"Expected 1 batch published, got {final_batches - initial_batches}"


def test_worker_object_store_integration(mock_message_queue):
    """Test worker object store integration in isolation."""
    from bccp.worker import documents_stored_counter, storage_errors_counter

    message_queue = mock_message_queue

    # Get initial metric values
    initial_stored = documents_stored_counter._value.get()
    initial_errors = storage_errors_counter.labels(
        error_type="store_document"
    )._value.get()

    # Mock downloader that returns valid WARC data
    mock_worker_downloader = MagicMock()
    mock_worker_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/test\r\nWARC-Date: 2024-07-22T12:07:56Z\r\nWARC-Record-ID: <urn:uuid:12345>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 100\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><body><h1>Test Page</h1><p>Sample content for testing.</p></body></html>"""

    # Create test batch data with all required metadata
    test_batch = [
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

    with patch("bccp.worker.start_http_server"):
        with patch("bccp.worker.trafilatura.extract") as mock_extract:
            mock_extract.return_value = "Test Page Sample content for testing."

            mock_object_store = MagicMock()
            mock_object_store.store_document.return_value = True

            worker = Worker(
                downloader=mock_worker_downloader,
                channel=message_queue,
                object_store=mock_object_store,
                bucket_name="test-integration-bucket",
                min_doc_length=10,  # Allow shorter text for testing
                max_doc_length=1000000,
            )

            # Simulate message processing
            mock_method = MagicMock()
            mock_method.delivery_tag = "test_tag"

            worker.process_batch(
                message_queue, mock_method, None, json.dumps(test_batch).encode()
            )

    # Verify object store was called with proper document structure  
    mock_object_store.store_document.assert_called_once()
    call_args = mock_object_store.store_document.call_args

    assert call_args[0][0] == "test-integration-bucket"  # bucket name
    document_data = call_args[0][1]  # document data

    # Verify document data structure
    expected_keys = [
        "url",
        "surt_url",
        "timestamp",
        "text",
        "text_length",
        "filename",
        "processing_timestamp",
    ]
    for key in expected_keys:
        assert key in document_data, f"Missing key: {key}"

    assert document_data["url"] == "http://example.com/test"
    assert document_data["surt_url"] == "example.com/test"
    assert document_data["timestamp"] == "20240722120756"
    assert document_data["text"] == "Test Page Sample content for testing."
    assert document_data["text_length"] == len("Test Page Sample content for testing.")
    assert document_data["filename"] == "test.warc.gz"

    # Verify metrics were updated
    final_stored = documents_stored_counter._value.get()
    assert (
        final_stored > initial_stored
    ), "Documents stored counter should have increased"


def test_worker_object_store_failure_integration(mock_message_queue):
    """Test worker object store failure handling in integration context."""
    from bccp.worker import documents_stored_counter, storage_errors_counter

    message_queue = mock_message_queue

    # Get initial metric values
    initial_stored = documents_stored_counter._value.get()
    initial_errors = storage_errors_counter.labels(
        error_type="store_document"
    )._value.get()

    # Mock downloader that returns valid WARC data
    mock_worker_downloader = MagicMock()
    mock_worker_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/test\r\nWARC-Date: 2024-07-22T12:07:56Z\r\nWARC-Record-ID: <urn:uuid:12345>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 100\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><body><h1>Test Page</h1><p>Sample content for testing.</p></body></html>"""

    test_batch = [
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

    with patch("bccp.worker.start_http_server"):
        with patch("bccp.worker.trafilatura.extract") as mock_extract:
            mock_extract.return_value = "Test content that will fail to store"

            mock_object_store = MagicMock()
            mock_object_store.store_document.return_value = False  # Simulate failure

            worker = Worker(
                downloader=mock_worker_downloader,
                channel=message_queue,
                object_store=mock_object_store,
                min_doc_length=10,  # Allow shorter text for testing
                max_doc_length=1000000,
            )

            mock_method = MagicMock()
            mock_method.delivery_tag = "test_tag"

            # Should not raise exception despite storage failure
            worker.process_batch(
                message_queue, mock_method, None, json.dumps(test_batch).encode()
            )

    # Verify error handling
    mock_object_store.store_document.assert_called_once()

    # Verify error metrics were incremented
    final_errors = storage_errors_counter.labels(
        error_type="store_document"
    )._value.get()
    assert final_errors > initial_errors, "Storage error counter should have increased"

    # Verify stored counter was NOT incremented
    final_stored = documents_stored_counter._value.get()
    assert (
        final_stored == initial_stored
    ), "Documents stored counter should not have increased on failure"


def test_prometheus_filtering_metrics_integration(
    mock_message_queue, mock_single_index_reader
):
    """Test that Prometheus filtering metrics work correctly."""
    from bccp.batcher import urls_filtered_counter

    message_queue = mock_message_queue

    # Get initial filtering metric values for specific reasons
    try:
        initial_non_english = urls_filtered_counter.labels(
            reason="non_english"
        )._value.get()
    except:
        initial_non_english = 0

    try:
        initial_non_200 = urls_filtered_counter.labels(
            reason="non_200_status"
        )._value.get()
    except:
        initial_non_200 = 0

    # Mock downloader with data that will trigger specific filters
    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"]}
url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"]}
url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"]}"""

    index_reader = mock_single_index_reader

    with patch("bccp.batcher.start_http_server"):
        batcher = Batcher(
            cluster_idx_filename="dummy.csv",
            channel=message_queue,
            downloader=mock_downloader,
            index_reader=index_reader,
        )
        batcher.process_index()

    # Check specific filtering metrics
    final_non_english = urls_filtered_counter.labels(reason="non_english")._value.get()
    final_non_200 = urls_filtered_counter.labels(reason="non_200_status")._value.get()

    # Should have filtered 1 non-English URL (url3)
    assert (
        final_non_english - initial_non_english == 1
    ), f"Expected 1 non-English URL filtered, got {final_non_english - initial_non_english}"

    # Should have filtered 1 non-200 status URL (url2)
    assert (
        final_non_200 - initial_non_200 == 1
    ), f"Expected 1 non-200 status URL filtered, got {final_non_200 - initial_non_200}"


def test_worker_prometheus_metrics_integration(mock_message_queue):
    """Test that worker Prometheus metrics are correctly incremented during processing."""
    from bccp.worker import (
        batches_processed_counter,
        documents_processed_counter,
        download_bytes_counter,
        text_extracted_counter,
    )

    message_queue = mock_message_queue

    # Get initial worker metric values
    initial_batches_processed = batches_processed_counter._value.get()
    initial_docs_processed = documents_processed_counter._value.get()
    initial_text_extracted = text_extracted_counter._value.get()
    initial_bytes_downloaded = download_bytes_counter._value.get()

    # Create test batch data
    test_batch = [
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

    # Mock downloader that returns valid WARC data
    mock_worker_downloader = MagicMock()
    mock_worker_downloader.download_and_unzip.return_value = b"""WARC/1.0\r\nWARC-Type: response\r\nWARC-Target-URI: http://example.com/test\r\nWARC-Date: 2024-07-22T12:07:56Z\r\nWARC-Record-ID: <urn:uuid:12345>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: 100\r\n\r\nHTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html><body><h1>Test Page</h1><p>Sample content for testing.</p></body></html>"""

    with patch("bccp.worker.start_http_server"):
        with patch("bccp.worker.trafilatura.extract") as mock_extract:
            mock_extract.return_value = "Test Page Sample content for testing."

            mock_object_store = MagicMock()
            mock_object_store.store_document.return_value = True

            worker = Worker(
                downloader=mock_worker_downloader,
                channel=message_queue,
                object_store=mock_object_store,
            )

            # Simulate message processing
            mock_method = MagicMock()
            mock_method.delivery_tag = "test_tag"

            worker.process_batch(
                message_queue, mock_method, None, json.dumps(test_batch).encode()
            )

    # Verify worker metrics were incremented
    final_batches_processed = batches_processed_counter._value.get()
    final_docs_processed = documents_processed_counter._value.get()
    final_text_extracted = text_extracted_counter._value.get()
    final_bytes_downloaded = download_bytes_counter._value.get()

    # Check that metrics increased appropriately
    assert (
        final_batches_processed > initial_batches_processed
    ), "Worker batches processed counter should have increased"
    assert (
        final_docs_processed > initial_docs_processed
    ), "Worker documents processed counter should have increased"
    assert (
        final_text_extracted > initial_text_extracted
    ), "Worker text extracted counter should have increased"
    assert (
        final_bytes_downloaded > initial_bytes_downloaded
    ), "Worker download bytes counter should have increased"

    # Verify specific increments
    assert (
        final_batches_processed - initial_batches_processed == 1
    ), f"Expected 1 batch processed, got {final_batches_processed - initial_batches_processed}"
    assert (
        final_docs_processed - initial_docs_processed == 1
    ), f"Expected 1 document processed, got {final_docs_processed - initial_docs_processed}"
    assert (
        final_text_extracted - initial_text_extracted == 1
    ), f"Expected 1 text extracted, got {final_text_extracted - initial_text_extracted}"
