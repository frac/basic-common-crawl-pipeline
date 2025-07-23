import json
import os
from unittest.mock import MagicMock, patch

import pika
import pytest

from bccp.batcher import Batcher
from bccp.rabbitmq import QUEUE_NAME
from bccp.worker import Worker


@pytest.fixture
def rabbitmq_connection_string():
    """Provide RabbitMQ connection string for tests."""
    return os.environ.get(
        "RABBITMQ_CONNECTION_STRING", "amqp://guest:guest@localhost:5672/%2F"
    )


@pytest.fixture
def rabbitmq_channel(rabbitmq_connection_string):
    """Create a real RabbitMQ channel for e2e testing."""
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_connection_string))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    # Purge queue to start clean
    channel.queue_purge(queue=QUEUE_NAME)

    yield channel

    # Cleanup
    channel.queue_purge(queue=QUEUE_NAME)
    connection.close()


# MockDownloader functionality is provided by fixtures in conftest.py


@pytest.mark.docker
@patch("bccp.worker.start_http_server")
@patch("bccp.batcher.start_http_server")
def test_batcher_worker_integration(
    mock_batcher_server, mock_worker_server, rabbitmq_channel,
    mock_batcher_downloader, mock_downloader, mock_index_reader
):
    """Test end-to-end integration between batcher and worker."""

    # Set up batcher
    batcher = Batcher(
        "dummy.csv",
        channel=rabbitmq_channel,
        downloader=mock_batcher_downloader,
        index_reader=mock_index_reader,
    )

    # Process the index (should publish to RabbitMQ)
    batcher.process_index()

    # Verify message was published
    method_frame, header_frame, body = rabbitmq_channel.basic_get(queue=QUEUE_NAME)
    assert method_frame is not None, "No message found in queue"

    # Parse the message
    batch = json.loads(body)
    assert len(batch) == 4  # 2 CDX entries * 2 index entries = 4 total URLs
    # Check first URL details
    first_url = batch[0]
    assert first_url["metadata"]["status"] == "200"
    assert "eng" in first_url["metadata"]["languages"]

    # Acknowledge the message so it's removed from queue
    rabbitmq_channel.basic_ack(method_frame.delivery_tag)


@pytest.mark.docker
@patch("bccp.worker.start_http_server")
def test_worker_processes_batch(mock_worker_server, rabbitmq_channel, mock_downloader):
    """Test worker consuming and processing batches from RabbitMQ."""

    # Create a test batch message
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
                "length": "200",
            },
        }
    ]

    # Publish test message to queue
    rabbitmq_channel.basic_publish(
        exchange="", routing_key=QUEUE_NAME, body=json.dumps(test_batch)
    )

    # Create worker with mock downloader (uses real ObjectStore from env vars)
    worker = Worker(downloader=mock_downloader, channel=rabbitmq_channel)

    # Mock trafilatura to avoid real text extraction
    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = "Extracted text content"

        # Process one message
        method_frame, header_frame, body = rabbitmq_channel.basic_get(queue=QUEUE_NAME)
        assert method_frame is not None, "No message found in queue"

        # Create mock method object
        mock_method = MagicMock()
        mock_method.delivery_tag = method_frame.delivery_tag

        # Process the batch
        worker.process_batch(rabbitmq_channel, mock_method, None, body)

        # Verify text extraction was called
        mock_extract.assert_called()


@pytest.mark.docker
@patch("bccp.worker.start_http_server")
@patch("bccp.batcher.start_http_server")
def test_full_pipeline_integration(
    mock_batcher_server, mock_worker_server, rabbitmq_channel, mock_downloader, mock_index_reader
):
    """Test complete pipeline: batcher processes index, worker consumes."""

    # Mock downloaders
    mock_batcher_downloader = MagicMock()
    mock_batcher_downloader.download_and_unzip.return_value = b"""0,100,22,165)/ 20240722120756 {"url": "http://example1.com/", "mime": "text/html", "status": "200", "languages": ["eng"], "digest": "ABC123", "length": "200", "offset": "0", "filename": "test1.warc.gz"}
101,141,199,66)/ 20240722120757 {"url": "http://example2.com/", "mime": "text/html", "status": "200", "languages": ["eng"], "digest": "DEF456", "length": "300", "offset": "200", "filename": "test2.warc.gz"}"""

    # Use fixture mock_downloader for worker

    # Use fixture mock_index_reader

    # Set up and run batcher
    batcher = Batcher(
        "dummy.csv",
        channel=rabbitmq_channel,
        downloader=mock_batcher_downloader,
        index_reader=mock_index_reader,
    )
    batcher.process_index()

    # Verify messages were published
    message_count = 0
    processed_urls = []

    # Set up worker (uses real ObjectStore from env vars)
    worker = Worker(downloader=mock_downloader, channel=rabbitmq_channel)

    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = "Extracted text content"

        # Process all messages in queue
        while True:
            method_frame, header_frame, body = rabbitmq_channel.basic_get(
                queue=QUEUE_NAME
            )
            if method_frame is None:
                break

            message_count += 1
            batch = json.loads(body)

            # Track processed URLs
            for item in batch:
                processed_urls.append(item["surt_url"])

            # Create mock method object
            mock_method = MagicMock()
            mock_method.delivery_tag = method_frame.delivery_tag

            # Process the batch
            worker.process_batch(rabbitmq_channel, mock_method, None, body)

    # Verify pipeline processed data correctly
    assert message_count > 0, "No messages were processed"
    assert len(processed_urls) == 4, f"Expected 4 URLs, got {len(processed_urls)}"
    assert "0,100,22,165)/" in processed_urls
    assert "101,141,199,66)/" in processed_urls


@pytest.mark.docker
@patch("bccp.batcher.start_http_server")
def test_prometheus_counters_e2e(mock_server, rabbitmq_channel, mock_downloader, mock_index_reader):
    """Test that Prometheus counters work correctly in end-to-end scenario."""
    from bccp.batcher import batches_published_counter, urls_processed_counter
    from bccp.worker import (
        batches_processed_counter,
        documents_processed_counter,
        download_bytes_counter,
        text_extracted_counter,
    )

    # Get initial metric values
    initial_batcher_batches = batches_published_counter._value.get()
    initial_urls_processed = urls_processed_counter._value.get()
    initial_worker_batches = batches_processed_counter._value.get()
    initial_docs_processed = documents_processed_counter._value.get()
    initial_text_extracted = text_extracted_counter._value.get()
    initial_bytes_downloaded = download_bytes_counter._value.get()

    # Mock downloaders
    mock_batcher_downloader = MagicMock()
    mock_batcher_downloader.download_and_unzip.return_value = b'url1 20240722120756 {"url": "http://example.com/", "status": "200", "languages": ["eng"], "filename": "test.warc.gz", "offset": "0", "length": "200"}'

    # Use fixtures for downloader and index reader

    # Run batcher
    batcher = Batcher(
        "dummy.csv",
        channel=rabbitmq_channel,
        downloader=mock_batcher_downloader,
        index_reader=mock_index_reader,
    )
    batcher.process_index()

    # Verify batcher metrics increased
    final_batcher_batches = batches_published_counter._value.get()
    final_urls_processed = urls_processed_counter._value.get()

    assert (
        final_batcher_batches > initial_batcher_batches
    ), "Batcher should have published batches"
    assert (
        final_urls_processed > initial_urls_processed
    ), "Batcher should have processed URLs"

    # Process the message with worker
    with patch("bccp.worker.start_http_server"):
        with patch("bccp.worker.trafilatura.extract") as mock_extract:
            mock_extract.return_value = "Extracted text"

            worker = Worker(downloader=mock_downloader, channel=rabbitmq_channel)

            # Get and process message
            method_frame, header_frame, body = rabbitmq_channel.basic_get(
                queue=QUEUE_NAME
            )
            if method_frame:
                mock_method = MagicMock()
                mock_method.delivery_tag = method_frame.delivery_tag

                worker.process_batch(rabbitmq_channel, mock_method, None, body)

                # Verify worker metrics increased
                final_worker_batches = batches_processed_counter._value.get()
                final_docs_processed = documents_processed_counter._value.get()
                final_text_extracted = text_extracted_counter._value.get()
                final_bytes_downloaded = download_bytes_counter._value.get()

                assert (
                    final_worker_batches > initial_worker_batches
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


@pytest.mark.docker
def test_rabbitmq_connection(rabbitmq_connection_string):
    """Test basic RabbitMQ connectivity."""
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_connection_string))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)

    # Test basic publish/consume
    test_message = {"test": "message"}
    channel.basic_publish(
        exchange="", routing_key=QUEUE_NAME, body=json.dumps(test_message)
    )

    method_frame, header_frame, body = channel.basic_get(queue=QUEUE_NAME)
    assert method_frame is not None
    assert json.loads(body) == test_message

    channel.basic_ack(method_frame.delivery_tag)
    connection.close()


@pytest.mark.docker
@patch("bccp.batcher.start_http_server")
def test_batcher_filtering_logic(mock_server, rabbitmq_channel, mock_single_index_reader):
    """Test batcher properly filters non-English and non-200 status URLs."""

    # Mock downloader with mixed data (some should be filtered)
    mock_downloader = MagicMock()
    mock_downloader.download_and_unzip.return_value = b"""url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"]}
url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"]}
url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"]}
url4 20240722120759 {"url": "http://example4.com/", "status": "200", "languages": ["eng"]}"""

    # Use single-entry fixture for precise filtering test

    batcher = Batcher(
        "dummy.csv",
        channel=rabbitmq_channel,
        downloader=mock_downloader,
        index_reader=mock_single_index_reader,
    )

    batcher.process_index()

    # Get published messages
    messages = []
    while True:
        method_frame, header_frame, body = rabbitmq_channel.basic_get(queue=QUEUE_NAME)
        if method_frame is None:
            break
        messages.append(json.loads(body))
        rabbitmq_channel.basic_ack(method_frame.delivery_tag)

    # Should only have 2 valid URLs (url1 and url4)
    total_urls = sum(len(batch) for batch in messages)
    assert total_urls == 2, f"Expected 2 URLs after filtering, got {total_urls}"


@pytest.mark.docker
@patch("bccp.worker.start_http_server")
@patch("bccp.batcher.start_http_server")
def test_object_store_integration_e2e(
    mock_batcher_server, mock_worker_server, rabbitmq_channel, mock_downloader, mock_index_reader
):
    """Test end-to-end object store integration with real MinIO."""
    from bccp.objectstore import ObjectStore
    from bccp.worker import documents_stored_counter, storage_errors_counter

    # Get initial metrics
    initial_stored = documents_stored_counter._value.get()
    initial_errors = storage_errors_counter.labels(
        error_type="store_document"
    )._value.get()

    # Test that we can connect to MinIO
    object_store = ObjectStore()
    assert object_store.health_check(), "MinIO should be healthy for e2e tests"

    # Mock downloaders
    mock_batcher_downloader = MagicMock()
    mock_batcher_downloader.download_and_unzip.return_value = b'test-url 20240722120756 {"url": "http://e2e-example.com/", "status": "200", "languages": ["eng"], "filename": "e2e-test.warc.gz", "offset": "0", "length": "200"}'

    # Use fixtures for downloader and index reader

    # Run batcher
    batcher = Batcher(
        "dummy.csv",
        channel=rabbitmq_channel,
        downloader=mock_batcher_downloader,
        index_reader=mock_index_reader,
    )
    batcher.process_index()

    # Process with worker using real object store
    with patch("bccp.worker.trafilatura.extract") as mock_extract:
        mock_extract.return_value = "This is extracted text from e2e test document."

        worker = Worker(
            downloader=mock_downloader,
            channel=rabbitmq_channel,
            bucket_name="commoncrawl-extracted",
        )

        # Get and process message
        method_frame, header_frame, body = rabbitmq_channel.basic_get(queue=QUEUE_NAME)
        if method_frame:
            mock_method = MagicMock()
            mock_method.delivery_tag = method_frame.delivery_tag

            worker.process_batch(rabbitmq_channel, mock_method, None, body)

            # Verify metrics increased
            final_stored = documents_stored_counter._value.get()
            assert (
                final_stored > initial_stored
            ), "Documents should have been stored to MinIO"

            final_errors = storage_errors_counter.labels(
                error_type="store_document"
            )._value.get()
            assert (
                final_errors == initial_errors
            ), "No storage errors should have occurred"


@pytest.mark.docker
def test_object_store_health_check_e2e():
    """Test object store health check with real MinIO."""
    from bccp.objectstore import ObjectStore

    # Should connect to MinIO running in Docker
    object_store = ObjectStore()
    assert (
        object_store.health_check()
    ), "MinIO health check should pass in e2e environment"


@pytest.mark.docker
def test_object_store_bucket_operations_e2e():
    """Test object store bucket operations with real MinIO."""
    from bccp.objectstore import ObjectStore

    object_store = ObjectStore()

    # Test bucket creation
    test_bucket = "test-e2e-bucket"
    assert object_store.ensure_bucket_exists(
        test_bucket
    ), "Should be able to create test bucket"

    # Test document storage
    test_document = {
        "url": "http://e2e-test.com/page",
        "surt_url": "e2e-test.com/page",
        "timestamp": "20240722120756",
        "text": "This is test content for e2e testing.",
        "text_length": 38,
        "filename": "e2e-test.warc.gz",
        "processing_timestamp": "2024-07-22T12:07:56Z",
    }

    assert object_store.store_document(
        test_bucket, test_document
    ), "Should be able to store document to MinIO"
