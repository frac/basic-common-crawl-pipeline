import pytest

from bccp.batcher import Batcher
from bccp.commoncrawl import Downloader, IndexReader
from bccp.rabbitmq import MessageQueueChannel


# All fixtures now centralized in conftest.py


def test_publish_batch(channel_spy):
    from bccp.commoncrawl import IndexReader
    
    class FakeReader(IndexReader):
        def __init__(self, data):
            self.data = data
        def __iter__(self):
            return iter(self.data)
    
    index_reader = FakeReader([])
    batcher = Batcher("dummy.csv", channel=channel_spy, index_reader=index_reader)
    batch = [{"id": 1}, {"id": 2}]
    batcher.publish_batch(batch)
    assert channel_spy.num_called == 1
    assert channel_spy.sent_messages[0] == '[{"id": 1}, {"id": 2}]'


def test_batcher_prometheus_metrics(mock_all_batcher_metrics, mock_single_index_reader, channel_spy, fake_downloader):
    downloader = fake_downloader(
        'url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"]}\n'
        + 'url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"]}\n'
        + 'url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"]}'
    )
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv", channel=channel_spy, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()

    # Verify metrics were called using fixture
    mock_all_batcher_metrics["cdx_chunks_processed_counter"].inc.assert_called_once()
    assert mock_all_batcher_metrics["urls_processed_counter"].inc.call_count == 3
    assert mock_all_batcher_metrics["urls_filtered_counter"].labels().inc.call_count == 2


def test_batcher_batch_publication_metrics(monkeypatch, channel_spy, mock_single_index_reader):
    from unittest.mock import MagicMock

    mock_counter = MagicMock()
    monkeypatch.setattr("bccp.batcher.batches_published_counter", mock_counter)

    batcher = Batcher("dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader)

    batch = [{"id": 1}, {"id": 2}]
    batcher.publish_batch(batch)

    mock_counter.inc.assert_called_once()


def test_publish_batch_error_handling(channel_spy, mock_single_index_reader):
    """Test error handling in publish_batch method."""
    from unittest.mock import MagicMock

    channel_spy.basic_publish = MagicMock(side_effect=Exception("Connection failed"))

    batcher = Batcher("dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader)

    batch = [{"id": 1}]

    # Should raise the exception after logging and incrementing error counter
    try:
        batcher.publish_batch(batch)
        assert False, "Expected exception was not raised"
    except Exception as e:
        assert str(e) == "Connection failed"


def test_process_index_with_invalid_json(mock_single_index_reader, channel_spy, fake_downloader):
    """Test processing with invalid JSON data."""
    downloader = fake_downloader("url1 20240722120756 {invalid json}")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv", channel=channel_spy, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()

    # Should complete without publishing any batches due to invalid JSON
    assert len(channel_spy.sent_messages) == 0


def test_process_index_with_malformed_lines(mock_single_index_reader, channel_spy, fake_downloader):
    """Test processing with malformed CDX lines."""
    downloader = fake_downloader("incomplete_line_without_enough_parts")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv", channel=channel_spy, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()

    # Should complete without publishing any batches due to malformed lines
    assert len(channel_spy.sent_messages) == 0


def test_batcher_filter_methods(channel_spy, mock_single_index_reader):
    """Test individual filter methods."""
    batcher = Batcher("dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader)

    # Test language filter
    assert batcher._passes_language_filter({"languages": ["eng"]})
    assert not batcher._passes_language_filter({"languages": ["fra"]})
    assert not batcher._passes_language_filter({})

    # Test status filter
    assert batcher._passes_status_filter({"status": "200"})
    assert not batcher._passes_status_filter({"status": "404"})
    assert not batcher._passes_status_filter({})


def test_batcher_run_method(channel_spy, mock_single_index_reader):
    """Test the run method."""
    from unittest.mock import patch

    with patch("bccp.batcher.start_http_server") as mock_server:
        batcher = Batcher("dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader)
        batcher.run()

        mock_server.assert_called_once_with(9000)


def test_batcher_with_download_error(monkeypatch, mock_single_index_reader, channel_spy):
    """Test batcher resilience with download errors."""
    from unittest.mock import MagicMock

    mock_error_counter = MagicMock()
    monkeypatch.setattr("bccp.batcher.errors_counter", mock_error_counter)

    downloader = MagicMock()
    downloader.download_and_unzip.side_effect = Exception("Download failed")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv", channel=channel_spy, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()

    # Should complete processing despite download error
    assert len(channel_spy.sent_messages) == 0
    mock_error_counter.labels.assert_called_with(error_type="cdx_chunk_processing")


def test_batcher_logging_integration(channel_spy, mock_single_index_reader):
    """Test that batcher logging works correctly."""
    from unittest.mock import patch

    with patch("bccp.batcher.log_with_fields") as mock_log:
        batcher = Batcher("dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader)
        batch = [{"id": 1}]
        batcher.publish_batch(batch)

        # Verify logging was called
        mock_log.assert_called()


def test_filter_non_english_documents(channel_spy, fake_downloader, mock_index_reader):
    downloader = fake_downloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
    )
    
    batcher = Batcher(
        "dummy.csv", channel=channel_spy, downloader=downloader, index_reader=mock_index_reader
    )
    batcher.process_index()
    assert channel_spy.num_called == 0


def test_filter_bad_status_code(channel_spy, fake_downloader, mock_index_reader):
    downloader = fake_downloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
    )
    
    batcher = Batcher(
        "dummy.csv", channel=channel_spy, downloader=downloader, index_reader=mock_index_reader
    )
    batcher.process_index()
    assert channel_spy.num_called == 0


def test_publish_all_urls(channel_spy, fake_downloader, mock_index_reader):
    downloader = fake_downloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
        '0,100,22,165)/robots.txt 20240722120755 {"url": "http://165.22.100.0/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": "eng", "digest": "LYEE2BXON4MCQCP5FDVDNILOWBKCZZ6G", "length": "700", "offset": "4656", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/robotstxt/CC-MAIN-20240722095039-20240722125039-00410.warc.gz", "redirect": "https://157.245.55.71/robots.txt"}'
    )
    
    batcher = Batcher(
        "dummy.csv", channel=channel_spy, downloader=downloader, index_reader=mock_index_reader
    )
    batcher.process_index()
    assert channel_spy.num_called == 1
