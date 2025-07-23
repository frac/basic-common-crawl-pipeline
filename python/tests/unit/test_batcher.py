import pytest

from bccp.batcher import Batcher
from bccp.commoncrawl import Downloader, IndexReader
from bccp.rabbitmq import MessageQueueChannel


class FakeReader(IndexReader):
    def __init__(self, data):
        self.data = data

    def __iter__(self):
        return iter(self.data)


class FakeDownloader(Downloader):
    def __init__(self, row: str):
        self.row = row

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        return f"{self.row}".encode("utf-8")


class ChannelSpy(MessageQueueChannel):
    def __init__(self):
        self.num_called = 0
        self.sent_messages = []

    def basic_publish(self, exchange, routing_key, body):
        self.num_called += 1
        self.sent_messages.append(body)


@pytest.fixture
def valid_cdx_line():
    """Valid CDX line with English content and 200 status."""
    return 'url1 20240722120756 {"url": "http://example.com/", "status": "200", "languages": ["eng"], "filename": "test.warc.gz"}'


@pytest.fixture
def mixed_cdx_data():
    """Mixed CDX data with valid and invalid entries."""
    return """url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"], "filename": "test1.warc.gz"}
url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"], "filename": "test2.warc.gz"}
url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"], "filename": "test3.warc.gz"}
url4 20240722120759 {"url": "http://example4.com/", "status": "200", "languages": ["eng"], "filename": "test4.warc.gz"}"""


@pytest.fixture
def channel_spy():
    """Channel spy for testing message publishing."""
    return ChannelSpy()


@pytest.fixture
def test_batcher_data():
    """Standard test data for batcher."""
    return [["test_entry", "test.gz", "0", "100", "1"]]


@pytest.fixture
def batcher_with_valid_data(valid_cdx_line, test_batcher_data, channel_spy):
    """Batcher configured with valid test data."""
    return Batcher(
        cluster_idx_filename="dummy.csv",
        channel=channel_spy,
        downloader=FakeDownloader(valid_cdx_line),
        index_reader=FakeReader(test_batcher_data),
    )


def test_publish_batch():
    channel = ChannelSpy()
    index_reader = FakeReader([])
    batcher = Batcher("dummy.csv", channel=channel, index_reader=index_reader)
    batch = [{"id": 1}, {"id": 2}]
    batcher.publish_batch(batch)
    assert channel.num_called == 1
    assert channel.sent_messages[0] == '[{"id": 1}, {"id": 2}]'


def test_batcher_prometheus_metrics(monkeypatch, mock_single_index_reader):
    from unittest.mock import MagicMock

    # Mock the prometheus counters
    mock_chunks = MagicMock()
    mock_processed = MagicMock()
    mock_filtered = MagicMock()

    monkeypatch.setattr("bccp.batcher.cdx_chunks_processed_counter", mock_chunks)
    monkeypatch.setattr("bccp.batcher.urls_processed_counter", mock_processed)
    monkeypatch.setattr("bccp.batcher.urls_filtered_counter", mock_filtered)

    channel = ChannelSpy()
    downloader = FakeDownloader(
        'url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"]}\n'
        + 'url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"]}\n'
        + 'url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"]}'
    )
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv", channel=channel, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()

    # Verify metrics were called
    mock_chunks.inc.assert_called_once()  # One CDX chunk processed
    assert mock_processed.inc.call_count == 3  # Three URLs processed
    assert (
        mock_filtered.labels().inc.call_count == 2
    )  # Two URLs filtered (404 + French)


def test_batcher_batch_publication_metrics(monkeypatch):
    from unittest.mock import MagicMock

    mock_counter = MagicMock()
    monkeypatch.setattr("bccp.batcher.batches_published_counter", mock_counter)

    channel = ChannelSpy()
    index_reader = FakeReader([])
    batcher = Batcher("dummy.csv", channel=channel, index_reader=index_reader)

    batch = [{"id": 1}, {"id": 2}]
    batcher.publish_batch(batch)

    mock_counter.inc.assert_called_once()


def test_publish_batch_error_handling():
    """Test error handling in publish_batch method."""
    from unittest.mock import MagicMock

    channel = ChannelSpy()
    channel.basic_publish = MagicMock(side_effect=Exception("Connection failed"))

    index_reader = FakeReader([])
    batcher = Batcher("dummy.csv", channel=channel, index_reader=index_reader)

    batch = [{"id": 1}]

    # Should raise the exception after logging and incrementing error counter
    try:
        batcher.publish_batch(batch)
        assert False, "Expected exception was not raised"
    except Exception as e:
        assert str(e) == "Connection failed"


def test_process_index_with_invalid_json(mock_single_index_reader):
    """Test processing with invalid JSON data."""
    channel = ChannelSpy()
    downloader = FakeDownloader("url1 20240722120756 {invalid json}")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv", channel=channel, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()

    # Should complete without publishing any batches due to invalid JSON
    assert len(channel.sent_messages) == 0


def test_process_index_with_malformed_lines(mock_single_index_reader):
    """Test processing with malformed CDX lines."""
    channel = ChannelSpy()
    downloader = FakeDownloader("incomplete_line_without_enough_parts")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv", channel=channel, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()

    # Should complete without publishing any batches due to malformed lines
    assert len(channel.sent_messages) == 0


def test_batcher_filter_methods():
    """Test individual filter methods."""
    channel = ChannelSpy()
    index_reader = FakeReader([])
    batcher = Batcher("dummy.csv", channel=channel, index_reader=index_reader)

    # Test language filter
    assert batcher._passes_language_filter({"languages": ["eng"]})
    assert not batcher._passes_language_filter({"languages": ["fra"]})
    assert not batcher._passes_language_filter({})

    # Test status filter
    assert batcher._passes_status_filter({"status": "200"})
    assert not batcher._passes_status_filter({"status": "404"})
    assert not batcher._passes_status_filter({})


def test_batcher_run_method():
    """Test the run method."""
    from unittest.mock import patch

    channel = ChannelSpy()
    index_reader = FakeReader([])

    with patch("bccp.batcher.start_http_server") as mock_server:
        batcher = Batcher("dummy.csv", channel=channel, index_reader=index_reader)
        batcher.run()

        mock_server.assert_called_once_with(9000)


def test_batcher_with_download_error(monkeypatch, mock_single_index_reader):
    """Test batcher resilience with download errors."""
    from unittest.mock import MagicMock

    mock_error_counter = MagicMock()
    monkeypatch.setattr("bccp.batcher.errors_counter", mock_error_counter)

    channel = ChannelSpy()
    downloader = MagicMock()
    downloader.download_and_unzip.side_effect = Exception("Download failed")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv", channel=channel, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()

    # Should complete processing despite download error
    assert len(channel.sent_messages) == 0
    mock_error_counter.labels.assert_called_with(error_type="cdx_chunk_processing")


def test_batcher_logging_integration():
    """Test that batcher logging works correctly."""
    from unittest.mock import patch

    channel = ChannelSpy()
    index_reader = FakeReader([])

    with patch("bccp.batcher.log_with_fields") as mock_log:
        batcher = Batcher("dummy.csv", channel=channel, index_reader=index_reader)
        batch = [{"id": 1}]
        batcher.publish_batch(batch)

        # Verify logging was called
        mock_log.assert_called()


def test_filter_non_english_documents():
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
    )
    index_reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    batcher = Batcher(
        "dummy.csv", channel=channel, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()
    assert channel.num_called == 0


def test_filter_bad_status_code():
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
    )
    index_reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    batcher = Batcher(
        "dummy.csv", channel=channel, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()
    assert channel.num_called == 0


def test_publish_all_urls():
    channel = ChannelSpy()
    downloader = FakeDownloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'
        '0,100,22,165)/robots.txt 20240722120755 {"url": "http://165.22.100.0/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": "eng", "digest": "LYEE2BXON4MCQCP5FDVDNILOWBKCZZ6G", "length": "700", "offset": "4656", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/robotstxt/CC-MAIN-20240722095039-20240722125039-00410.warc.gz", "redirect": "https://157.245.55.71/robots.txt"}'
    )
    index_reader = FakeReader(
        [
            ["0,100,22,165)/ 20240722120756", "cdx-00000.gz", "0", "188224", "1"],
            [
                "101,141,199,66)/robots.txt 20240714155331",
                "cdx-00000.gz",
                "188224",
                "178351",
                "2",
            ],
            ["104,223,1,100)/ 20240714230020", "cdx-00000.gz", "366575", "178055", "3"],
        ]
    )
    batcher = Batcher(
        "dummy.csv", channel=channel, downloader=downloader, index_reader=index_reader
    )
    batcher.process_index()
    assert channel.num_called == 1
