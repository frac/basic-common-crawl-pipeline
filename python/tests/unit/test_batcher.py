from unittest.mock import MagicMock, mock_open, patch

import pytest

from bccp.batcher import Batcher
from bccp.commoncrawl import DEFAULT_CRAWL_VERSION, IndexReader

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


def test_batcher_prometheus_metrics(
    mock_all_batcher_metrics, mock_single_index_reader, channel_spy, fake_downloader
):
    downloader = fake_downloader(
        'url1 20240722120756 {"url": "http://example1.com/", "status": "200", "languages": ["eng"]}\n'  # noqa: E501
        + 'url2 20240722120757 {"url": "http://example2.com/", "status": "404", "languages": ["eng"]}\n'  # noqa: E501
        + 'url3 20240722120758 {"url": "http://example3.com/", "status": "200", "languages": ["fra"]}'  # noqa: E501
    )
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=index_reader,
    )
    batcher.process_index()

    # Verify metrics were called using fixture
    mock_all_batcher_metrics["cdx_chunks_processed_counter"].inc.assert_called_once()
    assert mock_all_batcher_metrics["urls_processed_counter"].inc.call_count == 3
    assert (
        mock_all_batcher_metrics["urls_filtered_counter"].labels().inc.call_count == 2
    )


def test_batcher_batch_publication_metrics(
    monkeypatch, channel_spy, mock_single_index_reader
):
    from unittest.mock import MagicMock

    mock_counter = MagicMock()
    monkeypatch.setattr("bccp.batcher.batches_published_counter", mock_counter)

    batcher = Batcher(
        "dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader
    )

    batch = [{"id": 1}, {"id": 2}]
    batcher.publish_batch(batch)

    mock_counter.inc.assert_called_once()


def test_publish_batch_error_handling(channel_spy, mock_single_index_reader):
    """Test error handling in publish_batch method."""
    from unittest.mock import MagicMock

    channel_spy.basic_publish = MagicMock(side_effect=Exception("Connection failed"))

    batcher = Batcher(
        "dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader
    )

    batch = [{"id": 1}]

    # Should raise the exception after logging and incrementing error counter
    try:
        batcher.publish_batch(batch)
        assert False, "Expected exception was not raised"
    except Exception as e:
        assert str(e) == "Connection failed"


def test_process_index_with_invalid_json(
    mock_single_index_reader, channel_spy, fake_downloader
):
    """Test processing with invalid JSON data."""
    downloader = fake_downloader("url1 20240722120756 {invalid json}")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=index_reader,
    )
    batcher.process_index()

    # Should complete without publishing any batches due to invalid JSON
    assert len(channel_spy.sent_messages) == 0


def test_process_index_with_malformed_lines(
    mock_single_index_reader, channel_spy, fake_downloader
):
    """Test processing with malformed CDX lines."""
    downloader = fake_downloader("incomplete_line_without_enough_parts")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=index_reader,
    )
    batcher.process_index()

    # Should complete without publishing any batches due to malformed lines
    assert len(channel_spy.sent_messages) == 0


def test_batcher_filter_methods(channel_spy, mock_single_index_reader):
    """Test individual filter methods."""
    batcher = Batcher(
        "dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader
    )

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
        batcher = Batcher(
            "dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader
        )
        batcher.run()

        mock_server.assert_called_once_with(9000)


def test_batcher_with_download_error(
    monkeypatch, mock_single_index_reader, channel_spy
):
    """Test batcher resilience with download errors."""
    from unittest.mock import MagicMock

    mock_error_counter = MagicMock()
    monkeypatch.setattr("bccp.batcher.errors_counter", mock_error_counter)

    downloader = MagicMock()
    downloader.download_and_unzip.side_effect = Exception("Download failed")
    index_reader = mock_single_index_reader

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=index_reader,
    )
    batcher.process_index()

    # Should complete processing despite download error
    assert len(channel_spy.sent_messages) == 0
    mock_error_counter.labels.assert_called_with(error_type="cdx_chunk_processing")


def test_batcher_logging_integration(channel_spy, mock_single_index_reader):
    """Test that batcher logging works correctly."""
    from unittest.mock import patch

    with patch("bccp.batcher.log_with_fields") as mock_log:
        batcher = Batcher(
            "dummy.csv", channel=channel_spy, index_reader=mock_single_index_reader
        )
        batch = [{"id": 1}]
        batcher.publish_batch(batch)

        # Verify logging was called
        mock_log.assert_called()


def test_filter_non_english_documents(channel_spy, fake_downloader, mock_index_reader):
    downloader = fake_downloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'  # noqa: E501
    )

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=mock_index_reader,
    )
    batcher.process_index()
    assert channel_spy.num_called == 0


def test_filter_bad_status_code(channel_spy, fake_downloader, mock_index_reader):
    downloader = fake_downloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "301", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'  # noqa: E501
    )

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=mock_index_reader,
    )
    batcher.process_index()
    assert channel_spy.num_called == 0


def test_publish_all_urls(channel_spy, fake_downloader, mock_index_reader):
    downloader = fake_downloader(
        '0,100,22,165)/ 20240722120756 {"url": "http://165.22.100.0/", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": "eng", "digest": "DCNYNIFG5SBRCVS5PCUY4YY2UM2WAQ4R", "length": "689", "offset": "3499", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/crawldiagnostics/CC-MAIN-20240722095039-20240722125039-00443.warc.gz", "redirect": "https://157.245.55.71/"}\n'  # noqa: E501
        '0,100,22,165)/robots.txt 20240722120755 {"url": "http://165.22.100.0/robots.txt", "mime": "text/html", "mime-detected": "text/html", "status": "200", "languages": "eng", "digest": "LYEE2BXON4MCQCP5FDVDNILOWBKCZZ6G", "length": "700", "offset": "4656", "filename": "crawl-data/CC-MAIN-2024-30/segments/1720763517846.73/robotstxt/CC-MAIN-20240722095039-20240722125039-00410.warc.gz", "redirect": "https://157.245.55.71/robots.txt"}'  # noqa: E501
    )

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=mock_index_reader,
    )
    batcher.process_index()
    assert channel_spy.num_called == 1


def test_batcher_default_crawl_version(channel_spy, fake_downloader, mock_index_reader):
    """Test that batcher uses default crawl version when not specified."""
    downloader = fake_downloader("dummy data")

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=mock_index_reader,
    )

    assert batcher.crawl_version == DEFAULT_CRAWL_VERSION
    assert batcher.crawl_path == f"cc-index/collections/{DEFAULT_CRAWL_VERSION}/indexes"


def test_batcher_custom_crawl_version(channel_spy, fake_downloader, mock_index_reader):
    """Test that batcher uses custom crawl version when specified."""
    custom_version = "CC-MAIN-2023-14"
    downloader = fake_downloader("dummy data")

    batcher = Batcher(
        "dummy.csv",
        channel=channel_spy,
        downloader=downloader,
        index_reader=mock_index_reader,
        crawl_version=custom_version,
    )

    assert batcher.crawl_version == custom_version
    assert batcher.crawl_path == f"cc-index/collections/{custom_version}/indexes"


def test_batcher_invalid_crawl_version(channel_spy, fake_downloader, mock_index_reader):
    """Test that batcher raises error for invalid crawl version."""
    downloader = fake_downloader("dummy data")

    with pytest.raises(ValueError, match="Invalid crawl version format"):
        Batcher(
            "dummy.csv",
            channel=channel_spy,
            downloader=downloader,
            index_reader=mock_index_reader,
            crawl_version="invalid-format",
        )


def test_batcher_progress_tracking_count_lines():
    """Test that batcher correctly counts lines in index file."""
    fake_file_content = "line1\nline2\nline3\n"

    with patch("builtins.open", mock_open(read_data=fake_file_content)):
        with patch("bccp.batcher.setup_logger"):
            # Need to mock CSVIndexReader since it tries to open the file
            mock_reader = MagicMock()
            mock_reader.__iter__ = MagicMock(return_value=iter([]))

            batcher = Batcher(
                "dummy.csv", channel=MagicMock(), index_reader=mock_reader
            )

            assert batcher.total_index_lines == 3


def test_batcher_progress_tracking_count_lines_error():
    """Test handling of file read error during line counting."""
    with patch("builtins.open", side_effect=IOError("File not found")):
        with patch("bccp.batcher.setup_logger"):
            with patch("bccp.batcher.log_with_fields") as mock_log:
                mock_reader = MagicMock()
                mock_reader.__iter__ = MagicMock(return_value=iter([]))

                batcher = Batcher(
                    "dummy.csv", channel=MagicMock(), index_reader=mock_reader
                )

                assert batcher.total_index_lines == 0
                mock_log.assert_called()


def test_batcher_progress_metrics_initialization():
    """Test that progress metrics are properly initialized."""
    with patch("builtins.open", mock_open(read_data="line1\nline2\n")):
        with patch("bccp.batcher.setup_logger"):
            with patch("bccp.batcher.total_index_lines_gauge") as mock_gauge:
                mock_reader = MagicMock()
                mock_reader.__iter__ = MagicMock(return_value=iter([]))

                Batcher("dummy.csv", channel=MagicMock(), index_reader=mock_reader)

                mock_gauge.set.assert_called_once_with(2)


def test_batcher_progress_update():
    """Test progress update functionality."""
    with patch("builtins.open", mock_open(read_data="line1\nline2\nline3\nline4\n")):
        with patch("bccp.batcher.setup_logger"):
            with patch(
                "bccp.batcher.index_processing_progress_gauge"
            ) as mock_progress_gauge:
                with patch(
                    "bccp.batcher.estimated_remaining_time_gauge"
                ) as mock_time_gauge:
                    with patch("time.time", return_value=100.0):
                        mock_reader = MagicMock()
                        mock_reader.__iter__ = MagicMock(return_value=iter([]))

                        batcher = Batcher(
                            "dummy.csv", channel=MagicMock(), index_reader=mock_reader
                        )

                        # Simulate processing progress
                        batcher.start_time = 90.0  # 10 seconds ago
                        batcher.processed_lines = 2  # 50% progress

                        batcher._update_progress()

                        # Should set progress to 50%
                        mock_progress_gauge.set.assert_called_with(50.0)

                        # Should calculate remaining time
                        # (2 lines in 10 seconds = 0.2 lines/sec)
                        # Remaining: 2 lines / 0.2 lines/sec = 10 seconds
                        mock_time_gauge.set.assert_called_with(10.0)


def test_batcher_progress_logging():
    """Test that progress is logged at regular intervals."""
    with patch("builtins.open", mock_open(read_data="line1\nline2\n")):
        with patch("bccp.batcher.setup_logger"):
            with patch("bccp.batcher.log_with_fields") as mock_log:
                with patch("bccp.batcher.batches_published_counter") as mock_counter:
                    mock_counter._value.get.return_value = 5

                    mock_reader = MagicMock()
                    mock_reader.__iter__ = MagicMock(return_value=iter([]))

                    batcher = Batcher(
                        "dummy.csv", channel=MagicMock(), index_reader=mock_reader
                    )

                    # Set up progress state
                    batcher.start_time = 100.0
                    batcher.processed_lines = 1
                    batcher.last_progress_log_time = 100.0
                    batcher.progress_log_interval = 30

                    # Simulate 31 seconds later (should trigger log)
                    with patch("time.time", return_value=131.0):
                        batcher._update_progress()

                    # Verify progress logging was called
                    mock_log.assert_called()
                    args, kwargs = mock_log.call_args
                    assert "Processing progress update" in args


def test_batcher_process_index_with_progress_tracking(channel_spy, fake_downloader):
    """Test that process_index properly tracks progress."""
    with patch("builtins.open", mock_open(read_data="line1\nline2\nline3\n")):
        with patch("bccp.batcher.setup_logger"):
            with patch("bccp.batcher.processed_index_lines_counter") as mock_counter:
                with patch("bccp.batcher.errors_counter"):
                    # Mock time.time to return enough values
                    with patch("time.time", return_value=100.0):
                        # Create a fake index reader with 3 items
                        class FakeProgressReader(IndexReader):
                            def __init__(self):
                                self.data = [
                                    ["chunk1", "file1.gz", "0", "100"],
                                    ["chunk2", "file2.gz", "100", "100"],
                                    ["chunk3", "file3.gz", "200", "100"],
                                ]
                                self.index = 0

                            def __iter__(self):
                                return self

                            def __next__(self):
                                if self.index >= len(self.data):
                                    raise StopIteration
                                result = self.data[self.index]
                                self.index += 1
                                return result

                        # Setup downloader to return data that will be filtered out
                        downloader = fake_downloader(
                            'url1 20240722120756 {"status": "404"}\n'
                        )

                        batcher = Batcher(
                            "dummy.csv",
                            channel=channel_spy,
                            downloader=downloader,
                            index_reader=FakeProgressReader(),
                        )

                        batcher.process_index()

                        # Should have processed 3 lines
                        assert mock_counter.inc.call_count == 3
                        assert batcher.processed_lines == 3


def test_batcher_completion_logging():
    """Test that completion is properly logged with statistics."""
    with patch("builtins.open", mock_open(read_data="line1\nline2\n")):
        with patch("bccp.batcher.setup_logger"):
            with patch("bccp.batcher.log_with_fields") as mock_log:
                with patch("bccp.batcher.batches_published_counter") as mock_counter:
                    mock_counter._value.get.return_value = 3

                    class FakeEmptyReader(IndexReader):
                        def __iter__(self):
                            return iter([])

                    batcher = Batcher(
                        "dummy.csv", channel=MagicMock(), index_reader=FakeEmptyReader()
                    )

                    # Mock time progression - provide enough values
                    # for all time.time() calls
                    time_values = [100.0, 100.1, 125.0]  # start, progress check, end

                    with patch("time.time", side_effect=time_values):
                        batcher.process_index()

                    # Verify completion logging
                    completion_log_calls = [
                        call
                        for call in mock_log.call_args_list
                        if len(call[0]) > 2
                        and "Index processing completed" in call[0][2]
                    ]
                    assert len(completion_log_calls) > 0


def test_batcher_progress_with_zero_total_lines():
    """Test progress handling when total lines is zero."""
    with patch("builtins.open", mock_open(read_data="")):
        with patch("bccp.batcher.setup_logger"):
            with patch("bccp.batcher.index_processing_progress_gauge") as mock_gauge:
                mock_reader = MagicMock()
                mock_reader.__iter__ = MagicMock(return_value=iter([]))

                batcher = Batcher(
                    "dummy.csv", channel=MagicMock(), index_reader=mock_reader
                )

                batcher._update_progress()

                # Should not set progress when total_lines is 0
                mock_gauge.set.assert_not_called()


def test_batcher_progress_rate_calculation():
    """Test processing rate calculation in completion logging."""
    with patch("builtins.open", mock_open(read_data="line1\nline2\nline3\nline4\n")):
        with patch("bccp.batcher.setup_logger"):
            with patch("bccp.batcher.log_with_fields") as mock_log:
                with patch("bccp.batcher.errors_counter"):

                    class FakeEmptyReader(IndexReader):
                        def __iter__(self):
                            return iter([])

                    # Mock time progression: one for start_time, one for completion
                    start_time = 100.0
                    end_time = 102.0  # 2 seconds, so rate should be 2 lines/second

                    # Provide many time values to avoid StopIteration
                    time_values = [start_time] + [end_time] * 10
                    with patch("time.time", side_effect=time_values):
                        batcher = Batcher(
                            "dummy.csv",
                            channel=MagicMock(),
                            index_reader=FakeEmptyReader(),
                        )

                        # Set up state for rate calculation after initialization
                        batcher.processed_lines = 4
                        batcher.process_index()

                    # Find the completion log call
                    completion_calls = [
                        call
                        for call in mock_log.call_args_list
                        if len(call[0]) > 2
                        and "Index processing completed" in call[0][2]
                    ]

                    assert len(completion_calls) > 0
                    # Check that processing rate is included in kwargs
                    completion_kwargs = completion_calls[0][1]
                    assert "processing_rate_lines_per_second" in completion_kwargs
                    assert completion_kwargs["processing_rate_lines_per_second"] == 2.0
