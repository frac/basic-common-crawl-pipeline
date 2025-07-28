import gzip
from unittest.mock import MagicMock, patch

import pytest

from bccp.commoncrawl import CCDownloader, CSVIndexReader, validate_crawl_version, build_crawl_path


def test_ccdownloader_download_and_unzip():
    original_data = b"hello world"
    compressed_data = gzip.compress(original_data)

    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.content = compressed_data
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        downloader = CCDownloader("http://example.com")
        result = downloader.download_and_unzip("file.gz", 0, 10)
        assert result == original_data
        mock_get.assert_called_once()
        mock_response.raise_for_status.assert_called_once()


def test_csvindexreader_reads_rows(tmp_path):
    test_content = "a\tb\tc\n1\t2\t3\nx\ty\tz\n"
    test_file = tmp_path / "test.csv"
    test_file.write_text(test_content)

    reader = CSVIndexReader(str(test_file))
    rows = list(reader)
    assert rows == [["a", "b", "c"], ["1", "2", "3"], ["x", "y", "z"]]


def test_csvindexreader_iter_and_next(tmp_path):
    test_content = "foo\tbar\nbaz\tqux\n"
    test_file = tmp_path / "test2.csv"
    test_file.write_text(test_content)

    reader = CSVIndexReader(str(test_file))
    it = iter(reader)
    assert next(it) == ["foo", "bar"]
    assert next(it) == ["baz", "qux"]
    with pytest.raises(StopIteration):
        next(it)


def test_validate_crawl_version():
    """Test crawl version validation."""
    # Valid formats
    assert validate_crawl_version("CC-MAIN-2024-30") is True
    assert validate_crawl_version("CC-MAIN-2023-14") is True
    assert validate_crawl_version("CC-MAIN-2025-01") is True
    assert validate_crawl_version("CC-MAIN-2022-50") is True
    
    # Invalid formats
    assert validate_crawl_version("CM-MAIN-2024-30") is False  # Wrong prefix
    assert validate_crawl_version("CC-MAIN-24-30") is False    # Wrong year format
    assert validate_crawl_version("CC-MAIN-2024-3") is False   # Wrong week format
    assert validate_crawl_version("CC-MAIN-2024-100") is False # Invalid week number
    assert validate_crawl_version("CC-MAIN-2024") is False     # Missing week
    assert validate_crawl_version("2024-30") is False          # Missing prefix
    assert validate_crawl_version("") is False                 # Empty string
    assert validate_crawl_version("CC-MAIN-2024-30-extra") is False # Extra content


def test_build_crawl_path():
    """Test crawl path building."""
    # Valid input
    result = build_crawl_path("CC-MAIN-2024-30")
    assert result == "cc-index/collections/CC-MAIN-2024-30/indexes"
    
    result = build_crawl_path("CC-MAIN-2023-14") 
    assert result == "cc-index/collections/CC-MAIN-2023-14/indexes"
    
    # Invalid input should raise ValueError
    with pytest.raises(ValueError, match="Invalid crawl version format"):
        build_crawl_path("invalid-format")
        
    with pytest.raises(ValueError, match="Invalid crawl version format"):
        build_crawl_path("CC-MAIN-24-30")
