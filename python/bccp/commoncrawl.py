import csv
import gzip
import re
from abc import ABC, abstractmethod

import requests

BASE_URL = "https://data.commoncrawl.org"


def validate_crawl_version(crawl_version: str) -> bool:
    """Validate that crawl version follows the expected format: CC-MAIN-YYYY-WW."""
    pattern = r"^CC-MAIN-\d{4}-\d{2}$"
    return bool(re.match(pattern, crawl_version))


def build_crawl_path(crawl_version: str) -> str:
    """Build the crawl path for a given crawl version."""
    if not validate_crawl_version(crawl_version):
        raise ValueError(
            f"Invalid crawl version format: {crawl_version}. "
            "Expected format: CC-MAIN-YYYY-WW"
        )
    return f"cc-index/collections/{crawl_version}/indexes"


# Default crawl version for backward compatibility
DEFAULT_CRAWL_VERSION = "CC-MAIN-2024-30"
CRAWL_PATH = build_crawl_path(DEFAULT_CRAWL_VERSION)


class Downloader(ABC):
    @abstractmethod
    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        pass


class CCDownloader(Downloader):
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url

    def download_and_unzip(self, url: str, start: int, length: int) -> bytes:
        headers = {"Range": f"bytes={start}-{start + length - 1}"}
        response = requests.get(f"{self.base_url}/{url}", headers=headers)
        response.raise_for_status()
        buffer = response.content
        return gzip.decompress(buffer)


class IndexReader(ABC):
    @abstractmethod
    def __iter__(self):
        pass


class CSVIndexReader(IndexReader):
    def __init__(self, filename: str) -> None:
        self.file = open(filename, "r")
        self.reader = csv.reader(self.file, delimiter="\t")

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.reader)

    def __del__(self) -> None:
        self.file.close()


def test_can_read_index(tmp_path):
    filename = tmp_path / "test.csv"
    index = "0,100,22,165)/ 20240722120756	cdx-00000.gz	0	188224	1\n\
101,141,199,66)/robots.txt 20240714155331	cdx-00000.gz	188224	178351	2\n\
104,223,1,100)/ 20240714230020	cdx-00000.gz	366575	178055	3"
    filename.write_text(index)
    reader = CSVIndexReader(filename)
    assert list(reader) == [
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
