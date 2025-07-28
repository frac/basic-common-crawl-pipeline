import argparse
import json
from typing import Any, Mapping, Optional, Sequence

from prometheus_client import Counter, start_http_server

from .commoncrawl import (
    BASE_URL,
    CRAWL_PATH,
    CCDownloader,
    CSVIndexReader,
    DEFAULT_CRAWL_VERSION,
    Downloader,
    IndexReader,
    build_crawl_path,
)
from .logging import log_with_fields, setup_logger
from .rabbitmq import QUEUE_NAME, RabbitMQChannel

BATCH_SIZE = 50

# Prometheus metrics
batches_published_counter = Counter(
    "batcher_batches_published_total", "Number of batches published"
)
urls_processed_counter = Counter("batcher_urls_processed_total", "Total URLs processed")
urls_filtered_counter = Counter(
    "batcher_urls_filtered_total", "URLs filtered out", ["reason"]
)
cdx_chunks_processed_counter = Counter(
    "batcher_cdx_chunks_processed_total", "CDX chunks processed"
)
errors_counter = Counter("batcher_errors_total", "Processing errors", ["error_type"])


class Batcher:
    def __init__(
        self,
        cluster_idx_filename: str,
        channel: Optional[RabbitMQChannel] = None,
        downloader: Optional[Downloader] = None,
        index_reader: Optional[IndexReader] = None,
        crawl_version: str = DEFAULT_CRAWL_VERSION,
    ):
        self.cluster_idx_filename = cluster_idx_filename
        self.crawl_version = crawl_version
        self.crawl_path = build_crawl_path(crawl_version)
        self.channel = channel or RabbitMQChannel()
        self.downloader = downloader or CCDownloader(f"{BASE_URL}/{self.crawl_path}")
        self.index_reader = index_reader or CSVIndexReader(cluster_idx_filename)
        self.logger = setup_logger("batcher")

    def publish_batch(self, batch: Sequence[Mapping[str, Any]]) -> None:
        log_with_fields(self.logger, "info", "Publishing batch", batch_size=len(batch))
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=json.dumps(batch),
            )
            batches_published_counter.inc()
        except Exception as e:
            errors_counter.labels(error_type="batch_publish").inc()
            log_with_fields(
                self.logger,
                "error",
                "Failed to publish batch",
                error=str(e),
                batch_size=len(batch),
            )
            raise

    def process_index(self) -> None:
        found_urls = []
        try:
            for cdx_chunk in self.index_reader:
                cdx_chunks_processed_counter.inc()
                try:
                    data = self.downloader.download_and_unzip(
                        cdx_chunk[1], int(cdx_chunk[2]), int(cdx_chunk[3])
                    ).decode("utf-8")

                    for line in data.split("\n"):
                        if line.strip() == "":
                            continue

                        try:
                            values = line.split(" ", 2)
                            if len(values) < 3:
                                urls_filtered_counter.labels(
                                    reason="invalid_format"
                                ).inc()
                                continue

                            metadata = json.loads(values[2])
                            urls_processed_counter.inc()

                            # Apply filtering with metrics
                            if not self._passes_language_filter(metadata):
                                urls_filtered_counter.labels(reason="non_english").inc()
                                continue

                            if not self._passes_status_filter(metadata):
                                urls_filtered_counter.labels(
                                    reason="non_200_status"
                                ).inc()
                                continue

                            found_urls.append(
                                {
                                    "surt_url": values[0],
                                    "timestamp": values[1],
                                    "metadata": metadata,
                                }
                            )

                            if len(found_urls) >= BATCH_SIZE:
                                self.publish_batch(found_urls)
                                found_urls = []

                        except json.JSONDecodeError:
                            urls_filtered_counter.labels(reason="invalid_json").inc()
                            continue
                        except Exception:
                            errors_counter.labels(error_type="line_processing").inc()
                            continue

                except Exception:
                    errors_counter.labels(error_type="cdx_chunk_processing").inc()
                    continue

            if len(found_urls) > 0:
                self.publish_batch(found_urls)

        except Exception:
            errors_counter.labels(error_type="index_processing").inc()
            raise

    def _passes_language_filter(self, metadata: dict) -> bool:
        """Check if document passes language filter."""
        return "languages" in metadata and "eng" in metadata["languages"]

    def _passes_status_filter(self, metadata: dict) -> bool:
        """Check if document passes status code filter."""
        return metadata.get("status") == "200"

    def run(self) -> None:
        start_http_server(9000)
        self.process_index()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batcher")
    parser.add_argument(
        "--cluster-idx-filename", type=str, help="Input file path", required=True
    )
    parser.add_argument(
        "--crawl-version", 
        type=str, 
        default=DEFAULT_CRAWL_VERSION,
        help=f"Common Crawl version to process (format: CC-MAIN-YYYY-WW, default: {DEFAULT_CRAWL_VERSION})"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    batcher = Batcher(args.cluster_idx_filename, crawl_version=args.crawl_version)
    
    log_with_fields(
        setup_logger("batcher"),
        "info",
        "Starting batcher",
        cluster_idx_filename=args.cluster_idx_filename,
        crawl_version=args.crawl_version,
        crawl_path=batcher.crawl_path,
    )
    
    batcher.run()


if __name__ == "__main__":
    main()
