import argparse
import json
import time
from typing import Any, Mapping, Optional, Sequence

from prometheus_client import Counter, Gauge, start_http_server

from .commoncrawl import (
    BASE_URL,
    DEFAULT_CRAWL_VERSION,
    CCDownloader,
    CSVIndexReader,
    Downloader,
    IndexReader,
    build_crawl_path,
)
from .deduplication import URLDeduplicator
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

# Progress monitoring metrics
index_processing_progress_gauge = Gauge(
    "batcher_index_processing_progress_percent",
    "Index file processing progress percentage",
)
estimated_remaining_time_gauge = Gauge(
    "batcher_estimated_remaining_seconds",
    "Estimated remaining processing time in seconds",
)
total_index_lines_gauge = Gauge(
    "batcher_total_index_lines", "Total number of lines in the index file"
)
processed_index_lines_counter = Counter(
    "batcher_processed_index_lines_total", "Number of index lines processed"
)

# Deduplication metrics
urls_deduplicated_counter = Counter(
    "batcher_urls_deduplicated_total", "URLs skipped due to deduplication"
)


class Batcher:
    def __init__(
        self,
        cluster_idx_filename: str,
        channel: Optional[RabbitMQChannel] = None,
        downloader: Optional[Downloader] = None,
        index_reader: Optional[IndexReader] = None,
        crawl_version: str = DEFAULT_CRAWL_VERSION,
        enable_deduplication: bool = False,
        deduplicator: Optional["URLDeduplicator"] = None,
    ):
        self.cluster_idx_filename = cluster_idx_filename
        self.crawl_version = crawl_version
        self.crawl_path = build_crawl_path(crawl_version)
        self.channel = channel or RabbitMQChannel()
        self.downloader = downloader or CCDownloader(f"{BASE_URL}/{self.crawl_path}")
        self.index_reader = index_reader or CSVIndexReader(cluster_idx_filename)
        self.logger = setup_logger("batcher")
        
        # Deduplication setup
        self.enable_deduplication = enable_deduplication
        self.deduplicator = deduplicator
        if self.enable_deduplication and self.deduplicator is None:
            # Import here to avoid circular import
            from .deduplication import URLDeduplicator
            self.deduplicator = URLDeduplicator()

        # Progress tracking
        self.total_index_lines = self._count_index_lines()
        self.processed_lines = 0
        self.start_time: Optional[float] = None
        self.last_progress_log_time = 0.0
        self.progress_log_interval = 30  # Log progress every 30 seconds

        # Set total lines metric
        total_index_lines_gauge.set(self.total_index_lines)

    def _count_index_lines(self) -> int:
        """Count total number of lines in the index file for progress tracking."""
        try:
            with open(self.cluster_idx_filename, "r") as f:
                return sum(1 for _ in f)
        except Exception as e:
            log_with_fields(
                setup_logger("batcher"),
                "warning",
                "Could not count index lines, progress tracking will be approximate",
                error=str(e),
            )
            return 0

    def _update_progress(self) -> None:
        """Update progress metrics and optionally log progress."""
        if self.total_index_lines > 0:
            progress_percent = (self.processed_lines / self.total_index_lines) * 100
            index_processing_progress_gauge.set(progress_percent)

            # Update estimated remaining time
            if self.start_time is not None and self.processed_lines > 0:
                elapsed_time = time.time() - self.start_time
                if elapsed_time > 0:
                    rate = self.processed_lines / elapsed_time
                    remaining_lines = self.total_index_lines - self.processed_lines
                    estimated_remaining = remaining_lines / rate if rate > 0 else 0.0
                    estimated_remaining_time_gauge.set(estimated_remaining)

            # Log progress at regular intervals
            current_time = time.time()
            if (
                current_time - self.last_progress_log_time
            ) >= self.progress_log_interval:
                self._log_progress(progress_percent)
                self.last_progress_log_time = current_time

    def _log_progress(self, progress_percent: float) -> None:
        """Log current processing progress."""
        batches_published = int(batches_published_counter._value.get())
        elapsed_time = (
            time.time() - self.start_time if self.start_time is not None else 0.0
        )

        estimated_remaining = estimated_remaining_time_gauge._value.get()

        log_with_fields(
            self.logger,
            "info",
            "Processing progress update",
            progress_percent=round(progress_percent, 2),
            processed_lines=self.processed_lines,
            total_lines=self.total_index_lines,
            batches_published=batches_published,
            elapsed_time_seconds=round(elapsed_time, 1),
            estimated_remaining_seconds=(
                round(estimated_remaining, 1) if estimated_remaining else None
            ),
        )

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
        self.start_time = time.time()
        self.last_progress_log_time = self.start_time

        log_with_fields(
            self.logger,
            "info",
            "Starting index processing with progress tracking",
            total_lines=self.total_index_lines,
        )

        try:
            for cdx_chunk in self.index_reader:
                cdx_chunks_processed_counter.inc()
                self.processed_lines += 1
                processed_index_lines_counter.inc()

                # Update progress tracking
                self._update_progress()

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

                            # Check for deduplication if enabled
                            if self.enable_deduplication and self.deduplicator:
                                url = metadata.get("url", "")
                                if url and not self.deduplicator.try_reserve_url(url, self.crawl_version):
                                    urls_deduplicated_counter.inc()
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

            # Final progress update
            self._update_progress()

            # Log completion
            total_time = time.time() - (
                self.start_time if self.start_time is not None else 0.0
            )
            batches_published = int(batches_published_counter._value.get())

            log_with_fields(
                self.logger,
                "info",
                "Index processing completed",
                total_lines_processed=self.processed_lines,
                total_batches_published=batches_published,
                total_processing_time_seconds=round(total_time, 2),
                processing_rate_lines_per_second=(
                    round(self.processed_lines / total_time, 2) if total_time > 0 else 0
                ),
            )

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
        help=(
            f"Common Crawl version to process "
            f"(format: CC-MAIN-YYYY-WW, default: {DEFAULT_CRAWL_VERSION})"
        ),
    )
    parser.add_argument(
        "--enable-deduplication",
        action="store_true",
        help="Enable URL deduplication across multiple crawls (requires Redis)",
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="Redis host for deduplication (default: localhost)",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="Redis port for deduplication (default: 6379)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    
    # Initialize deduplicator if enabled
    deduplicator = None
    if args.enable_deduplication:
        try:
            from .deduplication import URLDeduplicator
            deduplicator = URLDeduplicator(
                redis_host=args.redis_host,
                redis_port=args.redis_port,
            )
            log_with_fields(
                setup_logger("batcher"),
                "info",
                "URL deduplication enabled",
                redis_host=args.redis_host,
                redis_port=args.redis_port,
            )
        except Exception as e:
            log_with_fields(
                setup_logger("batcher"),
                "error",
                "Failed to initialize deduplication - Redis required",
                error=str(e),
            )
            raise
    
    batcher = Batcher(
        args.cluster_idx_filename,
        crawl_version=args.crawl_version,
        enable_deduplication=args.enable_deduplication,
        deduplicator=deduplicator,
    )

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
