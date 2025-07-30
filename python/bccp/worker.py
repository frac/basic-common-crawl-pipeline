import argparse
import io
import json
import os
import time
from typing import Any, Dict, Optional

import trafilatura
from prometheus_client import Counter, Histogram, start_http_server
from transformers import AutoTokenizer
from warcio.archiveiterator import WARCIterator

from .commoncrawl import BASE_URL, CCDownloader, Downloader
from .logging import log_with_fields, setup_logger
from .objectstore import ObjectStore
from .rabbitmq import QUEUE_NAME, rabbitmq_channel

# Prometheus metrics
batches_processed_counter = Counter(
    "worker_batches_processed_total", "Number of batches processed"
)
documents_processed_counter = Counter(
    "worker_documents_processed_total", "Documents processed"
)
text_extracted_counter = Counter(
    "worker_text_extracted_total", "Documents with text extracted"
)
download_bytes_counter = Counter(
    "worker_download_bytes_total", "Total bytes downloaded"
)
processing_errors_counter = Counter(
    "worker_processing_errors_total", "Processing errors", ["error_type"]
)
batch_processing_time_histogram = Histogram(
    "worker_batch_processing_seconds", "Time spent processing batches"
)
document_processing_time_histogram = Histogram(
    "worker_document_processing_seconds", "Time spent processing individual documents"
)
warc_records_processed_counter = Counter(
    "worker_warc_records_processed_total", "WARC records processed", ["record_type"]
)
documents_stored_counter = Counter(
    "worker_documents_stored_total", "Documents stored in object store"
)
storage_errors_counter = Counter(
    "worker_storage_errors_total", "Object store errors", ["error_type"]
)
tokenization_counter = Counter(
    "worker_documents_tokenized_total", "Documents successfully tokenized"
)
tokenization_errors_counter = Counter(
    "worker_tokenization_errors_total", "Tokenization errors", ["error_type"]
)
token_count_histogram = Histogram("worker_token_count", "Number of tokens per document")
documents_filtered_counter = Counter(
    "worker_documents_filtered_total", "Documents filtered by length", ["filter_reason"]
)
document_length_histogram = Histogram(
    "worker_document_length_characters", "Document length in characters"
)


class Worker:
    def __init__(
        self,
        downloader: Optional[Downloader] = None,
        channel=None,
        object_store=None,
        bucket_name: Optional[str] = None,
        tokenizer_name: Optional[str] = None,
        min_doc_length: int = 500,
        max_doc_length: int = 1000000,
    ):
        self.downloader = downloader or CCDownloader(BASE_URL)
        self.channel = channel or rabbitmq_channel()
        self.object_store = object_store or ObjectStore()
        self.bucket_name = bucket_name or os.getenv(
            "BUCKET_NAME", "commoncrawl-extracted"
        )
        self.min_doc_length = min_doc_length
        self.max_doc_length = max_doc_length
        self.tokenizer = None

        # Initialize tokenizer if specified
        if tokenizer_name:
            try:
                self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_name)
                log_with_fields(
                    setup_logger("worker"),
                    "info",
                    "Tokenizer initialized",
                    tokenizer_name=tokenizer_name,
                )
            except Exception as e:
                log_with_fields(
                    setup_logger("worker"),
                    "error",
                    "Failed to initialize tokenizer",
                    tokenizer_name=tokenizer_name,
                    error=str(e),
                )
                self.tokenizer = None

        self.logger = setup_logger("worker")

    def _tokenize_text(self, text: str) -> Optional[Dict[str, Any]]:
        """Tokenize text if tokenizer is available."""
        if not self.tokenizer:
            return None

        try:
            # Tokenize the text
            tokens = self.tokenizer.encode(
                text,
                truncation=True,
                max_length=(
                    self.tokenizer.model_max_length
                    if hasattr(self.tokenizer, "model_max_length")
                    else 512
                ),
            )

            # Record metrics
            tokenization_counter.inc()
            token_count_histogram.observe(len(tokens))

            log_with_fields(
                self.logger,
                "debug",
                "Text tokenized successfully",
                token_count=len(tokens),
                tokenizer_name=(
                    self.tokenizer.name_or_path
                    if hasattr(self.tokenizer, "name_or_path")
                    else "unknown"
                ),
            )

            return {
                "tokens": tokens,
                "token_count": len(tokens),
                "tokenizer_name": (
                    self.tokenizer.name_or_path
                    if hasattr(self.tokenizer, "name_or_path")
                    else "unknown"
                ),
                "truncated": len(self.tokenizer.encode(text)) > len(tokens),
            }

        except Exception as e:
            tokenization_errors_counter.labels(error_type="tokenization_failed").inc()
            log_with_fields(
                self.logger,
                "error",
                "Tokenization failed",
                error=str(e),
                text_length=len(text),
            )
            return None

    def _is_document_length_valid(self, text: str) -> bool:
        """Check if document length is within acceptable limits."""
        text_length = len(text)
        document_length_histogram.observe(text_length)

        if text_length < self.min_doc_length:
            documents_filtered_counter.labels(filter_reason="too_short").inc()
            log_with_fields(
                self.logger,
                "debug",
                "Document filtered: too short",
                text_length=text_length,
                min_length=self.min_doc_length,
            )
            return False

        if text_length > self.max_doc_length:
            documents_filtered_counter.labels(filter_reason="too_long").inc()
            log_with_fields(
                self.logger,
                "debug",
                "Document filtered: too long",
                text_length=text_length,
                max_length=self.max_doc_length,
            )
            return False

        return True

    def process_batch(self, ch, method, _properties, body):
        batch_start_time = time.time()

        try:
            batch = json.loads(body)
            log_with_fields(
                self.logger,
                "info",
                "Processing batch",
                batch_size=len(batch),
                delivery_tag=method.delivery_tag,
            )

            for item in batch:
                self._process_document(item)

            batches_processed_counter.inc()
            batch_processing_time_histogram.observe(time.time() - batch_start_time)

            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            processing_errors_counter.labels(error_type="batch_processing").inc()
            log_with_fields(
                self.logger,
                "error",
                "Error processing batch",
                error=str(e),
                delivery_tag=method.delivery_tag,
            )
            # Still ack to avoid infinite retries - consider dead letter queue
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def _process_document(self, item):
        """Process a single document from the batch."""
        doc_start_time = time.time()

        try:
            data = self.downloader.download_and_unzip(
                item["metadata"]["filename"],
                int(item["metadata"]["offset"]),
                int(item["metadata"]["length"]),
            )

            download_bytes_counter.inc(len(data))
            documents_processed_counter.inc()

            log_with_fields(
                self.logger,
                "debug",
                "Processing document",
                filename=item["metadata"]["filename"],
                size_bytes=len(data),
            )

            for record in WARCIterator(io.BytesIO(data)):
                warc_records_processed_counter.labels(record_type=record.rec_type).inc()

                if record.rec_type == "response":
                    try:
                        content = record.content_stream().read()
                        extracted_text = trafilatura.extract(content)

                        if extracted_text:
                            text_extracted_counter.inc()

                            # Check document length filter
                            if not self._is_document_length_valid(extracted_text):
                                # Skip documents that don't meet length requirements
                                continue

                            # Tokenize text if enabled
                            tokenization_data = self._tokenize_text(extracted_text)

                            # Prepare document data for storage
                            document_data = {
                                "url": item["metadata"].get(
                                    "url", item.get("surt_url", "unknown")
                                ),
                                "surt_url": item.get("surt_url", "unknown"),
                                "timestamp": item.get("timestamp", "unknown"),
                                "text": extracted_text,
                                "text_length": len(extracted_text),
                                "filename": item["metadata"]["filename"],
                                "processing_timestamp": time.strftime(
                                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                                ),
                            }

                            # Add tokenization data if available
                            if tokenization_data:
                                document_data["tokenization"] = tokenization_data

                            # Store to object store
                            if self.object_store.store_document(
                                self.bucket_name, document_data
                            ):
                                documents_stored_counter.inc()
                                log_with_fields(
                                    self.logger,
                                    "debug",
                                    "Document stored successfully",
                                    text_length=len(extracted_text),
                                    url=document_data["url"],
                                )
                            else:
                                storage_errors_counter.labels(
                                    error_type="store_document"
                                ).inc()
                                log_with_fields(
                                    self.logger,
                                    "warning",
                                    "Failed to store document",
                                    url=document_data["url"],
                                )

                    except Exception as e:
                        processing_errors_counter.labels(
                            error_type="text_extraction"
                        ).inc()
                        log_with_fields(
                            self.logger,
                            "warning",
                            "Text extraction failed",
                            error=str(e),
                            url=item.get("surt_url", "unknown"),
                        )

            document_processing_time_histogram.observe(time.time() - doc_start_time)

        except Exception as e:
            processing_errors_counter.labels(error_type="document_processing").inc()
            log_with_fields(
                self.logger,
                "error",
                "Document processing failed",
                error=str(e),
                filename=item["metadata"]["filename"],
            )

    def run(self):
        start_http_server(9001)

        while True:
            try:
                self.channel.basic_qos(prefetch_count=1)
                self.channel.basic_consume(
                    queue=QUEUE_NAME,
                    on_message_callback=self.process_batch,
                )

                log_with_fields(
                    self.logger,
                    "info",
                    "Worker started consuming messages",
                    queue_name=QUEUE_NAME,
                )

                self.channel.start_consuming()

            except KeyboardInterrupt:
                log_with_fields(self.logger, "info", "Worker shutting down")
                self.channel.stop_consuming()
                break
            except Exception as e:
                log_with_fields(
                    self.logger,
                    "error",
                    "Worker encountered error, reconnecting",
                    error=str(e),
                )
                # Wait before reconnecting
                time.sleep(5)
                # Recreate the channel connection
                self.channel = rabbitmq_channel()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Common Crawl Worker")
    parser.add_argument(
        "--tokenizer",
        type=str,
        default="gpt2",
        help=(
            "Tokenizer to use (e.g., 'gpt2', 'bert-base-uncased', "
            "'microsoft/DialoGPT-medium'). Default: gpt2."
        ),
    )
    parser.add_argument(
        "--min-doc-length",
        type=int,
        default=500,
        help="Minimum document length in characters (default: 500)",
    )
    parser.add_argument(
        "--max-doc-length",
        type=int,
        default=1000000,
        help="Maximum document length in characters (default: 1000000)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Create worker with tokenization and length filtering settings
    worker = Worker(
        tokenizer_name=args.tokenizer,
        min_doc_length=args.min_doc_length,
        max_doc_length=args.max_doc_length,
    )

    log_with_fields(
        setup_logger("worker"),
        "info",
        "Starting worker",
        bucket_name=worker.bucket_name,
        tokenizer_enabled=args.tokenizer is not None,
        tokenizer_name=args.tokenizer,
        min_doc_length=args.min_doc_length,
        max_doc_length=args.max_doc_length,
    )

    worker.run()


if __name__ == "__main__":
    main()
