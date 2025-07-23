import io
import json
import time
from typing import Optional

import trafilatura
from prometheus_client import Counter, Histogram, start_http_server
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


class Worker:
    def __init__(
        self,
        downloader: Optional[Downloader] = None,
        channel=None,
        object_store=None,
        bucket_name: str = "commoncrawl-extracted",
    ):
        self.downloader = downloader or CCDownloader(BASE_URL)
        self.channel = channel or rabbitmq_channel()
        self.object_store = object_store or ObjectStore()
        self.bucket_name = bucket_name
        self.logger = setup_logger("worker")

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
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=self.process_batch,
        )
        self.channel.start_consuming()


def main() -> None:
    worker = Worker()
    worker.run()


if __name__ == "__main__":
    main()
