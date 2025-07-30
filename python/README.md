# Basic Common Crawl Pipeline

A Python-based pipeline for processing Common Crawl data with RabbitMQ message queuing, text extraction, and object storage capabilities.

## Overview

This pipeline processes Common Crawl data through a two-stage architecture:

1. **Batcher**: Downloads CDX index files, filters URLs, and publishes batches to RabbitMQ
2. **Worker**: Consumes batches, downloads WARC files, extracts text content, and stores processed documents

## Architecture

```
Common Crawl CDX Index → Batcher → RabbitMQ → Worker → Object Storage
                           ↓                    ↓
                     Progress Monitoring   Text Extraction
                        & Filtering        & Tokenization
```

## Key Features

### ✅ Implemented Features

- **Configurable Common Crawl Versions**: Support for different CC-MAIN releases
- **Advanced Filtering**: Language and HTTP status code filtering with metrics
- **RabbitMQ Integration**: Robust message queuing with retry logic and error handling
- **Progress Monitoring**: Real-time progress tracking with percentage completion and time estimation
- **Prometheus Metrics**: Comprehensive monitoring and observability
- **Text Extraction**: HTML content extraction using trafilatura
- **Tokenization**: Optional text tokenization with Hugging Face transformers
- **Object Storage**: MinIO/S3-compatible storage for processed documents
- **Document Length Filtering**: Configurable min/max document length limits
- **Comprehensive Testing**: Unit, integration, and end-to-end tests

### 🚀 Recent Enhancements

#### Task #12: Batcher Progress Monitoring
- **Progress Tracking**: Real-time percentage completion of cluster.idx file processing
- **Batch Counting**: Track total number of batches published
- **Time Estimation**: Estimate remaining processing time based on current rate
- **Regular Logging**: Progress updates at configurable intervals (default: 30 seconds)
- **Prometheus Metrics**: New metrics for monitoring progress:
  - `index_processing_progress_gauge`: Progress percentage (0-100)
  - `estimated_remaining_time_gauge`: Estimated time remaining in seconds
  - `total_index_lines_gauge`: Total lines in index file
  - `processed_index_lines_counter`: Number of index lines processed

## Project Structure

```
bccp/
├── __init__.py
├── batcher.py          # CDX processing and batch publishing
├── worker.py           # WARC processing and text extraction
├── commoncrawl.py      # Common Crawl utilities and downloaders
├── rabbitmq.py         # RabbitMQ message queue implementation
└── objectstore.py      # MinIO/S3 object storage client

tests/
├── unit/               # Unit tests for individual components
├── integration/        # Integration tests for component interactions
├── e2e/               # End-to-end tests with real services
└── conftest.py        # Shared test fixtures and utilities
```

## Installation

1. **Install dependencies**:
   ```bash
   poetry install
   ```

2. **Start services** (requires Docker):
   ```bash
   docker-compose up -d
   ```
   
   or if not using S3 use this to spin a minio instance also
   
   ```bash
   docker-compose --profile no_s3 up -d
   ```
   


## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RABBITMQ_CONNECTION_STRING` | RabbitMQ connection URL | `amqp://guest:guest@localhost:5672/%2F` |
| `MINIO_ENDPOINT` | MinIO endpoint | `localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `DEFAULT_CRAWL_VERSION` | Common Crawl version | `CC-MAIN-2024-30` |

### Batcher Configuration

- **Crawl Version**: Specify Common Crawl release (e.g., `CC-MAIN-2024-30`)
- **Batch Size**: Number of URLs per batch (default: 50)
- **Progress Logging**: Interval for progress updates (default: 30 seconds)
- **Filtering**: Language (English only) and status code (200 only) filters

### Worker Configuration

- **Document Length**: Min/max character limits for processed documents
- **Tokenization**: Optional tokenizer for text analysis
- **Bucket Name**: S3/MinIO bucket for storing processed documents
- **Concurrency**: Prefetch count for RabbitMQ message processing

## Usage

### Running the Batcher

```bash
# Process a specific cluster index file
poetry run python -m bccp.batcher cluster-001.csv

# Use a different Common Crawl version
poetry run python -m bccp.batcher cluster-001.csv --crawl-version CC-MAIN-2024-26
```

### Running the Worker

```bash
# Basic worker with default settings
poetry run python -m bccp.worker

# Worker with custom tokenizer and document limits
poetry run python -m bccp.worker \
  --tokenizer-name bert-base-uncased \
  --min-doc-length 100 \
  --max-doc-length 10000 \
  --bucket-name my-processed-docs
```

## Testing

### Running All Tests

```bash
# Run complete test suite with coverage
make test

# Run tests without coverage
poetry run pytest
```

### Test Categories

```bash
# Unit tests only
poetry run pytest tests/unit/

# Integration tests only  
poetry run pytest tests/integration/

# End-to-end tests (requires Docker services)
make test-e2e
```

### Specific Test Examples

```bash
# Test progress monitoring functionality
poetry run pytest tests/unit/test_batcher.py::test_batcher_progress_tracking_count_lines -v

# Test RabbitMQ error handling
poetry run pytest tests/unit/test_rabbitmq.py::test_rabbitmq_retry_logic -v

# Test full pipeline integration
poetry run pytest tests/integration/test_pipeline_integration.py::test_batcher_worker_pipeline_integration -v
```

## Code Quality

### Linting and Formatting

```bash
# Run all linting checks
make lint

# Individual tools
poetry run flake8        # Style and error checking
poetry run black .       # Code formatting
poetry run isort .       # Import sorting
poetry run mypy -p bccp  # Type checking
```

### Test Coverage

```bash
# Generate coverage report
poetry run coverage run -m pytest
poetry run coverage report
poetry run coverage html  # HTML report in htmlcov/
```

## Monitoring and Metrics

### Prometheus Metrics

The pipeline exposes comprehensive metrics for monitoring:

#### Batcher Metrics
- `cdx_chunks_processed_counter`: Number of CDX chunks processed
- `urls_processed_counter`: Total URLs processed
- `urls_filtered_counter`: URLs filtered by reason (non_english, non_200_status, etc.)
- `batches_published_counter`: Number of batches published to RabbitMQ
- `index_processing_progress_gauge`: Progress percentage (0-100)
- `estimated_remaining_time_gauge`: Estimated time remaining
- `errors_counter`: Processing errors by type

#### Worker Metrics
- `batches_processed_counter`: Number of batches consumed
- `documents_processed_counter`: Documents processed from WARC files
- `text_extracted_counter`: Successful text extractions
- `documents_stored_counter`: Documents stored to object storage
- `documents_filtered_counter`: Documents filtered by length
- `download_bytes_counter`: Bytes downloaded from Common Crawl
- `processing_errors_counter`: Processing errors by type

### Health Checks

Both batcher and worker expose HTTP health check endpoints:
- Batcher: `http://localhost:9000/health`
- Worker: `http://localhost:9001/health`

## Development

### Adding New Features

1. **Create feature branch**: `git checkout -b feature/new-feature`
2. **Implement functionality** with comprehensive tests
3. **Run quality checks**: `make lint && make test`
4. **Update documentation** as needed
5. **Submit pull request** with description of changes

### Testing Guidelines

- **Unit tests**: Test individual functions and classes in isolation
- **Integration tests**: Test component interactions with mocked external services
- **E2E tests**: Test complete workflows with real services (Docker required)
- **Mock external dependencies**: Use fixtures from `tests/conftest.py`
- **Test edge cases**: Error conditions, filtering, retries, etc.

## Troubleshooting

### Common Issues

1. **RabbitMQ Connection Failed**
   ```bash
   # Check if RabbitMQ is running
   docker-compose ps rabbitmq
   
   # View RabbitMQ logs
   docker-compose logs rabbitmq
   ```

2. **MinIO Storage Errors**
   ```bash
   # Check MinIO status
   docker-compose ps minio
   
   # Verify bucket exists
   poetry run python -c "from bccp.objectstore import ObjectStore; print(ObjectStore().health_check())"
   ```

3. **Test Failures**
   ```bash
   # Run failed tests only
   poetry run pytest --lf -v
   
   # Run with detailed output
   poetry run pytest -vvv --tb=long
   ```

4. **Memory Issues with Large Files**
   - Adjust batch size: smaller batches use less memory
   - Monitor worker memory usage during processing
   - Configure appropriate document length limits

### Debugging

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

View detailed processing logs:
```bash
# Batcher logs show progress and filtering details
# Worker logs show text extraction and storage operations
```

## Performance Considerations

- **Batch Size**: Balance between memory usage and throughput (default: 50)
- **Worker Concurrency**: RabbitMQ prefetch count affects memory and throughput
- **Document Limits**: Filter very short/long documents to improve processing efficiency
- **Network**: Common Crawl downloads can be bandwidth-intensive
- **Storage**: Consider object storage performance for high-throughput scenarios

## License

This project is part of a coding challenge implementation.