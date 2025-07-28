import os
import time
import logging
from abc import ABC, abstractmethod
from typing import Optional

import pika
import pika.adapters.blocking_connection
from pika.exceptions import AMQPConnectionError, AMQPChannelError, ConnectionClosed, ChannelClosed
from prometheus_client import Counter, Histogram
from tenacity import (
    retry,
    stop_after_attempt, 
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)

from .logging import log_with_fields, setup_logger

QUEUE_NAME = "batches"
DEAD_LETTER_QUEUE_NAME = "batches.dead_letter"

# Prometheus metrics for RabbitMQ operations
rabbitmq_publish_attempts_counter = Counter(
    "rabbitmq_publish_attempts_total", "Total number of publish attempts"
)
rabbitmq_publish_failures_counter = Counter(
    "rabbitmq_publish_failures_total", "Number of publish failures", ["error_type"]
)
rabbitmq_publish_retries_counter = Counter(
    "rabbitmq_publish_retries_total", "Number of publish retries"
)
rabbitmq_connection_failures_counter = Counter(
    "rabbitmq_connection_failures_total", "Number of connection failures"
)
rabbitmq_publish_duration_histogram = Histogram(
    "rabbitmq_publish_duration_seconds", "Time spent publishing messages"
)


class MessageQueueChannel(ABC):
    @abstractmethod
    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        pass


class RabbitMQChannel(MessageQueueChannel):
    def __init__(self) -> None:
        self.connection: Optional[pika.adapters.blocking_connection.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self.logger = setup_logger("rabbitmq")
        self._connect()

    def _connect(self) -> None:
        """Establish connection to RabbitMQ with retry logic."""
        try:
            self.connection = pika.BlockingConnection(
                pika.URLParameters(os.environ["RABBITMQ_CONNECTION_STRING"])
            )
            self.channel = self.connection.channel()
            
            # Declare main queue with dead letter exchange
            self.channel.queue_declare(
                queue=QUEUE_NAME,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': DEAD_LETTER_QUEUE_NAME,
                    'x-message-ttl': 3600000,  # 1 hour TTL
                }
            )
            
            # Declare dead letter queue
            self.channel.queue_declare(queue=DEAD_LETTER_QUEUE_NAME, durable=True)
            
        except Exception as e:
            rabbitmq_connection_failures_counter.inc()
            raise

    def _ensure_connection(self) -> None:
        """Ensure we have a valid connection, reconnect if necessary."""
        if self.connection is None or self.connection.is_closed:
            self._connect()
        elif self.channel is None or self.channel.is_closed:
            assert self.connection is not None  # For mypy
            self.channel = self.connection.channel()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((AMQPConnectionError, AMQPChannelError, ConnectionClosed, ChannelClosed)),
        before_sleep=before_sleep_log(setup_logger("rabbitmq"), logging.WARNING)
    )
    def basic_publish(self, exchange: str, routing_key: str, body: str) -> None:
        start_time = time.time()
        rabbitmq_publish_attempts_counter.inc()
        
        try:
            self._ensure_connection()
            
            # Type assertions for mypy
            assert self.channel is not None
            
            # Publish with delivery confirmation
            self.channel.confirm_delivery()
            success = self.channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                ),
                mandatory=True  # Return message if queue doesn't exist
            )
            
            if not success:
                rabbitmq_publish_failures_counter.labels(error_type="delivery_failed").inc()
                raise AMQPChannelError("Message delivery was not confirmed")
                
        except (AMQPConnectionError, AMQPChannelError, ConnectionClosed, ChannelClosed) as e:
            rabbitmq_publish_retries_counter.inc()
            rabbitmq_publish_failures_counter.labels(error_type=type(e).__name__).inc()
            # Invalidate connection for retry
            self.connection = None
            self.channel = None
            raise
        except Exception as e:
            rabbitmq_publish_failures_counter.labels(error_type="unexpected_error").inc()
            raise
        finally:
            rabbitmq_publish_duration_histogram.observe(time.time() - start_time)

    def close(self) -> None:
        """Close the connection gracefully."""
        if self.channel is not None and not self.channel.is_closed:
            self.channel.close()
        if self.connection is not None and not self.connection.is_closed:
            self.connection.close()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((AMQPConnectionError, ConnectionClosed)),
    before_sleep=before_sleep_log(setup_logger("rabbitmq"), logging.WARNING)
)
def rabbitmq_channel() -> pika.adapters.blocking_connection.BlockingChannel:
    """Create a RabbitMQ channel with retry logic for worker consumption."""
    try:
        connection = pika.BlockingConnection(
            pika.URLParameters(os.environ["RABBITMQ_CONNECTION_STRING"])
        )
        channel = connection.channel()
        
        # Declare main queue with dead letter exchange for workers
        channel.queue_declare(
            queue=QUEUE_NAME,
            durable=True,
            arguments={
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': DEAD_LETTER_QUEUE_NAME,
                'x-message-ttl': 3600000,  # 1 hour TTL
            }
        )
        
        # Declare dead letter queue
        channel.queue_declare(queue=DEAD_LETTER_QUEUE_NAME, durable=True)
        
        return channel
        
    except Exception as e:
        rabbitmq_connection_failures_counter.inc()
        raise
