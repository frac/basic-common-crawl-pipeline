from unittest.mock import MagicMock, call, patch

import pytest
from pika.exceptions import AMQPConnectionError
from tenacity import RetryError

from bccp.rabbitmq import (
    DEAD_LETTER_QUEUE_NAME,
    QUEUE_NAME,
    RabbitMQChannel,
    rabbitmq_channel,
    rabbitmq_connection_failures_counter,
    rabbitmq_publish_attempts_counter,
    rabbitmq_publish_failures_counter,
    rabbitmq_publish_retries_counter,
)


@patch.dict("os.environ", {"RABBITMQ_CONNECTION_STRING": "amqp://localhost"})
def test_rabbitmq_channel_successful_publish():
    """Test successful message publishing."""
    with patch("bccp.rabbitmq.pika.BlockingConnection") as mock_connection_class:
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        mock_connection_class.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_channel.basic_publish.return_value = True

        # Create RabbitMQ channel
        rabbitmq_channel_instance = RabbitMQChannel()

        # Test publishing
        initial_attempts = rabbitmq_publish_attempts_counter._value.get()
        rabbitmq_channel_instance.basic_publish("", QUEUE_NAME, "test message")
        final_attempts = rabbitmq_publish_attempts_counter._value.get()

        # Verify metrics
        assert final_attempts > initial_attempts

        # Verify calls
        mock_channel.confirm_delivery.assert_called_once()
        mock_channel.basic_publish.assert_called_once()


@patch.dict("os.environ", {"RABBITMQ_CONNECTION_STRING": "amqp://localhost"})
def test_rabbitmq_channel_connection_retry():
    """Test connection retry logic."""
    with patch("bccp.rabbitmq.pika.BlockingConnection") as mock_connection_class:
        # First call fails, second succeeds
        mock_connection_class.side_effect = [
            AMQPConnectionError("Connection failed"),
            MagicMock(),
        ]

        initial_failures = rabbitmq_connection_failures_counter._value.get()

        with pytest.raises(AMQPConnectionError):
            RabbitMQChannel()

        final_failures = rabbitmq_connection_failures_counter._value.get()
        assert final_failures > initial_failures


@patch.dict("os.environ", {"RABBITMQ_CONNECTION_STRING": "amqp://localhost"})
def test_rabbitmq_channel_publish_retry():
    """Test publish retry logic on connection errors."""
    with patch("bccp.rabbitmq.pika.BlockingConnection") as mock_connection_class:
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        mock_connection_class.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        # First publish fails with connection error, subsequent retries succeed
        mock_channel.basic_publish.side_effect = [
            AMQPConnectionError("Connection lost"),
            True,  # Success on retry
        ]

        rabbitmq_channel_instance = RabbitMQChannel()

        initial_retries = rabbitmq_publish_retries_counter._value.get()
        initial_failures = rabbitmq_publish_failures_counter.labels(
            error_type="AMQPConnectionError"
        )._value.get()

        # This should eventually succeed after retry
        rabbitmq_channel_instance.basic_publish("", QUEUE_NAME, "test message")

        final_retries = rabbitmq_publish_retries_counter._value.get()
        final_failures = rabbitmq_publish_failures_counter.labels(
            error_type="AMQPConnectionError"
        )._value.get()

        assert final_retries > initial_retries
        assert final_failures > initial_failures


@patch.dict("os.environ", {"RABBITMQ_CONNECTION_STRING": "amqp://localhost"})
def test_rabbitmq_channel_publish_delivery_failed():
    """Test handling of delivery confirmation failures."""
    with patch("bccp.rabbitmq.pika.BlockingConnection") as mock_connection_class:
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        mock_connection_class.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_channel.basic_publish.return_value = False  # Delivery not confirmed

        rabbitmq_channel_instance = RabbitMQChannel()

        initial_failures = rabbitmq_publish_failures_counter.labels(
            error_type="delivery_failed"
        )._value.get()

        with pytest.raises(RetryError):
            rabbitmq_channel_instance.basic_publish("", QUEUE_NAME, "test message")

        final_failures = rabbitmq_publish_failures_counter.labels(
            error_type="delivery_failed"
        )._value.get()
        assert final_failures > initial_failures


@patch.dict("os.environ", {"RABBITMQ_CONNECTION_STRING": "amqp://localhost"})
def test_rabbitmq_channel_ensure_connection():
    """Test connection recovery logic."""
    with patch("bccp.rabbitmq.pika.BlockingConnection") as mock_connection_class:
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        mock_connection_class.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_channel.basic_publish.return_value = True

        rabbitmq_channel_instance = RabbitMQChannel()

        # Simulate connection closed
        rabbitmq_channel_instance.connection.is_closed = True

        # Should reconnect when publishing
        rabbitmq_channel_instance.basic_publish("", QUEUE_NAME, "test message")

        # Should have called connection setup again
        assert mock_connection_class.call_count >= 2


@patch.dict("os.environ", {"RABBITMQ_CONNECTION_STRING": "amqp://localhost"})
def test_rabbitmq_channel_queue_declarations():
    """Test that queues are properly declared with dead letter configuration."""
    with patch("bccp.rabbitmq.pika.BlockingConnection") as mock_connection_class:
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        mock_connection_class.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel

        RabbitMQChannel()

        # Verify main queue declaration with dead letter config
        main_queue_call = call(
            queue=QUEUE_NAME,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": DEAD_LETTER_QUEUE_NAME,
                "x-message-ttl": 3600000,
            },
        )

        # Verify dead letter queue declaration
        dlq_call = call(queue=DEAD_LETTER_QUEUE_NAME, durable=True)

        mock_channel.queue_declare.assert_has_calls([main_queue_call, dlq_call])


@patch.dict("os.environ", {"RABBITMQ_CONNECTION_STRING": "amqp://localhost"})
def test_rabbitmq_channel_close():
    """Test graceful connection closing."""
    with patch("bccp.rabbitmq.pika.BlockingConnection") as mock_connection_class:
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        mock_connection_class.return_value = mock_connection
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_channel.is_closed = False

        rabbitmq_channel_instance = RabbitMQChannel()
        rabbitmq_channel_instance.close()

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()


@patch.dict("os.environ", {"RABBITMQ_CONNECTION_STRING": "amqp://test"})
def test_rabbitmq_channel_function_retry():
    """Test the standalone rabbitmq_channel function with retry logic."""
    with patch("bccp.rabbitmq.pika.BlockingConnection") as mock_connection_class:
        mock_connection = MagicMock()
        mock_channel = MagicMock()

        # First call fails, second succeeds
        mock_connection_class.side_effect = [
            AMQPConnectionError("Connection failed"),
            mock_connection,
        ]
        mock_connection.channel.return_value = mock_channel

        # Should eventually succeed after retry
        result = rabbitmq_channel()

        assert result == mock_channel
        assert mock_connection_class.call_count == 2
