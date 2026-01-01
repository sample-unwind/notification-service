import json
import logging
import os
import threading
import time
from typing import Optional

import httpx
import pika

logger = logging.getLogger("notification")

# ntfy.sh configuration
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "parkora-notification-service")
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"


class PaymentEventConsumer:
    def __init__(self):
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = (
            None
        )

        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        self._host = os.getenv("RABBITMQ_HOST", "localhost")
        self._port = int(os.getenv("RABBITMQ_PORT", "5672"))
        self._user = os.getenv("RABBITMQ_USER", "guest")
        self._password = os.getenv("RABBITMQ_PASSWORD", "guest")

        self._exchange = "payments"
        self._queue = "notification_queue"
        self._routing_key = "payment.processed"

    def _connect(self) -> None:
        creds = pika.PlainCredentials(self._user, self._password)
        params = pika.ConnectionParameters(
            host=self._host,
            port=self._port,
            credentials=creds,
            heartbeat=30,
            blocked_connection_timeout=30,
            connection_attempts=5,
            retry_delay=2,
        )

        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

        # Bind to exchange
        self._channel.exchange_declare(
            exchange=self._exchange, exchange_type="topic", durable=True
        )

        # Declare/bind durable queue
        self._channel.queue_declare(queue=self._queue, durable=True)
        self._channel.queue_bind(
            exchange=self._exchange, queue=self._queue, routing_key=self._routing_key
        )

        # Fair dispatch
        self._channel.basic_qos(prefetch_count=10)

    def callback(self, ch, method, properties, body):
        """
        body is bytes. We log it as JSON string to match expected output.
        """
        try:
            body_str = body.decode("utf-8")
            logger.info("Received payment.processed event: %s", body_str)

            message = json.loads(body_str)

            # Dummy notification (log)
            self.send_notification(message)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            logger.exception("Failed processing message; rejecting (requeue=false)")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def send_notification(self, payment_event: dict) -> None:
        """
        Send notification via ntfy.sh push notification service.
        Also logs the notification for debugging.
        """
        user_id = payment_event.get("user_id", "unknown")
        amount = payment_event.get("amount", 0)
        currency = payment_event.get("currency", "EUR")
        reservation_id = payment_event.get("reservation_id", "unknown")
        transaction_id = payment_event.get("transaction_id", "unknown")

        # Log the notification
        logger.info(
            f"[NOTIFICATION] Payment confirmed for user {user_id}: "
            f"{amount} {currency} for reservation {reservation_id}"
        )

        # Send to ntfy.sh
        try:
            message = (
                f"Amount: {amount} {currency}\n"
                f"Reservation: {reservation_id}\n"
                f"Transaction: {transaction_id}"
            )
            response = httpx.post(
                NTFY_URL,
                content=message,
                headers={
                    "Title": "Parkora Payment Confirmed",
                    "Priority": "default",
                    "Tags": "credit_card,white_check_mark",
                },
                timeout=10.0,
            )
            response.raise_for_status()
            logger.info(f"ntfy.sh notification sent successfully to {NTFY_TOPIC}")
        except httpx.TimeoutException:
            logger.warning(f"Timeout sending ntfy notification to {NTFY_TOPIC}")
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP error sending ntfy notification: {e}")
        except Exception as e:
            logger.warning(f"Failed to send ntfy notification: {e}")

    def start_consuming(self) -> None:
        """
        Blocking consumer loop with reconnect.
        """
        logger.info("Starting to consume messages from notification_queue...")

        while not self._stop_event.is_set():
            try:
                if not self._connection or self._connection.is_closed:
                    self._connect()

                assert self._channel is not None
                self._channel.basic_consume(
                    queue=self._queue, on_message_callback=self.callback
                )

                # Blocks until connection breaks or channel/connection is closed
                self._channel.start_consuming()

            except pika.exceptions.AMQPConnectionError:
                logger.warning("RabbitMQ connection error; retrying in 2s...")
                self._cleanup()
                time.sleep(2)
            except Exception:
                logger.exception("Unexpected consumer error; retrying in 2s...")
                self._cleanup()
                time.sleep(2)

        self._cleanup()
        logger.info("Consumer stopped.")

    def start_in_thread(self) -> None:
        """
        Run consumer in background thread (for FastAPI app).
        """
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self.start_consuming, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        try:
            # Closing connection forces start_consuming to break out.
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception:
            logger.exception("Error while stopping consumer")
        finally:
            self._cleanup()

    def _cleanup(self) -> None:
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
        except Exception:
            pass

        try:
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception:
            pass

        self._channel = None
        self._connection = None

    def is_connected(self) -> bool:
        return bool(
            self._connection
            and self._connection.is_open
            and self._channel
            and self._channel.is_open
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = PaymentEventConsumer()
    consumer.start_consuming()
