"""
Notification Service - Main Application

FastAPI application that consumes RabbitMQ events and sends notifications via ntfy.sh.
"""

import logging
import os
from typing import Any

import pika
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from consumer import PaymentEventConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Pydantic models for event schema documentation
class PaymentProcessedEvent(BaseModel):
    """Schema for payment.processed RabbitMQ event."""

    event_type: str = Field(
        default="payment.processed",
        description="Type of the event",
        examples=["payment.processed"],
    )
    transaction_id: str = Field(
        description="UUID of the payment transaction",
        examples=["b1c0d802-2bee-4ddc-aa55-8afbeec8ceec"],
    )
    reservation_id: str = Field(
        description="UUID of the reservation",
        examples=["162cf241-62fd-4e4b-89cf-69b7bb47ca8f"],
    )
    user_id: str = Field(
        description="UUID of the user who made the payment",
        examples=["19bcfab8-80ab-445e-a10f-a0ca0cfaf582"],
    )
    amount: float = Field(
        description="Payment amount",
        examples=[4.0, 10.50],
    )
    currency: str = Field(
        default="EUR",
        description="Currency code",
        examples=["EUR", "USD"],
    )
    timestamp: str = Field(
        description="ISO 8601 timestamp of the event",
        examples=["2026-01-01T12:00:00Z"],
    )


# OpenAPI tags for documentation organization
tags_metadata = [
    {
        "name": "info",
        "description": "Service information",
    },
    {
        "name": "health",
        "description": "Health check endpoints for Kubernetes liveness and readiness probes",
    },
    {
        "name": "events",
        "description": "RabbitMQ event schema documentation",
    },
]

# Create FastAPI app with enhanced OpenAPI metadata
app = FastAPI(
    title="Notification Service",
    description="""
## Notification service for Parkora

This service consumes RabbitMQ events and sends push notifications via **ntfy.sh**.

### RabbitMQ Consumer

The service listens to a RabbitMQ queue for payment events:

| Property | Value |
|----------|-------|
| **Exchange** | `parkora_events` (topic) |
| **Queue** | `notification_queue` |
| **Routing Key** | `payment.processed` |

### Notification Channels

When a payment event is received, notifications are sent to:

1. **ntfy.sh** - Push notifications
   - Topic: `parkora-notification-service`
   - Web: https://ntfy.sh/parkora-notification-service
   - Subscribe in ntfy app with topic: `parkora-notification-service`

### Event Schema

See the `/events/schema` endpoint for the JSON schema of consumed events.
    """,
    version="1.0.0",
    contact={
        "name": "Parkora Team",
        "email": "team@parkora.crn.si",
        "url": "https://parkora.crn.si",
    },
    license_info={
        "name": "MIT",
        "url": "https://opensource.org/licenses/MIT",
    },
    openapi_tags=tags_metadata,
)

consumer = PaymentEventConsumer()


@app.on_event("startup")
def startup():
    """Start RabbitMQ consumer in a background thread."""
    logger.info("Starting Notification Service...")
    consumer.start_in_thread()


@app.on_event("shutdown")
def shutdown():
    """Stop RabbitMQ consumer."""
    logger.info("Shutting down Notification Service...")
    consumer.stop()


@app.get("/", tags=["info"])
def root():
    """
    Root endpoint with service information.

    Returns basic information about the service and available endpoints.
    """
    return {
        "service": "Notification Service",
        "version": "1.0.0",
        "description": "RabbitMQ consumer that sends notifications via ntfy.sh",
        "endpoints": {
            "health": "/health/live, /health/ready",
            "docs": "/docs, /redoc, /openapi.json",
            "events": "/events/schema",
        },
        "rabbitmq": {
            "exchange": "parkora_events",
            "queue": "notification_queue",
            "routing_key": "payment.processed",
        },
        "ntfy_topic": "parkora-notification-service",
    }


@app.get("/health/live", tags=["health"])
def health_live():
    """
    Liveness probe endpoint.

    Returns 200 if the service is running.
    Used by Kubernetes for liveness checks.
    """
    return {"status": "alive"}


@app.get("/health/ready", tags=["health"])
def health_ready():
    """
    Readiness probe endpoint.

    Returns 200 if the service is ready to accept requests.
    Checks RabbitMQ connectivity.
    Used by Kubernetes for readiness checks.
    """
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASSWORD", "guest")

    try:
        creds = pika.PlainCredentials(user, password)
        params = pika.ConnectionParameters(
            host=host, port=port, credentials=creds, socket_timeout=3
        )
        conn = pika.BlockingConnection(params)
        conn.close()
        return {"status": "ready", "rabbitmq": "connected"}
    except Exception as e:
        logger.warning(f"RabbitMQ health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "rabbitmq": "disconnected",
                "error": str(e),
            },
        )


@app.get("/events/schema", tags=["events"])
def get_event_schema() -> dict[str, Any]:
    """
    Get the JSON schema for consumed RabbitMQ events.

    Returns the schema definitions for all event types that this service
    consumes from RabbitMQ.
    """
    return {
        "description": "RabbitMQ event schemas consumed by notification-service",
        "events": {
            "payment.processed": {
                "description": "Emitted by payment-service when a payment is processed",
                "routing_key": "payment.processed",
                "exchange": "parkora_events",
                "schema": PaymentProcessedEvent.model_json_schema(),
            }
        },
    }
