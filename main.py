import logging
import os

import pika
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from consumer import PaymentEventConsumer

logging.basicConfig(level=logging.INFO)

app = FastAPI()
consumer = PaymentEventConsumer()


@app.on_event("startup")
def startup():
    consumer.start_in_thread()


@app.on_event("shutdown")
def shutdown():
    consumer.stop()


@app.get("/")
def root():
    return {"message": "notification-service"}


@app.get("/health/live")
def health_live():
    return {"status": "alive"}


@app.get("/health/ready")
def health_ready():
    host = os.getenv("RABBITMQ_HOST", "localhost")
    port = int(os.getenv("RABBITMQ_PORT", "5672"))
    user = os.getenv("RABBITMQ_USER", "guest")
    password = os.getenv("RABBITMQ_PASSWORD", "guest")

    try:
        creds = pika.PlainCredentials(user, password)
        params = pika.ConnectionParameters(host=host, port=port, credentials=creds, socket_timeout=3)
        conn = pika.BlockingConnection(params)
        conn.close()
        return {"status": "ready"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "not_ready", "error": str(e)})
