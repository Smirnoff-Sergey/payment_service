"""Payment consumer entry point: `faststream run app.consumer.main:app`."""

import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.config import get_settings
from app.consumer.gateway import EmulatedGateway
from app.consumer.handler import PaymentMessageHandler
from app.consumer.processor import PaymentProcessor
from app.consumer.webhook import WebhookSender
from app.db import session_factory
from app.messaging.topology import declare_topology, payments_exchange, payments_queue


def create_app() -> FastStream:
    settings = get_settings()
    broker = RabbitBroker(settings.rabbit_url)
    app = FastStream(broker)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    processor = PaymentProcessor(
        session_factory,
        EmulatedGateway(settings.gateway_min_delay, settings.gateway_max_delay, settings.gateway_success_rate),
        WebhookSender(http_client),
    )
    handler = PaymentMessageHandler(processor, broker, settings.max_retries)

    @broker.subscriber(payments_queue, payments_exchange, ack_policy=AckPolicy.MANUAL)
    async def handle_payment(msg: RabbitMessage) -> None:
        await handler.handle(msg)

    @app.after_startup
    async def declare() -> None:
        await declare_topology(broker, settings.max_retries, settings.retry_base_delay)

    @app.after_shutdown
    async def close_http_client() -> None:
        await http_client.aclose()

    return app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
app = create_app()
