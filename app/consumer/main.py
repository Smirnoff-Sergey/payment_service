"""Payment consumer: one handler that charges, stores the result and sends the webhook.

Failures are retried through the broker (see app.messaging.topology): the message
is republished to a delayed retry queue with an incremented `x-retry-count`
header. After `max_retries` retries it is rejected and lands in the DLQ.
"""

import logging

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError

from app.config import Settings, get_settings
from app.consumer.gateway import EmulatedGateway
from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.consumer.webhook import WebhookSender
from app.db import session_factory
from app.messaging.topology import (
    RETRY_COUNT_HEADER,
    declare_topology,
    payments_exchange,
    payments_queue,
    retry_exchange,
    retry_routing_key,
)
from app.schemas import PaymentEvent

logger = logging.getLogger("payment_consumer")


async def handle_message(
    msg: RabbitMessage, processor: PaymentProcessor, broker: RabbitBroker, settings: Settings
) -> None:
    try:
        event = PaymentEvent.model_validate_json(msg.body)
    except ValidationError:
        logger.error("Malformed message %s, sending to DLQ", msg.message_id)
        await msg.reject()
        return

    try:
        await processor.process(event.payment_id)
    except PaymentNotFoundError:
        logger.error("Payment %s not found, sending to DLQ", event.payment_id)
        await msg.reject()
        return
    except Exception:
        logger.exception("Failed to process payment %s", event.payment_id)
        await _retry_or_dead_letter(msg, broker, settings)
        return

    await msg.ack()


async def _retry_or_dead_letter(msg: RabbitMessage, broker: RabbitBroker, settings: Settings) -> None:
    attempt = int(msg.headers.get(RETRY_COUNT_HEADER, 0)) + 1
    if attempt > settings.max_retries:
        logger.error("Message %s exhausted %d retries, sending to DLQ", msg.message_id, settings.max_retries)
        await msg.reject()
        return

    try:
        await broker.publish(
            msg.body,
            exchange=retry_exchange,
            routing_key=retry_routing_key(attempt),
            headers={
                # x-death is managed by RabbitMQ itself and must not be republished.
                **{k: v for k, v in msg.headers.items() if k != "x-death"},
                RETRY_COUNT_HEADER: attempt,
            },
            message_id=msg.message_id,
            content_type=msg.content_type,
            persist=True,
        )
    except Exception:
        # Could not schedule a retry: return the message to the queue as is.
        logger.exception("Failed to schedule retry for message %s", msg.message_id)
        await msg.nack(requeue=True)
        return

    logger.warning("Retry %d/%d scheduled for message %s", attempt, settings.max_retries, msg.message_id)
    await msg.ack()


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

    @broker.subscriber(payments_queue, payments_exchange, ack_policy=AckPolicy.MANUAL)
    async def handle_payment(msg: RabbitMessage) -> None:
        await handle_message(msg, processor, broker, settings)

    @app.after_startup
    async def declare() -> None:
        await declare_topology(broker, settings.max_retries, settings.retry_base_delay)

    @app.after_shutdown
    async def close_http_client() -> None:
        await http_client.aclose()

    return app


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
app = create_app()
