"""Message handling with broker-side retries.

Failures are retried through the broker (see app.messaging.topology): the message
is republished to a delayed retry queue with an incremented `x-retry-count`
header. After `max_retries` retries it is rejected and lands in the DLQ.
"""

import logging

from faststream.rabbit import RabbitBroker, RabbitMessage
from pydantic import ValidationError

from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.messaging.topology import RETRY_COUNT_HEADER, retry_exchange, retry_routing_key
from app.schemas import PaymentEvent

logger = logging.getLogger("payment_consumer")


class PaymentMessageHandler:
    def __init__(self, processor: PaymentProcessor, broker: RabbitBroker, max_retries: int) -> None:
        self._processor = processor
        self._broker = broker
        self._max_retries = max_retries

    async def handle(self, msg: RabbitMessage) -> None:
        try:
            event = PaymentEvent.model_validate_json(msg.body)
        except ValidationError:
            logger.error("Malformed message %s, sending to DLQ", msg.message_id)
            await msg.reject()
            return

        try:
            await self._processor.process(event.payment_id)
        except PaymentNotFoundError:
            logger.error("Payment %s not found, sending to DLQ", event.payment_id)
            await msg.reject()
            return
        except Exception:
            logger.exception("Failed to process payment %s", event.payment_id)
            await self._retry_or_dead_letter(msg)
            return

        await msg.ack()

    async def _retry_or_dead_letter(self, msg: RabbitMessage) -> None:
        attempt = int(msg.headers.get(RETRY_COUNT_HEADER, 0)) + 1
        if attempt > self._max_retries:
            logger.error("Message %s exhausted %d retries, sending to DLQ", msg.message_id, self._max_retries)
            await msg.reject()
            return

        try:
            await self._broker.publish(
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

        logger.warning("Retry %d/%d scheduled for message %s", attempt, self._max_retries, msg.message_id)
        await msg.ack()
