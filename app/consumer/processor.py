import logging
import uuid
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.errors import PermanentProcessingError
from app.models import Payment, PaymentStatus
from app.schemas import WebhookPayload

logger = logging.getLogger(__name__)


class Gateway(Protocol):
    async def charge(self, payment: Payment, idempotency_key: str) -> bool: ...


class Notifier(Protocol):
    async def send(self, url: str, payload: WebhookPayload) -> None: ...


class PaymentNotFoundError(PermanentProcessingError):
    """The event refers to a payment that does not exist."""


class PaymentProcessor:
    """Processes a payment event idempotently.

    The message may be delivered more than once (outbox at-least-once, retries),
    so every step is guarded by the persisted state:
      * the gateway is called only while the payment is `pending`, with the payment
        id as the idempotency key, so a repeated call cannot charge twice;
      * the status is switched with a conditional UPDATE, so only one outcome is stored;
      * the webhook is sent until `webhook_delivered_at` is set, so a retry after
        a failed webhook re-sends the notification without charging again.
    """

    def __init__(
        self, sessions: async_sessionmaker[AsyncSession], gateway: Gateway, notifier: Notifier
    ) -> None:
        self._sessions = sessions
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: uuid.UUID) -> None:
        async with self._sessions() as session:
            payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)

        if payment.status == PaymentStatus.PENDING:
            payment = await self._charge(payment)

        if payment.webhook_delivered_at is None:
            await self._notify(payment)

    async def _charge(self, payment: Payment) -> Payment:
        # The gateway deduplicates by this key, so a repeated call (crash before the
        # UPDATE below, concurrent delivery) returns the same outcome without a
        # second debit.
        succeeded = await self._gateway.charge(payment, idempotency_key=str(payment.id))
        status = PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED

        async with self._sessions() as session, session.begin():
            updated = await session.scalar(
                update(Payment)
                .where(Payment.id == payment.id, Payment.status == PaymentStatus.PENDING)
                .values(status=status, processed_at=datetime.now(UTC))
                .returning(Payment)
            )
            if updated is None:
                # A concurrent delivery of the same event has already stored the outcome.
                logger.info("Payment %s was already processed concurrently", payment.id)
                updated = await session.get(Payment, payment.id, populate_existing=True)
        logger.info("Payment %s processed: %s", updated.id, updated.status)
        return updated

    async def _notify(self, payment: Payment) -> None:
        payload = WebhookPayload(
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
        )
        await self._notifier.send(payment.webhook_url, payload)

        async with self._sessions() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(Payment.id == payment.id, Payment.webhook_delivered_at.is_(None))
                .values(webhook_delivered_at=datetime.now(UTC))
            )
        logger.info("Webhook for payment %s delivered", payment.id)
