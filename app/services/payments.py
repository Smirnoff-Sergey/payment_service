import hashlib
import json
import uuid
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate, PaymentEvent

PAYMENT_CREATED_EVENT = "payment.created"


class IdempotencyConflictError(Exception):
    """The idempotency key was already used with a different request body."""


def request_fingerprint(data: PaymentCreate) -> str:
    body = data.model_dump(mode="json")
    # "10", "10.0" and "10.00" are the same amount and must produce the same fingerprint.
    body["amount"] = str(data.amount.quantize(Decimal("0.01")))
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def create_payment(session: AsyncSession, data: PaymentCreate, idempotency_key: str) -> Payment:
    """Create a payment and its outbox event atomically.

    Repeating a request with the same key and body returns the existing payment;
    the same key with a different body raises IdempotencyConflictError.
    """
    fingerprint = request_fingerprint(data)
    payment_id = uuid.uuid4()

    async with session.begin():
        stmt = (
            insert(Payment)
            .values(
                id=payment_id,
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
                webhook_url=str(data.webhook_url),
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        payment = (await session.scalars(stmt)).one_or_none()

        if payment is None:
            existing = await session.scalar(
                select(Payment).where(Payment.idempotency_key == idempotency_key)
            )
            if existing is None or existing.request_hash != fingerprint:
                raise IdempotencyConflictError(idempotency_key)
            return existing

        event_id = uuid.uuid4()
        session.add(
            OutboxEvent(
                id=event_id,
                aggregate_id=payment.id,
                event_type=PAYMENT_CREATED_EVENT,
                routing_key=PAYMENTS_NEW_ROUTING_KEY,
                payload=PaymentEvent(event_id=event_id, payment_id=payment.id).model_dump(mode="json"),
            )
        )
    return payment


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
