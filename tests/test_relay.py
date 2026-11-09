import pytest
from sqlalchemy import select

from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY, payments_exchange
from app.models import OutboxEvent
from app.relay.main import publish_batch
from app.services.payments import create_payment
from tests.helpers import FakeBroker


async def load_event(sessions) -> OutboxEvent:
    async with sessions() as session:
        return (await session.scalars(select(OutboxEvent))).one()


async def test_publish_batch_publishes_and_marks_events(sessions, payment_data):
    async with sessions() as session:
        payment = await create_payment(session, payment_data, "key-1")
    broker = FakeBroker()

    assert await publish_batch(sessions, broker, batch_size=10) == 1
    assert await publish_batch(sessions, broker, batch_size=10) == 0

    published = broker.published[0]
    event = await load_event(sessions)
    assert published["message"]["payment_id"] == str(payment.id)
    assert published["exchange"] == payments_exchange
    assert published["routing_key"] == PAYMENTS_NEW_ROUTING_KEY
    assert published["message_id"] == str(event.id)
    assert published["persist"] is True
    assert event.published_at is not None


async def test_publish_failure_keeps_events_pending(sessions, payment_data):
    async with sessions() as session:
        await create_payment(session, payment_data, "key-1")

    with pytest.raises(ConnectionError):
        await publish_batch(sessions, FakeBroker(fail=True), batch_size=10)

    assert (await load_event(sessions)).published_at is None
    assert await publish_batch(sessions, FakeBroker(), batch_size=10) == 1
