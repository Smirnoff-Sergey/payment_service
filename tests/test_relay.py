from sqlalchemy import select

from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY, payments_exchange
from app.models import OutboxEvent
from app.relay.main import publish_batch
from app.services.payments import create_payment


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[tuple[dict, dict]] = []

    async def publish(self, message, **kwargs) -> None:
        if self.fail:
            raise ConnectionError("broker is down")
        self.published.append((message, kwargs))


async def test_publish_batch_publishes_and_marks_events(sessions, payment_data):
    async with sessions() as session:
        payment = await create_payment(session, payment_data, "key-1")
    broker = FakeBroker()

    assert await publish_batch(sessions, broker, batch_size=10) == 1
    assert await publish_batch(sessions, broker, batch_size=10) == 0

    message, kwargs = broker.published[0]
    assert message["payment_id"] == str(payment.id)
    assert kwargs["exchange"] == payments_exchange
    assert kwargs["routing_key"] == PAYMENTS_NEW_ROUTING_KEY
    assert kwargs["persist"] is True
    async with sessions() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.published_at is not None
    assert kwargs["message_id"] == str(event.id)


async def test_publish_failure_keeps_events_pending(sessions, payment_data):
    async with sessions() as session:
        await create_payment(session, payment_data, "key-1")

    try:
        await publish_batch(sessions, FakeBroker(fail=True), batch_size=10)
    except ConnectionError:
        pass

    async with sessions() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.published_at is None

    assert await publish_batch(sessions, FakeBroker(), batch_size=10) == 1
