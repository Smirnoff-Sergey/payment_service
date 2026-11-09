import uuid

import httpx
import pytest

from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.models import Payment, PaymentStatus
from app.services.payments import create_payment
from tests.helpers import KEY_TTL, FakeGateway, FakeNotifier


async def load(sessions, payment_id) -> Payment:
    async with sessions() as session:
        return await session.get(Payment, payment_id)


@pytest.fixture
async def payment(sessions, payment_data) -> Payment:
    async with sessions() as session:
        return await create_payment(session, payment_data, "key-1", KEY_TTL)


@pytest.mark.parametrize(("charged", "status"), [(True, PaymentStatus.SUCCEEDED), (False, PaymentStatus.FAILED)])
async def test_process_stores_outcome_and_sends_webhook(sessions, payment, charged, status):
    notifier = FakeNotifier()

    await PaymentProcessor(sessions, FakeGateway(charged), notifier).process(payment.id)

    stored = await load(sessions, payment.id)
    assert stored.status == status
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    url, payload = notifier.sent[0]
    assert url == payment.webhook_url
    assert payload.payment_id == payment.id
    assert payload.status == status


async def test_retry_after_webhook_failure_does_not_charge_again(sessions, payment):
    gateway = FakeGateway()

    with pytest.raises(httpx.ConnectError):
        await PaymentProcessor(sessions, gateway, FakeNotifier(fail=True)).process(payment.id)

    stored = await load(sessions, payment.id)
    assert stored.status == PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is None

    notifier = FakeNotifier()
    await PaymentProcessor(sessions, gateway, notifier).process(payment.id)

    assert gateway.calls == 1
    assert len(notifier.sent) == 1
    assert (await load(sessions, payment.id)).webhook_delivered_at is not None


async def test_duplicate_delivery_is_noop(sessions, payment):
    gateway, notifier = FakeGateway(), FakeNotifier()
    processor = PaymentProcessor(sessions, gateway, notifier)

    await processor.process(payment.id)
    await processor.process(payment.id)

    assert gateway.calls == 1
    assert len(notifier.sent) == 1


async def test_gateway_receives_payment_id_as_idempotency_key(sessions, payment):
    gateway = FakeGateway()

    await PaymentProcessor(sessions, gateway, FakeNotifier()).process(payment.id)

    assert gateway.keys == [str(payment.id)]


async def test_retry_after_lost_gateway_response_does_not_charge_twice(sessions, payment):
    gateway = FakeGateway(result=False, lose_response=True)

    with pytest.raises(ConnectionError):
        await PaymentProcessor(sessions, gateway, FakeNotifier()).process(payment.id)
    assert (await load(sessions, payment.id)).status == PaymentStatus.PENDING

    # The gateway changes its mind for new keys, but the retry reuses the old one.
    gateway.result = True
    notifier = FakeNotifier()
    await PaymentProcessor(sessions, gateway, notifier).process(payment.id)

    assert gateway.calls == 2
    assert gateway.keys == [str(payment.id)] * 2
    assert gateway.charges == {str(payment.id): False}
    assert (await load(sessions, payment.id)).status == PaymentStatus.FAILED
    assert notifier.sent[0][1].status == PaymentStatus.FAILED


async def test_unknown_payment(sessions):
    with pytest.raises(PaymentNotFoundError):
        await PaymentProcessor(sessions, FakeGateway(), FakeNotifier()).process(uuid.uuid4())
