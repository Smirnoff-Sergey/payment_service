import uuid

import httpx
import pytest
import respx

from app.consumer.processor import PaymentNotFoundError, PaymentProcessor
from app.consumer.webhook import WebhookSender
from app.models import Payment, PaymentStatus
from app.services.payments import create_payment


class FakeGateway:
    def __init__(self, result: bool = True) -> None:
        self.result = result
        self.calls = 0

    async def charge(self, payment: Payment) -> bool:
        self.calls += 1
        return self.result


class FakeNotifier:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list = []

    async def send(self, url, payload) -> None:
        if self.fail:
            raise httpx.ConnectError("webhook is down")
        self.sent.append((url, payload))


async def load(sessions, payment_id) -> Payment:
    async with sessions() as session:
        return await session.get(Payment, payment_id)


@pytest.fixture
async def payment(sessions, payment_data) -> Payment:
    async with sessions() as session:
        return await create_payment(session, payment_data, "key-1")


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


async def test_unknown_payment(sessions):
    with pytest.raises(PaymentNotFoundError):
        await PaymentProcessor(sessions, FakeGateway(), FakeNotifier()).process(uuid.uuid4())


@respx.mock
async def test_webhook_sender_raises_on_error_status():
    respx.post("http://webhook.test/ok").respond(200)
    respx.post("http://webhook.test/fail").respond(500)
    from app.schemas import WebhookPayload

    payload = WebhookPayload(
        payment_id=uuid.uuid4(), status="succeeded", amount="1.00", currency="RUB", processed_at=None
    )
    async with httpx.AsyncClient() as client:
        sender = WebhookSender(client)
        await sender.send("http://webhook.test/ok", payload)
        with pytest.raises(httpx.HTTPStatusError):
            await sender.send("http://webhook.test/fail", payload)
