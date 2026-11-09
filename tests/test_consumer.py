"""Retry / DLQ routing of the consumer (no broker or database needed)."""

import uuid

import pytest

from app.config import Settings
from app.consumer.main import handle_message
from app.consumer.processor import PaymentNotFoundError
from app.messaging.topology import RETRY_COUNT_HEADER, retry_delay, retry_exchange, retry_queue

SETTINGS = Settings(max_retries=3, retry_base_delay=1.0)


class FakeMessage:
    def __init__(self, body: bytes, headers: dict | None = None) -> None:
        self.body = body
        self.headers = headers or {}
        self.message_id = "msg-1"
        self.content_type = "application/json"
        self.outcome: str | None = None

    async def ack(self) -> None:
        self.outcome = "ack"

    async def reject(self) -> None:
        self.outcome = "reject"

    async def nack(self, requeue: bool = True) -> None:
        self.outcome = "nack"


class FakeBroker:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict] = []

    async def publish(self, message, **kwargs) -> None:
        if self.fail:
            raise ConnectionError("broker is down")
        self.published.append({"message": message, **kwargs})


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def process(self, payment_id) -> None:
        if self.error:
            raise self.error


def event_body() -> bytes:
    return f'{{"event_id": "{uuid.uuid4()}", "payment_id": "{uuid.uuid4()}"}}'.encode()


async def test_success_acks():
    msg, broker = FakeMessage(event_body()), FakeBroker()
    await handle_message(msg, FakeProcessor(), broker, SETTINGS)
    assert msg.outcome == "ack"
    assert broker.published == []


@pytest.mark.parametrize("previous_attempts", [0, 1, 2])
async def test_failure_schedules_delayed_retry(previous_attempts):
    msg = FakeMessage(event_body(), {RETRY_COUNT_HEADER: previous_attempts, "x-death": [{}]})
    broker = FakeBroker()

    await handle_message(msg, FakeProcessor(RuntimeError("boom")), broker, SETTINGS)

    assert msg.outcome == "ack"
    published = broker.published[0]
    attempt = previous_attempts + 1
    assert published["message"] == msg.body
    assert published["exchange"] == retry_exchange
    assert published["routing_key"] == f"retry.{attempt}"
    assert published["headers"] == {RETRY_COUNT_HEADER: attempt}


async def test_exhausted_retries_go_to_dlq():
    msg, broker = FakeMessage(event_body(), {RETRY_COUNT_HEADER: 3}), FakeBroker()
    await handle_message(msg, FakeProcessor(RuntimeError("boom")), broker, SETTINGS)
    assert msg.outcome == "reject"
    assert broker.published == []


async def test_retry_publish_failure_requeues():
    msg = FakeMessage(event_body())
    await handle_message(msg, FakeProcessor(RuntimeError("boom")), FakeBroker(fail=True), SETTINGS)
    assert msg.outcome == "nack"


@pytest.mark.parametrize(
    ("body", "error"),
    [(b"not json", None), (event_body(), PaymentNotFoundError())],
)
async def test_permanent_errors_go_to_dlq(body, error):
    msg, broker = FakeMessage(body), FakeBroker()
    await handle_message(msg, FakeProcessor(error), broker, SETTINGS)
    assert msg.outcome == "reject"
    assert broker.published == []


def test_exponential_backoff():
    assert [retry_delay(n, 1.0) for n in (1, 2, 3)] == [1.0, 2.0, 4.0]
    assert retry_queue(3, 1.0).arguments["x-message-ttl"] == 4000
