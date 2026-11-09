"""Retry / DLQ routing of the consumer (no broker or database needed)."""

import uuid

import pytest

from app.consumer.handler import PaymentMessageHandler
from app.consumer.processor import PaymentNotFoundError
from app.messaging.topology import (
    RETRY_COUNT_HEADER,
    retry_delay,
    retry_exchange,
    retry_queue,
    retry_routing_key,
)
from tests.helpers import FakeBroker

MAX_RETRIES = 3


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


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    async def process(self, payment_id) -> None:
        if self.error:
            raise self.error


def event_body() -> bytes:
    return f'{{"event_id": "{uuid.uuid4()}", "payment_id": "{uuid.uuid4()}"}}'.encode()


async def handle(msg: FakeMessage, error: Exception | None = None, broker: FakeBroker | None = None) -> FakeBroker:
    broker = broker or FakeBroker()
    await PaymentMessageHandler(FakeProcessor(error), broker, MAX_RETRIES).handle(msg)
    return broker


async def test_success_acks():
    msg = FakeMessage(event_body())
    broker = await handle(msg)
    assert msg.outcome == "ack"
    assert broker.published == []


@pytest.mark.parametrize("previous_attempts", [0, 1, 2])
async def test_failure_schedules_delayed_retry(previous_attempts):
    msg = FakeMessage(event_body(), {RETRY_COUNT_HEADER: previous_attempts, "x-death": [{}]})

    broker = await handle(msg, RuntimeError("boom"))

    assert msg.outcome == "ack"
    published = broker.published[0]
    attempt = previous_attempts + 1
    assert published["message"] == msg.body
    assert published["exchange"] == retry_exchange
    assert published["routing_key"] == retry_routing_key(attempt)
    assert published["headers"] == {RETRY_COUNT_HEADER: attempt}


async def test_exhausted_retries_go_to_dlq():
    msg = FakeMessage(event_body(), {RETRY_COUNT_HEADER: MAX_RETRIES})
    broker = await handle(msg, RuntimeError("boom"))
    assert msg.outcome == "reject"
    assert broker.published == []


async def test_retry_publish_failure_requeues():
    msg = FakeMessage(event_body())
    await handle(msg, RuntimeError("boom"), FakeBroker(fail=True))
    assert msg.outcome == "nack"


@pytest.mark.parametrize(
    ("body", "error"),
    [(b"not json", None), (event_body(), PaymentNotFoundError())],
)
async def test_permanent_errors_go_to_dlq(body, error):
    msg = FakeMessage(body)
    broker = await handle(msg, error)
    assert msg.outcome == "reject"
    assert broker.published == []


def test_exponential_backoff():
    assert [retry_delay(n, 1.0) for n in (1, 2, 3)] == [1.0, 2.0, 4.0]
    assert retry_queue(3, 1.0).arguments["x-message-ttl"] == 4000
