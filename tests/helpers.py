"""Shared test data and fakes."""

from datetime import timedelta

import httpx

from app.models import Payment

PAYMENT_BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://webhook.test/hook",
}


KEY_TTL = timedelta(hours=24)


class FakeBroker:
    """Records publish() calls as {"message": ..., **kwargs}."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict] = []

    async def publish(self, message, **kwargs) -> None:
        if self.fail:
            raise ConnectionError("broker is down")
        self.published.append({"message": message, **kwargs})


class FakeGateway:
    """Deduplicates charges by idempotency key like a real gateway.

    `calls` counts requests, `charges` holds the outcome per key (one debit per key).
    With `lose_response=True` the first request is charged, but the response is
    lost (raises), emulating a crash between the charge and storing its outcome.
    """

    def __init__(self, result: bool = True, lose_response: bool = False) -> None:
        self.result = result
        self.lose_response = lose_response
        self.calls = 0
        self.keys: list[str] = []
        self.charges: dict[str, bool] = {}

    async def charge(self, payment: Payment, idempotency_key: str) -> bool:
        self.calls += 1
        self.keys.append(idempotency_key)
        outcome = self.charges.setdefault(idempotency_key, self.result)
        if self.lose_response:
            self.lose_response = False
            raise ConnectionError("gateway response lost")
        return outcome


class FakeNotifier:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list = []

    async def send(self, url, payload) -> None:
        if self.fail:
            raise httpx.ConnectError("webhook is down")
        self.sent.append((url, payload))
