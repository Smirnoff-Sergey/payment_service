"""Shared test data and fakes."""

import httpx

from app.models import Payment

PAYMENT_BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://webhook.test/hook",
}


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
