import asyncio
import itertools

import pytest

from app.consumer import gateway as gateway_module
from app.consumer.gateway import EmulatedGateway


@pytest.fixture
def rolls(monkeypatch):
    """Make random outcomes alternate: success, failure, success, ..."""
    values = itertools.cycle([0.0, 0.99])
    calls = []

    def fake_random() -> float:
        calls.append(1)
        return next(values)

    monkeypatch.setattr(gateway_module.random, "random", fake_random)
    return calls


def make_gateway() -> EmulatedGateway:
    return EmulatedGateway(min_delay=0.01, max_delay=0.01, success_rate=0.5)


async def test_same_key_returns_original_outcome(rolls):
    gateway = make_gateway()

    first = await gateway.charge(None, idempotency_key="p-1")
    second = await gateway.charge(None, idempotency_key="p-1")

    assert first is second is True
    assert len(rolls) == 1


async def test_concurrent_charges_with_same_key_are_charged_once(rolls):
    gateway = make_gateway()

    results = await asyncio.gather(*(gateway.charge(None, idempotency_key="p-1") for _ in range(5)))

    assert results == [True] * 5
    assert len(rolls) == 1


async def test_different_keys_are_charged_independently(rolls):
    gateway = make_gateway()

    assert await gateway.charge(None, idempotency_key="p-1") is True
    assert await gateway.charge(None, idempotency_key="p-2") is False
    assert len(rolls) == 2
