import asyncio
import random
from collections import defaultdict

from app.models import Payment


class EmulatedGateway:
    """Emulates an external payment gateway: random latency, random outcome.

    Like a real gateway (e.g. Stripe's `Idempotency-Key`), it deduplicates charges
    by the idempotency key: a repeated request with the same key - after a consumer
    crash between the charge and storing its outcome, or a concurrent duplicate
    delivery - returns the original outcome instead of charging again.
    The emulator keeps outcomes in process memory; a real gateway stores them on
    its side.
    """

    def __init__(self, min_delay: float, max_delay: float, success_rate: float) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._outcomes: dict[str, bool] = {}
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def charge(self, payment: Payment, idempotency_key: str) -> bool:
        """Return True if the gateway accepted the payment."""
        async with self._locks[idempotency_key]:
            if idempotency_key not in self._outcomes:
                await asyncio.sleep(random.uniform(self._min_delay, self._max_delay))
                self._outcomes[idempotency_key] = random.random() < self._success_rate
            return self._outcomes[idempotency_key]
