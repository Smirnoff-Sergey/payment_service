import asyncio
import random

from app.models import Payment


class EmulatedGateway:
    """Emulates an external payment gateway: random latency, random outcome."""

    def __init__(self, min_delay: float, max_delay: float, success_rate: float) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate

    async def charge(self, payment: Payment) -> bool:
        """Return True if the gateway accepted the payment.

        A real gateway would receive `payment.id` as its idempotency key, so a
        repeated charge after a crash could not debit the customer twice.
        """
        await asyncio.sleep(random.uniform(self._min_delay, self._max_delay))
        return random.random() < self._success_rate
