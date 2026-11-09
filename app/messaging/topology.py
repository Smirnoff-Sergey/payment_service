"""RabbitMQ topology shared by the outbox relay and the consumer.

    relay ──► [payments] ──payments.new──► (payments.new) ──► consumer
                  ▲                              │ reject
                  │ TTL expired                  ▼
     (payments.retry.N) ◄──retry.N── [payments.retry]   [payments.dlx] ──► (payments.dlq)
                                         ▲
                                         └── consumer republishes a failed message

A failed message is republished to `payments.retry.N` (N = attempt number),
a queue with a per-queue TTL and no consumers. When the TTL expires RabbitMQ
dead-letters the message back to `payments.new`. Once the retries are spent
the consumer rejects the message and RabbitMQ routes it to `payments.dlq`.
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_NEW_ROUTING_KEY = "payments.new"
DLQ_ROUTING_KEY = "payments.dlq"
RETRY_COUNT_HEADER = "x-retry-count"

payments_exchange = RabbitExchange("payments", type=ExchangeType.DIRECT)
retry_exchange = RabbitExchange("payments.retry", type=ExchangeType.DIRECT)
dead_letter_exchange = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT)

payments_queue = RabbitQueue(
    "payments.new",
    routing_key=PAYMENTS_NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": dead_letter_exchange.name,
        "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
    },
)

dead_letter_queue = RabbitQueue("payments.dlq", routing_key=DLQ_ROUTING_KEY)


def retry_routing_key(attempt: int) -> str:
    return f"retry.{attempt}"


def retry_delay(attempt: int, base_delay: float) -> float:
    """Exponential backoff: base, 2*base, 4*base, ..."""
    return base_delay * 2 ** (attempt - 1)


def retry_queue(attempt: int, base_delay: float) -> RabbitQueue:
    delay_ms = int(retry_delay(attempt, base_delay) * 1000)
    return RabbitQueue(
        f"payments.retry.{attempt}",
        routing_key=retry_routing_key(attempt),
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": payments_exchange.name,
            "x-dead-letter-routing-key": PAYMENTS_NEW_ROUTING_KEY,
        },
    )


async def _declare_bound(
    broker: RabbitBroker, queue: RabbitQueue, exchange: RabbitExchange, routing_key: str
) -> None:
    robust_exchange = await broker.declare_exchange(exchange)
    robust_queue = await broker.declare_queue(queue)
    await robust_queue.bind(robust_exchange, routing_key=routing_key)


async def declare_topology(broker: RabbitBroker, max_retries: int, retry_base_delay: float) -> None:
    """Declare all exchanges, queues and bindings (idempotent)."""
    await _declare_bound(broker, payments_queue, payments_exchange, PAYMENTS_NEW_ROUTING_KEY)
    await _declare_bound(broker, dead_letter_queue, dead_letter_exchange, DLQ_ROUTING_KEY)
    for attempt in range(1, max_retries + 1):
        await _declare_bound(
            broker, retry_queue(attempt, retry_base_delay), retry_exchange, retry_routing_key(attempt)
        )
