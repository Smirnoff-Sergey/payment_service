"""Outbox relay: publishes unpublished outbox events to RabbitMQ.

Each batch is locked with SELECT ... FOR UPDATE SKIP LOCKED, so several relay
instances can run side by side. An event is marked as published only after the
broker has confirmed it; if publishing fails the transaction is rolled back and
the event is retried on the next iteration (at-least-once delivery, duplicates
are handled by the idempotent consumer).
"""

import asyncio
import logging
import signal
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import get_settings
from app.db import session_factory
from app.messaging.topology import declare_topology, payments_exchange
from app.models import OutboxEvent

logger = logging.getLogger("outbox_relay")


async def publish_batch(
    sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker, batch_size: int
) -> int:
    """Publish one batch of pending events. Returns the number of published events."""
    async with sessions() as session, session.begin():
        events = (
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()

        for event in events:
            await broker.publish(
                event.payload,
                exchange=payments_exchange,
                routing_key=event.routing_key,
                message_id=str(event.id),
                message_type=event.event_type,
                persist=True,
            )
            event.published_at = datetime.now(UTC)
        return len(events)


async def run() -> None:
    settings = get_settings()
    broker = RabbitBroker(settings.rabbit_url)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with broker:
        await broker.start()
        await declare_topology(broker, settings.max_retries, settings.retry_base_delay)
        logger.info("Outbox relay started")

        while not stop.is_set():
            try:
                published = await publish_batch(session_factory, broker, settings.outbox_batch_size)
            except Exception:
                logger.exception("Failed to publish outbox batch, will retry")
                published = 0
            if published:
                logger.info("Published %d outbox event(s)", published)
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.outbox_poll_interval)
            except TimeoutError:
                pass

    logger.info("Outbox relay stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(run())
