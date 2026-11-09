"""Housekeeping run periodically by the outbox relay.

* Published outbox events are only needed for troubleshooting, so they are kept
  for `retention` and then deleted.
* Expired idempotency keys are deleted (an expired key is also released lazily
  when it is reused, see app.services.payments).

Rows are deleted in batches locked with SKIP LOCKED, so several relay instances
do not block each other and a large backlog does not hold one long transaction.
One pass deletes at most `max_batches` batches: cleanup runs inside the relay loop
and must not stall publishing while a large backlog is drained over several passes.
"""

from datetime import timedelta

from sqlalchemy import Delete, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import IdempotencyKey, OutboxEvent


MAX_BATCHES = 10


async def delete_published_events(
    sessions: async_sessionmaker[AsyncSession],
    retention: timedelta,
    batch_size: int,
    max_batches: int = MAX_BATCHES,
) -> int:
    batch = (
        select(OutboxEvent.id)
        .where(OutboxEvent.published_at < func.now() - retention)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    return await _delete_in_batches(
        sessions, delete(OutboxEvent).where(OutboxEvent.id.in_(batch)), batch_size, max_batches
    )


async def delete_expired_idempotency_keys(
    sessions: async_sessionmaker[AsyncSession], batch_size: int, max_batches: int = MAX_BATCHES
) -> int:
    batch = (
        select(IdempotencyKey.key)
        .where(IdempotencyKey.expires_at <= func.now())
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    return await _delete_in_batches(
        sessions, delete(IdempotencyKey).where(IdempotencyKey.key.in_(batch)), batch_size, max_batches
    )


async def _delete_in_batches(
    sessions: async_sessionmaker[AsyncSession], stmt: Delete, batch_size: int, max_batches: int
) -> int:
    total = 0
    for _ in range(max_batches):
        async with sessions() as session, session.begin():
            result = await session.execute(stmt.execution_options(synchronize_session=False))
        total += result.rowcount
        if result.rowcount < batch_size:
            break
    return total
