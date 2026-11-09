from datetime import timedelta

from sqlalchemy import func, select, update

from app.models import IdempotencyKey, OutboxEvent, Payment
from app.relay.cleanup import delete_expired_idempotency_keys, delete_published_events
from app.services.payments import create_payment
from tests.helpers import KEY_TTL

RETENTION = timedelta(hours=24)


async def create_payments(sessions, payment_data, n: int) -> None:
    for i in range(n):
        async with sessions() as session:
            await create_payment(session, payment_data, f"key-{i}", KEY_TTL)


async def execute(sessions, stmt) -> None:
    async with sessions() as session, session.begin():
        await session.execute(stmt)


async def count(sessions, model) -> int:
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def test_deletes_only_old_published_events(sessions, payment_data):
    await create_payments(sessions, payment_data, 5)
    async with sessions() as session:
        ids = (await session.scalars(select(OutboxEvent.id).order_by(OutboxEvent.id))).all()
    old, fresh, unpublished = ids[:3], ids[3], ids[4]
    await execute(
        sessions,
        update(OutboxEvent).where(OutboxEvent.id.in_(old)).values(published_at=func.now() - timedelta(days=2)),
    )
    await execute(sessions, update(OutboxEvent).where(OutboxEvent.id == fresh).values(published_at=func.now()))

    # One pass is bounded; the next pass continues where it stopped.
    assert await delete_published_events(sessions, RETENTION, batch_size=1, max_batches=2) == 2
    assert await delete_published_events(sessions, RETENTION, batch_size=2) == 1

    async with sessions() as session:
        left = set((await session.scalars(select(OutboxEvent.id))).all())
    assert left == {fresh, unpublished}


async def test_deletes_only_expired_idempotency_keys(sessions, payment_data):
    await create_payments(sessions, payment_data, 3)
    await execute(
        sessions,
        update(IdempotencyKey)
        .where(IdempotencyKey.key.in_(["key-0", "key-1"]))
        .values(expires_at=func.now() - timedelta(seconds=1)),
    )

    assert await delete_expired_idempotency_keys(sessions, batch_size=1) == 2

    async with sessions() as session:
        assert (await session.scalars(select(IdempotencyKey.key))).all() == ["key-2"]
    assert await count(sessions, Payment) == 3  # payments themselves are kept
