import asyncio
import uuid

from sqlalchemy import func, select

from app.messaging.topology import PAYMENTS_NEW_ROUTING_KEY
from app.models import OutboxEvent, Payment

BODY = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://webhook.test/hook",
}


def idem(key: str | None = None) -> dict[str, str]:
    return {"Idempotency-Key": key or str(uuid.uuid4())}


async def count(sessions, model) -> int:
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def test_requires_api_key(client):
    response = await client.post("/api/v1/payments", json=BODY, headers={**idem(), "X-API-Key": ""})
    assert response.status_code == 401

    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})
    assert response.status_code == 401


async def test_create_payment_writes_payment_and_outbox_event(client, sessions):
    response = await client.post("/api/v1/payments", json=BODY, headers=idem())

    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert set(data) == {"payment_id", "status", "created_at"}

    async with sessions() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.published_at is None
    assert event.routing_key == PAYMENTS_NEW_ROUTING_KEY
    assert event.payload["payment_id"] == data["payment_id"]
    assert event.payload["event_id"] == str(event.id)


async def test_same_idempotency_key_returns_existing_payment(client, sessions):
    headers = idem("key-1")
    first = await client.post("/api/v1/payments", json=BODY, headers=headers)
    second = await client.post("/api/v1/payments", json=BODY, headers=headers)

    assert second.status_code == 202
    assert second.json() == first.json()
    assert await count(sessions, Payment) == 1
    assert await count(sessions, OutboxEvent) == 1


async def test_same_idempotency_key_with_other_body_conflicts(client, sessions):
    headers = idem("key-1")
    await client.post("/api/v1/payments", json=BODY, headers=headers)
    response = await client.post("/api/v1/payments", json={**BODY, "amount": "1.00"}, headers=headers)

    assert response.status_code == 409
    assert await count(sessions, Payment) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(client, sessions):
    headers = idem("key-race")
    responses = await asyncio.gather(
        *(client.post("/api/v1/payments", json=BODY, headers=headers) for _ in range(5))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert await count(sessions, Payment) == 1
    assert await count(sessions, OutboxEvent) == 1


async def test_create_payment_validation(client):
    assert (await client.post("/api/v1/payments", json=BODY)).status_code == 422  # no Idempotency-Key
    for invalid in (
        {**BODY, "currency": "GBP"},
        {**BODY, "amount": "0"},
        {**BODY, "amount": "1.001"},
        {**BODY, "webhook_url": "not-a-url"},
    ):
        response = await client.post("/api/v1/payments", json=invalid, headers=idem())
        assert response.status_code == 422, invalid


async def test_get_payment(client):
    created = (await client.post("/api/v1/payments", json=BODY, headers=idem("key-get"))).json()

    response = await client.get(f"/api/v1/payments/{created['payment_id']}")

    assert response.status_code == 200
    data = response.json()
    assert data["payment_id"] == created["payment_id"]
    assert data["amount"] == "100.50"
    assert data["currency"] == "RUB"
    assert data["metadata"] == {"order_id": 42}
    assert data["idempotency_key"] == "key-get"
    assert data["webhook_url"] == "http://webhook.test/hook"
    assert data["status"] == "pending"
    assert data["processed_at"] is None


async def test_get_unknown_payment(client):
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 404
