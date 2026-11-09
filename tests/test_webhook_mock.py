import json

import pytest
from httpx import ASGITransport, AsyncClient

from app.consumer.signing import signature_headers
from tools import webhook_mock

SECRET = b"mock-secret"
BODY = json.dumps({"payment_id": "1", "status": "succeeded"}).encode()


@pytest.fixture
async def client():
    webhook_mock.app.dependency_overrides[webhook_mock.webhook_secret] = lambda: SECRET
    webhook_mock.received.clear()
    async with AsyncClient(transport=ASGITransport(app=webhook_mock.app), base_url="http://mock") as client:
        yield client
    webhook_mock.app.dependency_overrides.clear()


async def test_accepts_signed_request(client):
    response = await client.post("/webhook", content=BODY, headers=signature_headers(SECRET, BODY))

    assert response.status_code == 200
    assert webhook_mock.received == [json.loads(BODY)]


@pytest.mark.parametrize("secret", [b"wrong-secret", None])
async def test_rejects_unsigned_or_forged_request(client, secret):
    headers = signature_headers(secret, BODY) if secret else {}

    response = await client.post("/webhook", content=BODY, headers=headers)

    assert response.status_code == 401
    assert webhook_mock.received == []
