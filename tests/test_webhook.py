import json
import uuid

import httpx
import pytest
import respx

from app.consumer.webhook import WebhookSender
from app.schemas import WebhookPayload

PAYLOAD = WebhookPayload(
    payment_id=uuid.uuid4(), status="succeeded", amount="1.00", currency="RUB", processed_at=None
)


@respx.mock
async def test_webhook_sender_posts_payload():
    route = respx.post("http://webhook.test/ok").respond(200)
    async with httpx.AsyncClient() as client:
        await WebhookSender(client).send("http://webhook.test/ok", PAYLOAD)
    assert json.loads(route.calls.last.request.content) == PAYLOAD.model_dump(mode="json")


@respx.mock
async def test_webhook_sender_raises_on_error_status():
    respx.post("http://webhook.test/fail").respond(500)
    async with httpx.AsyncClient() as client:
        with pytest.raises(httpx.HTTPStatusError):
            await WebhookSender(client).send("http://webhook.test/fail", PAYLOAD)
