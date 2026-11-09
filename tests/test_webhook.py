import json
import uuid

import httpx
import pytest
import respx

from app.consumer.signing import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify_signature
from app.consumer.ssrf import WebhookBlockedError, WebhookTargetGuard
from app.consumer.webhook import WebhookSender
from app.schemas import WebhookPayload

SECRET = b"webhook-secret"
PUBLIC_IP = "93.184.216.34"
PAYLOAD = WebhookPayload(
    payment_id=uuid.uuid4(), status="succeeded", amount="1.00", currency="RUB", processed_at=None
)


def resolver(*addresses: str):
    calls = []

    async def resolve(host: str, port: int) -> list[str]:
        calls.append((host, port))
        return list(addresses)

    resolve.calls = calls
    return resolve


async def send(url: str, guard: WebhookTargetGuard) -> None:
    async with httpx.AsyncClient() as client:
        await WebhookSender(client, SECRET, guard).send(url, PAYLOAD)


@respx.mock
async def test_posts_signed_payload_to_checked_address():
    route = respx.post(f"https://{PUBLIC_IP}:8443/hook?x=1").respond(200)
    resolve = resolver(PUBLIC_IP)

    await send("https://client.example:8443/hook?x=1", WebhookTargetGuard(resolve=resolve))

    request = route.calls.last.request
    assert resolve.calls == [("client.example", 8443)]
    assert request.headers["Host"] == "client.example:8443"
    assert request.extensions["sni_hostname"] == "client.example"
    assert json.loads(request.content) == PAYLOAD.model_dump(mode="json")
    assert verify_signature(
        SECRET, request.content, request.headers[TIMESTAMP_HEADER], request.headers[SIGNATURE_HEADER]
    )


@respx.mock
@pytest.mark.parametrize("addresses", [("10.0.0.5",), (PUBLIC_IP, "127.0.0.1"), ("::ffff:169.254.169.254",)])
async def test_blocks_non_public_target_without_request(addresses):
    route = respx.route().respond(200)

    with pytest.raises(WebhookBlockedError):
        await send("http://client.example/hook", WebhookTargetGuard(resolve=resolver(*addresses)))

    assert not route.called


@respx.mock
async def test_allowed_host_is_sent_as_is():
    route = respx.post("http://webhook-mock:9000/webhook").respond(200)
    resolve = resolver("172.18.0.5")

    await send("http://webhook-mock:9000/webhook", WebhookTargetGuard({"webhook-mock"}, resolve=resolve))

    assert route.called
    assert resolve.calls == []
    assert "sni_hostname" not in route.calls.last.request.extensions


@respx.mock
async def test_raises_on_error_status():
    respx.post(f"http://{PUBLIC_IP}/fail").respond(500)
    with pytest.raises(httpx.HTTPStatusError):
        await send("http://client.example/fail", WebhookTargetGuard(resolve=resolver(PUBLIC_IP)))


@respx.mock
async def test_redirect_is_not_followed():
    respx.post(f"http://{PUBLIC_IP}/hook").respond(307, headers={"Location": "http://127.0.0.1/admin"})
    internal = respx.post("http://127.0.0.1/admin").respond(200)

    with pytest.raises(httpx.HTTPStatusError):
        await send("http://client.example/hook", WebhookTargetGuard(resolve=resolver(PUBLIC_IP)))

    assert not internal.called
