import httpx

from app.consumer.signing import signature_headers
from app.consumer.ssrf import WebhookTargetGuard
from app.schemas import WebhookPayload


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, secret: bytes, guard: WebhookTargetGuard) -> None:
        self._client = client
        self._secret = secret
        self._guard = guard

    async def send(self, url: str, payload: WebhookPayload) -> None:
        """POST the signed payload to the checked address.

        Raises WebhookBlockedError for non-public targets, httpx.HTTPError on network
        errors and non-2xx responses (redirects are not followed, so they fail too).
        """
        target = httpx.URL(url)
        body = payload.model_dump_json().encode()
        headers = {"Content-Type": "application/json", **signature_headers(self._secret, body)}
        extensions = {}

        host = target.raw_host.decode("ascii")
        address = await self._guard.public_address(host, target.port or _default_port(target))
        if address is not None:
            # Connect to the address that passed the check; keep the original name for
            # virtual hosting and for TLS (SNI and certificate verification).
            headers["Host"] = target.netloc.decode("ascii")
            extensions["sni_hostname"] = host
            target = target.copy_with(host=address)

        response = await self._client.post(target, content=body, headers=headers, extensions=extensions)
        response.raise_for_status()


def _default_port(url: httpx.URL) -> int:
    return 443 if url.scheme == "https" else 80
