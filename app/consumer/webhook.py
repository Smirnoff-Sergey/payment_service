import httpx

from app.schemas import WebhookPayload


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, url: str, payload: WebhookPayload) -> None:
        """POST the payload; raises httpx.HTTPError on network errors and non-2xx responses."""
        response = await self._client.post(url, json=payload.model_dump(mode="json"))
        response.raise_for_status()
