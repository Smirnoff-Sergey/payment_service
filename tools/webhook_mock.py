"""Tiny webhook receiver for local demos (not part of the payment service itself).

Every request must carry a valid HMAC signature (see app.consumer.signing),
otherwise it is answered with 401 - this is how a real receiver checks the sender.

POST /webhook       -> 200, stores the notification
POST /webhook/fail  -> 500, lets you watch retries and the DLQ
GET  /webhook       -> notifications received so far
"""

import json
import logging
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status

from app.config import get_settings
from app.consumer.signing import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify_signature

logger = logging.getLogger("uvicorn.error")
app = FastAPI(title="Webhook mock")
received: list[dict[str, Any]] = []


def webhook_secret() -> bytes:
    return get_settings().webhook_secret.get_secret_value().encode()


async def signed_payload(request: Request, secret: Annotated[bytes, Depends(webhook_secret)]) -> dict[str, Any]:
    body = await request.body()
    if not verify_signature(
        secret, body, request.headers.get(TIMESTAMP_HEADER), request.headers.get(SIGNATURE_HEADER)
    ):
        logger.warning("Webhook with an invalid signature rejected")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid signature")
    return json.loads(body)


Payload = Annotated[dict[str, Any], Depends(signed_payload)]


@app.post("/webhook")
async def receive(payload: Payload) -> dict[str, str]:
    received.append(payload)
    logger.info("Webhook received: %s", payload)
    return {"status": "ok"}


@app.post("/webhook/fail")
async def fail(payload: Payload) -> Response:
    logger.info("Webhook rejected on purpose: %s", payload)
    return Response(status_code=500)


@app.get("/webhook")
async def list_received() -> list[dict[str, Any]]:
    return received
