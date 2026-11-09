"""Tiny webhook receiver for local demos (not part of the payment service itself).

POST /webhook       -> 200, stores the notification
POST /webhook/fail  -> 500, lets you watch retries and the DLQ
GET  /webhook       -> notifications received so far
"""

import logging
from typing import Any

from fastapi import FastAPI, Request, Response

logger = logging.getLogger("uvicorn.error")
app = FastAPI(title="Webhook mock")
received: list[dict[str, Any]] = []


@app.post("/webhook")
async def receive(request: Request) -> dict[str, str]:
    payload = await request.json()
    received.append(payload)
    logger.info("Webhook received: %s", payload)
    return {"status": "ok"}


@app.post("/webhook/fail")
async def fail(request: Request) -> Response:
    logger.info("Webhook rejected on purpose: %s", await request.json())
    return Response(status_code=500)


@app.get("/webhook")
async def list_received() -> list[dict[str, Any]]:
    return received
