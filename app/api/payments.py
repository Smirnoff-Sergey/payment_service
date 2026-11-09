import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.api.deps import SessionDep, verify_api_key
from app.config import Settings, get_settings
from app.consumer.ssrf import is_forbidden_host
from app.schemas import PaymentAccepted, PaymentCreate, PaymentRead
from app.services import payments as service

router = APIRouter(prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(verify_api_key)])


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def create_payment(
    data: PaymentCreate,
    session: SessionDep,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
    settings: Annotated[Settings, Depends(get_settings)],
):
    # Early rejection of obviously internal targets; domain names are checked
    # after resolving, right before the webhook is sent.
    host = data.webhook_url.host or ""
    if is_forbidden_host(host) and host.lower() not in settings.webhook_allowed_hosts:
        raise HTTPException(
            status_code=422,  # the constant was renamed in recent Starlette versions
            detail="webhook_url must point to a public address",
        )
    try:
        return await service.create_payment(session, data, idempotency_key, settings.idempotency_key_ttl)
    except service.IdempotencyConflictError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Idempotency-Key was already used with a different request body",
        ) from None


@router.get("/{payment_id}", response_model=PaymentRead)
async def get_payment(payment_id: uuid.UUID, session: SessionDep):
    payment = await service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return payment
