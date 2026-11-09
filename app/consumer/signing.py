"""HMAC-SHA256 signature of webhook requests.

The receiver recomputes the signature over `"{timestamp}.{raw body}"` with the
shared secret. The timestamp is signed too, so a captured request cannot be
replayed after the tolerance window.
"""

import hashlib
import hmac
import time

TIMESTAMP_HEADER = "X-Webhook-Timestamp"
SIGNATURE_HEADER = "X-Webhook-Signature"
SIGNATURE_SCHEME = "sha256="
DEFAULT_TOLERANCE = 300  # seconds


def compute_signature(secret: bytes, timestamp: int, body: bytes) -> str:
    digest = hmac.new(secret, f"{timestamp}.".encode() + body, hashlib.sha256).hexdigest()
    return SIGNATURE_SCHEME + digest


def signature_headers(secret: bytes, body: bytes, now: float | None = None) -> dict[str, str]:
    timestamp = int(time.time() if now is None else now)
    return {
        TIMESTAMP_HEADER: str(timestamp),
        SIGNATURE_HEADER: compute_signature(secret, timestamp, body),
    }


def verify_signature(
    secret: bytes,
    body: bytes,
    timestamp: str | None,
    signature: str | None,
    tolerance: int = DEFAULT_TOLERANCE,
    now: float | None = None,
) -> bool:
    if not timestamp or not signature:
        return False
    try:
        ts = int(timestamp)
    except ValueError:
        return False
    current = time.time() if now is None else now
    if abs(current - ts) > tolerance:
        return False
    return hmac.compare_digest(compute_signature(secret, ts, body).encode(), signature.encode())
