from app.consumer.signing import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    compute_signature,
    signature_headers,
    verify_signature,
)

SECRET = b"secret"
BODY = b'{"payment_id":"1"}'
NOW = 1_800_000_000


def test_signature_covers_timestamp_and_body():
    headers = signature_headers(SECRET, BODY, now=NOW)

    assert headers[TIMESTAMP_HEADER] == str(NOW)
    assert headers[SIGNATURE_HEADER].startswith("sha256=")
    assert verify_signature(SECRET, BODY, headers[TIMESTAMP_HEADER], headers[SIGNATURE_HEADER], now=NOW)


def test_rejects_tampering_and_wrong_secret():
    signature = compute_signature(SECRET, NOW, BODY)

    assert not verify_signature(SECRET, BODY + b" ", str(NOW), signature, now=NOW)
    assert not verify_signature(SECRET, BODY, str(NOW + 1), signature, now=NOW)
    assert not verify_signature(b"other", BODY, str(NOW), signature, now=NOW)


def test_rejects_stale_and_malformed_headers():
    signature = compute_signature(SECRET, NOW, BODY)

    assert verify_signature(SECRET, BODY, str(NOW), signature, tolerance=300, now=NOW + 300)
    assert not verify_signature(SECRET, BODY, str(NOW), signature, tolerance=300, now=NOW + 301)
    assert not verify_signature(SECRET, BODY, None, signature, now=NOW)
    assert not verify_signature(SECRET, BODY, "abc", signature, now=NOW)
    assert not verify_signature(SECRET, BODY, str(NOW), None, now=NOW)
    assert not verify_signature(SECRET, BODY, str(NOW), "sha256=ключ", now=NOW)  # non-ASCII must not raise
