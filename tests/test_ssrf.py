import ipaddress

import pytest

from app.consumer.ssrf import WebhookBlockedError, WebhookTargetGuard, is_forbidden_host, is_public_address


@pytest.mark.parametrize(
    ("address", "public"),
    [
        ("93.184.216.34", True),
        ("2606:2800:220:1:248:1893:25c8:1946", True),
        ("127.0.0.1", False),
        ("10.1.2.3", False),
        ("172.16.0.1", False),
        ("192.168.10.111", False),
        ("169.254.169.254", False),  # cloud metadata
        ("100.64.0.1", False),  # carrier-grade NAT
        ("0.0.0.0", False),
        ("224.0.0.1", False),
        ("::1", False),
        ("fe80::1", False),
        ("fc00::1", False),
        ("::ffff:127.0.0.1", False),
        ("::ffff:93.184.216.34", True),
        ("64:ff9b::7f00:1", False),  # NAT64 -> 127.0.0.1
        ("64:ff9b::5db8:d822", True),  # NAT64 -> 93.184.216.34
        ("::ffff:0:a00:1", False),  # SIIT -> 10.0.0.1
        ("2002:7f00:1::", False),  # 6to4 -> 127.0.0.1
    ],
)
def test_is_public_address(address, public):
    assert is_public_address(ipaddress.ip_address(address)) is public


@pytest.mark.parametrize(
    ("host", "forbidden"),
    [
        ("localhost", True),
        ("api.localhost", True),
        ("LOCALHOST.", True),
        ("127.0.0.1", True),
        ("[::1]", True),
        ("10.0.0.1", True),
        ("93.184.216.34", False),
        ("client.example", False),  # names are checked after resolving
    ],
)
def test_is_forbidden_host(host, forbidden):
    assert is_forbidden_host(host) is forbidden


async def test_dns_failure_is_retryable():
    async def resolve(host, port):
        return []

    with pytest.raises(OSError) as exc_info:
        await WebhookTargetGuard(resolve=resolve).public_address("client.example", 443)
    assert not isinstance(exc_info.value, WebhookBlockedError)
