"""SSRF protection for webhook URLs.

A webhook URL is supplied by the client, so without a check the service could be
made to POST into its own network (databases, metadata endpoints, admin UIs).
The host is resolved right before sending and every address must be public; the
request is then sent to the checked address, so a DNS answer that changes between
the check and the connection (DNS rebinding) cannot redirect it.
"""

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Iterable

from app.consumer.errors import PermanentProcessingError

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
Resolver = Callable[[str, int], Awaitable[list[str]]]

# IPv6 prefixes that carry an IPv4 address in the last 32 bits and are reported as
# global by `ipaddress`: NAT64 (64:ff9b::7f00:1 reaches 127.0.0.1 behind DNS64)
# and SIIT (::ffff:0:a.b.c.d).
_EMBEDDED_IPV4_NETWORKS = (ipaddress.IPv6Network("64:ff9b::/96"), ipaddress.IPv6Network("::ffff:0:0:0/96"))


class WebhookBlockedError(PermanentProcessingError):
    """The webhook URL points to a non-public address."""


def is_public_address(address: IPAddress) -> bool:
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:
            return is_public_address(address.ipv4_mapped)  # ::ffff:127.0.0.1 is loopback, too
        if any(address in network for network in _EMBEDDED_IPV4_NETWORKS):
            return is_public_address(ipaddress.IPv4Address(int(address) & 0xFFFFFFFF))
    return address.is_global and not address.is_multicast


def is_forbidden_host(host: str) -> bool:
    """Cheap check without DNS: `localhost` names and non-public IP literals."""
    host = host.lower().rstrip(".")
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return not is_public_address(ipaddress.ip_address(host.strip("[]")))
    except ValueError:
        return False  # a domain name: checked after resolving, when sending


async def resolve_host(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return list(dict.fromkeys(info[4][0] for info in infos))


class WebhookTargetGuard:
    def __init__(self, allowed_hosts: Iterable[str] = (), resolve: Resolver = resolve_host) -> None:
        self._allowed_hosts = frozenset(h.lower() for h in allowed_hosts)
        self._resolve = resolve

    def is_allowed(self, host: str) -> bool:
        return host.lower().rstrip(".") in self._allowed_hosts

    async def public_address(self, host: str, port: int) -> str | None:
        """Return the address to connect to, None for an allowed host.

        Raises WebhookBlockedError if any resolved address is not public: a name
        resolving to both a public and a private address must not be trusted.
        DNS errors propagate as OSError and are retried.
        """
        if self.is_allowed(host):
            return None
        addresses = await self._resolve(host, port)
        if not addresses:
            raise OSError(f"{host} did not resolve")
        blocked = [a for a in addresses if not is_public_address(ipaddress.ip_address(a))]
        if blocked:
            raise WebhookBlockedError(f"{host} resolves to non-public address(es) {', '.join(blocked)}")
        return addresses[0]
