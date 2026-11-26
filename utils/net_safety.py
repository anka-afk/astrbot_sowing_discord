"""SSRF-safe URL validation, connect-time DNS pinning resolver, and bounded streaming HTTP fetch."""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_CHUNK_SIZE = 65536
_SENSITIVE_REDIRECT_HEADERS = frozenset(
    {"authorization", "cookie", "proxy-authorization", "x-user-token"}
)

_NAT64_WELL_KNOWN = ipaddress.IPv6Network("64:ff9b::/96")
_NAT64_LOCAL_USE = ipaddress.IPv6Network("64:ff9b:1::/48")
_IPV4_TRANSLATED = ipaddress.IPv6Network("::ffff:0:0:0/96")

_IPV4_PART_RE = re.compile(r"^(?:0[xX][0-9a-fA-F]+|0[0-7]*|[1-9][0-9]*)$")


@dataclass
class SafeFetchResponse:
    """Result of an SSRF-checked, size-bounded HTTP GET."""

    status: int
    data: bytes
    content_type: str
    final_url: str
    headers: dict[str, str] = field(default_factory=dict)


def _normalize_origin(url: str) -> Optional[tuple[str, str, int]]:
    """Extract canonical `(scheme, hostname, effective_port)` tuple from an HTTP(S) URL."""
    if not is_safe_http_url(url):
        return None
    parsed = urlparse(url.strip())
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").strip().rstrip(".").lower()
    port = parsed.port if parsed.port is not None else (443 if scheme == "https" else 80)
    return (scheme, host, port)


def _parse_trusted_origins(trusted_origins: Sequence[str] = ()) -> frozenset[tuple[str, str, int]]:
    parsed: set[tuple[str, str, int]] = set()
    for raw in trusted_origins or ():
        origin = _normalize_origin(str(raw))
        if origin is not None:
            parsed.add(origin)
    return frozenset(parsed)


def _parse_obsolete_ipv4(hostname: str) -> Optional[ipaddress.IPv4Address]:
    """
    Parse non-canonical/shorthand IPv4 literals accepted by OS resolvers
    (e.g., `127.1`, `2130706433`, `0x7f000001`, `0177.0.0.1`).
    """
    if not hostname or ":" in hostname:
        return None
    parts = hostname.split(".")
    if not (1 <= len(parts) <= 4):
        return None
    values: list[int] = []
    for part in parts:
        if not _IPV4_PART_RE.match(part):
            return None
        try:
            if part.startswith(("0x", "0X")):
                val = int(part, 16)
            elif len(part) > 1 and part.startswith("0"):
                val = int(part, 8)
            else:
                val = int(part, 10)
        except ValueError:
            return None
        values.append(val)

    try:
        if len(values) == 1:
            addr_int = values[0]
            if not (0 <= addr_int <= 0xFFFFFFFF):
                return None
        elif len(values) == 2:
            if values[0] > 0xFF or values[1] > 0xFFFFFF:
                return None
            addr_int = (values[0] << 24) | values[1]
        elif len(values) == 3:
            if values[0] > 0xFF or values[1] > 0xFF or values[2] > 0xFFFF:
                return None
            addr_int = (values[0] << 24) | (values[1] << 16) | values[2]
        else:
            if any(v > 0xFF for v in values):
                return None
            addr_int = (values[0] << 24) | (values[1] << 16) | (values[2] << 8) | values[3]
        return ipaddress.IPv4Address(addr_int)
    except ValueError:
        return None


def is_safe_http_url(url: str) -> bool:
    """Return True strictly for well-formed http:// or https:// URLs with a safe authority."""
    if not isinstance(url, str):
        return False
    cleaned = url.strip()
    if not cleaned:
        return False
    # Reject ASCII control characters (CR, LF, NUL, tabs, etc.)
    if any(ord(ch) <= 0x1F or ord(ch) == 0x7F for ch in cleaned):
        return False
    try:
        parsed = urlparse(cleaned)
        port = parsed.port
    except ValueError:
        return False
    if parsed.scheme.lower() not in ("http", "https"):
        return False
    if port is not None and not (1 <= port <= 65535):
        return False
    if not parsed.netloc:
        return False
    # Reject backslash / userinfo parser differentials (e.g. http://127.0.0.1\@93.184.216.34/)
    if "\\" in parsed.netloc or "@" in parsed.netloc:
        return False
    if parsed.username is not None or parsed.password is not None:
        return False
    if any(ch.isspace() for ch in parsed.netloc):
        return False
    hostname = (parsed.hostname or "").strip()
    if not hostname:
        return False
    return True


def _is_public_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Return True iff `ip` is a globally routable public address."""
    if isinstance(ip, ipaddress.IPv6Address):
        # Unwrap IPv4-mapped IPv6 (::ffff:0:0/96)
        if ip.ipv4_mapped is not None:
            return _is_public_ip(ip.ipv4_mapped)
        # Explicitly block NAT64 (64:ff9b::/96, 64:ff9b:1::/48) & IPv4-translated (::ffff:0:0:0/96)
        if ip in _NAT64_WELL_KNOWN or ip in _NAT64_LOCAL_USE or ip in _IPV4_TRANSLATED:
            return False
        if ip.sixtofour is not None and not _is_public_ip(ip.sixtofour):
            return False
        if ip.teredo is not None:
            server_ip, client_ip = ip.teredo
            if not _is_public_ip(server_ip) or not _is_public_ip(client_ip):
                return False

    if (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
        or not ip.is_global
    ):
        return False
    return True


def is_public_http_url_literal(url: str, trusted_origins: Sequence[str] = ()) -> bool:
    """
    Synchronous SSRF check for URLs passed to external sinks/adapters.

    Enforces `is_safe_http_url(url)` AND rejects `localhost`, `*.localhost`, `.local`,
    `.internal`, and any non-public IP literal (IPv4, IPv6, IPv4-mapped, NAT64,
    shorthand/decimal/hex/octal IPv4 like `127.1` or `2130706433`), unless matching
    an explicitly configured `trusted_origins` entry `(scheme, host, port)`.
    """
    if not is_safe_http_url(url):
        return False
    origin = _normalize_origin(url)
    if origin is None:
        return False
    if origin in _parse_trusted_origins(trusted_origins):
        return True

    hostname = origin[1]
    if (
        hostname == "localhost"
        or hostname.endswith(".localhost")
        or hostname.endswith(".local")
        or hostname.endswith(".internal")
    ):
        return False

    bare_host = hostname.split("%", 1)[0]
    try:
        literal_ip = ipaddress.ip_address(bare_host)
    except ValueError:
        literal_ip = None

    if literal_ip is not None:
        return _is_public_ip(literal_ip)

    obsolete_v4 = _parse_obsolete_ipv4(bare_host)
    if obsolete_v4 is not None:
        return _is_public_ip(obsolete_v4)

    return True


class SafePublicResolver(AbstractResolver):
    """
    Connect-time aiohttp DNS resolver that validates every resolved IP address
    immediately before TCP connect, eliminating DNS rebinding TOCTOU windows.
    """

    def __init__(self, trusted_origins: Sequence[str] = ()):
        parsed = _parse_trusted_origins(trusted_origins)
        self._trusted_host_ports: frozenset[tuple[str, int]] = frozenset(
            (host, port) for _, host, port in parsed
        )

    async def resolve(
        self,
        host: str,
        port: int = 0,
        family: int = socket.AF_UNSPEC,
    ) -> list[ResolveResult]:
        clean_host = (host or "").strip().rstrip(".").lower()
        is_trusted = (clean_host, int(port)) in self._trusted_host_ports

        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(
                host,
                port,
                type=socket.SOCK_STREAM,
                family=family,
            )
        except socket.gaierror as exc:
            raise ValueError("Failed to resolve target hostname") from exc

        if not infos:
            raise ValueError("Hostname resolved to no addresses")

        results: list[ResolveResult] = []
        for fam, _socktype, proto, _canonname, sockaddr in infos:
            raw_addr = str(sockaddr[0])
            bare_addr = raw_addr.split("%", 1)[0]
            try:
                resolved_ip = ipaddress.ip_address(bare_addr)
            except ValueError as exc:
                raise ValueError("Invalid resolved IP address") from exc
            if not is_trusted and not _is_public_ip(resolved_ip):
                raise ValueError("Blocked non-public resolved IP address")
            results.append(
                {
                    "hostname": host,
                    "host": raw_addr,
                    "port": int(sockaddr[1]),
                    "family": fam,
                    "proto": proto,
                    "flags": socket.AI_NUMERICHOST,
                }
            )
        return results

    async def close(self) -> None:
        return None


def create_safe_http_session(
    *,
    timeout: float = 20.0,
    trusted_origins: Sequence[str] = (),
) -> aiohttp.ClientSession:
    """Create an aiohttp ClientSession with connect-time SafePublicResolver, no DNS cache, trust_env=False."""
    resolver = SafePublicResolver(trusted_origins=trusted_origins)
    connector = aiohttp.TCPConnector(resolver=resolver, use_dns_cache=False)
    return aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=timeout),
        trust_env=False,
    )


async def validate_public_url(
    url: str,
    *,
    trusted_origins: Sequence[str] = (),
) -> str:
    """
    Validate that `url` is http(s):// and that all resolved IP addresses are public
    (or exact-match an explicitly configured `trusted_origins` entry).

    Raises ValueError on unsafe scheme, parser-differential authority, port 0,
    DNS resolution failure, or any non-public IP.
    """
    if not is_safe_http_url(url):
        raise ValueError("URL must be a valid http:// or https:// address")

    cleaned = url.strip()
    origin = _normalize_origin(cleaned)
    if origin is None:
        raise ValueError("URL must be a valid http:// or https:// address")
    if origin in _parse_trusted_origins(trusted_origins):
        return cleaned

    _, hostname, port = origin
    if (
        hostname == "localhost"
        or hostname.endswith(".localhost")
        or hostname.endswith(".local")
        or hostname.endswith(".internal")
    ):
        raise ValueError("Blocked non-public hostname")

    # Direct IP literal check (handles IPv4, IPv6, IPv4-mapped, NAT64, and obsolete IPv4)
    bare_host = hostname.split("%", 1)[0]
    try:
        literal_ip = ipaddress.ip_address(bare_host)
    except ValueError:
        literal_ip = _parse_obsolete_ipv4(bare_host)

    if literal_ip is not None:
        if not _is_public_ip(literal_ip):
            raise ValueError("Blocked non-public IP address")
        return cleaned

    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError("Failed to resolve target hostname") from exc

    if not infos:
        raise ValueError("Hostname resolved to no addresses")

    for info in infos:
        raw_addr = str(info[4][0]).split("%", 1)[0]
        try:
            resolved_ip = ipaddress.ip_address(raw_addr)
        except ValueError as exc:
            raise ValueError("Invalid resolved IP address") from exc
        if not _is_public_ip(resolved_ip):
            raise ValueError("Blocked non-public resolved IP address")

    return cleaned


def _strip_sensitive_headers(headers: Optional[dict[str, str]]) -> Optional[dict[str, str]]:
    if not headers:
        return headers
    return {
        k: v
        for k, v in headers.items()
        if k.lower() not in _SENSITIVE_REDIRECT_HEADERS
    }


def _session_has_safe_resolver(session: Any) -> bool:
    connector = getattr(session, "connector", None)
    resolver = getattr(connector, "_resolver", None)
    return isinstance(resolver, SafePublicResolver)


async def safe_fetch(
    session: Optional[aiohttp.ClientSession],
    url: str,
    max_bytes: int,
    timeout: float = 20.0,
    max_redirects: int = 3,
    *,
    headers: Optional[Mapping[str, str]] = None,
    allow_not_modified: bool = False,
    trusted_origins: Sequence[str] = (),
) -> SafeFetchResponse:
    """
    Perform an SSRF-safe, connect-time DNS-validated, redirect-validated,
    size-bounded streaming HTTP GET.

    Validates URL & DNS on the initial hop and every redirect hop (`allow_redirects=False`),
    strips sensitive headers across origin boundaries, rejects non-2xx responses,
    and streams chunks via `resp.content.iter_chunked(65536)` with a hard `max_bytes` cap.
    """
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    if max_redirects < 0:
        raise ValueError("max_redirects must be non-negative")

    own_session = False
    active_session: Any = session
    # Enforce connect-time SafePublicResolver for real aiohttp sessions that lack one
    if active_session is None or (
        isinstance(active_session, aiohttp.ClientSession)
        and not _session_has_safe_resolver(active_session)
    ):
        active_session = create_safe_http_session(
            timeout=timeout,
            trusted_origins=trusted_origins,
        )
        own_session = True

    current_url = url.strip()
    req_timeout = aiohttp.ClientTimeout(total=timeout) if timeout is not None else None
    req_headers: Optional[dict[str, str]] = dict(headers) if headers else None

    try:
        for hop in range(max_redirects + 1):
            validated_url = await validate_public_url(
                current_url, trusted_origins=trusted_origins
            )
            current_origin = _normalize_origin(validated_url)

            async with active_session.get(
                validated_url,
                headers=req_headers,
                allow_redirects=False,
                timeout=req_timeout,
            ) as resp:
                if resp.status in _REDIRECT_STATUSES:
                    if hop >= max_redirects:
                        raise ValueError(f"Exceeded maximum redirect limit ({max_redirects})")
                    location = resp.headers.get("Location")
                    if not location:
                        raise ValueError("Redirect response missing Location header")
                    next_url = urljoin(validated_url, location.strip())
                    next_origin = _normalize_origin(next_url)
                    if next_origin != current_origin:
                        req_headers = _strip_sensitive_headers(req_headers)
                    current_url = next_url
                    continue

                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                if allow_not_modified and resp.status == 304:
                    return SafeFetchResponse(
                        status=304,
                        data=b"",
                        content_type="",
                        final_url=validated_url,
                        headers=resp_headers,
                    )

                resp.raise_for_status()
                if resp.status < 200 or resp.status >= 300:
                    raise ValueError(f"Unexpected non-2xx HTTP status: {resp.status}")

                content_length_raw = resp.headers.get("Content-Length")
                if content_length_raw:
                    try:
                        if int(content_length_raw) > max_bytes:
                            raise ValueError(
                                f"Response exceeds maximum allowed size ({max_bytes} bytes)"
                            )
                    except ValueError as exc:
                        if "exceeds maximum allowed size" in str(exc):
                            raise

                buf = bytearray()
                async for chunk in resp.content.iter_chunked(_CHUNK_SIZE):
                    buf.extend(chunk)
                    if len(buf) > max_bytes:
                        raise ValueError(
                            f"Response exceeds maximum allowed size ({max_bytes} bytes)"
                        )

                content_type = (
                    (resp.headers.get("Content-Type") or "application/octet-stream")
                    .split(";", 1)[0]
                    .strip()
                )
                return SafeFetchResponse(
                    status=resp.status,
                    data=bytes(buf),
                    content_type=content_type,
                    final_url=validated_url,
                    headers=resp_headers,
                )

        raise ValueError(f"Exceeded maximum redirect limit ({max_redirects})")
    finally:
        if own_session and active_session is not None:
            await active_session.close()


async def safe_fetch_bytes(
    session: Optional[aiohttp.ClientSession],
    url: str,
    max_bytes: int,
    timeout: float = 20.0,
    max_redirects: int = 3,
    *,
    headers: Optional[Mapping[str, str]] = None,
    trusted_origins: Sequence[str] = (),
) -> tuple[bytes, str, str]:
    """
    Fetch bytes from a public http(s) URL with connect-time SSRF protection and hard byte limit.

    Returns `(data, content_type, final_url)`.
    """
    res = await safe_fetch(
        session,
        url,
        max_bytes=max_bytes,
        timeout=timeout,
        max_redirects=max_redirects,
        headers=headers,
        allow_not_modified=False,
        trusted_origins=trusted_origins,
    )
    return res.data, res.content_type, res.final_url
