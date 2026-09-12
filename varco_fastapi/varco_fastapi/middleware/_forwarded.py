"""
varco_fastapi.middleware._forwarded
====================================
Shared, private trusted-proxy helpers for ``SecurityHeadersMiddleware``
(HSTS's ``X-Forwarded-Proto`` trust rule, §D-S7-default) and
``RateLimitMiddleware`` (the ``IP`` scope's client-address resolution,
§D-S10-ip). Not part of the public API — both call sites need the exact
same "is the immediate peer a configured trusted proxy?" and
"skip N entries from the right" rules, and a second, slightly-different
implementation of either is exactly the kind of drift brief 008 §3 warns
about (a blindly-trusted forwarded header is a rate-limit bypass; here it
would also be an HSTS-spoofing vector).

DESIGN: fail-closed — an unparseable peer or CIDR is "not trusted"
    ✅ A malformed ``trusted_proxies`` entry (typo'd CIDR) degrades to
       "never trust the header" rather than raising at request time —
       matches §D-S10-ip's "default-empty is fail-closed in the direction
       that matters" reasoning.
    ❌ A typo in ``trusted_proxies`` silently never takes effect rather
       than erroring loudly. Accepted: validating CIDR syntax at
       ``SecurityHeadersSettings``/``RateLimitSettings`` construction time
       is a straightforward follow-up if this proves to bite anyone; it is
       not this plan's row.
"""

from __future__ import annotations

import ipaddress

__all__ = ["peer_is_trusted_proxy", "resolve_forwarded_for", "resolve_forwarded_proto"]


def peer_is_trusted_proxy(client_host: str | None, trusted_proxies: tuple[str, ...]) -> bool:
    """
    Whether the immediate TCP peer matches one of the configured CIDRs.

    Args:
        client_host:     ``scope["client"][0]`` — the immediate peer's
                         address, or ``None`` when ASGI reports no peer
                         (e.g. a Unix socket, or a test transport that
                         omits it).
        trusted_proxies: CIDR strings (e.g. ``"10.0.0.0/8"``). Default
                         empty — see §D-S10-ip: no configuration means the
                         header is never trusted.

    Returns:
        ``True`` only when ``client_host`` parses as an IP address AND at
        least one ``trusted_proxies`` entry parses as a network containing
        it. Any parse failure — on either side — resolves to ``False``.

    Edge cases:
        - ``client_host is None`` (no resolvable peer) → ``False``.
        - An empty ``trusted_proxies`` tuple → ``False`` unconditionally,
          without attempting to parse ``client_host`` at all.
    """
    if not client_host or not trusted_proxies:
        return False
    try:
        addr = ipaddress.ip_address(client_host)
    except ValueError:
        return False
    for cidr in trusted_proxies:
        try:
            network = ipaddress.ip_network(cidr, strict=False)
        except ValueError:
            continue
        if addr in network:
            return True
    return False


def _split_forwarded_list(header_value: str) -> list[str]:
    """Split a comma-separated forwarded-header value into trimmed entries."""
    return [part.strip() for part in header_value.split(",") if part.strip()]


def resolve_forwarded_for(header_value: str, hops: int) -> str:
    """
    Resolve the real client address from an ``X-Forwarded-For`` value.

    ``X-Forwarded-For`` is ordered left-to-right as
    ``client, proxy1, proxy2, ...`` — each proxy appends itself on the
    right. ``trusted_proxy_hops`` is the number of *known* proxy hops
    closest to us (i.e. the rightmost entries) to skip, per §D-S10-ip.

    Args:
        header_value: The raw ``X-Forwarded-For`` header value.
        hops:         Number of trusted proxy hops to skip from the right.

    Returns:
        The resolved entry, or the trimmed raw value if the header
        contains no comma-separated entries at all.

    Edge cases:
        - ``hops`` ≥ the number of entries clamps to the leftmost
          (original client) entry — the best available answer rather than
          an index error.
        - An empty/whitespace-only header resolves to the (empty, trimmed)
          input — callers should treat that as "no usable value".
    """
    parts = _split_forwarded_list(header_value)
    if not parts:
        return header_value.strip()
    index = max(len(parts) - 1 - hops, 0)
    return parts[index]


def resolve_forwarded_proto(header_value: str) -> str:
    """
    Resolve the effective scheme from an ``X-Forwarded-Proto`` value.

    Takes the leftmost entry (the original client's scheme, per the same
    left-to-right convention as ``X-Forwarded-For``), lower-cased for a
    case-insensitive ``== "https"`` comparison at the call site.

    Args:
        header_value: The raw ``X-Forwarded-Proto`` header value.

    Returns:
        The lower-cased leftmost entry, or the lower-cased trimmed raw
        value if there is no comma-separated list.
    """
    parts = _split_forwarded_list(header_value)
    if not parts:
        return header_value.strip().lower()
    return parts[0].lower()
