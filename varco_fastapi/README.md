# varco-fastapi

[![PyPI version](https://img.shields.io/pypi/v/varco-fastapi)](https://pypi.org/project/varco-fastapi/)
[![Python](https://img.shields.io/pypi/pyversions/varco-fastapi)](https://pypi.org/project/varco-fastapi/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/edoardoscarpaci/varco/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-edoardoscarpaci%2Fvarco-blue?logo=github)](https://github.com/edoardoscarpaci/varco)

FastAPI integration and HTTP client utilities for **varco**.

Provides structured HTTP connection configuration, TLS trust-store management,
JWT authority, and HTTP middleware wiring on top of FastAPI and httpx.
Requires [`varco-core`](https://pypi.org/project/varco-core/).

---

## Install

```bash
pip install varco-fastapi
```

---

## HTTP connection settings

`HttpConnectionSettings` is a structured config object that produces kwargs
for `httpx.AsyncClient` (or `httpx.Client`).

Unlike the Postgres/Redis/Kafka settings, **there is no fixed env-var prefix**.
A service typically calls many different external HTTP APIs — a hardcoded
`HTTP_` prefix would only allow one of them to be configured from env vars at a
time.  Instead, you supply a prefix when loading from env:

```python
payment = HttpConnectionSettings.from_env(prefix="PAYMENT_API_")
notify  = HttpConnectionSettings.from_env(prefix="NOTIF_API_")
```

### Plain connection (no auth, no TLS)

```python
import httpx
from varco_fastapi.connection import HttpConnectionSettings

# Direct construction — no env vars read
conn = HttpConnectionSettings(base_url="https://api.example.com/v1", timeout=10.0)

async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
    response = await client.get("/users")
```

### From environment variables (multi-client)

```bash
# Payment API
PAYMENT_API_BASE_URL=https://pay.example.com/v1
PAYMENT_API_TIMEOUT=5.0

# Notification API
NOTIF_API_BASE_URL=https://notify.example.com
NOTIF_API_TIMEOUT=10.0
```

```python
payment = HttpConnectionSettings.from_env(prefix="PAYMENT_API_")
notify  = HttpConnectionSettings.from_env(prefix="NOTIF_API_")

async with httpx.AsyncClient(**payment.to_httpx_kwargs()) as client:
    await client.post("/charge", json={"amount": 9.99})
```

You can also configure via host and port instead of a full URL:

```bash
MY_SVC_HOST=api.example.com
MY_SVC_PORT=8080
# effective base_url → "http://api.example.com:8080"
```

### With Basic authentication

```python
from varco_core.connection import BasicAuthConfig

conn = HttpConnectionSettings(
    base_url="https://api.example.com",
    auth=BasicAuthConfig(username="svc-user", password="secret"),
)
# to_httpx_kwargs() includes auth=("svc-user", "secret") automatically

async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
    response = await client.get("/protected")
```

From env:

```bash
MY_SVC_BASE_URL=https://api.example.com
MY_SVC_AUTH__TYPE=basic
MY_SVC_AUTH__USERNAME=svc-user
MY_SVC_AUTH__PASSWORD=secret
```

```python
conn = HttpConnectionSettings.from_env(prefix="MY_SVC_")
```

### With OAuth2 static bearer token

```python
from varco_core.connection import OAuth2Config

conn = HttpConnectionSettings(
    base_url="https://api.example.com",
    auth=OAuth2Config(token="eyJhbGciOiJSUzI1NiJ9..."),
)
# OAuth2 is NOT injected into kwargs automatically — httpx has no built-in
# OAuth2 flow.  Add the Authorization header via a middleware or event hook:
async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
    response = await client.get(
        "/protected",
        headers={"Authorization": f"Bearer {conn.auth.token}"},
    )
```

> **Note:** For OAuth2 client-credentials flows (token refresh), use an
> `httpx` event hook or middleware — `HttpConnectionSettings` is a pure config
> object and does not manage token lifecycle.

### With TLS / SSL (custom CA)

```python
from varco_core.connection import SSLConfig
from pathlib import Path

ssl = SSLConfig(ca_cert=Path("/etc/ssl/api-ca.pem"), verify=True)
conn = HttpConnectionSettings.with_ssl(
    ssl,
    base_url="https://secure-api.example.com",
)
# to_httpx_kwargs()["verify"] → ssl.SSLContext built from the CA cert

async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
    response = await client.get("/data")
```

From env:

```bash
MY_SVC_BASE_URL=https://secure-api.example.com
MY_SVC_SSL__CA_CERT=/etc/ssl/api-ca.pem
MY_SVC_SSL__VERIFY=true
```

```python
conn = HttpConnectionSettings.from_env(prefix="MY_SVC_")
```

### Disable TLS verification (dev / testing only)

```python
ssl = SSLConfig(verify=False, check_hostname=False)
conn = HttpConnectionSettings.with_ssl(ssl, base_url="https://localhost:8443")
# to_httpx_kwargs()["verify"] → False
```

### With mTLS (client certificates)

```python
ssl = SSLConfig(
    ca_cert=Path("/etc/ssl/ca.pem"),
    client_cert=Path("/etc/ssl/client.crt"),
    client_key=Path("/etc/ssl/client.key"),
)
conn = HttpConnectionSettings.with_ssl(ssl, base_url="https://mtls-api.example.com")
async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
    response = await client.get("/secure")
```

### Bridge to `TrustStore` (legacy `ClientProfile`)

```python
trust_store = conn.to_trust_store()   # None when ssl is not set
# use with ClientProfile.production(trust_store=trust_store)
```

### Connection settings reference

All field names below assume a prefix of `MY_SVC_` — replace it with your own.

| Env var | Default | Description |
|---|---|---|
| `{PREFIX}HOST` | `localhost` | API hostname (used when `BASE_URL` is empty) |
| `{PREFIX}PORT` | `443` | API port (used when `BASE_URL` is empty) |
| `{PREFIX}BASE_URL` | _(empty)_ | Full base URL — overrides host/port when set |
| `{PREFIX}TIMEOUT` | `30.0` | Default request timeout in seconds |
| `{PREFIX}SSL__CA_CERT` | — | Path to CA certificate |
| `{PREFIX}SSL__CLIENT_CERT` | — | Path to client certificate (mTLS) |
| `{PREFIX}SSL__CLIENT_KEY` | — | Path to client private key (mTLS) |
| `{PREFIX}SSL__VERIFY` | `true` | TLS peer verification (`false` = skip) |
| `{PREFIX}AUTH__TYPE` | — | `basic` or `oauth2` |
| `{PREFIX}AUTH__USERNAME` | — | Basic auth username |
| `{PREFIX}AUTH__PASSWORD` | — | Basic auth password |
| `{PREFIX}AUTH__TOKEN` | — | OAuth2 static bearer token |

---

## Related packages

| Package | Description |
|---|---|
| [`varco-core`](https://pypi.org/project/varco-core/) | Domain model, service layer, JWT authority — required dependency |
| [`varco-sa`](https://pypi.org/project/varco-sa/) | SQLAlchemy async backend |
| [`varco-kafka`](https://pypi.org/project/varco-kafka/) | Kafka event bus backend |
| [`varco-redis`](https://pypi.org/project/varco-redis/) | Redis event bus + cache backend |

---

## Links

- **Repository**: https://github.com/edoardoscarpaci/varco
- **Issue tracker**: https://github.com/edoardoscarpaci/varco/issues
