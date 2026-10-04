# 08 — Field-Level Encryption

Demonstrates **transparent at-rest field-level encryption** using `varco_core.encryption` and `varco_sa`.

Sensitive PII fields (`ssn`, `notes`) on the `Patient` domain model are annotated with `EncryptedHint()`. The `SAModelFactory` generates an ORM mapper that transparently:
- **encrypts** the plaintext value on `INSERT` / `UPDATE` → stored as `LargeBinary` (Fernet token)
- **decrypts** the ciphertext on `SELECT` → the service and API always see plaintext

The HTTP API, service layer, and assembler are **identical** to a non-encrypted entity. Encryption is entirely transparent above the repository layer.

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/patients` | Create patient — `ssn` and `notes` encrypted at rest |
| `GET` | `/v1/patients` | List all patients — fields decrypted on read |
| `GET` | `/v1/patients/{id}` | Fetch single patient |
| `PUT` | `/v1/patients/{id}` | Full replace — new values re-encrypted |
| `PATCH` | `/v1/patients/{id}` | Partial update |
| `DELETE` | `/v1/patients/{id}` | Remove |
| `GET` | `/health` | Liveness probe |

---

## Running locally

```bash
export DATABASE_URL="postgresql+asyncpg://user:pw@localhost:5432/mydb"
cd examples/08-field-encryption
VARCO_SECURITY_ENV=development uv run uvicorn app:app --reload
```

Try it:

```bash
# Create a patient (ssn stored encrypted)
curl -X POST http://localhost:8000/v1/patients \
     -H "Content-Type: application/json" \
     -d '{"name": "Alice Smith", "ssn": "123-45-6789", "notes": "Allergic to penicillin"}'

# Read back — ssn returned as plaintext
curl http://localhost:8000/v1/patients/<id>
```

---

## Running the integration tests

```bash
# From the workspace root (requires Docker)
VARCO_SECURITY_ENV=development uv run pytest examples/08-field-encryption/tests/ -v -m integration
```

The tests spin up a PostgreSQL 16 container via testcontainers. One test directly queries the `patients` table and verifies that the `ssn` column contains ciphertext bytes, not the original string.

---

## Key design choices

### `EncryptedHint()` on domain fields

```python
@dataclass
class Patient(AuditedDomainModel):
    name: Annotated[str, FieldHint(max_length=255)]  # plaintext
    ssn: Annotated[str | None, EncryptedHint()] = None  # encrypted → LargeBinary
    notes: Annotated[str | None, EncryptedHint()] = None  # encrypted → LargeBinary
```

`EncryptedHint` is a presence marker — its position in `Annotated` is all that matters. `SAModelFactory` detects it and generates a `LargeBinary` column with an encrypt/decrypt mapper.

### `FernetFieldEncryptor` — single-key symmetric encryption

```python
from cryptography.fernet import Fernet
from varco_core.encryption import FernetFieldEncryptor

key = Fernet.generate_key()
encryptor = FernetFieldEncryptor(key)
```

Fernet provides AES-128-CBC encryption + HMAC-SHA256 authentication — tamper-resistant and authenticated.

### Injecting the encryptor into the ORM mapper

The encryptor must be passed to `SAModelFactory.build()`:

```python
factory = SAModelFactory(base=Base)
factory.build(Patient, encryptor=encryptor)
```

See `app.py` `_build_container()` for the full DI wiring pattern.

### Key management options

| Mode | When to use |
|------|-------------|
| `generate_ephemeral_encryptor()` | Tests / demos — key is random and in-memory |
| `build_persistent_encryptor(engine)` | Production — keys stored in `varco_encryption_keys` table |
| `MultiKeyEncryptorRegistry` | Zero-downtime key rotation across multiple key versions |
| `TenantAwareEncryptorRegistry` | Per-tenant key isolation in multi-tenant deployments |

### Wire format

Encrypted fields are stored as raw Fernet tokens (bytes). A `NULL` plaintext (`None`) is stored as a database `NULL` — never encrypted. This is intentional: encrypting `None` is meaningless and would prevent NULL queries.

---

## Production checklist

- [ ] Load the master key from a secure source (Vault, HSM, KMS env var) — never hardcode it
- [ ] Use `build_persistent_encryptor(engine)` so keys survive pod restarts
- [ ] Add TLS at the transport layer — encryption is at-rest only
- [ ] Plan a re-encryption migration before retiring old key versions
- [ ] Never use encrypted fields in SQL `WHERE` clauses — they are opaque bytes in the DB
