# Developer Guide — Fabric Control Plane

A deep-dive reference for engineers joining the project. Covers every design decision, explains each file in detail, and walks through local setup, testing, and Azure deployment from scratch.

---

## Table of Contents

1. [What the Service Does](#1-what-the-service-does)
2. [Technology Choices](#2-technology-choices)
3. [Repository Layout](#3-repository-layout)
4. [Layer Architecture](#4-layer-architecture)
5. [File-by-File Walkthrough](#5-file-by-file-walkthrough)
   - [app/config.py](#appconfigpy)
   - [app/exceptions.py](#appexceptionspy)
   - [app/logging_config.py](#applogging_configpy)
   - [app/auth.py](#appauthpy)
   - [app/main.py](#appmainpy)
   - [app/models/schemas.py](#appmodelsschemspy)
   - [app/utils/retry.py](#apputilsretrypy)
   - [app/utils/aad_lookup.py](#apputilsaad_lookuppy)
   - [app/utils/fabric_lookup.py](#apputilsfabric_lookuppy)
   - [app/clients/fabric_client.py](#appclientsfabric_clientpy)
   - [app/services/provisioner.py](#appservicesprovisionerpy)
   - [app/api/v1/routes_fabric.py](#appapiv1routes_fabricpy)
   - [app/cli.py](#appclipy)
   - [function_app.py](#function_apppy)
6. [Key Design Patterns Explained](#6-key-design-patterns-explained)
   - [Idempotency](#idempotency)
   - [Retry with Exponential Backoff](#retry-with-exponential-backoff)
   - [Dependency Injection in FastAPI](#dependency-injection-in-fastapi)
   - [Structured Logging and Correlation IDs](#structured-logging-and-correlation-ids)
   - [JWT Authentication](#jwt-authentication)
   - [Azure Identity — DefaultAzureCredential](#azure-identity--defaultazurecredential)
7. [Configuration System](#7-configuration-system)
8. [Testing Strategy](#8-testing-strategy)
   - [Why Tests Need a Root conftest.py](#why-tests-need-a-root-conftestpy)
   - [Why DummyFabricClient Exists](#why-dummyfabricclient-exists)
   - [Why dependency_overrides Instead of monkeypatch](#why-dependency_overrides-instead-of-monkeypatch)
   - [Why Patch the Provisioner Module Not aad_lookup](#why-patch-the-provisioner-module-not-aad_lookup)
9. [Azure Functions Wrapper — How It Works](#9-azure-functions-wrapper--how-it-works)
10. [Infrastructure as Code — Bicep](#10-infrastructure-as-code--bicep)
11. [CI/CD Pipeline](#11-cicd-pipeline)
12. [Local Setup from Zero](#12-local-setup-from-zero)
13. [DevOps Runbook — Full Azure Deployment](#13-devops-runbook--full-azure-deployment)
14. [Common Errors and Fixes](#14-common-errors-and-fixes)
15. [How to Extend the Service](#15-how-to-extend-the-service)

---

## 1. What the Service Does

The Fabric Control Plane automates the provisioning of **Microsoft Fabric workspaces** in a repeatable, governed, and auditable way. Rather than clicking through the Fabric portal for every new data team, an operator sends a single API call (or YAML file) and the service handles the entire lifecycle:

```
Request arrives
    ↓
Resolve capacity display name → GUID  (Fabric API)
Resolve domain display name   → GUID  (Fabric API)
    ↓
Find workspace by name (does it already exist?)
    ↓  YES → maybe update description
    ↓  NO  → create workspace, bind to capacity
    ↓
Assign domain
    ↓
Reconcile four AAD group role assignments (Admin, Member, Contributor, Viewer)
    ↓
Return ProvisionResult
```

Every step is **idempotent** — calling the API twice with the same payload produces the same result without creating duplicates. This is a deliberate architectural guarantee, not a nice-to-have.

---

## 2. Technology Choices

| Technology | What it is | Why it was chosen |
|------------|-----------|-------------------|
| **Python 3.11** | Runtime | Async support, type hints, widespread Azure SDK support |
| **FastAPI** | Web framework | Native async, automatic OpenAPI docs, built-in dependency injection, Pydantic integration |
| **Pydantic v2** | Data validation | Strict type validation at the boundary, auto-generated JSON schemas, settings management |
| **structlog** | Logging | Outputs JSON in production (machine-parseable), coloured console in dev — same API either way |
| **requests + Retry** | HTTP client | Mature, stable, built-in retry adapter. `httpx` is also installed but not used for Fabric calls |
| **azure-identity** | Azure auth | `DefaultAzureCredential` works in every environment (dev laptop, CI, Azure VM, Managed Identity) without code changes |
| **PyJWT** | JWT validation | Validates Entra ID bearer tokens without a round-trip to Microsoft; uses cached public keys |
| **Azure Functions v2 (Python)** | Hosting | Serverless, scales to zero, pay-per-use, native Managed Identity support |
| **Bicep** | Infrastructure as code | Azure-native, more readable than ARM JSON, compiles to ARM templates |

**Why FastAPI over Flask or Django?**

Flask requires manual request parsing and no built-in dependency injection. Django is full-stack and far heavier than needed for a microservice. FastAPI gives automatic request validation, automatic docs at `/docs`, and a clean way to inject services (like `Provisioner`) into route handlers — all with minimal boilerplate.

---

## 3. Repository Layout

```
fabric_control_plane_mvp/
│
├── app/                        ← All application code lives here
│   ├── main.py                 ← FastAPI app definition
│   ├── config.py               ← All env var config in one place
│   ├── auth.py                 ← JWT validation, DEV_MODE bypass
│   ├── exceptions.py           ← Custom exception hierarchy
│   ├── logging_config.py       ← structlog setup + correlation middleware
│   ├── cli.py                  ← YAML-driven CLI entry point
│   │
│   ├── api/v1/
│   │   └── routes_fabric.py    ← HTTP route handlers (thin layer)
│   │
│   ├── clients/
│   │   └── fabric_client.py    ← Microsoft Fabric REST API wrapper
│   │
│   ├── services/
│   │   └── provisioner.py      ← All business logic lives here
│   │
│   ├── models/
│   │   └── schemas.py          ← Input/output data shapes
│   │
│   └── utils/
│       ├── retry.py            ← Shared HTTP session with retry
│       ├── aad_lookup.py       ← AAD group name → object ID
│       └── fabric_lookup.py    ← Capacity/domain name → GUID
│
├── tests/
│   ├── conftest.py             ← DummyFabricClient + provisioner fixture
│   ├── test_provisioner.py     ← Service layer tests
│   └── test_routes_fabric.py  ← API route tests
│
├── infra/
│   ├── main.bicep              ← Azure infrastructure definition
│   └── main.parameters.json   ← Environment-specific values
│
├── .github/workflows/
│   └── ci-cd.yml              ← GitHub Actions pipeline
│
├── function_app.py            ← Azure Functions entry point
├── host.json                  ← Azure Functions host configuration
├── local.settings.json        ← Local dev settings (git-ignored)
├── conftest.py                ← Root pytest config (env var seed)
├── pyproject.toml             ← Build + pytest configuration
├── requirements.txt           ← Pinned Python dependencies
└── .funcignore                ← Files excluded from Functions deployment
```

**The golden rule of this layout:** business logic only ever lives in `services/`. Routes are thin — they validate input, call a service, return a response. Clients only know about HTTP calls. Nothing else.

---

## 4. Layer Architecture

```
┌─────────────────────────────────────────────────────┐
│  HTTP Layer  (app/api/v1/routes_fabric.py)           │
│  - Validates request bodies (Pydantic)               │
│  - Calls provisioner service                         │
│  - Returns JSON responses                            │
│  - Handles auth via get_current_user dependency      │
└────────────────────────┬────────────────────────────┘
                         │ calls
┌────────────────────────▼────────────────────────────┐
│  Service Layer  (app/services/provisioner.py)        │
│  - All business logic: workspace lifecycle           │
│  - Idempotency decisions                             │
│  - Role reconciliation                               │
│  - Calls client and utils                            │
└──────┬─────────────────┬───────────────┬────────────┘
       │                 │               │
       ▼                 ▼               ▼
┌────────────┐  ┌───────────────┐  ┌──────────────┐
│  Client    │  │  aad_lookup   │  │ fabric_lookup │
│  Layer     │  │  (Graph API)  │  │ (Fabric API)  │
│  fabric_   │  │               │  │               │
│  client.py │  │               │  │               │
└────────────┘  └───────────────┘  └──────────────┘
       │                 │               │
       └─────────────────┴───────────────┘
                         │ all use
┌────────────────────────▼────────────────────────────┐
│  Utility Layer                                       │
│  - retry.py: shared HTTP session                     │
│  - config.py: all settings                           │
│  - logging_config.py: structured logging             │
└─────────────────────────────────────────────────────┘
```

Each layer only talks to the layer below it. A route handler never directly calls `fabric_client`. The provisioner service never parses HTTP requests. This separation makes the code easy to test and easy to change.

---

## 5. File-by-File Walkthrough

### app/config.py

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", ...)

    USE_AZURE_IDENTITY: bool = Field(True, ...)
    AZURE_TENANT_ID: Optional[str] = Field(None, ...)
    ...

settings = Settings()
```

**What it does:** Reads all configuration from environment variables (or a `.env` file) and exposes it as a typed Python object. Every module that needs config imports `settings` from here.

**Key detail — `settings = Settings()` runs at import time.** This means the moment Python loads any file that imports from `app.config`, it tries to read the environment. This is why the root `conftest.py` must set environment variables with `os.environ.setdefault(...)` *before* any `from app import ...` statement runs in the test suite.

**Why Pydantic Settings over `os.getenv()`?**
- Type coercion is automatic: `USE_AZURE_IDENTITY: bool` reads the string `"true"` from the environment and converts it to Python `True`
- Validation happens at startup — a missing required variable raises a clear error immediately rather than crashing later
- All configuration is documented in one place with types and defaults visible

---

### app/exceptions.py

```python
class ProvisioningError(Exception): pass
class ExternalApiError(ProvisioningError): pass
class ValidationError(ProvisioningError): pass
```

**What it does:** Defines a small, specific exception hierarchy for this service.

**Why not just use built-in exceptions?**

Using `raise ValueError("workspace not found")` anywhere in a large codebase makes it impossible to catch provisioning failures specifically without accidentally catching every `ValueError` in the Python standard library. With a custom hierarchy, a caller can do:

```python
except ProvisioningError:
    # handle any provisioning failure
except ExternalApiError:
    # handle specifically an upstream API call failing
```

The hierarchy means `except ProvisioningError` catches `ExternalApiError` and `ValidationError` too, because both are subclasses — useful at the route handler level where the intent is "catch any provisioning problem and return 500".

---

### app/logging_config.py

```python
configure_logging()         # runs at import time
logger = structlog.get_logger()

async def correlation_middleware(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    ...
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response
```

**What structlog gives over Python's built-in logging:**

The standard `logging` module produces unstructured strings like:
```
2025-11-01 INFO Workspace created: my-workspace capacity: cap-123
```

structlog produces key-value pairs that log aggregators (Splunk, Datadog, Azure Monitor) can index and query:
```json
{"event": "workspace_created", "workspace": "my-workspace", "capacity": "cap-123", "timestamp": "..."}
```

In production, you can run queries like `event="workspace_created" AND capacity="cap-123"`. With free-text logs you cannot.

**Correlation middleware:**

Every HTTP request gets a `correlation_id` (either from the incoming `X-Correlation-ID` header, or a new UUID). This ID is attached to every log line for that request and echoed back in the response header. If a customer reports a problem, they provide the correlation ID and an operator can instantly pull every log line related to that exact request — across multiple services.

**DEV_MODE logging:**
- `DEV_MODE=true` → coloured human-readable console output
- `DEV_MODE=false` → compact JSON, one object per line, ready for a log aggregator

Same `logger.info(...)` call in the code, different renderer at runtime.

---

### app/auth.py

```python
def get_current_user(token: str | None = Depends(oauth2_scheme)) -> Dict[str, Any]:
    if settings.DEV_MODE:
        return {"sub": "dev-user", "name": "Dev User (no auth)"}

    # production: validate JWT signature, audience, issuer, expiry
    signing_key = _get_jwks_client().get_signing_key_from_jwt(token)
    claims = jwt_decode(token, signing_key.key, algorithms=["RS256"], ...)
    return claims
```

**How Entra ID JWT validation works:**

When a user authenticates via the Swagger UI or a client application, Entra ID (Azure AD) issues a JWT (JSON Web Token). A JWT is a base64-encoded JSON object containing claims (who the user is, what permissions they have), signed by Microsoft's private key.

The service validates the token by:
1. Fetching Microsoft's public keys from the JWKS endpoint (`login.microsoftonline.com/{tenant}/discovery/v2.0/keys`)
2. Using the correct public key to verify the token's RS256 signature — this proves Microsoft issued it and it hasn't been tampered with
3. Checking the `audience` (this token was meant for this app), `issuer` (this tenant), and `exp` (not expired)

`PyJWKClient` caches the public keys and only re-fetches them when it sees an unfamiliar key ID. This avoids a network call on every request.

**DEV_MODE bypass:** When `DEV_MODE=true`, the entire validation is skipped and a synthetic identity is returned. This only exists so the service can be run locally without setting up a real Entra ID app registration. **It must never be enabled in any environment other than a developer's local machine.**

---

### app/main.py

```python
app = FastAPI(
    title="Fabric Provisioner API",
    swagger_ui_init_oauth={
        "clientId": settings.SWAGGER_CLIENT_ID,
        "usePkceWithAuthorizationCodeGrant": True,
    },
)

app.middleware("http")(correlation_middleware)
app.include_router(fabric_router)
```

**What this file does:** Assembles the FastAPI application. It:
1. Creates the `FastAPI` app object with Swagger UI OAuth2 configuration
2. Registers the correlation middleware (runs on every request)
3. Mounts the fabric router (all `/v1/fabric/*` routes)

**Why is this file so short?** Because all the logic is in other layers. `main.py` is intentionally a composition root — it wires things together but contains no business logic itself.

**PKCE (Proof Key for Code Exchange):** The Swagger UI uses the Authorization Code + PKCE flow for authentication. This means the browser never handles a client secret — instead it generates a cryptographic challenge at login time that proves the same browser completed the flow. This is the secure pattern for single-page apps.

---

### app/models/schemas.py

```python
class WorkspaceSpec(BaseModel):
    name: str
    description: Optional[str]
    capacity: Optional[str]
    domain: Optional[str]
    groups: Dict[str, str]

class ProvisionResult(BaseModel):
    workspace_id: str
    workspace_name: str
    capacity_assigned: Optional[str]
    domain_assigned: Optional[str]
    roles_assigned: Dict[str, str]
    roles_failed: Dict[str, str] = {}
    raw: Optional[Dict[str, Any]] = None
```

**What these are:** Pydantic models that define the shape of data flowing into and out of the service layer.

`WorkspaceSpec` — what the provisioner needs to do its job. Comes from the HTTP request body (after being mapped in the route handler) or from the CLI YAML file.

`ProvisionResult` — what the provisioner reports back. Every field is typed. When the route handler calls `result.model_dump()`, Pydantic serialises this to a clean JSON-safe dict.

**Why have separate request/response models?**

The HTTP request shape (`WorkspaceRequest` in `routes_fabric.py`) is different from `WorkspaceSpec`. The request uses flat field names like `group_platform_admins` for readability in Swagger. The provisioner expects `groups: Dict[str, str]` for programmatic use. The route handler bridges the two. This separation means the HTTP API and the service layer can evolve independently.

---

### app/utils/retry.py

```python
def get_retry_session(total: int = 5, backoff_factor: float = 0.5):
    session = requests.Session()
    retries = Retry(
        total=total,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT", "PATCH", "DELETE"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
```

**What it does:** Creates a `requests.Session` that automatically retries failed HTTP calls with exponential backoff.

**The `status_forcelist`:** HTTP status codes worth retrying:
- `429` — Too Many Requests (rate limited). The Fabric API has rate limits.
- `500/502/503/504` — Server errors. The API may be briefly unavailable.

`400/401/403/404` are NOT retried — these indicate a problem with the request itself (bad input, not authenticated, not found), and retrying would not help.

**Exponential backoff with `backoff_factor=0.5`:**

| Attempt | Wait before retry |
|---------|-------------------|
| 1st retry | 0 seconds |
| 2nd retry | 0.5 seconds |
| 3rd retry | 1.0 seconds |
| 4th retry | 2.0 seconds |
| 5th retry | 4.0 seconds |

Formula: `backoff_factor × (2 ^ (attempt - 1))`. This avoids hammering a struggling API with rapid retries.

**Why a shared session?** HTTP sessions maintain connection pools. Reusing a session means TCP connections to the same host are kept alive and reused across requests rather than establishing a new TCP handshake every time. This is meaningfully faster when making multiple API calls.

---

### app/utils/aad_lookup.py

```python
def resolve_group_identifier(group_identifier: str) -> Optional[str]:
    if re.match(r"^[0-9a-fA-F-]{36}$", group_identifier):
        return group_identifier  # already a GUID, pass through

    token = _get_graph_token()
    resp = _session.get(f"{GRAPH_API}/groups", headers=...,
                        params={"$filter": f"displayName eq '{group_identifier}'"})
    ...
    return data[0]["id"]
```

**What it does:** Accepts either a raw GUID or an AAD group display name. If it's a GUID, returns it immediately. If it's a name, calls Microsoft Graph to look up the group and return its object ID.

**Why this flexibility?** Operators sometimes know the GUID, sometimes only the display name. Accepting both makes the API easier to use. The GUID passthrough avoids a network call when it's not needed.

**Microsoft Graph API:** Graph is Microsoft's unified API for everything in Azure AD — users, groups, applications, emails, calendar. The endpoint used here is `GET /groups?$filter=displayName eq '...'` which returns groups matching the exact display name. The `$select=id,displayName` parameter limits the response to only the fields needed, reducing payload size.

---

### app/utils/fabric_lookup.py

```python
def resolve_capacity_identifier(identifier: str, fabric_client) -> Optional[str]:
    if _GUID_RE.match(identifier):
        return identifier  # already a GUID

    capacity = fabric_client.find_capacity_by_name(identifier)
    if not capacity:
        raise ValueError(f"Fabric capacity not found: '{identifier}'")
    return capacity.get("id")
```

**What it does:** Same pattern as `aad_lookup` but for Fabric capacities and domains. Resolves a display name to a GUID using the Fabric API.

**Why is `fabric_client` passed as a parameter?** Because this utility is called from the provisioner, which already has a `FabricClient` instance. Passing it in avoids creating a second client. It also makes the function testable — in tests, a stub `DummyFabricClient` is passed, which returns a fake result without making any real API calls.

This is a form of dependency injection at the function level.

---

### app/clients/fabric_client.py

```python
class FabricClient:
    def __init__(self):
        self.base = settings.FABRIC_BASE_URL
        self.session = get_retry_session()

    def _get_token(self) -> str:
        if settings.USE_AZURE_IDENTITY:
            cred = DefaultAzureCredential()
            return cred.get_token(FABRIC_SCOPE).token
        cred = ClientSecretCredential(...)
        return cred.get_token(FABRIC_SCOPE).token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}", ...}
```

**What it does:** Wraps every Microsoft Fabric REST API call the service needs. Each method is responsible for exactly one API operation — building the URL, making the call, checking for errors, and returning parsed JSON.

**`_safe_json(resp)`:** The Fabric API sometimes returns `200 OK` or `202 Accepted` with an empty body (particularly for async operations like `assignToCapacity`). Calling `.json()` on an empty body raises a `JSONDecodeError`. `_safe_json` guards against this and returns an empty dict instead.

**`_raise_for_status(resp)`:** The standard `requests` `raise_for_status()` only includes the HTTP status code in the error message. This custom version includes the full response body, so when Fabric returns `"Capacity not found"` or `"Insufficient permissions"`, that message appears in the log rather than just `403 Forbidden`.

**`_get_token()` called on every request:** A fresh token is fetched for each API call. Azure Identity's credential objects cache tokens internally and only actually call Entra ID when the cached token is about to expire. This means there is no meaningful performance cost to calling `_get_token()` repeatedly.

---

### app/services/provisioner.py

This is the most important file in the service. All business logic lives here.

```python
ROLE_MAP = {
    "platform_admins":    "Admin",
    "data_product_owners": "Member",
    "data_engineers":     "Contributor",
    "data_viewers":       "Viewer"
}

class Provisioner:
    def __init__(self, fabric_client: FabricClient | None = None):
        self.fabric = fabric_client or FabricClient()

    def provision_workspace(self, spec: WorkspaceSpec) -> ProvisionResult:
        # Stage 0: resolve capacity/domain names → GUIDs
        # Stage 1: find existing workspace, or create
        # Stage 2: assign capacity (if needed)
        # Stage 3: assign domain (if needed)
        # Stage 4: reconcile role assignments
```

**Staged execution:** Each logical phase of provisioning is a named stage. This makes logs easy to scan — each stage prints a banner line, and all log entries within that stage are visually grouped.

**Idempotency in every stage:**

| Stage | How idempotency is achieved |
|-------|----------------------------|
| Workspace | `find_workspace_by_name` before `create_workspace`. Create only runs if result is `None`. |
| Description | Compare `spec.description != ws.get("description")` before patching. Skip if equal. |
| Capacity | Compare `ws.get("capacityId") == capacity_id` before calling `assignToCapacity`. Skip if already set. New workspaces pass `capacity_id` at creation time — no second call needed. |
| Domain | Compare `ws.get("domainId") == domain_id`. Skip if already assigned. |
| Roles | Build a map of existing assignments by `principalId`. For each desired role: add if missing, patch if role changed, skip if already correct. |

**Why the constructor accepts `fabric_client=None`?**

```python
def __init__(self, fabric_client: FabricClient | None = None):
    self.fabric = fabric_client or FabricClient()
```

In normal operation, `Provisioner()` is called without arguments and creates a real `FabricClient`. In tests, `Provisioner(fabric_client=DummyFabricClient())` injects a stub. This is the **dependency injection** pattern — the provisioner doesn't control what HTTP client it uses; the caller decides. This makes the provisioner fully testable without any network connectivity.

---

### app/api/v1/routes_fabric.py

```python
class WorkspaceRequest(BaseModel):
    name: str = Field(..., min_length=3, max_length=128)
    description: Optional[str] = Field("", max_length=1024)
    capacity_id_or_name: Optional[str] = None
    ...

def get_provisioner() -> Provisioner:
    return Provisioner()

@router.post("/workspace")
def create_or_update_workspace(
    payload: WorkspaceRequest,
    svc: Provisioner = Depends(get_provisioner),
    _user: Dict[str, Any] = Depends(get_current_user),
):
    spec = WorkspaceSpec(name=payload.name, ...)
    result = svc.provision_workspace(spec)
    return {"status": "ok", "workspace": result.model_dump()}
```

**What this file does:** Handles HTTP concerns only.
1. `WorkspaceRequest` validates the incoming JSON — if a required field is missing or a field violates a constraint (e.g. `name` shorter than 3 chars), FastAPI returns a `422 Unprocessable Entity` automatically before the handler runs
2. `get_provisioner()` is a **FastAPI dependency** — see the pattern explanation in Section 6
3. `get_current_user` dependency handles auth — the route handler never sees the token directly
4. The handler maps the flat HTTP payload to the service layer's `WorkspaceSpec` and calls `svc.provision_workspace`
5. The result is serialised to JSON via `result.model_dump()`

**Why is `get_provisioner` a function and not just `Provisioner()`?**

If the code were `svc: Provisioner = Depends(Provisioner)`, it would work — FastAPI would call `Provisioner()` once per request. But by wrapping it in `get_provisioner()`, tests can swap it out using `app.dependency_overrides[get_provisioner] = lambda: FakeProv()`. If `Provisioner` itself were the dependency key, tests would need to monkeypatch the class constructor, which is messier.

---

### app/cli.py

The CLI entry point reads a YAML file, builds a `WorkspaceSpec`, and calls `Provisioner.provision_workspace`. It shares the same service and client code as the HTTP API — there is no duplication of business logic.

---

### function_app.py

```python
import azure.functions as func
from app.main import app as fastapi_app

functions_app = func.FunctionApp()

@functions_app.route(route="{*route}", auth_level=func.AuthLevel.ANONYMOUS, methods=[...])
async def http_trigger(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return await func.AsgiMiddleware(fastapi_app).handle_async(req, context)
```

**What it does:** This is the only Azure Functions-specific file. The entire FastAPI application is untouched. `AsgiMiddleware` translates between Azure Functions' HTTP request/response format and Python's ASGI standard that FastAPI uses.

**`route="{*route}"`:** The `{*route}` wildcard captures every path. Without this, only requests to the root path would be forwarded. All FastAPI routes (`/v1/fabric/workspace`, `/docs`, etc.) are preserved unchanged.

**`auth_level=func.AuthLevel.ANONYMOUS`:** Azure Functions can enforce its own function keys for authentication. `ANONYMOUS` disables that — authentication is handled by the application layer (the JWT validation in `auth.py`), not by Azure Functions. This is correct because the service has its own auth model.

**`host.json` — `"routePrefix": ""`:** By default, Azure Functions prefixes all HTTP routes with `api/`. A request to `POST /v1/fabric/workspace` would become `POST /api/v1/fabric/workspace`. Setting `routePrefix` to an empty string removes this prefix so FastAPI's routes work as defined.

---

## 6. Key Design Patterns Explained

### Idempotency

An operation is **idempotent** if calling it once produces the same result as calling it ten times. The HTTP spec says `GET`, `PUT`, and `DELETE` should be idempotent. `POST` is not required to be, but this service makes its `POST /v1/fabric/workspace` idempotent by design.

**Why it matters:** Platform automation needs to be safe to re-run. If a deployment pipeline runs twice, if a CI job is re-triggered, or if an operator isn't sure whether a previous call succeeded — re-running should never create duplicates or corrupt state.

**How it's implemented here:**

```python
# Check before create
ws = self.fabric.find_workspace_by_name(spec.name)
if ws is None:
    ws = self.fabric.create_workspace(spec.name, ...)

# Check before update
if spec.description and spec.description != ws.get("description"):
    ws = self.fabric.update_workspace(...)

# Check before role assignment
existing = existing_assignments.get(principal_id)
if existing is None:
    self.fabric.add_role_assignment(...)
elif existing["role"] != desired_role:
    self.fabric.update_role_assignment(...)
# else: skip — already correct
```

Every write is preceded by a read that checks current state. If the state already matches the desired state, no API call is made.

---

### Retry with Exponential Backoff

External APIs (Fabric, Graph, ARM) are not 100% reliable. Network blips, temporary overloads, and rate limits (`429`) are expected. Rather than failing immediately and requiring the caller to retry, the HTTP session retries automatically.

Exponential backoff means each successive retry waits longer than the previous one. This gives the server time to recover and prevents the service from contributing to an overload by hammering a struggling endpoint with rapid retries.

---

### Dependency Injection in FastAPI

FastAPI's `Depends()` system is a form of **dependency injection** — a design pattern where objects receive their dependencies from the outside rather than creating them internally.

```python
# Without dependency injection:
@router.post("/workspace")
def create_workspace(payload: WorkspaceRequest):
    svc = Provisioner()      # hard-coded — impossible to test
    result = svc.provision_workspace(...)

# With dependency injection:
@router.post("/workspace")
def create_workspace(
    payload: WorkspaceRequest,
    svc: Provisioner = Depends(get_provisioner),  # injected
):
    result = svc.provision_workspace(...)
```

In tests, `app.dependency_overrides[get_provisioner] = lambda: FakeProv()` replaces the real provisioner with a test double for the duration of the test. The route handler code does not change.

**Critical detail:** `Depends(get_provisioner)` stores a reference to the `get_provisioner` function object. It does NOT do a name lookup every time. If code does `import app.api.v1.routes_fabric as routes; routes.get_provisioner = fake`, that replaces the name in the module's namespace but FastAPI still holds the original function object. `dependency_overrides` is the correct API for test substitution.

---

### Structured Logging and Correlation IDs

**Structured logging** means every log entry is a key-value object rather than a free-text string.

```python
# Unstructured (bad for searching):
logger.info(f"Workspace created: {name}, capacity: {capacity_id}")

# Structured (good for searching):
logger.info("workspace_created", workspace=name, capacity=capacity_id)
```

A log aggregator like Azure Monitor or Datadog can index `workspace` and `capacity` as searchable fields. Queries like "all workspaces assigned to capacity X in the last hour" become trivial.

**Correlation IDs** tie together all log entries from a single HTTP request — even if those entries come from different functions called during that request. This makes debugging dramatically easier: provide the correlation ID and pull every log line from that request.

---

### JWT Authentication

A **JWT (JSON Web Token)** is a compact, signed token that carries identity claims. When a user logs in via Entra ID, they receive a JWT that says "I am user X, I belong to tenant Y, this token is valid until time Z, issued by Entra ID's key `kid=abc`".

The service validates this without calling Entra ID on every request:
1. Fetch Microsoft's public keys from the JWKS endpoint (cached)
2. Find the key matching the `kid` (key ID) in the token header
3. Use that public key to verify the token's RS256 signature
4. Check `aud` (audience), `iss` (issuer), and `exp` (expiry)

If any check fails, return `401 Unauthorized`. If all pass, return the decoded claims so the route handler can see who made the request.

---

### Azure Identity — DefaultAzureCredential

`DefaultAzureCredential` is a chain of credential providers that tries each in sequence until one succeeds:

1. `EnvironmentCredential` — reads `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
2. `WorkloadIdentityCredential` — Kubernetes workload identity
3. `ManagedIdentityCredential` — Azure VM, App Service, Functions Managed Identity
4. `AzureCliCredential` — `az login` on a developer's machine
5. ... (several more)

**The benefit:** the same code works everywhere. On a developer's machine, it uses `az login`. In Azure Functions, it uses the Managed Identity (no credentials stored anywhere). In a CI pipeline, it uses environment variables from GitHub Secrets. No code changes required between environments.

---

## 7. Configuration System

All configuration flows through a single path:

```
Environment variables
        or
  .env file on disk
        ↓
  app/config.py (Pydantic Settings)
        ↓
  settings = Settings()   ← module-level singleton
        ↓
  imported by any module that needs config
```

**Never read `os.environ` directly** anywhere outside `config.py`. All configuration goes through `settings`. This keeps configuration centralised and type-safe.

**`.env` vs environment variables:** In local development, create a `.env` file at the project root. Pydantic Settings reads it automatically. In Azure Functions, set Application Settings in the portal or via CLI — those become environment variables at runtime. The code behaves identically in both cases.

**`local.settings.json`:** This is Azure Functions Core Tools' local equivalent of Application Settings. The `func start` command reads it and injects the values into the process environment before the Python code starts. It is git-ignored because it contains credentials.

---

## 8. Testing Strategy

### Why Tests Need a Root conftest.py

`app/config.py` has this at the bottom:

```python
settings = Settings()
```

This is a **module-level statement** — Python executes it the moment `config.py` is imported. `Settings()` reads from the environment. If the required environment variables are not set, it raises a validation error.

pytest imports test files during collection. Those test files import `app.main`, which imports `app.config`, which triggers `Settings()`. By the time any test fixture runs, the import has already happened.

The root `conftest.py` is the solution:

```python
# conftest.py (project root)
import os
os.environ.setdefault("DEV_MODE", "true")
os.environ.setdefault("AZURE_SUBSCRIPTION_ID", "00000000-0000-0000-0000-000000000000")
...
```

pytest loads `conftest.py` files **before** it collects test files. The root `conftest.py` runs before any `app` module is imported, so `Settings()` finds the environment variables already set.

`setdefault` is used instead of direct assignment — if a real `.env` file is present (a developer running tests against a real environment), those values take priority.

---

### Why DummyFabricClient Exists

`FabricClient.__init__` creates a real HTTP session and requires Azure credentials. A test that instantiates `FabricClient()` directly would try to connect to the Fabric API, which:
- Requires real Azure credentials
- Makes the test slow (network round trip)
- Makes the test fragile (fails if Fabric is down)
- Makes the test non-deterministic (results depend on what's in the tenant)

`DummyFabricClient` solves this by:
1. Skipping `super().__init__()` — no HTTP session, no credential lookup
2. Overriding every method with a function that returns a hard-coded result

Tests run in milliseconds with zero network calls. They are deterministic — the same input always produces the same output regardless of the state of the Azure tenant.

**Important:** Every method called by the provisioner must be stubbed. If `DummyFabricClient` inherits a method from `FabricClient` that the provisioner calls, the inherited method will try to use `self.base` and `self.session` — which don't exist because `__init__` was skipped. This is why `find_capacity_by_name` and `find_domain_by_name` stubs were needed.

---

### Why dependency_overrides Instead of monkeypatch

```python
# This does NOT work for FastAPI:
monkeypatch.setattr(routes, "get_provisioner", lambda: FakeProv())

# This WORKS:
app.dependency_overrides[get_provisioner] = lambda: FakeProv()
```

`Depends(get_provisioner)` in the route definition evaluates `get_provisioner` as a Python expression at module import time. The result — the function object itself — is stored inside FastAPI's dependency graph. It is not a name lookup.

When `monkeypatch.setattr(routes, "get_provisioner", ...)` runs, it replaces the name `get_provisioner` in the `routes` module's `__dict__`. FastAPI's dependency graph still holds the original function object — the monkeypatch has no effect on it.

`app.dependency_overrides` is FastAPI's purpose-built mechanism. It is a dict keyed by the original dependency callable. When FastAPI resolves a dependency, it checks `dependency_overrides` first. If the original function is found there, the override is called instead.

---

### Why Patch the Provisioner Module Not aad_lookup

```python
# provisioner.py line 6:
from app.utils.aad_lookup import resolve_group_identifier
```

This import binds the name `resolve_group_identifier` in `provisioner.py`'s own namespace as a direct reference to the function object. It is equivalent to:

```python
provisioner.resolve_group_identifier = aad_lookup.resolve_group_identifier
```

If a test does `monkeypatch.setattr(aad_lookup, "resolve_group_identifier", fake)`, that replaces `aad_lookup.resolve_group_identifier`. But `provisioner.resolve_group_identifier` still points at the original function.

The correct patch target is the name as it exists in the module that uses it:

```python
import app.services.provisioner as prov_module
monkeypatch.setattr(prov_module, "resolve_group_identifier", lambda x: f"gid-{x}")
```

**General rule:** patch where the name is used, not where it is defined. If `moduleA` does `from moduleB import foo`, patch `moduleA.foo`.

---

## 9. Azure Functions Wrapper — How It Works

The FastAPI application is a standard **ASGI** app. ASGI (Asynchronous Server Gateway Interface) is a Python standard that defines how a web server communicates with a web application. Any ASGI server (uvicorn, hypercorn) can serve it.

Azure Functions uses its own HTTP request/response model (`func.HttpRequest`, `func.HttpResponse`). `AsgiMiddleware` bridges the two formats:

```
Azure Functions runtime
        │
        │ func.HttpRequest
        ▼
  func.AsgiMiddleware(fastapi_app)
        │
        │ translates to ASGI scope/receive/send
        ▼
  FastAPI application
        │
        │ processes request normally
        │ runs middleware, routing, dependencies
        ▼
  ASGI response
        │
        │ AsgiMiddleware translates back
        ▼
  func.HttpResponse
        │
        ▼
  Azure Functions runtime returns to caller
```

**Result:** the FastAPI application has zero knowledge that it is running inside Azure Functions. It could be served by uvicorn locally, run in a Docker container, or hosted in Azure Functions — the same code, the same routes, the same middleware.

---

## 10. Infrastructure as Code — Bicep

`infra/main.bicep` defines every Azure resource the service needs. Running the Bicep file creates the exact same infrastructure every time — in any subscription, in any region, consistently.

**Resources created:**

| Resource | Purpose |
|----------|---------|
| Storage Account | Azure Functions runtime requires a storage account for internal bookkeeping (lease locks, queue triggers) |
| Log Analytics Workspace | Stores all Application Insights telemetry |
| Application Insights | Collects request traces, exceptions, and custom events from the Functions app |
| App Service Plan (Y1/Dynamic) | The serverless compute tier — billed per execution, scales to zero |
| Function App | The compute resource that runs the Python code |
| Key Vault | Secure secret storage; the Managed Identity is granted `Key Vault Secrets User` by the Bicep template |

**System Assigned Managed Identity:**

```bicep
identity: {
    type: 'SystemAssigned'
}
```

This tells Azure to create an identity for the Function App automatically. The identity can be granted roles on other Azure resources (Monitoring Contributor, Key Vault access, etc.) without any passwords or client secrets. `DefaultAzureCredential` in the Python code picks this up automatically when running in Azure.

**Bicep `uniqueString()`:**

```bicep
param suffix string = uniqueString(resourceGroup().id)
var storageAccountName = 'stfabric${environment}${take(suffix, 6)}'
```

Storage account names must be globally unique across all of Azure. `uniqueString()` generates a deterministic hash of the resource group ID — the same resource group always produces the same suffix, so re-running the deployment does not create new resources.

---

## 11. CI/CD Pipeline

`.github/workflows/ci-cd.yml` defines three jobs:

```
push to main
    │
    ▼
[test]
  - Set up Python 3.11
  - pip install -r requirements.txt
  - pytest -q
    │ (only on push to main, not PRs)
    ▼
[deploy-infra]
  - az login (using AZURE_CREDENTIALS secret)
  - az deployment group create --template-file infra/main.bicep
    │
    ▼
[deploy-app]
  - pip install into .python_packages/
  - func publish (or Azure Functions Action)
```

**`[test]` runs on every push and every pull request.** A failing test blocks the PR from merging.

**`[deploy-infra]` and `[deploy-app]` only run on push to `main`.** Pull requests do not deploy.

**`needs: test`** on the deploy jobs means they only start if tests passed. A failed test cancels the entire pipeline.

**`needs: deploy-infra`** on `deploy-app` means infrastructure is always up to date before new code is deployed. If Bicep adds a new app setting, it exists before the code that reads it is deployed.

**GitHub Secrets** hold all credentials. They are injected as environment variables at runtime and never appear in logs.

---

## 12. Local Setup from Zero

```bash
# 1. Clone the repository
git clone <repo-url>
cd fabric_control_plane_mvp

# 2. Create a virtual environment
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set up configuration
cp .env.example .env
# Open .env in an editor and fill in:
#   AZURE_SUBSCRIPTION_ID=<your subscription>
#   AZURE_RESOURCE_GROUP=<your resource group>
#   SWAGGER_CLIENT_ID=<your SPA app reg>
#   DEV_MODE=true          ← for local dev without real auth

# 5. Run tests (no .env values needed — conftest.py seeds dummies)
pytest -v

# 6. Run the API with uvicorn (plain FastAPI, no Functions host)
uvicorn app.main:app --reload --port 8000
# → http://localhost:8000/docs

# 7. Run via Azure Functions Core Tools (same as production)
func start --python
# → http://localhost:7071/v1/fabric/health
```

---

## 13. DevOps Runbook — Full Azure Deployment

This section is aimed at the team member with DevOps skills.

### Prerequisites

```bash
# Azure CLI
winget install Microsoft.AzureCLI          # Windows
brew install azure-cli                      # macOS

# Azure Functions Core Tools v4
winget install Microsoft.AzureFunctionsCore Tools-4
brew install azure-functions-core-tools@4

# Bicep extension (built into az cli >= 2.20)
az bicep install
az bicep version

az login
az account set --subscription "<subscription-id>"
```

### Step 1 — Resource Group

```bash
az group create --name rg-fabric-controlplane --location eastus
```

### Step 2 — Deploy Infrastructure

```bash
# Edit infra/main.parameters.json — set swaggerClientId and environment

az deployment group create \
  --resource-group rg-fabric-controlplane \
  --template-file infra/main.bicep \
  --parameters @infra/main.parameters.json

# Capture outputs
az deployment group show \
  --resource-group rg-fabric-controlplane \
  --name main \
  --query "properties.outputs" \
  --output table
```

Note `functionAppName` and `functionAppPrincipalId` from the outputs.

### Step 3 — Grant Managed Identity Permissions

```bash
PRINCIPAL_ID="<functionAppPrincipalId>"
SUB="<subscription-id>"
RG="rg-fabric-controlplane"

# ARM — Monitoring Contributor
az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Monitoring Contributor" \
  --scope "/subscriptions/$SUB/resourceGroups/$RG"

# Microsoft Graph — Group.Read.All
# Must be done in Azure Portal:
# Portal → Azure Active Directory → App registrations
# → Find the Function App's managed identity → API permissions
# → Add Microsoft Graph → Application permission → Group.Read.All
# → Grant admin consent
```

For Fabric API access: open the Fabric Admin Portal → Tenant settings → add the Managed Identity as Fabric Administrator.

### Step 4 — Deploy the Code

```bash
FUNC_APP_NAME="<functionAppName>"

func azure functionapp publish $FUNC_APP_NAME \
  --python \
  --build remote
```

### Step 5 — Verify

```bash
FUNC_URL="https://$FUNC_APP_NAME.azurewebsites.net"
curl "$FUNC_URL/v1/fabric/health"
# → {"status":"healthy"}
```

### Setting Up the CI/CD Pipeline

In GitHub → Settings → Secrets → Actions, add:

```bash
# Generate service principal credentials
az ad sp create-for-rbac \
  --name "github-fabric-cp-deploy" \
  --role contributor \
  --scopes /subscriptions/<id>/resourceGroups/rg-fabric-controlplane \
  --sdk-auth
```

Copy the JSON output as the `AZURE_CREDENTIALS` secret. Add the remaining secrets:

| Secret name | Value |
|-------------|-------|
| `AZURE_CREDENTIALS` | JSON from the command above |
| `AZURE_SUBSCRIPTION_ID` | Subscription GUID |
| `AZURE_RESOURCE_GROUP` | `rg-fabric-controlplane` |
| `AZURE_FUNCTIONAPP_NAME` | From Bicep output |
| `SWAGGER_CLIENT_ID` | SPA app registration client ID |

Push to `main` to trigger the pipeline.

---

## 14. Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `ModuleNotFoundError: No module named 'app'` | Running `pytest` from wrong directory or `pythonpath` not set | Run from the project root; `pyproject.toml` sets `pythonpath = ["."]` for pytest |
| `ValidationError` on startup | Required env var missing when `Settings()` is instantiated | Check `.env` is populated or env vars are set in the shell |
| `AttributeError: 'DummyFabricClient' has no attribute 'base'` | A method in `DummyFabricClient` was not overridden; inherited method from `FabricClient` ran and tried to use `self.base` which doesn't exist because `super().__init__()` was skipped | Add the missing stub method to `DummyFabricClient` in `tests/conftest.py` |
| `assert 500 == 200` in route tests | Either `dependency_overrides` not used (monkeypatch doesn't work for FastAPI Depends), or `FakeProv` returns a plain `dict` instead of `ProvisionResult` | Use `app.dependency_overrides`; return a `ProvisionResult` from the fake |
| `Authentication failed: Unable to get authority configuration` | Dummy tenant GUID `00000000-0000-0000-0000-000000000000` sent to real Entra ID | Only occurs if `USE_AZURE_IDENTITY=false` and real Azure calls are made in tests — patch the function that makes the Azure call so it never runs |
| `func: command not found` | Azure Functions Core Tools not installed | `winget install Microsoft.AzureFunctionsCore Tools-4` |
| Routes return `404` on Functions | `routePrefix` not set to empty string | Check `host.json` has `"http": { "routePrefix": "" }` |

---

## 15. How to Extend the Service

### Adding a new API endpoint

1. Add the request/response models to `app/models/schemas.py`
2. Add the business logic method to `app/services/provisioner.py`
3. Add the route handler to `app/api/v1/routes_fabric.py`
4. Add a test in `tests/test_routes_fabric.py` using `app.dependency_overrides`
5. Add a service layer test in `tests/test_provisioner.py` if the logic is non-trivial

### Adding a new Fabric API call

1. Add the method to `app/clients/fabric_client.py`
2. Add the corresponding stub method to `tests/conftest.py`'s `DummyFabricClient`
3. Call the method from the provisioner service

### Adding a new Azure resource to the infrastructure

1. Add the resource to `infra/main.bicep`
2. Add any required app settings to the Function App's `appSettings` block in the same file
3. If the resource needs access control, add a `roleAssignment` resource

### Adding a new environment variable

1. Add the field to `app/config.py`'s `Settings` class with a default value and `validation_alias`
2. Add it to `.env.example` with a comment explaining what it does
3. Add it to `local.settings.json` with a blank or default value
4. Add it to the `appSettings` block in `infra/main.bicep`
5. Add it to the `env:` block in `.github/workflows/ci-cd.yml` (test job) if tests need it
6. Add `os.environ.setdefault("NEW_VAR", "dummy")` to the root `conftest.py`
