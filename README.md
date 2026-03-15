# Fabric Control Plane

> Idempotent, API-driven provisioning and governance for Microsoft Fabric workspaces — deployable as a standalone FastAPI service **or** as an Azure Functions app.

---

## Table of Contents

1. [What This Does](#what-this-does)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Local Development Setup](#local-development-setup)
5. [Environment Variables Reference](#environment-variables-reference)
6. [Running the API Locally](#running-the-api-locally)
7. [Running with Azure Functions Core Tools](#running-with-azure-functions-core-tools)
8. [Running the Tests](#running-the-tests)
9. [CLI Usage](#cli-usage)
10. [API Reference](#api-reference)
11. [Planned API Roadmap](#planned-api-roadmap)
12. [Deploying to Azure Functions](#deploying-to-azure-functions)
13. [Post-Deployment: Role Assignments](#post-deployment-role-assignments)
14. [CI/CD Pipeline](#cicd-pipeline)
15. [Monitoring Limitations](#monitoring-limitations)
16. [Idempotency Model](#idempotency-model)
17. [Role Mapping](#role-mapping)

---

## What This Does

The Fabric Control Plane is an idempotent automation service that provisions and reconciles **Microsoft Fabric workspaces** at scale. It accepts a workspace specification — either via REST API or YAML file — and drives the full lifecycle:

- Creates or updates a Fabric workspace (idempotent by display name)
- Assigns an F-SKU/A-SKU capacity and a Fabric domain
- Reconciles Azure AD group role assignments (adds missing, updates changed roles, skips unchanged)
- Emits structured JSON logs with per-request correlation IDs
- Retries all outbound HTTP calls with exponential backoff

> **Note on monitoring:** Fabric workspaces and F-SKU capacities are not ARM resources — diagnostic settings and metric alert rules cannot be configured programmatically via the REST API. See [Monitoring Limitations](#monitoring-limitations) for details and workarounds.

### Deployment Note: Azure Functions & FastAPI
This project is built with FastAPI and deployed on Azure Functions. We use an `AsgiMiddleware` wrapper to translate Azure's native HTTP requests into the standard ASGI format that FastAPI expects. This allows us to run a standard FastAPI app in a serverless environment with zero rewrites, preventing vendor lock-in and keeping local development simple.

---

## Architecture

```
fabric_control_plane_mvp/
├── function_app.py              # Azure Functions v2 entry point (AsgiMiddleware)
├── host.json                    # Functions host config (routePrefix: "")
├── local.settings.json          # Local Functions settings (git-ignored)
├── Dockerfile                   # Container-based Functions deployment
├── requirements.txt
├── conftest.py                  # Root pytest env-var seed (fixes import-time Settings())
├── app/
│   ├── main.py                  # FastAPI app — also used as ASGI target by Functions
│   ├── config.py                # Pydantic Settings (env / .env file)
│   ├── auth.py                  # Entra ID JWT validation via JWKS
│   ├── exceptions.py
│   ├── logging_config.py        # structlog — JSON (prod) / coloured (dev)
│   ├── cli.py                   # YAML-driven CLI provisioner
│   ├── api/v1/
│   │   └── routes_fabric.py     # POST /v1/fabric/workspace, GET /v1/fabric/health
│   ├── clients/
│   │   └── fabric_client.py     # Microsoft Fabric REST API client
│   ├── services/
│   │   └── provisioner.py       # Orchestration layer
│   ├── models/
│   │   └── schemas.py           # Pydantic request / response models
│   └── utils/
│       ├── retry.py             # Exponential backoff Session
│       ├── aad_lookup.py        # AAD group name → object ID via Graph API
│       └── fabric_lookup.py     # Capacity / domain display name → GUID
├── tests/
│   ├── conftest.py              # DummyFabricClient fixture
│   ├── test_provisioner.py
│   └── test_routes_fabric.py
└── infra/
    ├── main.bicep               # Full Azure infrastructure (Functions, KV, AppInsights)
    └── main.parameters.json
```

**Request flow on Azure Functions**

```
HTTPS → Azure Functions HTTP Trigger
           └─ function_app.py (AsgiMiddleware)
                 └─ FastAPI (app/main.py)
                       └─ routes_fabric.py
                             └─ provisioner.py
                                   ├─ fabric_client.py  → Fabric REST API
                                   ├─ aad_lookup.py     → Microsoft Graph
                                   └─ fabric_lookup.py  → Fabric REST API (capacity/domain resolution)
```

---

## Prerequisites

| Requirement | Version / Notes |
|-------------|-----------------|
| Python | 3.11+ |
| Azure CLI | Latest — `az login` for local identity |
| Azure Functions Core Tools | v4 — for `func start` and `func publish` |
| An Azure subscription | With a Fabric capacity provisioned |
| Service principal **or** Managed Identity | Roles listed below |

### Required Azure Permissions

| Target | Role |
|--------|------|
| Microsoft Fabric | `Fabric Administrator` (tenant level) or workspace Admin |
| Azure Resource Group | `Monitoring Contributor` |
| Microsoft Graph API | `Group.Read.All` (application permission) |

---

## Local Development Setup

### 1. Clone and create a virtual environment

```bash
git clone <repo-url>
cd fabric_control_plane_mvp

# Using venv (recommended)
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate
```

Or with conda:

```bash
conda create -n fabric-cp python=3.11
conda activate fabric-cp
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and fill in the values. The minimum set for local development:

```env
DEV_MODE=true                          # Skips JWT validation — local only
USE_AZURE_IDENTITY=true                # Uses az login / DefaultAzureCredential

AZURE_SUBSCRIPTION_ID=<your-sub-id>
AZURE_RESOURCE_GROUP=rg-fabric-platform
SWAGGER_CLIENT_ID=<spa-app-reg-client-id>
```

For service principal auth instead of managed identity / `az login`:

```env
USE_AZURE_IDENTITY=false
AZURE_TENANT_ID=<tenant-id>
AZURE_CLIENT_ID=<sp-client-id>
AZURE_CLIENT_SECRET=<sp-secret>
```

> **`DEV_MODE=true` must never be set in production.** It bypasses JWT validation entirely.

---

## Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DEV_MODE` | No | `false` | Bypass JWT auth — local dev only |
| `USE_AZURE_IDENTITY` | No | `true` | Use DefaultAzureCredential (Managed Identity / az login) |
| `AZURE_TENANT_ID` | If SP auth | — | Entra ID tenant GUID |
| `AZURE_CLIENT_ID` | If SP auth | — | Service principal app (client) ID |
| `AZURE_CLIENT_SECRET` | If SP auth | — | Service principal secret |
| `AZURE_SUBSCRIPTION_ID` | Yes | — | Subscription hosting Fabric capacity |
| `AZURE_RESOURCE_GROUP` | Yes | — | Resource group for ARM resources |
| `SWAGGER_CLIENT_ID` | Yes | — | SPA app registration for Swagger Authorize button |
| `SWAGGER_SCOPES` | No | `openid profile email` | Scopes shown in Swagger dialog |
| `FABRIC_BASE_URL` | No | `https://api.fabric.microsoft.com/v1` | Override for sovereign clouds |
| `ARM_BASE_URL` | No | `https://management.azure.com` | Override for sovereign clouds |

---

## Running the API Locally

```bash
uvicorn app.main:app --reload --port 8000
```

| Endpoint | URL |
|----------|-----|
| REST API | `http://localhost:8000` |
| Swagger UI | `http://localhost:8000/docs` |
| Health check | `http://localhost:8000/v1/fabric/health` |

---

## Running with Azure Functions Core Tools

This runs the exact same code path as production — the Functions host wraps FastAPI via `AsgiMiddleware`.

### Prerequisites

```bash
# Install Azure Functions Core Tools v4
# Windows (winget)
winget install Microsoft.AzureFunctionsCore Tools-4

# macOS
brew tap azure/functions
brew install azure-functions-core-tools@4

# Verify
func --version   # should print 4.x.x
```

### Start locally

```bash
# Ensure local.settings.json has your values filled in, then:
func start --python
```

The Functions host starts on `http://localhost:7071`. All FastAPI routes are preserved:

| Endpoint | URL |
|----------|-----|
| Health check | `http://localhost:7071/v1/fabric/health` |
| Provision workspace | `http://localhost:7071/v1/fabric/workspace` |
| Swagger UI | `http://localhost:7071/docs` |

> `routePrefix` is set to `""` in `host.json` — this is what keeps the `/v1/...` paths intact instead of the default `api/v1/...` prefixing that Functions normally adds.

---

## Running the Tests

### Why the fix was needed

`app.config.Settings()` is instantiated at **module import time** (`settings = Settings()` in `config.py`). When `pytest` imports `app.main` or `app.services.provisioner`, Python evaluates that line immediately — before any pytest fixture can run. Without the required env vars already present in `os.environ`, Pydantic validation fails and the import crashes with a module-not-found-style error.

Two things fix this permanently:

1. **`pyproject.toml`** — adds the project root to `sys.path` via `pythonpath = ["."]` so `import app` resolves without installing the package.
2. **`conftest.py`** (root level) — sets safe dummy env vars using `os.environ.setdefault(...)` **before** any app module is imported. Root conftest is the only place that is reliably evaluated before collection begins.

### Running tests

```bash
# From the project root — no PYTHONPATH export, no .env needed
pytest -v
```

```
tests/test_provisioner.py::test_provision_new_workspace     PASSED
tests/test_provisioner.py::test_provision_existing_workspace PASSED
tests/test_routes_fabric.py::test_route_create_workspace    PASSED
```

### Running a single test file

```bash
pytest tests/test_provisioner.py -v
```

### Running with coverage

```bash
pip install pytest-cov
pytest --cov=app --cov-report=term-missing
```

> Tests use `DummyFabricClient` — a fully in-memory stub wired in `tests/conftest.py`. No network calls, no Azure credentials needed.

---

## CLI Usage

Provision a workspace from a YAML specification file:

```bash
python -m app.cli --config workspace.yaml
```

Example `workspace.yaml`:

```yaml
workspaceName: EDP_ObservabilityControlCenter_Workspace
description: Data product workspace
capacity: miniazurefabricpoc              # GUID or display name
domain: Finance                           # GUID or display name
logAnalyticsResourceId: /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.OperationalInsights/workspaces/<name>
groups:
  platform_admins: AAD-SEC-ASG-RBAC-AZ-EDPGenAI-Owner
  data_product_owners: AAD-SEC-ASG-RBAC-AZ-EDPGenAI-Contributor
  data_engineers: AAD-SEC-ASG-RBAC-AZ-EDPGenAI-Engineer
  data_viewers: AAD-SEC-ASG-RBAC-AZ-EDPGenAI-Reader
```

---

## API Reference

### Implemented endpoints

The following endpoints are live in this release.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/fabric/workspace` | Provision or reconcile a Fabric workspace |
| `GET` | `/v1/fabric/health` | Liveness probe |

---

### POST /v1/fabric/workspace

Provisions or reconciles a Fabric workspace. Safe to call repeatedly — all operations are idempotent.

**Request body**

```json
{
  "name": "my-workspace",
  "description": "Data product workspace",
  "capacity_id_or_name": "miniazurefabricpoc",
  "domain_id_or_name": "Finance",
  "group_platform_admins": "AAD-SEC-ASG-RBAC-AZ-GenAI-Owner",
  "group_data_product_owners": "AAD-SEC-ASG-RBAC-AZ-GenAI-Contributor",
  "group_data_engineers": "AAD-SEC-ASG-RBAC-AZ-GenAI-Engineer",
  "group_data_viewers": "AAD-SEC-ASG-RBAC-AZ-GenAI-Reader"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Workspace display name (3–128 chars). Idempotency key. |
| `description` | No | Free-text description. Updated only if the value has changed. |
| `capacity_id_or_name` | No | Fabric capacity GUID or display name. Resolved via Fabric API. |
| `domain_id_or_name` | No | Fabric domain GUID or display name. Resolved via Fabric API. |
| `group_platform_admins` | Yes | AAD group display name or object GUID — assigned `Admin` role. |
| `group_data_product_owners` | Yes | AAD group display name or object GUID — assigned `Member` role. |
| `group_data_engineers` | Yes | AAD group display name or object GUID — assigned `Contributor` role. |
| `group_data_viewers` | Yes | AAD group display name or object GUID — assigned `Viewer` role. |

Group display names are resolved to object GUIDs via Microsoft Graph. Raw GUIDs are passed through as-is.

**Response `200 OK`**

```json
{
  "status": "ok",
  "workspace": {
    "workspace_id": "00000000-0000-0000-0000-000000000000",
    "workspace_name": "my-workspace",
    "capacity_assigned": "00000000-0000-0000-0000-000000000000",
    "domain_assigned": "00000000-0000-0000-0000-000000000000",
    "roles_assigned": {
      "platform_admins": "00000000-0000-0000-0000-000000000000",
      "data_product_owners": "00000000-0000-0000-0000-000000000000",
      "data_engineers": "00000000-0000-0000-0000-000000000000",
      "data_viewers": "00000000-0000-0000-0000-000000000000"
    },
    "roles_failed": {}
  }
}
```

---

### GET /v1/fabric/health

Liveness probe used by Azure health checks and load balancers.

**Response `200 OK`**

```json
{ "status": "healthy" }
```

---

## Planned API Roadmap

The following endpoints are **not yet implemented**. They define the intended API contract for future releases. Calling them will return `404`.

| Method | Path | Plane | Description |
|--------|------|-------|-------------|
| `GET` | `/v1/fabric/workspaces` | CP | List all workspaces with capacity and domain |
| `GET` | `/v1/fabric/workspaces/{workspace_id}` | CP | Get workspace state and role assignments |
| `DELETE` | `/v1/fabric/workspaces/{workspace_id}` | CP | Decommission a workspace (requires confirmation) |
| `GET` | `/v1/fabric/capacities` | IP/CP | List available Fabric capacities |
| `GET` | `/v1/fabric/capacities/{capacity_id}` | IP/CP | Get capacity detail and utilisation |
| `POST` | `/v1/fabric/capacities/{capacity_id}/scale` | IP/CP | Request a SKU resize |
| `POST` | `/v1/fabric/capacities/{capacity_id}/suspend` | IP/CP | Suspend a capacity to stop billing |
| `POST` | `/v1/fabric/capacities/{capacity_id}/resume` | IP/CP | Resume a suspended capacity |
| `GET` | `/v1/governance/domains` | CP | List Fabric domains and workspace counts |
| `POST` | `/v1/governance/domains` | CP | Create a new Fabric domain |
| `PATCH` | `/v1/governance/domains/{domain_id}` | CP | Update domain metadata or contributors |
| `GET` | `/v1/governance/guardrails` | CP | Get active guardrail definitions |
| `POST` | `/v1/governance/guardrails/validate` | CP | Pre-flight validation without provisioning |
| `GET` | `/v1/fabric/workspaces/{id}/onelake/roles` | CP/DP | Get OneLake security role assignments |
| `POST` | `/v1/fabric/workspaces/{id}/onelake/roles` | CP/DP | Create or update a OneLake security role |
| `DELETE` | `/v1/fabric/workspaces/{id}/onelake/roles/{role_id}` | CP/DP | Remove a OneLake security role |
| `GET` | `/v1/fabric/workspaces/{id}/encryption` | CP | Get workspace encryption configuration |
| `POST` | `/v1/fabric/workspaces/{id}/encryption/cmk` | CP | Configure or rotate CMK |
| `GET` | `/v1/governance/purview/collections` | CP | List Purview collections |
| `POST` | `/v1/governance/purview/scan` | CP | Trigger an incremental Purview scan |
| `GET` | `/v1/governance/purview/policies` | CP | List active data access policies |
| `POST` | `/v1/governance/purview/policies` | CP | Create or update a data access policy |
| `GET` | `/v1/fabric/workspaces/{id}/items` | DP | List Fabric items in a workspace |
| `POST` | `/v1/fabric/workspaces/{id}/lakehouses` | DP | Provision a lakehouse |
| `POST` | `/v1/fabric/workspaces/{id}/pipelines` | DP | Provision a Data Pipeline |
| `GET` | `/v1/governance/sensitivity-labels` | CP | List approved sensitivity label catalogue |
| `POST` | `/v1/fabric/workspaces/{id}/items/{item_id}/labels` | CP/DP | Apply a sensitivity label |
| `GET` | `/v1/governance/audit` | CP | Query the audit log |
| `GET` | `/v1/governance/compliance/report` | CP | Generate a compliance report |
| `GET` | `/v1/fabric/readiness` | CP | Readiness probe — checks Fabric and Graph API reachability |

**Plane key:** CP = Control Plane (Data Platform Engineering) · IP = Infrastructure Plane (GTIS) · DP = Data Plane (Data Application Teams)

---

## Deploying to Azure Functions

### Overview

The deployment uses:
- **Azure Functions v4** runtime on **Linux / Python 3.11**
- **Consumption (Y1) plan** — serverless, auto-scales to zero
- **System Assigned Managed Identity** — `DefaultAzureCredential` in production, no secrets stored in code
- **Application Insights** backed by a **Log Analytics Workspace**
- **Key Vault** — optional secret storage; Managed Identity granted `Key Vault Secrets User`

### Step 1 — Install tooling

```bash
# Azure CLI
winget install Microsoft.AzureCLI          # Windows
brew install azure-cli                      # macOS

# Azure Functions Core Tools v4
winget install Microsoft.AzureFunctionsCore Tools-4   # Windows
brew install azure-functions-core-tools@4             # macOS

# Bicep (built into az cli >= 2.20, or install standalone)
az bicep install

# Login
az login
az account set --subscription "<subscription-name-or-id>"
```

### Step 2 — Create a resource group

```bash
az group create \
  --name rg-fabric-controlplane \
  --location uksouth
```

### Step 3 — Edit the Bicep parameters

Open [infra/main.parameters.json](infra/main.parameters.json) and update:

```json
{
  "parameters": {
    "environment":    { "value": "prod" },
    "swaggerClientId": { "value": "<spa-app-registration-client-id>" }
  }
}
```

### Step 4 — Deploy infrastructure

```bash
az deployment group create \
  --resource-group rg-fabric-controlplane \
  --template-file infra/main.bicep \
  --parameters @infra/main.parameters.json

# Capture outputs for use in later steps
az deployment group show \
  --resource-group rg-fabric-controlplane \
  --name main \
  --query "properties.outputs" \
  --output table
```

The Bicep template provisions:
- Storage Account (required by Functions runtime)
- Log Analytics Workspace
- Application Insights
- App Service Plan (Consumption Y1, Linux)
- Function App (Python 3.11, System Assigned Identity)
- Key Vault with Managed Identity role assignment

### Step 5 — Set remaining app settings

```bash
FUNC_APP_NAME="<functionAppName from deployment output>"
RG="rg-fabric-controlplane"

# Only needed if USE_AZURE_IDENTITY=false (service principal auth)
az functionapp config appsettings set \
  --name $FUNC_APP_NAME \
  --resource-group $RG \
  --settings \
    "AZURE_CLIENT_ID=<sp-client-id>" \
    "AZURE_CLIENT_SECRET=<sp-secret>"
```

> With `USE_AZURE_IDENTITY=true` (the default set by Bicep), the Managed Identity handles all authentication — no client secret needed.

### Step 6 — Grant Managed Identity permissions

```bash
PRINCIPAL_ID="<functionAppPrincipalId from deployment output>"

# Microsoft Graph — Group.Read.All (requires admin consent)
# Azure Portal → App Registrations → <Function App identity>
# → API Permissions → Add → Microsoft Graph → Application → Group.Read.All → Grant admin consent
```

For Fabric API access, open the [Microsoft Fabric admin portal](https://app.fabric.microsoft.com) and add the Managed Identity (search by name) as a **Fabric Administrator** under **Admin portal → Tenant settings → Delegated tenant settings**.

### Step 7 — Deploy the function code

**Option A — Azure Functions Core Tools (recommended for first deploy)**

```bash
cd D:/projects/edp_fabric_control_plane_mvp

func azure functionapp publish $FUNC_APP_NAME \
  --python \
  --build remote
```

**Option B — ZIP deploy via Azure CLI**

```bash
# Windows PowerShell
Compress-Archive -Path * -DestinationPath deploy.zip `
  -ExcludeFromPath @(".venv", ".git", "tests", ".env", "local.settings.json")

az functionapp deployment source config-zip \
  --name $FUNC_APP_NAME \
  --resource-group $RG \
  --src deploy.zip
```

**Option C — Docker container (Premium / Dedicated plan)**

```bash
ACR_NAME="<your-acr-name>"
az acr build \
  --registry $ACR_NAME \
  --image fabric-cp:latest .

az functionapp config container set \
  --name $FUNC_APP_NAME \
  --resource-group $RG \
  --image "$ACR_NAME.azurecr.io/fabric-cp:latest" \
  --registry-server "$ACR_NAME.azurecr.io"
```

### Step 8 — Verify the deployment

```bash
FUNC_URL="https://$FUNC_APP_NAME.azurewebsites.net"

# Health check
curl "$FUNC_URL/v1/fabric/health"
# Expected: {"status":"healthy"}

# Swagger UI — open in browser
echo "$FUNC_URL/docs"
```

---

## Post-Deployment: Role Assignments

After deployment, verify the service has the correct RBAC:

| Resource | Identity | Role | How to assign |
|----------|----------|------|---------------|
| Microsoft Graph | Function App Managed Identity | `Group.Read.All` | Azure Portal → App Registrations → API Permissions |
| Microsoft Fabric (tenant) | Function App Managed Identity | `Fabric Administrator` | Fabric Admin Portal |
| Key Vault | Function App Managed Identity | `Key Vault Secrets User` | Bicep-provisioned automatically |

---

## CI/CD Pipeline

The [.github/workflows/ci-cd.yml](.github/workflows/ci-cd.yml) pipeline has three stages:

```
push to main
    │
    ├─ [test]          pytest -q (no Azure credentials needed)
    ├─ [deploy-infra]  az deployment group create (Bicep)
    └─ [deploy-app]    func publish / Azure Functions Action
```

### Required GitHub Secrets

Navigate to **Repository → Settings → Secrets and variables → Actions** and add:

| Secret | How to get the value |
|--------|----------------------|
| `AZURE_CREDENTIALS` | `az ad sp create-for-rbac --name "github-fabric-cp" --role contributor --scopes /subscriptions/<id>/resourceGroups/rg-fabric-controlplane --sdk-auth` |
| `AZURE_SUBSCRIPTION_ID` | `az account show --query id -o tsv` |
| `AZURE_RESOURCE_GROUP` | `rg-fabric-controlplane` |
| `AZURE_FUNCTIONAPP_NAME` | From Bicep deployment output |
| `SWAGGER_CLIENT_ID` | SPA app registration client ID |

---

## Monitoring Limitations

### Diagnostic settings

Microsoft Fabric workspaces and capacities are **not ARM resources**. The `Microsoft.Fabric/capacities` resource type rejects diagnostic settings with `ResourceTypeNotSupported`. There is no programmatic REST API to configure this at present.

**Manual steps per workspace:**
1. Open the workspace in the [Fabric portal](https://app.fabric.microsoft.com).
2. Go to **Workspace settings → Monitoring**.
3. Enable **Workspace monitoring** and point it at the Log Analytics workspace.

The `log_analytics_resource_id` supplied in the API request is recorded in the provision result so operators have the correct ID available during the manual step.

### Metric alerts

Fabric **F-SKU** capacities do not expose metrics to Azure Monitor. `microsoft.insights/metricAlerts` only targets PowerBI Dedicated **A-SKU** (`Microsoft.PowerBIDedicated/capacities`).

**Alternatives for F-SKU monitoring:**
- [Fabric Capacity Metrics app](https://learn.microsoft.com/en-us/fabric/enterprise/metrics-app) — built-in utilisation dashboard.
- Schedule an Azure Function or Logic App to poll the Fabric REST API and trigger alerts when utilisation exceeds a threshold.
- Watch the [Fabric roadmap](https://roadmap.fabric.microsoft.com) for native Azure Monitor integration.

---

## Idempotency Model

Every operation is safe to re-run. Calling `POST /v1/fabric/workspace` twice with the same payload is a no-op on the second call.

| Operation | Strategy |
|-----------|----------|
| Workspace create | Find by display name; create only if absent |
| Description update | Compare before patching |
| Capacity assignment | Compare `capacityId`; skip if already correct |
| Domain assignment | Compare `domainId`; skip if already correct |
| Role assignments | Add if missing; update if role changed; skip if unchanged |

---

## Role Mapping

| Request field | Fabric workspace role |
|---------------|-----------------------|
| `group_platform_admins` | Admin |
| `group_data_product_owners` | Member |
| `group_data_engineers` | Contributor |
| `group_data_viewers` | Viewer |
