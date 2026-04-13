"""
03_fabric_provisioning/create_copy_job.py
==========================================
Creates:
  Step 1 — 24 CopyJob items (one per TPC-DS table)
            Parquet (raw-zone) → Iceberg (prepared-zone)
  Step 2 — Transform DataPipeline (tpcds_transform_pipeline)
            Notebook_Silver → Notebook_Gold

Note: Copy orchestration pipeline is SKIPPED.
      Stage 5 in run_all.py triggers each CopyJob directly via REST API.
      (DataPipeline with InvokeCopyJob fails REST API creation — known Fabric bug.)
"""

import sys
import json
import base64
import time
import yaml
import logging
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fabric_auth import fabric_get, fabric_headers, FABRIC_API_BASE

ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

FABRIC  = CFG["fabric"]
ADLS    = CFG["adls"]
PIPE    = CFG["pipeline"]

WORKSPACE_ID        = FABRIC["workspace_id"]
LAKEHOUSE_ID        = FABRIC["lakehouse_id"]
LAKEHOUSE_NAME      = FABRIC["lakehouse_name"]
RAW_CONTAINER       = ADLS["raw_container"]
RAW_BASE_PATH       = ADLS["raw_base_path"]
RAW_CONNECTION_ID   = ADLS["raw_connection_id"]
PREP_CONTAINER      = ADLS["prepared_container"]
PREP_BASE_PATH      = ADLS["prepared_base_path"]
PREP_CONNECTION_ID  = ADLS["prepared_connection_id"]
ADLS_ACCOUNT        = ADLS["account_name"]
XFORM_PIPELINE_NAME = PIPE["transform_pipeline_name"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

TABLES = [
    "date_dim", "time_dim", "customer", "customer_address",
    "customer_demographics", "household_demographics", "income_band",
    "item", "store", "call_center", "catalog_page",
    "web_site", "web_page", "warehouse", "ship_mode", "reason", "promotion",
    "store_sales", "store_returns", "catalog_sales", "catalog_returns",
    "web_sales", "web_returns", "inventory",
]


# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def _post(path: str, payload: dict, retries: int = 6) -> requests.Response:
    url      = f"{FABRIC_API_BASE}/{path.lstrip('/')}"
    interval = 15
    for attempt in range(1, retries + 1):
        resp = requests.post(url, json=payload, headers=fabric_headers(), timeout=120)
        if resp.status_code in (200, 201, 202):
            return resp
        body = resp.text
        if resp.status_code in (429,) or \
                (resp.status_code == 400 and "ItemDisplayNameNotAvailableYet" in body):
            if attempt < retries:
                log.warning("  [%d/%d] Retriable error — waiting %ds: %s",
                            attempt, retries, interval, body[:120])
                time.sleep(interval)
                interval = min(interval * 2, 60)
                continue
        log.error("POST %s → %d: %s", url, resp.status_code, body[:500])
        resp.raise_for_status()
    resp.raise_for_status()
    return resp


def _poll_lro(resp: requests.Response, label: str, timeout: int = 120) -> dict:
    if resp.status_code != 202:
        return resp.json() if resp.content else {}
    op_url = resp.headers.get("Operation-Location") or resp.headers.get("Location")
    if not op_url:
        return {}
    deadline = time.time() + timeout
    interval = 3
    while time.time() < deadline:
        r = requests.get(op_url, headers=fabric_headers(), timeout=30)
        r.raise_for_status()
        if r.status_code == 200 and r.content:
            data   = r.json()
            status = data.get("status", "").lower()
            if status in ("succeeded", "completed"):
                return data
            if status in ("failed", "canceled"):
                raise RuntimeError(f"{label} LRO failed: {data}")
        time.sleep(interval)
        interval = min(interval * 1.5, 15)
    raise TimeoutError(f"{label} LRO timed out after {timeout}s")


def _get_item_id(item_type: str, display_name: str,
                 retries: int = 6, wait: float = 3.0) -> str | None:
    for attempt in range(1, retries + 1):
        try:
            resp  = fabric_get(f"workspaces/{WORKSPACE_ID}/{item_type}")
            items = (resp or {}).get("value") or []
            for item in items:
                if item.get("displayName") == display_name:
                    return item.get("id")
            if attempt < retries:
                time.sleep(wait)
        except Exception as e:
            log.warning("    List %s attempt %d failed: %s", item_type, attempt, e)
            time.sleep(wait)
    return None


# ─── Step 1: CopyJob per table ────────────────────────────────────────────────

def copyjob_definition(table: str) -> dict:
    return {
        "properties": {
            "jobMode": "Batch",
            "source": {
                "type": "Parquet",
                "connectionSettings": {
                    "type": "AzureBlobFS",
                    "externalReferences": {"connection": RAW_CONNECTION_ID},
                },
            },
            "destination": {
                "type": "Iceberg",
                "connectionSettings": {
                    "type": "AzureBlobFS",
                    "externalReferences": {"connection": PREP_CONNECTION_ID},
                },
            },
            "policy": {"timeout": "0.12:00:00", "retry": 2},
        },
        "activities": [{
            "properties": {
                "source": {
                    "datasetSettings": {
                        "location": {
                            "type":       "AzureBlobFSLocation",
                            "folderPath": f"{RAW_BASE_PATH}/{table}",
                            "fileSystem": RAW_CONTAINER,
                        },
                        "compressionCodec": "snappy",
                    },
                    "storeSettings": {"recursive": True, "enablePartitionDiscovery": False},
                },
                "destination": {
                    "datasetSettings": {
                        "location": {
                            "type":       "AzureBlobFSLocation",
                            "folderPath": f"{PREP_BASE_PATH}/{table}",
                            "fileSystem": PREP_CONTAINER,
                        },
                    },
                },
                "enableStaging": False,
                "translator": {"type": "TabularTranslator"},
            },
        }],
    }


def upsert_copy_job(table: str) -> str:
    display_name = f"tpcds_copy_{table}"
    existing_id  = _get_item_id("copyJobs", display_name, retries=1, wait=0)
    if existing_id:
        log.info("  ↷  %-38s already exists (%s)", display_name, existing_id)
        return existing_id

    defn    = copyjob_definition(table)
    b64     = base64.b64encode(json.dumps(defn).encode()).decode()
    payload = {
        "displayName": display_name,
        "type":        "CopyJob",
        "definition": {
            "parts": [{"path": "copyjob-content.json", "payload": b64,
                       "payloadType": "InlineBase64"}],
        },
    }
    resp    = _post(f"workspaces/{WORKSPACE_ID}/copyJobs", payload)
    item_id = None

    if resp.status_code == 201 and resp.content:
        item_id = resp.json().get("id")

    if not item_id:
        lro = _poll_lro(resp, display_name)
        item_id = (lro or {}).get("id")

    if not item_id:
        time.sleep(5)
        item_id = _get_item_id("copyJobs", display_name, retries=10, wait=4.0)

    if not item_id:
        raise RuntimeError(f"CopyJob '{display_name}' created but ID not resolved")

    log.info("  ✓  %-38s  %s", display_name, item_id)
    return item_id


# ─── Step 2: Transform pipeline (Silver → Gold) ───────────────────────────────

def notebook_activity(name: str, notebook_id: str, depends_on: list) -> dict:
    """
    TridentNotebook activity.
    baseParameters injects all IDs/paths that the notebooks need at runtime,
    since pipeline-triggered notebooks have no default Lakehouse context.
    """
    return {
        "name":      name,
        "type":      "TridentNotebook",
        "dependsOn": [{"activity": d, "dependencyConditions": ["Succeeded"]}
                      for d in depends_on],
        "policy": {
            "timeout":                "02:00:00",
            "retry":                  1,
            "retryIntervalInSeconds": 60,
        },
        "typeProperties": {
            "notebookId":  notebook_id,
            "workspaceId": WORKSPACE_ID,
            "baseParameters": {
                "LAKEHOUSE_ID":        LAKEHOUSE_ID,
                "WORKSPACE_ID":        WORKSPACE_ID,
                "LAKEHOUSE_NAME":      LAKEHOUSE_NAME,
                "SCHEMA":              FABRIC.get("schema", "dbo"),
                "ADLS_ACCOUNT":        ADLS_ACCOUNT,
                "ADLS_CONTAINER":      PREP_CONTAINER,
                # Auth: workspace MSI OAuth — no account key passed to notebooks
                # Immuta data contracts govern read/write entitlement per layer
                "ADLS_SILVER_BASE":    CFG["adls"]["silver_base_path"],
                "ADLS_GOLD_BASE":      CFG["adls"]["gold_base_path"],
            },
        },
    }


def build_transform_pipeline(notebook_ids: dict) -> dict:
    return {
        "properties": {
            "activities": [
                notebook_activity("Notebook_Silver", notebook_ids["silver"], []),
                notebook_activity("Notebook_Gold",   notebook_ids["gold"], ["Notebook_Silver"]),
            ],
        },
    }


def upsert_pipeline(display_name: str, definition: dict) -> str:
    b64     = base64.b64encode(json.dumps(definition, indent=2).encode()).decode()
    payload = {
        "displayName": display_name,
        "type":        "DataPipeline",
        "definition": {
            "parts": [{"path": "pipeline-content.json", "payload": b64,
                       "payloadType": "InlineBase64"}],
        },
    }
    existing_id = _get_item_id("dataPipelines", display_name, retries=1, wait=0)
    if existing_id:
        log.info("  Updating pipeline '%s' (%s)", display_name, existing_id)
        resp = _post(
            f"workspaces/{WORKSPACE_ID}/dataPipelines/{existing_id}/updateDefinition",
            {"definition": payload["definition"]},
        )
        _poll_lro(resp, display_name)
        return existing_id
    else:
        log.info("  Creating pipeline '%s'", display_name)
        resp    = _post(f"workspaces/{WORKSPACE_ID}/dataPipelines", payload)
        _poll_lro(resp, display_name)
        time.sleep(2)
        item_id = _get_item_id("dataPipelines", display_name, retries=6, wait=3.0)
        if not item_id:
            raise RuntimeError(f"Pipeline '{display_name}' created but ID not found")
        log.info("  Created pipeline ID: %s", item_id)
        return item_id


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(notebook_ids: dict = None) -> dict:
    if notebook_ids is None:
        ids_path = ROOT / "03_fabric_provisioning" / "notebook_ids.json"
        if ids_path.exists():
            notebook_ids = json.loads(ids_path.read_text())
        else:
            raise FileNotFoundError(
                "notebook_ids.json not found — run Stage 3 (upload_notebooks.py) first"
            )

    log.info("🔧 Creating Fabric CopyJobs + Transform Pipeline")
    log.info("   Workspace : %s", WORKSPACE_ID)
    log.info("   Tables    : %d", len(TABLES))

    # ── Step 1: 24 CopyJobs ──────────────────────────────────────────────────
    log.info("\n[1/2] Creating CopyJobs (%d tables)...", len(TABLES))
    copy_job_ids = {}
    for table in TABLES:
        copy_job_ids[table] = upsert_copy_job(table)

    cj_out = ROOT / "03_fabric_provisioning" / "copy_job_ids.json"
    cj_out.write_text(json.dumps(copy_job_ids, indent=2))
    log.info("  Saved CopyJob IDs → %s", cj_out)

    # ── Step 2: Transform pipeline ───────────────────────────────────────────
    log.info("\n[2/2] Creating transform pipeline (Silver → Gold)...")
    xform_def = build_transform_pipeline(notebook_ids)
    xform_id  = upsert_pipeline(XFORM_PIPELINE_NAME, xform_def)

    out = ROOT / "05_pipeline" / "transform_pipeline.json"
    out.write_text(json.dumps(xform_def, indent=2))

    ids = {"transform": xform_id}
    (ROOT / "03_fabric_provisioning" / "pipeline_ids.json").write_text(
        json.dumps(ids, indent=2)
    )

    print(f"\n{'─'*65}")
    print(f"  ✅ Fabric items ready")
    print(f"     CopyJobs          : {len(copy_job_ids)}")
    print(f"     Transform pipeline: {xform_id}")
    return ids


if __name__ == "__main__":
    main()
