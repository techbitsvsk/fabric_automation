"""
03_fabric_provisioning/upload_notebooks.py
===========================================
Uploads Silver and Gold PySpark notebooks to Fabric via REST API.

Fabric Notebooks API returns 202 Accepted (LRO) on creation — this script
polls the operation URL until complete, then resolves the notebook ID.

Run:
    python 03_fabric_provisioning/upload_notebooks.py
"""

import sys
import json
import base64
import re
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

WORKSPACE_ID  = CFG["fabric"]["workspace_id"]
NOTEBOOKS_DIR = ROOT / "04_notebooks"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

NOTEBOOK_FILES = {
    CFG["pipeline"]["silver_notebook"]: NOTEBOOKS_DIR / "nb_02_silver_transform.py",
    CFG["pipeline"]["gold_notebook"]  : NOTEBOOKS_DIR / "nb_03_gold_curated.py",
}


# ─── Notebook source → ipynb ──────────────────────────────────────────────────

def py_to_ipynb(py_path: Path, display_name: str) -> dict:
    source     = py_path.read_text(encoding="utf-8")
    raw_cells  = re.split(r"\n(?=# %%)", source)
    cells      = []
    for block in raw_cells:
        block = block.strip()
        if not block:
            continue
        is_md = block.startswith("# %%[markdown]")
        if is_md:
            block = re.sub(r"^# %%\[markdown\]\s*\n?", "", block)
            lines = [l.lstrip("# ").rstrip() + "\n" for l in block.splitlines()]
            cells.append({"cell_type": "markdown", "metadata": {}, "source": lines})
        else:
            lines = [l + "\n" for l in block.splitlines()]
            cells.append({"cell_type": "code", "metadata": {},
                          "source": lines, "outputs": [], "execution_count": None})
    return {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Synapse PySpark",
                           "language": "Python", "name": "synapse_pyspark"},
            "language_info": {"name": "python"},
            "name": display_name,
            "save_output": True,
        },
        "cells": cells,
    }


# ─── LRO poller ───────────────────────────────────────────────────────────────

def poll_operation(operation_url: str, timeout_sec: int = 120) -> dict:
    """
    Poll a Fabric long-running operation URL until succeeded/failed.
    Returns the final operation result dict.
    """
    deadline = time.time() + timeout_sec
    interval = 3
    while time.time() < deadline:
        resp = requests.get(operation_url,
                            headers=fabric_headers(), timeout=30)
        resp.raise_for_status()

        # 200 with body = terminal state
        if resp.status_code == 200 and resp.content:
            data   = resp.json()
            status = data.get("status", "").lower()
            if status in ("succeeded", "completed"):
                log.info("    Operation succeeded")
                return data
            if status in ("failed", "canceled"):
                raise RuntimeError(f"Fabric operation failed: {data}")
            log.debug("    LRO status: %s — waiting %ds", status, interval)

        # 202 = still running
        time.sleep(interval)
        interval = min(interval * 1.5, 15)

    raise TimeoutError(f"Operation did not complete in {timeout_sec}s: {operation_url}")


def get_operation_url(resp: requests.Response) -> str | None:
    """Extract operation polling URL from response headers."""
    return (resp.headers.get("Operation-Location")
            or resp.headers.get("Location")
            or None)


# ─── Notebook lookup ──────────────────────────────────────────────────────────

def get_notebook_id_by_name(display_name: str, retries: int = 5,
                             wait_sec: float = 3.0) -> str | None:
    """
    Fetch notebook ID by display name from workspace.
    Retries several times to allow for Fabric indexing delay after creation.
    Guards against None/empty response from the notebooks list API.
    """
    for attempt in range(1, retries + 1):
        try:
            response = fabric_get(f"workspaces/{WORKSPACE_ID}/notebooks")
            # Guard: API can return None, empty dict, or missing 'value' key
            if not response or not isinstance(response, dict):
                log.debug("    Attempt %d: empty response, retrying...", attempt)
                time.sleep(wait_sec)
                continue
            items = response.get("value") or []
            for item in items:
                if item.get("displayName") == display_name:
                    return item.get("id")
            # Notebook not in list yet — may not be indexed
            if attempt < retries:
                log.debug("    Attempt %d: '%s' not found yet, retrying in %ds...",
                          attempt, display_name, wait_sec)
                time.sleep(wait_sec)
        except Exception as e:
            log.warning("    Attempt %d: could not list notebooks: %s", attempt, e)
            time.sleep(wait_sec)
    return None


# ─── Create / update notebook ─────────────────────────────────────────────────

def upload_notebook(display_name: str, py_path: Path) -> str:
    """
    Create or update a Fabric notebook.
    Handles 202 LRO response from Fabric Notebooks API.
    Returns the notebook ID.
    """
    log.info("  Processing: %s", display_name)
    if not py_path.exists():
        raise FileNotFoundError(f"Notebook source not found: {py_path}")

    ipynb = py_to_ipynb(py_path, display_name)
    b64   = base64.b64encode(json.dumps(ipynb, indent=2).encode()).decode()

    existing_id = get_notebook_id_by_name(display_name)

    definition = {
        "format": "ipynb",
        "parts": [{
            "path":        "notebook-content.py",
            "payload":     b64,
            "payloadType": "InlineBase64",
        }]
    }

    if existing_id:
        # ── Update existing notebook ──────────────────────────────────────────
        log.info("    Updating existing notebook: %s", existing_id)
        url  = f"{FABRIC_API_BASE}/workspaces/{WORKSPACE_ID}/notebooks/{existing_id}/updateDefinition"
        resp = requests.post(url, json={"definition": definition},
                             headers=fabric_headers(), timeout=120)

        if resp.status_code == 202:
            op_url = get_operation_url(resp)
            if op_url:
                poll_operation(op_url)
        elif resp.status_code not in (200, 201, 204):
            log.error("    Update failed %d: %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()

        log.info("    Updated notebook ID: %s", existing_id)
        return existing_id

    else:
        # ── Create new notebook ───────────────────────────────────────────────
        payload = {
            "displayName": display_name,
            "type":        "Notebook",
            "definition":  definition,
        }
        url  = f"{FABRIC_API_BASE}/workspaces/{WORKSPACE_ID}/notebooks"
        resp = requests.post(url, json=payload,
                             headers=fabric_headers(), timeout=120)

        if resp.status_code == 201:
            # Synchronous success — ID is in the response body
            nb_id = resp.json().get("id")
            log.info("    Created notebook ID: %s", nb_id)
            return nb_id

        elif resp.status_code == 202:
            # Async LRO — poll then fetch ID by name
            log.info("    202 Accepted — polling operation...")
            op_url = get_operation_url(resp)
            if op_url:
                poll_operation(op_url)
            # After LRO completes, fetch the ID — Fabric may take a few seconds to index
            log.info("    Waiting for Fabric to index new notebook...")
            time.sleep(5)
            nb_id = get_notebook_id_by_name(display_name, retries=8, wait_sec=4.0)
            if not nb_id:
                raise RuntimeError(
                    f"Notebook '{display_name}' created but ID not found — "
                    "check Fabric portal and re-run with --from-stage 3"
                )
            log.info("    Resolved notebook ID: %s", nb_id)
            return nb_id

        else:
            log.error("    Create failed %d: %s", resp.status_code, resp.text[:300])
            resp.raise_for_status()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> dict:
    log.info("📓 Uploading PySpark notebooks to Fabric")
    log.info("   Workspace : %s", WORKSPACE_ID)
    log.info("   Notebooks : %d (Silver + Gold)", len(NOTEBOOK_FILES))

    ids = {}
    for display_name, py_path in NOTEBOOK_FILES.items():
        nb_id = upload_notebook(display_name, py_path)
        if not nb_id:
            raise RuntimeError(
                f"Failed to get ID for notebook '{display_name}'. "
                "Check Fabric portal → Workspace → Notebooks."
            )
        ids[display_name] = nb_id

    result = {
        "silver": ids[CFG["pipeline"]["silver_notebook"]],
        "gold":   ids[CFG["pipeline"]["gold_notebook"]],
    }

    # Validate no None IDs before saving
    for k, v in result.items():
        if not v:
            raise RuntimeError(f"Notebook ID for '{k}' is None — cannot continue")

    out_path = ROOT / "03_fabric_provisioning" / "notebook_ids.json"
    out_path.write_text(json.dumps(result, indent=2))
    log.info("  Saved notebook IDs: %s", out_path)

    print(f"\n{'─'*60}")
    print(f"  ✅ Notebooks uploaded")
    print(f"     silver → {result['silver']}")
    print(f"     gold   → {result['gold']}")
    print()
    return result


if __name__ == "__main__":
    main()
