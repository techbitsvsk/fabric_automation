"""
03_fabric_provisioning/create_shortcuts.py
============================================
Creates ADLS Gen2 shortcuts in the Fabric Lakehouse for all three layers.
The Lakehouse acts purely as a catalog — zero data is stored in OneLake.

Shortcut layout in Lakehouse Tables/dbo/:
  {table}              → prepared-zone/tpcds/iceberg/{table}/   (raw Iceberg)
  silver_{table}       → prepared-zone/tpcds/silver/silver_{table}/
  gold_{table}         → prepared-zone/tpcds/gold/gold_{table}/

This means:
  - Spark:    spark.read.table("spark_catalog.<lh>.dbo.<table>")
  - Power BI: Direct Lake on gold_* tables via Lakehouse

Run:
    python 03_fabric_provisioning/create_shortcuts.py --layer raw
    python 03_fabric_provisioning/create_shortcuts.py --layer silver
    python 03_fabric_provisioning/create_shortcuts.py --layer gold
    python 03_fabric_provisioning/create_shortcuts.py --layer all    # default
    python 03_fabric_provisioning/create_shortcuts.py --layer gold --refresh
"""

import sys
import time
import yaml
import logging
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fabric_auth import fabric_post, fabric_get, fabric_delete

ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

FABRIC = CFG["fabric"]
ADLS   = CFG["adls"]

WORKSPACE_ID   = FABRIC["workspace_id"]
LAKEHOUSE_ID   = FABRIC["lakehouse_id"]
LAKEHOUSE_NAME = FABRIC["lakehouse_name"]
SCHEMA         = FABRIC.get("schema", "dbo")
ACCOUNT_NAME   = ADLS["account_name"]
CONTAINER      = ADLS["prepared_container"]
ICEBERG_BASE   = ADLS["prepared_base_path"]
SILVER_BASE    = ADLS["silver_base_path"]
GOLD_BASE      = ADLS["gold_base_path"]
CONNECTION_ID  = ADLS["prepared_connection_id"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")
log = logging.getLogger(__name__)

RAW_TABLES = [
    "date_dim", "time_dim", "customer", "customer_address",
    "customer_demographics", "household_demographics", "income_band",
    "item", "store", "call_center", "catalog_page",
    "web_site", "web_page", "warehouse", "ship_mode", "reason", "promotion",
    "store_sales", "store_returns", "catalog_sales", "catalog_returns",
    "web_sales", "web_returns", "inventory",
]

SILVER_TABLES = [f"silver_{t}" for t in RAW_TABLES]

GOLD_TABLES = [
    "gold_sales_fact",
    "gold_customer_360",
    "gold_product_performance",
]

SHORTCUTS_API = f"workspaces/{WORKSPACE_ID}/items/{LAKEHOUSE_ID}/shortcuts"


# ─── List / delete ────────────────────────────────────────────────────────────

def list_existing() -> dict:
    try:
        result = fabric_get(SHORTCUTS_API)
        return {s["name"]: s.get("id", s["name"]) for s in result.get("value", [])}
    except Exception as e:
        log.warning("Could not list shortcuts: %s", e)
        return {}


def delete_shortcut(name: str):
    try:
        fabric_delete(f"{SHORTCUTS_API}/{name}")
        log.debug("  Deleted: %s", name)
    except Exception as e:
        log.debug("  Could not delete %s: %s", name, e)


# ─── Payload builder ──────────────────────────────────────────────────────────

def shortcut_payload(name: str, adls_subpath: str) -> dict:
    """
    path  = parent dir in Lakehouse  → Tables/{schema}
    name  = shortcut leaf name       → the table name
    Result: Tables/{schema}/{name}   → ADLS subpath
    """
    return {
        "path":   f"Tables/{SCHEMA}",
        "name":   name,
        "target": {
            "type": "adlsGen2",
            "adlsGen2": {
                "location":     f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
                "subpath":      f"/{CONTAINER}/{adls_subpath}",
                "connectionId": CONNECTION_ID,
            }
        }
    }


def adls_subpath(layer: str, name: str) -> str:
    """Return the ADLS subpath for a given layer and shortcut name."""
    if layer == "raw":
        return f"{ICEBERG_BASE}/{name}"
    elif layer == "silver":
        return f"{SILVER_BASE}/{name}"
    elif layer == "gold":
        return f"{GOLD_BASE}/{name}"
    raise ValueError(f"Unknown layer: {layer}")


# ─── Create / refresh one shortcut ───────────────────────────────────────────

def process_shortcut(name: str, layer: str, existing: dict, refresh: bool) -> dict:
    if name in existing:
        if refresh:
            log.info("  🔄 %-42s refreshing...", name)
            delete_shortcut(name)
            time.sleep(0.3)
        else:
            log.info("  ↷  %-42s already exists", name)
            return {"name": name, "status": "skipped"}

    payload = shortcut_payload(name, adls_subpath(layer, name))
    try:
        result = fabric_post(SHORTCUTS_API, payload)
        sc_id  = result.get("id", "")
        action = "refreshed" if (name in existing and refresh) else "created"
        log.info("  ✓  %-42s %s", name, action)
        return {"name": name, "status": action, "id": sc_id}
    except Exception as e:
        log.error("  ✗  %-42s FAILED: %s", name, e)
        return {"name": name, "status": "error", "error": str(e)}


# ─── Process a layer ─────────────────────────────────────────────────────────

def process_layer(layer: str, tables: list, refresh: bool):
    log.info("\n── Layer: %s (%d shortcuts)", layer.upper(), len(tables))
    existing = list_existing()
    results  = []
    for name in tables:
        results.append(process_shortcut(name, layer, existing, refresh))
        time.sleep(0.2)

    created = sum(1 for r in results if r["status"] in ("created","refreshed"))
    skipped = sum(1 for r in results if r["status"] == "skipped")
    errors  = [r for r in results if r["status"] == "error"]

    log.info("   Created/Refreshed: %d  Skipped: %d  Errors: %d",
             created, skipped, len(errors))
    if errors:
        for e in errors:
            log.error("     ✗ %s — %s", e["name"], e.get("error",""))
    return errors


# ─── Main ─────────────────────────────────────────────────────────────────────

def main(layer: str = "all", refresh: bool = False):
    log.info("🔗 Creating shortcuts — Lakehouse as catalog, data stays in ADLS")
    log.info("   Workspace  : %s", WORKSPACE_ID)
    log.info("   Lakehouse  : %s  (schema: %s)", LAKEHOUSE_ID, SCHEMA)
    log.info("   ADLS       : %s/%s", ACCOUNT_NAME, CONTAINER)
    log.info("   Mode       : %s | refresh=%s", layer.upper(), refresh)

    all_errors = []

    if layer in ("raw", "all"):
        all_errors += process_layer("raw",    RAW_TABLES,    refresh)
    if layer in ("silver", "all"):
        all_errors += process_layer("silver", SILVER_TABLES, refresh)
    if layer in ("gold", "all"):
        all_errors += process_layer("gold",   GOLD_TABLES,   refresh)

    print(f"\n{'─'*65}")
    print(f"  ✅ Shortcuts complete")
    print(f"     ADLS raw    → Tables/{SCHEMA}/{{table}}")
    print(f"     ADLS silver → Tables/{SCHEMA}/silver_{{table}}")
    print(f"     ADLS gold   → Tables/{SCHEMA}/gold_{{table}}")
    print(f"\n  Power BI Direct Lake: connect to Lakehouse → gold_* tables")
    print()

    if all_errors:
        log.error("%d shortcut(s) failed — check ADLS paths and connection ID", len(all_errors))
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer",   default="all",
                        choices=["raw","silver","gold","all"])
    parser.add_argument("--refresh", action="store_true",
                        help="Delete and recreate existing shortcuts")
    args = parser.parse_args()
    main(layer=args.layer, refresh=args.refresh)
