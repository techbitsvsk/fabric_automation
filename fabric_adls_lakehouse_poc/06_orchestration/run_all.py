"""
06_orchestration/run_all.py
============================
End-to-end orchestrator — TPC-DS → Microsoft Fabric pipeline.

Stage flow:
  1. Generate TPC-DS synthetic parquet data locally       (~5 GB, 24 tables)
  2. Upload parquet to ADLS Gen2 raw-zone
  3. Upload Silver + Gold notebooks to Fabric
  4. Create 24 CopyJobs + Transform pipeline
  5. Trigger all 24 CopyJobs in parallel  (Parquet → Iceberg on ADLS)
  6. Create raw shortcuts   (Lakehouse Tables/dbo/{table} → ADLS iceberg)
  7. Trigger Transform pipeline  (Silver → Gold, both written to ADLS)
  8. Create silver + gold shortcuts  (Lakehouse catalog layer for Power BI)

DATA NEVER LEAVES TENANT ADLS:
  - Stages 5/7 write all Iceberg data to ADLS prepared-zone
  - Stages 6/8 register shortcuts in Lakehouse (metadata only, zero data copy)
  - Power BI connects via Direct Lake on gold shortcuts

Usage:
    python 06_orchestration/run_all.py                        # full run
    python 06_orchestration/run_all.py --from-stage 3         # resume
    python 06_orchestration/run_all.py --from-stage 6         # re-shortcut raw
    python 06_orchestration/run_all.py --from-stage 7         # re-run transform
    python 06_orchestration/run_all.py --from-stage 8         # re-shortcut silver+gold
    python 06_orchestration/run_all.py --from-stage 6 --refresh-shortcuts
    python 06_orchestration/run_all.py --dry-run
"""

import sys
import json
import time
import logging
import argparse
import requests
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "03_fabric_provisioning"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

WORKSPACE_ID        = CFG["fabric"]["workspace_id"]
XFORM_PIPELINE_NAME = CFG["pipeline"]["transform_pipeline_name"]


# ─── Stage runners ────────────────────────────────────────────────────────────

def stage_1_generate():
    _banner("STAGE 1 — Generate TPC-DS data locally")
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "gen", ROOT / "01_data_generation" / "generate_tpcds.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()
    log.info("✅ Stage 1 complete")


def stage_2_upload():
    _banner("STAGE 2 — Upload to ADLS Gen2 raw-zone")
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "upload", ROOT / "02_adls_upload" / "upload_to_adls.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.main()
    log.info("✅ Stage 2 complete")


def stage_3_notebooks() -> dict:
    _banner("STAGE 3 — Upload notebooks to Fabric")
    from upload_notebooks import main as nb_main
    ids = nb_main()
    log.info("✅ Stage 3 complete — %s", ids)
    return ids


def stage_4_pipelines(notebook_ids: dict) -> dict:
    _banner("STAGE 4 — Create CopyJobs + Transform pipeline")
    from create_copy_job import main as pipeline_main
    ids = pipeline_main(notebook_ids=notebook_ids)
    log.info("✅ Stage 4 complete — %s", ids)
    return ids


def stage_5_copy_jobs():
    """
    Trigger all 24 CopyJobs in parallel via REST API.
    Each CopyJob: Parquet (raw-zone) → Iceberg (prepared-zone on ADLS).
    Data stays in tenant ADLS throughout.
    """
    _banner("STAGE 5 — Trigger CopyJobs (Parquet → Iceberg, 24 tables)")
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from fabric_auth import fabric_get, fabric_headers, FABRIC_API_BASE

    cj_path = ROOT / "03_fabric_provisioning" / "copy_job_ids.json"
    if not cj_path.exists():
        raise FileNotFoundError("copy_job_ids.json not found — run stage 4 first")
    copy_job_ids = json.loads(cj_path.read_text())
    log.info("  Loaded %d CopyJob IDs", len(copy_job_ids))

    def trigger(table: str, job_id: str) -> tuple:
        url  = (f"{FABRIC_API_BASE}/workspaces/{WORKSPACE_ID}"
                f"/items/{job_id}/jobs/instances?jobType=Execute")
        resp = requests.post(url, json={}, headers=fabric_headers(), timeout=60)
        if resp.status_code not in (200, 201, 202):
            log.error("  ✗ %-35s  trigger %d: %s",
                      table, resp.status_code, resp.text[:200])
            return table, job_id, None
        location = resp.headers.get("Location", "")
        run_id   = location.rstrip("/").split("/")[-1] if location else None
        if not run_id or run_id == job_id:
            body   = resp.json() if resp.content else {}
            run_id = body.get("id") or body.get("runId")
        log.info("  ▶ %-35s  run_id=%s", table, run_id)
        return table, job_id, run_id

    log.info("  Triggering %d CopyJobs in parallel...", len(copy_job_ids))
    run_map = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(trigger, t, jid): t
                   for t, jid in copy_job_ids.items()}
        for fut in as_completed(futures):
            table, job_id, run_id = fut.result()
            run_map[table] = (job_id, run_id)

    log.info("  Polling %d CopyJobs (timeout 120 min)...", len(run_map))
    deadline = time.time() + 7200
    pending  = dict(run_map)
    failed   = {}
    interval = 20
    start    = time.time()

    while pending and time.time() < deadline:
        time.sleep(interval)
        elapsed   = (time.time() - start) / 60
        completed = []
        for table, (job_id, run_id) in list(pending.items()):
            if not run_id:
                completed.append(table)
                continue
            try:
                result = fabric_get(
                    f"workspaces/{WORKSPACE_ID}/copyJobs/{job_id}"
                    f"/jobs/instances/{run_id}"
                )
                status = (result or {}).get("status", "").lower()
                if status in ("succeeded", "completed"):
                    log.info("  ✓ %-35s  done (%.1f min)", table, elapsed)
                    completed.append(table)
                elif status in ("failed", "canceled"):
                    reason = (result or {}).get("failureReason", "")
                    log.error("  ✗ %-35s  %s — %s", table, status, reason)
                    failed[table] = reason
                    completed.append(table)
            except Exception as e:
                log.warning("  %-35s  poll error: %s", table, e)
        for t in completed:
            pending.pop(t, None)
        if pending:
            log.info("  [%.1f min] %d/%d running: %s",
                     elapsed, len(pending), len(run_map), list(pending.keys())[:4])
        interval = min(interval * 1.2, 60)

    elapsed = (time.time() - start) / 60
    if failed:
        raise RuntimeError(f"Stage 5: {len(failed)} CopyJob(s) failed: {failed}")
    if pending:
        raise TimeoutError(f"Stage 5: {len(pending)} still running after {elapsed:.0f} min")
    log.info("✅ Stage 5 complete — %d Iceberg tables on ADLS (%.1f min)",
             len(copy_job_ids), elapsed)


def stage_6_raw_shortcuts(refresh: bool = False):
    """Create shortcuts: Lakehouse Tables/dbo/{table} → ADLS iceberg/{table}"""
    _banner("STAGE 6 — Raw shortcuts (Lakehouse catalog → ADLS Iceberg)")
    from create_shortcuts import main as sc_main
    sc_main(layer="raw", refresh=refresh)
    log.info("✅ Stage 6 complete — raw shortcuts registered")


def stage_7_transform(pipeline_id: str):
    """
    Trigger Transform pipeline:
      Silver notebook: reads raw shortcuts, writes silver Iceberg to ADLS
      Gold notebook:   reads silver from ADLS, writes gold Iceberg to ADLS
    All data stays in tenant ADLS throughout.
    """
    _banner("STAGE 7 — Transform pipeline (Silver → Gold on ADLS)")
    _trigger_and_poll(
        pipeline_id  = pipeline_id,
        display_name = XFORM_PIPELINE_NAME,
        timeout_sec  = 10800,
    )
    log.info("✅ Stage 7 complete — Silver + Gold Iceberg tables on ADLS")


def stage_8_silver_gold_shortcuts(refresh: bool = False):
    """
    Create shortcuts for silver and gold layers:
      Tables/dbo/silver_{table} → ADLS silver/{table}
      Tables/dbo/gold_{table}   → ADLS gold/{table}
    After this, Power BI Direct Lake can connect to gold_* tables.
    """
    _banner("STAGE 8 — Silver + Gold shortcuts (Power BI access layer)")
    from create_shortcuts import main as sc_main
    sc_main(layer="silver", refresh=refresh)
    sc_main(layer="gold",   refresh=refresh)
    log.info("✅ Stage 8 complete — gold tables accessible via Power BI Direct Lake")


# ─── Pipeline trigger + poll ──────────────────────────────────────────────────

def _trigger_and_poll(pipeline_id: str, display_name: str, timeout_sec: int):
    from fabric_auth import fabric_get, fabric_headers, FABRIC_API_BASE

    log.info("  Triggering: %s (%s)", display_name, pipeline_id)
    url  = (f"{FABRIC_API_BASE}/workspaces/{WORKSPACE_ID}"
            f"/items/{pipeline_id}/jobs/instances?jobType=Pipeline")
    resp = requests.post(url, json={}, headers=fabric_headers(), timeout=60)
    log.info("  HTTP %d", resp.status_code)
    if resp.status_code not in (200, 201, 202):
        log.error("  Trigger failed: %s", resp.text[:300])
        resp.raise_for_status()

    location = resp.headers.get("Location", "")
    run_id   = location.rstrip("/").split("/")[-1] if location else None
    if not run_id or run_id == pipeline_id:
        body   = resp.json() if resp.content else {}
        run_id = body.get("id") or body.get("runId")
    if not run_id:
        raise RuntimeError(
            f"Could not extract run ID for '{display_name}'. "
            f"Location: '{location}'"
        )
    log.info("  Run ID: %s", run_id)

    poll_url = (f"workspaces/{WORKSPACE_ID}/items/{pipeline_id}"
                f"/jobs/instances/{run_id}")
    start    = time.time()
    deadline = start + timeout_sec
    interval = 15

    while time.time() < deadline:
        result  = fabric_get(poll_url)
        status  = result.get("status", "").lower()
        elapsed = (time.time() - start) / 60
        log.info("  [%.1fmin]  %s", elapsed, status)
        if status in ("succeeded", "completed"):
            log.info("  ✓ Done in %.1f min", elapsed)
            return result
        if status in ("failed", "canceled"):
            raise RuntimeError(f"Pipeline '{display_name}' {status}: {result}")
        interval = 30 if elapsed > 10 else 15
        time.sleep(interval)

    raise TimeoutError(f"Pipeline '{display_name}' timed out after {timeout_sec//60} min")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _load_notebook_ids() -> dict:
    p = ROOT / "03_fabric_provisioning" / "notebook_ids.json"
    return json.loads(p.read_text()) if p.exists() else {}


def _load_pipeline_ids() -> dict:
    p = ROOT / "03_fabric_provisioning" / "pipeline_ids.json"
    return json.loads(p.read_text()) if p.exists() else {}


def _resolve_pipeline_id(name: str, saved: dict, key: str) -> str:
    if saved.get(key):
        return saved[key]
    from fabric_auth import fabric_get
    for item in fabric_get(f"workspaces/{WORKSPACE_ID}/dataPipelines").get("value", []):
        if item["displayName"] == name:
            return item["id"]
    raise ValueError(f"Pipeline '{name}' not found — run stage 4 first")


def _banner(msg: str):
    log.info("")
    log.info("=" * 65)
    log.info("  %s", msg)
    log.info("=" * 65)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="TPC-DS → Microsoft Fabric pipeline orchestrator"
    )
    parser.add_argument("--from-stage", type=int, default=1,
                        choices=range(1, 9), metavar="N",
                        help="Resume from stage N (1-8)")
    parser.add_argument("--dry-run",           action="store_true")
    parser.add_argument("--skip-generate",     action="store_true")
    parser.add_argument("--refresh-shortcuts", action="store_true",
                        help="Delete and recreate shortcuts (use after re-running pipelines)")
    args = parser.parse_args()

    STAGE_LABELS = {
        1: "Generate TPC-DS data locally              (~5 GB, 24 tables)",
        2: "Upload parquet to ADLS raw-zone",
        3: "Upload Silver + Gold notebooks to Fabric",
        4: "Create 24 CopyJobs + Transform pipeline",
        5: "Trigger CopyJobs  (Parquet → Iceberg on ADLS)",
        6: "Create raw shortcuts  (Lakehouse catalog → ADLS iceberg)",
        7: "Trigger Transform    (Silver → Gold, both on ADLS)",
        8: "Create silver+gold shortcuts  (Power BI Direct Lake access)",
    }

    print("\n" + "═" * 70)
    print("  TPC-DS → Microsoft Fabric  |  Tenant-Controlled ADLS Architecture")
    print("  All data stays in ADLS — Lakehouse used as catalog layer only")
    print("═" * 70)
    for n, label in STAGE_LABELS.items():
        skip = n < args.from_stage or (n == 1 and args.skip_generate)
        tag  = "  SKIP" if skip else "▶ RUN "
        print(f"  Stage {n}: [{tag}]  {label}")
    print("═" * 70 + "\n")

    if args.dry_run:
        return

    notebook_ids = _load_notebook_ids()
    pipeline_ids = _load_pipeline_ids()
    wall_start   = time.time()

    try:
        if args.from_stage <= 1 and not args.skip_generate:
            stage_1_generate()
        if args.from_stage <= 2:
            stage_2_upload()
        if args.from_stage <= 3:
            notebook_ids = stage_3_notebooks()
        if args.from_stage <= 4:
            pipeline_ids = stage_4_pipelines(notebook_ids)
        if args.from_stage <= 5:
            stage_5_copy_jobs()
        if args.from_stage <= 6:
            stage_6_raw_shortcuts(refresh=args.refresh_shortcuts)
        if args.from_stage <= 7:
            xform_id = _resolve_pipeline_id(
                XFORM_PIPELINE_NAME, pipeline_ids, "transform")
            stage_7_transform(xform_id)
        if args.from_stage <= 8:
            stage_8_silver_gold_shortcuts(refresh=args.refresh_shortcuts)

    except Exception as e:
        log.error("Pipeline failed: %s", e)
        raise

    elapsed = (time.time() - wall_start) / 60
    account   = CFG["adls"]["account_name"]
    container = CFG["adls"]["prepared_container"]

    print("\n" + "═" * 70)
    print(f"  ✅  PIPELINE COMPLETE  ({elapsed:.1f} min)")
    print()
    print("  All data in tenant ADLS (CSO compliant):")
    print(f"    abfss://{container}@{account}.dfs.core.windows.net/")
    print(f"      tpcds/iceberg/{{table}}/   ← raw Iceberg (CopyJobs)")
    print(f"      tpcds/silver/silver_{{table}}/  ← Silver Iceberg")
    print(f"      tpcds/gold/gold_{{table}}/       ← Gold Iceberg")
    print()
    print("  Lakehouse shortcuts (catalog only — zero data in OneLake):")
    print("    Tables/dbo/{table}        → ADLS iceberg")
    print("    Tables/dbo/silver_{table} → ADLS silver")
    print("    Tables/dbo/gold_{table}   → ADLS gold")
    print()
    print("  Power BI: Get Data → Fabric → Lakehouse → tpcds_lakehouse")
    print("            Select gold_* → Direct Lake mode")
    print("═" * 70 + "\n")


if __name__ == "__main__":
    main()
