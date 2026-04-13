"""
02_adls_upload/upload_to_adls.py
==================================
Uploads locally generated TPC-DS parquet files to the ADLS Gen2
RAW layer using your personal Azure identity (az login).

Run:
    az login
    python 02_adls_upload/upload_to_adls.py
"""

import sys
import yaml
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from tenacity import retry, stop_after_attempt, wait_exponential

ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

ADLS_CFG = CFG["adls"]
DATA_DIR = Path(CFG["data_generation"]["output_dir"])

CONTAINER = ADLS_CFG["raw_container"]
BASE_PATH = ADLS_CFG["raw_base_path"]

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

TABLES = [
    "date_dim", "time_dim", "customer", "customer_address",
    "customer_demographics", "household_demographics", "income_band",
    "item", "store", "call_center", "catalog_page",
    "web_site", "web_page", "warehouse", "ship_mode", "reason", "promotion",
    "store_sales", "store_returns", "catalog_sales", "catalog_returns",
    "web_sales", "web_returns", "inventory",
]


def get_adls_client() -> DataLakeServiceClient:
    """Use personal identity from `az login` — no SPN needed."""
    credential = AzureCliCredential()
    return DataLakeServiceClient(
        account_url=f"https://{ADLS_CFG['account_name']}.dfs.core.windows.net",
        credential=credential,
    )


def ensure_container(client: DataLakeServiceClient, name: str):
    fs = client.get_file_system_client(name)
    try:
        fs.get_file_system_properties()
        log.info("  Container '%s' already exists.", name)
    except Exception:
        fs.create_file_system()
        log.info("  Created container '%s'.", name)
    return fs


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
def upload_file(fs_client, local_path: Path, adls_path: str, chunk_mb: int = 64) -> int:
    dir_path = "/".join(adls_path.split("/")[:-1])
    try:
        fs_client.get_directory_client(dir_path).create_directory()
    except Exception:
        pass

    file_size  = local_path.stat().st_size
    chunk_size = chunk_mb * 1024 * 1024
    fc = fs_client.get_file_client(adls_path)
    fc.create_file()

    with open(local_path, "rb") as fh:
        offset = 0
        while chunk := fh.read(chunk_size):
            fc.append_data(data=chunk, offset=offset, length=len(chunk))
            offset += len(chunk)
        fc.flush_data(offset)

    log.info("  ✓ %-40s  %6.1f MB", adls_path, file_size / 1024**2)
    return file_size


def upload_table(fs_client, table: str) -> dict:
    table_dir = DATA_DIR / table
    if not table_dir.exists():
        log.warning("  ⚠  %s not found — skipping", table)
        return {"table": table, "status": "skipped", "bytes": 0}

    files = list(table_dir.glob("*.parquet"))
    if not files:
        log.warning("  ⚠  %s — no parquet files — skipping", table)
        return {"table": table, "status": "skipped", "bytes": 0}

    total = 0
    try:
        for f in files:
            total += upload_file(fs_client, f, f"{BASE_PATH}/{table}/{f.name}")
        return {"table": table, "status": "ok", "bytes": total}
    except Exception as e:
        log.error("  ✗  %s FAILED: %s", table, e)
        return {"table": table, "status": "error", "bytes": 0, "error": str(e)}


def main():
    log.info("🚀 Uploading TPC-DS parquet to ADLS Gen2 raw layer")
    log.info("   Account  : %s", ADLS_CFG["account_name"])
    log.info("   Container: %s", CONTAINER)
    log.info("   Auth     : AzureCliCredential (az login)")

    client = get_adls_client()
    fs     = ensure_container(client, CONTAINER)
    ensure_container(client, ADLS_CFG["prepared_container"])

    results     = []
    total_bytes = 0

    log.info("\n[1/2] Dimension tables")
    for t in TABLES[:17]:
        r = upload_table(fs, t)
        results.append(r)
        total_bytes += r["bytes"]

    log.info("\n[2/2] Fact tables (parallel)")
    with ThreadPoolExecutor(max_workers=4) as ex:
        for r in as_completed({ex.submit(upload_table, fs, t): t for t in TABLES[17:]}):
            res = r.result()
            results.append(res)
            total_bytes += res["bytes"]

    ok     = sum(1 for r in results if r["status"] == "ok")
    errors = [r for r in results if r["status"] == "error"]

    print(f"\n{'─'*60}")
    print(f"  ✅ Upload complete — {ok} tables — {total_bytes/1024**3:.2f} GB")
    print(f"  ➡  Next: python 03_fabric_provisioning/create_copy_job.py")

    if errors:
        for r in errors:
            log.error("  %s: %s", r["table"], r.get("error"))
        sys.exit(1)


if __name__ == "__main__":
    main()
