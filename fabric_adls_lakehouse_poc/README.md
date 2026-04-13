# TPC-DS → Microsoft Fabric Pipeline

## Security Architecture

All data is stored in **tenant-controlled ADLS Gen2** (CSO compliant).
The Fabric Lakehouse acts purely as a **catalog layer** — no data is
copied to or stored in OneLake.

```
ADLS Gen2  (tenant subscription — CSO controlled)
  prepared-zone/
    tpcds/iceberg/{table}/          ← raw Iceberg    (Stage 5 — CopyJobs)
    tpcds/silver/silver_{table}/    ← Silver Iceberg  (Stage 7 — Silver notebook)
    tpcds/gold/gold_{table}/        ← Gold Iceberg    (Stage 7 — Gold notebook)

Fabric Lakehouse  (catalog + compute — metadata only, zero data)
  Tables/dbo/
    {table}              → shortcut → ADLS iceberg/{table}/
    silver_{table}       → shortcut → ADLS silver/silver_{table}/
    gold_{table}         → shortcut → ADLS gold/gold_{table}/

Power BI Direct Lake
  Reads gold_* via Lakehouse shortcuts → hits ADLS directly
```

---

## Prerequisites

| Requirement | Detail |
|---|---|
| `az login` | Personal Azure identity — no SPN needed |
| Python 3.11+ | `pip install -r requirements.txt` |
| ADLS Gen2 | `raw-zone` and `prepared-zone` containers created |
| Fabric Lakehouse | Named `tpcds_lakehouse` with schema `dbo` created via UI |
| Fabric Connections | Two ADLS Gen2 connections created in Fabric portal (Manage connections) |

---

## Setup

### 1. Fill in `config.yaml`

```yaml
adls:
  account_name:        "yourstorageaccount"
  account_key:         "your-storage-account-key"   # Azure portal → Access keys
  raw_connection_id:      "guid-from-fabric-portal"
  prepared_connection_id: "guid-from-fabric-portal"

fabric:
  workspace_id:   "your-workspace-guid"
  lakehouse_id:   "your-lakehouse-guid"
  lakehouse_name: "tpcds_lakehouse"
  schema:         "dbo"                    # must be created in Lakehouse UI first
```

### 2. Run

```bash
# Full pipeline (Stages 1-8)
python 06_orchestration/run_all.py

# Resume from stage 3 (ADLS data already uploaded)
python 06_orchestration/run_all.py --from-stage 3

# Re-run transform + refresh shortcuts only
python 06_orchestration/run_all.py --from-stage 7 --refresh-shortcuts

# Re-create silver+gold shortcuts only (after re-running notebooks manually)
python 06_orchestration/run_all.py --from-stage 8 --refresh-shortcuts

# Preview without running
python 06_orchestration/run_all.py --dry-run
```

---

## Stage Reference

| Stage | What | Output |
|---|---|---|
| 1 | Generate TPC-DS synthetic parquet | `./data/tpcds/*.parquet` |
| 2 | Upload to ADLS raw-zone | `raw-zone/tpcds/tables/{table}/` |
| 3 | Upload notebooks to Fabric | Notebook IDs saved to `notebook_ids.json` |
| 4 | Create 24 CopyJobs + Transform pipeline | IDs saved to `copy_job_ids.json` |
| 5 | Trigger 24 CopyJobs in parallel | `prepared-zone/tpcds/iceberg/{table}/` |
| 6 | Create raw shortcuts | `Tables/dbo/{table}` → ADLS iceberg |
| 7 | Trigger Transform pipeline | `tpcds/silver/` + `tpcds/gold/` on ADLS |
| 8 | Create silver + gold shortcuts | `Tables/dbo/silver_*` + `gold_*` visible in Lakehouse |

---

## Notebook Design

### Silver (`nb_02_silver_transform.py`)
- **Reads** via `spark.read.table("spark_catalog.tpcds_lakehouse.dbo.<table>")` — Lakehouse shortcuts, no ADLS credentials needed for reads
- **Writes** via `saveAsTable("iceberg.dbo.silver_<table>")` with explicit `LOCATION` on ADLS — data never enters OneLake
- Uses `%%configure` to set Iceberg catalog with ADLS warehouse
- `notebookutils.lakehouse.attachLakehouse()` called at start for pipeline-triggered runs

### Gold (`nb_03_gold_curated.py`)
- **Reads** silver directly from ADLS via `abfss://` path (no shortcut needed)
- **Writes** gold to ADLS via `saveAsTable("iceberg.dbo.gold_<table>")` with explicit `LOCATION`
- Produces 3 models: `gold_sales_fact`, `gold_customer_360`, `gold_product_performance`

### Key Spark config pattern
```python
%%configure -f
{
  "conf": {
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type": "hadoop",
    "spark.sql.catalog.iceberg.warehouse": "abfss://prepared-zone@<account>.dfs.core.windows.net/tpcds/silver",
    "spark.sql.defaultCatalog": "iceberg"
  }
}
```

---

## Shortcuts

```bash
# Create/update shortcuts by layer
python 03_fabric_provisioning/create_shortcuts.py --layer raw
python 03_fabric_provisioning/create_shortcuts.py --layer silver
python 03_fabric_provisioning/create_shortcuts.py --layer gold
python 03_fabric_provisioning/create_shortcuts.py --layer all --refresh
```

Shortcut path structure:
```
Lakehouse Tables/dbo/{name}  →  ADLS /{container}/{base}/{name}/
```
The `path` field is the **parent** (`Tables/dbo`), `name` is the **leaf** — Fabric Shortcuts API splits them this way.

---

## Power BI

1. Get Data → Microsoft Fabric → Lakehouse → `tpcds_lakehouse`
2. Select `gold_sales_fact`, `gold_customer_360`, `gold_product_performance`
3. Connection mode: **Direct Lake**
4. Data is read directly from tenant ADLS via shortcuts — no data import into Power BI

---

## File Structure

```
tpcds-fabric-pipeline/
├── config.yaml                              ← fill in your IDs/keys
├── requirements.txt
├── 01_data_generation/
│   └── generate_tpcds.py                    ← synthetic TPC-DS data
├── 02_adls_upload/
│   └── upload_to_adls.py                    ← upload to raw-zone
├── 03_fabric_provisioning/
│   ├── fabric_auth.py                       ← AzureCliCredential + retry
│   ├── upload_notebooks.py                  ← deploy notebooks to Fabric
│   ├── create_copy_job.py                   ← create 24 CopyJobs + pipeline
│   └── create_shortcuts.py                  ← register ADLS shortcuts (all 3 layers)
├── 04_notebooks/
│   ├── nb_02_silver_transform.py            ← PySpark silver notebook
│   └── nb_03_gold_curated.py               ← PySpark gold notebook
├── 05_pipeline/
│   └── transform_pipeline.json             ← generated by stage 4
└── 06_orchestration/
    └── run_all.py                           ← 8-stage orchestrator
```
