# %% [markdown]
# # Silver Layer — TPC-DS Cleansing & Conforming
#
# ┌─ SECURITY MODEL ─────────────────────────────────────────────────────────┐
# │  Identity  : Workspace MSI — zero secrets in notebook                    │
# │  AuthZ     : Immuta data contract (dc-tpcds-raw-001)                     │
# │              → OneLake security plane (item-level ReadWriteAll)          │
# │              → Purview DLP monitoring                                    │
# │              → Azure Storage RBAC (Storage Blob Data Contributor)        │
# │  No account keys, no SPN secrets, no PATs in this notebook.              │
# │                                                                           │
# │  SOURCE : Lakehouse shortcuts → ADLS prepared-zone Iceberg               │
# │           spark_catalog.<lakehouse>.dbo.<table>                          │
# │           Access resolved via Fabric Connection (Immuta-governed)        │
# │                                                                           │
# │  DEST   : ADLS silver Iceberg (external, explicit LOCATION)              │
# │           Auth via mssparkutils OAuth token (workspace MSI)              │
# │           Immuta contract dc-tpcds-silver-001 governs write entitlement  │
# └───────────────────────────────────────────────────────────────────────────┘

# %% ── %%configure — cell 1 when running interactively ───────────────────────
# %%configure -f
# {
#   "conf": {
#     "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
#     "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#     "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
#     "spark.sql.catalog.iceberg.type": "hadoop",
#     "spark.sql.catalog.iceberg.warehouse": "abfss://prepared-zone@<ADLS_ACCOUNT>.dfs.core.windows.net/tpcds/silver",
#     "spark.sql.defaultCatalog": "iceberg"
#   }
# }

# %% ── Parameters (pipeline baseParameters override at runtime) ───────────────
LAKEHOUSE_ID     = "YOUR_FABRIC_LAKEHOUSE_ID"
WORKSPACE_ID     = "YOUR_FABRIC_WORKSPACE_ID"
LAKEHOUSE_NAME   = "tpcds_lakehouse"
SCHEMA           = "dbo"
ADLS_ACCOUNT     = "YOUR_ADLS_ACCOUNT_NAME"
ADLS_CONTAINER   = "prepared-zone"
ADLS_SILVER_BASE = "tpcds/silver"

# %% ── Attach Lakehouse (required when triggered via pipeline) ────────────────
import notebookutils
notebookutils.lakehouse.attachLakehouse(LAKEHOUSE_ID, default=True)
print(f"✓ Lakehouse attached: {LAKEHOUSE_NAME} ({LAKEHOUSE_ID})")

# %% ── Auth: workspace MSI OAuth token → no secrets in notebook ───────────────
# Credential chain:
#   workspace MSI → Entra ID token (scope: storage)
#   → Immuta validates data contract dc-tpcds-raw-001 (read)
#   → Immuta validates data contract dc-tpcds-silver-001 (write)
#   → Azure Storage RBAC enforced as final layer
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from datetime import datetime

token = mssparkutils.credentials.getToken("storage")
spark.conf.set(
    f"fs.azure.account.auth.type.{ADLS_ACCOUNT}.dfs.core.windows.net",
    "OAuth"
)
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{ADLS_ACCOUNT}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.token.{ADLS_ACCOUNT}.dfs.core.windows.net",
    token
)
print(f"✓ Workspace MSI OAuth token acquired (scope: storage)")

SILVER_ROOT = (
    f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"
    f"/{ADLS_SILVER_BASE}"
)

# %% ── Iceberg catalog ────────────────────────────────────────────────────────
spark.conf.set("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.iceberg",
    "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.iceberg.type",      "hadoop")
spark.conf.set("spark.sql.catalog.iceberg.warehouse",  SILVER_ROOT)
spark.conf.set("spark.sql.shuffle.partitions", "100")
spark.conf.set("spark.sql.adaptive.enabled",   "true")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{SCHEMA}")

# %% ── Verify ADLS access (fail fast before processing) ──────────────────────
try:
    mssparkutils.fs.ls(f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/")
    print(f"✓ ADLS access confirmed via workspace MSI: {ADLS_ACCOUNT}")
except Exception as e:
    raise RuntimeError(
        f"ADLS access denied — verify: "
        f"(1) Immuta data contract dc-tpcds-raw-001 grants workspace MSI read access, "
        f"(2) ADLS RBAC: Storage Blob Data Contributor on prepared-zone, "
        f"(3) Purview DLP policy is not blocking. Error: {e}"
    )

print(f"\nSilver — {datetime.now().isoformat()}")
print(f"Source : spark_catalog.{LAKEHOUSE_NAME}.{SCHEMA}.<table>  [shortcuts, Immuta-governed]")
print(f"Dest   : {SILVER_ROOT}/silver_<table>/  [workspace MSI auth]")

# %% ── Helpers ────────────────────────────────────────────────────────────────

def src(table: str):
    """
    Read via Lakehouse shortcut using spark_catalog.
    Access enforced by:
      Immuta (dc-tpcds-raw-001) → OneLake security plane → Fabric Connection → ADLS RBAC
    No credentials needed here — resolved by Fabric Connection credential chain.
    """
    return spark.read.table(f"spark_catalog.{LAKEHOUSE_NAME}.{SCHEMA}.{table}")


def write_silver(df, table: str) -> int:
    """
    Write silver Iceberg to ADLS via workspace MSI.
    Immuta data contract dc-tpcds-silver-001 must grant write entitlement.
    LOCATION clause ensures data lands in ADLS — never in OneLake.
    """
    target   = f"iceberg.{SCHEMA}.silver_{table}"
    location = f"{SILVER_ROOT}/silver_{table}"

    spark.sql(f"DROP TABLE IF EXISTS {target}")
    (df
     .withColumn("_silver_processed_at", F.current_timestamp())
     .withColumn("_source_layer",        F.lit("silver"))
     .write
     .format("iceberg")
     .option("location", location)
     .mode("overwrite")
     .saveAsTable(target))

    cnt = spark.read.table(target).count()
    print(f"  ✓ silver_{table:38s}  {cnt:>10,} rows")
    return cnt


# %% ── [1] silver_customer ────────────────────────────────────────────────────
print("\n[1] silver_customer")
write_silver(
    src("customer")
    .dropDuplicates(["c_customer_sk"])
    .filter(F.col("c_customer_sk").isNotNull())
    .withColumn("c_full_name",
        F.trim(F.concat_ws(" ", F.initcap("c_first_name"), F.initcap("c_last_name"))))
    .withColumn("c_email_address", F.lower(F.trim("c_email_address")))
    .withColumn("c_birth_year",
        F.when((F.col("c_birth_year") < 1900) | (F.col("c_birth_year") > 2010), None)
         .otherwise(F.col("c_birth_year")))
    .withColumn("c_preferred_cust_flag",
        F.when(F.col("c_preferred_cust_flag").isin(["Y","N"]), F.col("c_preferred_cust_flag"))
         .otherwise(F.lit("N"))),
    "customer"
)

# %% ── [2] silver_customer_address ───────────────────────────────────────────
print("\n[2] silver_customer_address")
VALID_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC",
]
write_silver(
    src("customer_address")
    .dropDuplicates(["ca_address_sk"])
    .filter(F.col("ca_address_sk").isNotNull())
    .withColumn("ca_state",
        F.when(F.col("ca_state").isin(VALID_STATES), F.upper("ca_state"))
         .otherwise(F.lit("UNK")))
    .withColumn("ca_zip",
        F.when(F.length("ca_zip") == 5, F.col("ca_zip")).otherwise(None))
    .withColumn("ca_country", F.initcap(F.trim("ca_country"))),
    "customer_address"
)

# %% ── [3] silver_item ────────────────────────────────────────────────────────
print("\n[3] silver_item")
write_silver(
    src("item")
    .dropDuplicates(["i_item_sk"])
    .filter(F.col("i_item_sk").isNotNull())
    .withColumn("i_current_price",
        F.when(F.col("i_current_price") <= 0, None)
         .otherwise(F.col("i_current_price").cast(DecimalType(10,2))))
    .withColumn("i_wholesale_cost",
        F.when(F.col("i_wholesale_cost") <= 0, None)
         .otherwise(F.col("i_wholesale_cost").cast(DecimalType(10,2))))
    .withColumn("i_margin_pct",
        F.when(
            F.col("i_current_price").isNotNull() &
            F.col("i_wholesale_cost").isNotNull() &
            (F.col("i_wholesale_cost") > 0),
            F.round((F.col("i_current_price") - F.col("i_wholesale_cost")) /
                     F.col("i_current_price") * 100, 2))
         .otherwise(None))
    .withColumn("i_category", F.initcap(F.trim("i_category")))
    .withColumn("i_brand",    F.initcap(F.trim("i_brand"))),
    "item"
)

# %% ── [4] silver_store_sales ─────────────────────────────────────────────────
print("\n[4] silver_store_sales")
write_silver(
    src("store_sales")
    .filter(F.col("ss_item_sk").isNotNull() & F.col("ss_ticket_number").isNotNull())
    .withColumn("ss_quantity",
        F.when(F.col("ss_quantity") <= 0, None).otherwise(F.col("ss_quantity")))
    .withColumn("ss_sales_price",     F.col("ss_sales_price").cast(DecimalType(12,2)))
    .withColumn("ss_net_profit",      F.col("ss_net_profit").cast(DecimalType(12,2)))
    .withColumn("ss_ext_sales_price", F.col("ss_ext_sales_price").cast(DecimalType(14,2)))
    .withColumn("ss_discount_pct",
        F.when(F.col("ss_list_price") > 0,
               F.round((F.col("ss_list_price") - F.col("ss_sales_price")) /
                        F.col("ss_list_price") * 100, 2))
         .otherwise(F.lit(0.0))),
    "store_sales"
)

# %% ── [5] silver_catalog_sales ───────────────────────────────────────────────
print("\n[5] silver_catalog_sales")
write_silver(
    src("catalog_sales")
    .filter(F.col("cs_item_sk").isNotNull() & F.col("cs_order_number").isNotNull())
    .withColumn("cs_sales_price",     F.col("cs_sales_price").cast(DecimalType(12,2)))
    .withColumn("cs_net_profit",      F.col("cs_net_profit").cast(DecimalType(12,2)))
    .withColumn("cs_ext_sales_price", F.col("cs_ext_sales_price").cast(DecimalType(14,2)))
    .withColumn("cs_discount_pct",
        F.when(F.col("cs_list_price") > 0,
               F.round((F.col("cs_list_price") - F.col("cs_sales_price")) /
                        F.col("cs_list_price") * 100, 2))
         .otherwise(F.lit(0.0))),
    "catalog_sales"
)

# %% ── [6] silver_web_sales ───────────────────────────────────────────────────
print("\n[6] silver_web_sales")
write_silver(
    src("web_sales")
    .filter(F.col("ws_item_sk").isNotNull() & F.col("ws_order_number").isNotNull())
    .withColumn("ws_sales_price",     F.col("ws_sales_price").cast(DecimalType(12,2)))
    .withColumn("ws_net_profit",      F.col("ws_net_profit").cast(DecimalType(12,2)))
    .withColumn("ws_ext_sales_price", F.col("ws_ext_sales_price").cast(DecimalType(14,2))),
    "web_sales"
)

# %% ── [7] silver_store_returns ───────────────────────────────────────────────
print("\n[7] silver_store_returns")
write_silver(
    src("store_returns")
    .dropDuplicates(["sr_ticket_number","sr_item_sk"])
    .filter(F.col("sr_item_sk").isNotNull())
    .withColumn("sr_return_amt", F.col("sr_return_amt").cast(DecimalType(12,2)))
    .withColumn("sr_net_loss",   F.col("sr_net_loss").cast(DecimalType(12,2))),
    "store_returns"
)

# %% ── [8] silver_catalog_returns ─────────────────────────────────────────────
print("\n[8] silver_catalog_returns")
write_silver(
    src("catalog_returns")
    .dropDuplicates(["cr_order_number","cr_item_sk"])
    .filter(F.col("cr_item_sk").isNotNull())
    .withColumn("cr_return_amount", F.col("cr_return_amount").cast(DecimalType(12,2))),
    "catalog_returns"
)

# %% ── [9] silver_web_returns ─────────────────────────────────────────────────
print("\n[9] silver_web_returns")
write_silver(
    src("web_returns")
    .dropDuplicates(["wr_order_number","wr_item_sk"])
    .filter(F.col("wr_item_sk").isNotNull())
    .withColumn("wr_return_amt", F.col("wr_return_amt").cast(DecimalType(12,2))),
    "web_returns"
)

# %% ── [10] silver_date_dim ───────────────────────────────────────────────────
print("\n[10] silver_date_dim")
write_silver(
    src("date_dim")
    .dropDuplicates(["d_date_sk"])
    .filter(F.col("d_date_sk").isNotNull())
    .withColumn("d_is_weekend", F.col("d_weekend") == "Y")
    .withColumn("d_is_holiday", F.col("d_holiday") == "Y")
    .withColumn("d_season",
        F.when(F.col("d_moy").isin([12,1,2]),  F.lit("Winter"))
         .when(F.col("d_moy").isin([3,4,5]),   F.lit("Spring"))
         .when(F.col("d_moy").isin([6,7,8]),   F.lit("Summer"))
         .otherwise(F.lit("Fall")))
    .withColumn("d_half_year",
        F.when(F.col("d_moy") <= 6, F.lit("H1")).otherwise(F.lit("H2"))),
    "date_dim"
)

# %% ── [11] Pass-through dimensions ──────────────────────────────────────────
print("\n[11] Pass-through dimensions")
for t in [
    "store", "warehouse", "ship_mode", "call_center", "catalog_page",
    "web_site", "web_page", "promotion", "reason", "income_band",
    "household_demographics", "customer_demographics", "inventory",
]:
    try:
        write_silver(src(t), t)
    except Exception as e:
        print(f"  ⚠  {t}: {e}")

print(f"\n{'='*65}")
print(f"✅ Silver complete — {datetime.now().isoformat()}")
print(f"   Auth    : workspace MSI → Immuta → OneLake → ADLS RBAC")
print(f"   Written : {SILVER_ROOT}/silver_<table>/")
print(f"   Next    : create shortcuts silver_* then trigger Gold")
