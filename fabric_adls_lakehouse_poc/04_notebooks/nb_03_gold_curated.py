# %% [markdown]
# # Gold Layer — TPC-DS Curated Models
#
# ┌─ SECURITY MODEL ─────────────────────────────────────────────────────────┐
# │  Identity  : Workspace MSI — zero secrets in notebook                    │
# │  AuthZ     : Immuta data contract (dc-tpcds-silver-001 read,             │
# │              dc-tpcds-gold-001 write)                                    │
# │              → OneLake security plane (item-level ReadWriteAll)          │
# │              → Purview DLP monitoring (sensitivity: Internal)            │
# │              → Azure Storage RBAC (Blob Data Contributor, final layer)   │
# │  Posture   : Wiz scanning ADLS + Fabric (central team, agentless)        │
# │                                                                           │
# │  SOURCE : Silver Iceberg on ADLS — read via workspace MSI               │
# │  DEST   : Gold Iceberg on ADLS — write via workspace MSI                │
# │  Gold tables exposed to Power BI via Lakehouse shortcuts (read-only)     │
# └───────────────────────────────────────────────────────────────────────────┘

# %% ── %%configure — cell 1 when running interactively ───────────────────────
# %%configure -f
# {
#   "conf": {
#     "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
#     "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
#     "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
#     "spark.sql.catalog.iceberg.type": "hadoop",
#     "spark.sql.catalog.iceberg.warehouse": "abfss://prepared-zone@<ADLS_ACCOUNT>.dfs.core.windows.net/tpcds/gold",
#     "spark.sql.defaultCatalog": "iceberg"
#   }
# }

# %% ── Parameters ─────────────────────────────────────────────────────────────
LAKEHOUSE_ID     = "YOUR_FABRIC_LAKEHOUSE_ID"
WORKSPACE_ID     = "YOUR_FABRIC_WORKSPACE_ID"
LAKEHOUSE_NAME   = "tpcds_lakehouse"
SCHEMA           = "dbo"
ADLS_ACCOUNT     = "YOUR_ADLS_ACCOUNT_NAME"
ADLS_CONTAINER   = "prepared-zone"
ADLS_SILVER_BASE = "tpcds/silver"
ADLS_GOLD_BASE   = "tpcds/gold"

# %% ── Attach Lakehouse ───────────────────────────────────────────────────────
import notebookutils
notebookutils.lakehouse.attachLakehouse(LAKEHOUSE_ID, default=True)
print(f"✓ Lakehouse attached: {LAKEHOUSE_NAME} ({LAKEHOUSE_ID})")

# %% ── Auth: workspace MSI OAuth ──────────────────────────────────────────────
# Credential chain:
#   workspace MSI → Entra ID token (scope: storage)
#   → Immuta validates dc-tpcds-silver-001 (read) + dc-tpcds-gold-001 (write)
#   → OneLake security plane enforces item-level role (ReadWriteAll on Lakehouse)
#   → Purview DLP monitors data movement
#   → Azure Storage RBAC (Blob Data Contributor) as final enforcement layer
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window
from datetime import datetime

token = mssparkutils.credentials.getToken("storage")
spark.conf.set(
    f"fs.azure.account.auth.type.{ADLS_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{ADLS_ACCOUNT}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(
    f"fs.azure.account.oauth2.client.token.{ADLS_ACCOUNT}.dfs.core.windows.net", token)
print("✓ Workspace MSI OAuth token acquired")

SILVER_ROOT = (
    f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{ADLS_SILVER_BASE}"
)
GOLD_ROOT = (
    f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{ADLS_GOLD_BASE}"
)

# %% ── Iceberg catalog ────────────────────────────────────────────────────────
spark.conf.set("spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
spark.conf.set("spark.sql.catalog.iceberg",
    "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.iceberg.type",      "hadoop")
spark.conf.set("spark.sql.catalog.iceberg.warehouse",  GOLD_ROOT)
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled",   "true")

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{SCHEMA}")

print(f"\nGold — {datetime.now().isoformat()}")
print(f"Silver : {SILVER_ROOT}/silver_<table>/")
print(f"Gold   : {GOLD_ROOT}/gold_<table>/")

# %% ── Helpers ────────────────────────────────────────────────────────────────

def read_silver(table: str):
    """
    Read silver Iceberg from ADLS by abfss path.
    Requires Immuta dc-tpcds-silver-001 read entitlement on workspace MSI.
    """
    return spark.read.format("iceberg").load(f"{SILVER_ROOT}/silver_{table}")


def write_gold(df, table: str) -> int:
    """
    Write gold Iceberg to ADLS via workspace MSI.
    LOCATION clause ensures data stays in tenant ADLS, never in OneLake.
    Requires Immuta dc-tpcds-gold-001 write entitlement on workspace MSI.
    """
    target   = f"iceberg.{SCHEMA}.gold_{table}"
    location = f"{GOLD_ROOT}/gold_{table}"

    spark.sql(f"DROP TABLE IF EXISTS {target}")
    (df
     .withColumn("_gold_processed_at", F.current_timestamp())
     .withColumn("_source_layer",      F.lit("gold"))
     .write
     .format("iceberg")
     .option("location", location)
     .mode("overwrite")
     .saveAsTable(target))

    cnt = spark.read.table(target).count()
    print(f"  ✓ gold_{table:42s}  {cnt:>10,} rows")
    return cnt


# %% ── Load silver ────────────────────────────────────────────────────────────
print("\nLoading silver tables...")
ss   = read_silver("store_sales")
cs   = read_silver("catalog_sales")
ws   = read_silver("web_sales")
sr   = read_silver("store_returns")
cr   = read_silver("catalog_returns")
wr   = read_silver("web_returns")
dd   = read_silver("date_dim")
cust = read_silver("customer")
addr = read_silver("customer_address")
item = read_silver("item")
print("✓ Silver loaded")

# %% ── GOLD 1: gold_sales_fact ────────────────────────────────────────────────
print("\n[GOLD 1] gold_sales_fact — unified cross-channel sales + returns")

sr_agg = sr.groupBy("sr_ticket_number","sr_item_sk").agg(
    F.sum("sr_return_amt").alias("return_amt"),
    F.sum("sr_return_quantity").alias("return_qty"),
    F.sum("sr_net_loss").alias("return_net_loss"),
)
store_ch = (
    ss.join(sr_agg,
            (ss.ss_ticket_number == sr_agg.sr_ticket_number) &
            (ss.ss_item_sk       == sr_agg.sr_item_sk), "left")
    .select(
        F.lit("STORE").alias("channel"),
        ss.ss_sold_date_sk.alias("sold_date_sk"),
        ss.ss_item_sk.alias("item_sk"),
        ss.ss_customer_sk.alias("customer_sk"),
        ss.ss_store_sk.alias("location_sk"),
        ss.ss_ticket_number.alias("transaction_id"),
        ss.ss_quantity.alias("quantity"),
        ss.ss_sales_price.alias("unit_price"),
        ss.ss_ext_sales_price.alias("gross_sales_amt"),
        ss.ss_ext_discount_amt.alias("discount_amt"),
        ss.ss_ext_tax.alias("tax_amt"),
        ss.ss_net_profit.alias("net_profit"),
        ss.ss_coupon_amt.alias("coupon_amt"),
        ss.ss_discount_pct.alias("discount_pct"),
        F.coalesce(sr_agg.return_amt, F.lit(0)).alias("return_amt"),
        F.coalesce(sr_agg.return_qty, F.lit(0)).alias("return_qty"),
        (ss.ss_ext_sales_price - F.coalesce(sr_agg.return_amt, F.lit(0))).alias("net_sales_amt"),
    )
)

cr_agg = cr.groupBy("cr_order_number","cr_item_sk").agg(
    F.sum("cr_return_amount").alias("return_amt"),
    F.sum("cr_return_quantity").alias("return_qty"),
    F.sum("cr_net_loss").alias("return_net_loss"),
)
catalog_ch = (
    cs.join(cr_agg,
            (cs.cs_order_number == cr_agg.cr_order_number) &
            (cs.cs_item_sk      == cr_agg.cr_item_sk), "left")
    .select(
        F.lit("CATALOG").alias("channel"),
        cs.cs_sold_date_sk.alias("sold_date_sk"),
        cs.cs_item_sk.alias("item_sk"),
        cs.cs_bill_customer_sk.alias("customer_sk"),
        cs.cs_call_center_sk.alias("location_sk"),
        cs.cs_order_number.alias("transaction_id"),
        cs.cs_quantity.alias("quantity"),
        cs.cs_sales_price.alias("unit_price"),
        cs.cs_ext_sales_price.alias("gross_sales_amt"),
        cs.cs_ext_discount_amt.alias("discount_amt"),
        cs.cs_ext_tax.alias("tax_amt"),
        cs.cs_net_profit.alias("net_profit"),
        cs.cs_coupon_amt.alias("coupon_amt"),
        cs.cs_discount_pct.alias("discount_pct"),
        F.coalesce(cr_agg.return_amt, F.lit(0)).alias("return_amt"),
        F.coalesce(cr_agg.return_qty, F.lit(0)).alias("return_qty"),
        (cs.cs_ext_sales_price - F.coalesce(cr_agg.return_amt, F.lit(0))).alias("net_sales_amt"),
    )
)

wr_agg = wr.groupBy("wr_order_number","wr_item_sk").agg(
    F.sum("wr_return_amt").alias("return_amt"),
    F.sum("wr_return_quantity").alias("return_qty"),
    F.sum("wr_net_loss").alias("return_net_loss"),
)
web_ch = (
    ws.join(wr_agg,
            (ws.ws_order_number == wr_agg.wr_order_number) &
            (ws.ws_item_sk      == wr_agg.wr_item_sk), "left")
    .select(
        F.lit("WEB").alias("channel"),
        ws.ws_sold_date_sk.alias("sold_date_sk"),
        ws.ws_item_sk.alias("item_sk"),
        ws.ws_bill_customer_sk.alias("customer_sk"),
        ws.ws_web_site_sk.alias("location_sk"),
        ws.ws_order_number.alias("transaction_id"),
        ws.ws_quantity.alias("quantity"),
        ws.ws_sales_price.alias("unit_price"),
        ws.ws_ext_sales_price.alias("gross_sales_amt"),
        ws.ws_ext_discount_amt.alias("discount_amt"),
        ws.ws_ext_tax.alias("tax_amt"),
        ws.ws_net_profit.alias("net_profit"),
        F.lit(0).cast(DecimalType(12,2)).alias("coupon_amt"),
        F.lit(0.0).alias("discount_pct"),
        F.coalesce(wr_agg.return_amt, F.lit(0)).alias("return_amt"),
        F.coalesce(wr_agg.return_qty, F.lit(0)).alias("return_qty"),
        (ws.ws_ext_sales_price - F.coalesce(wr_agg.return_amt, F.lit(0))).alias("net_sales_amt"),
    )
)

date_lite = dd.select(
    "d_date_sk","d_date","d_year","d_moy","d_dom","d_qoy",
    "d_quarter_name","d_day_name","d_is_weekend","d_is_holiday","d_season","d_half_year",
)
item_lite = item.select(
    "i_item_sk","i_item_id","i_product_name","i_category","i_brand",
    "i_class","i_current_price","i_wholesale_cost","i_margin_pct","i_color","i_size","i_units",
)

write_gold(
    store_ch.union(catalog_ch).union(web_ch)
    .join(date_lite, F.col("sold_date_sk") == date_lite.d_date_sk, "left")
    .join(item_lite, F.col("item_sk")       == item_lite.i_item_sk, "left")
    .withColumn("return_rate_pct",
        F.when(F.col("quantity") > 0,
               F.round(F.col("return_qty") / F.col("quantity") * 100, 2))
         .otherwise(F.lit(0.0)))
    .withColumn("is_returned", F.col("return_qty") > 0),
    "sales_fact"
)

# %% ── GOLD 2: gold_customer_360 ──────────────────────────────────────────────
print("\n[GOLD 2] gold_customer_360 — LTV + RFM segmentation")

gsf           = spark.read.format("iceberg").load(f"{GOLD_ROOT}/gold_sales_fact")
ANALYSIS_DATE = "2003-12-31"

cust_agg = (
    gsf.filter(F.col("customer_sk").isNotNull())
    .groupBy("customer_sk")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("gross_sales_amt").alias("lifetime_gross_sales"),
        F.sum("net_sales_amt").alias("lifetime_net_sales"),
        F.sum("return_amt").alias("lifetime_returns"),
        F.sum("net_profit").alias("lifetime_net_profit"),
        F.avg("gross_sales_amt").alias("avg_transaction_value"),
        F.max("d_date").alias("last_purchase_date"),
        F.min("d_date").alias("first_purchase_date"),
        F.countDistinct("d_date").alias("unique_purchase_days"),
        F.sum(F.when(F.col("channel")=="STORE",   F.col("gross_sales_amt"))).alias("store_sales_amt"),
        F.sum(F.when(F.col("channel")=="CATALOG", F.col("gross_sales_amt"))).alias("catalog_sales_amt"),
        F.sum(F.when(F.col("channel")=="WEB",     F.col("gross_sales_amt"))).alias("web_sales_amt"),
        F.sum(F.when(F.col("is_returned"), F.lit(1)).otherwise(F.lit(0))).alias("total_returned_txns"),
    )
    .withColumn("days_since_last_purchase",
        F.datediff(F.lit(ANALYSIS_DATE).cast("date"), F.col("last_purchase_date")))
)

write_gold(
    cust_agg
    .withColumn("recency_score",   F.ntile(5).over(Window.orderBy("days_since_last_purchase")))
    .withColumn("frequency_score", F.ntile(5).over(Window.orderBy(F.col("total_transactions").desc())))
    .withColumn("monetary_score",  F.ntile(5).over(Window.orderBy(F.col("lifetime_net_sales").desc())))
    .withColumn("rfm_score",
        F.col("recency_score") + F.col("frequency_score") + F.col("monetary_score"))
    .withColumn("rfm_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
         .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
         .when(F.col("rfm_score") >= 8,  F.lit("Potential Loyalists"))
         .when(F.col("rfm_score") >= 6,  F.lit("At Risk"))
         .otherwise(F.lit("Churned")))
    .withColumn("preferred_channel",
        F.when((F.col("store_sales_amt")  >= F.col("catalog_sales_amt")) &
               (F.col("store_sales_amt")  >= F.col("web_sales_amt")),   F.lit("STORE"))
         .when(F.col("catalog_sales_amt") >= F.col("web_sales_amt"),    F.lit("CATALOG"))
         .otherwise(F.lit("WEB")))
    .withColumn("return_rate_pct",
        F.when(F.col("total_transactions") > 0,
               F.round(F.col("total_returned_txns") / F.col("total_transactions") * 100, 2))
         .otherwise(F.lit(0.0)))
    .join(cust.select("c_customer_sk","c_full_name","c_email_address","c_birth_year",
                       "c_birth_country","c_preferred_cust_flag","c_current_addr_sk","c_salutation"),
          cust_agg.customer_sk == cust["c_customer_sk"], "left")
    .join(addr.select("ca_address_sk","ca_city","ca_state","ca_zip","ca_country"),
          cust["c_current_addr_sk"] == addr["ca_address_sk"], "left"),
    "customer_360"
)

# %% ── GOLD 3: gold_product_performance ───────────────────────────────────────
print("\n[GOLD 3] gold_product_performance — margin, velocity, return rate")

write_gold(
    gsf.filter(F.col("item_sk").isNotNull())
    .groupBy("item_sk","i_item_id","i_product_name","i_category","i_brand",
             "i_class","i_current_price","i_wholesale_cost","i_margin_pct","i_color","i_size")
    .agg(
        F.sum("gross_sales_amt").alias("total_gross_sales"),
        F.sum("net_sales_amt").alias("total_net_sales"),
        F.sum("net_profit").alias("total_net_profit"),
        F.sum("quantity").alias("total_units_sold"),
        F.count("*").alias("num_transactions"),
        F.sum("return_amt").alias("total_returns"),
        F.sum("return_qty").alias("total_return_units"),
        F.avg("unit_price").alias("avg_selling_price"),
        F.avg("discount_pct").alias("avg_discount_pct"),
        F.countDistinct("customer_sk").alias("unique_customers"),
        F.sum(F.when(F.col("channel")=="STORE",   F.col("gross_sales_amt"))).alias("store_revenue"),
        F.sum(F.when(F.col("channel")=="CATALOG", F.col("gross_sales_amt"))).alias("catalog_revenue"),
        F.sum(F.when(F.col("channel")=="WEB",     F.col("gross_sales_amt"))).alias("web_revenue"),
        F.countDistinct("d_date").alias("active_selling_days"),
    )
    .withColumn("return_rate_pct",
        F.when(F.col("total_units_sold") > 0,
               F.round(F.col("total_return_units") / F.col("total_units_sold") * 100, 2))
         .otherwise(F.lit(0.0)))
    .withColumn("daily_sales_velocity",
        F.when(F.col("active_selling_days") > 0,
               F.round(F.col("total_units_sold") / F.col("active_selling_days"), 2))
         .otherwise(F.lit(0.0)))
    .withColumn("margin_tier",
        F.when(F.col("i_margin_pct") >= 50, F.lit("Premium"))
         .when(F.col("i_margin_pct") >= 30, F.lit("Standard"))
         .when(F.col("i_margin_pct") >= 10, F.lit("Low Margin"))
         .otherwise(F.lit("Loss Leader")))
    .withColumn("top_channel",
        F.when((F.col("store_revenue")  >= F.col("catalog_revenue")) &
               (F.col("store_revenue")  >= F.col("web_revenue")),  F.lit("STORE"))
         .when(F.col("catalog_revenue") >= F.col("web_revenue"),   F.lit("CATALOG"))
         .otherwise(F.lit("WEB")))
    .withColumn("revenue_rank",
        F.dense_rank().over(Window.orderBy(F.col("total_net_sales").desc())))
    .withColumn("revenue_pct_of_total",
        F.round(F.col("total_net_sales") /
                F.sum("total_net_sales").over(Window.partitionBy()) * 100, 4)),
    "product_performance"
)

# %% ── Validation ─────────────────────────────────────────────────────────────
print("\n── Validation: gold_sales_fact by channel")
spark.read.format("iceberg").load(f"{GOLD_ROOT}/gold_sales_fact") \
    .groupBy("channel").agg(
        F.format_number(F.count("*"), 0).alias("transactions"),
        F.concat(F.lit("$"), F.format_number(F.sum("gross_sales_amt")/1e6, 2),
                 F.lit("M")).alias("gross_sales"),
        F.round(F.avg("return_rate_pct"), 2).alias("avg_return_pct"),
    ).orderBy(F.sum("gross_sales_amt").desc()).show()

print("── Validation: gold_customer_360 by RFM segment")
spark.read.format("iceberg").load(f"{GOLD_ROOT}/gold_customer_360") \
    .groupBy("rfm_segment").agg(
        F.format_number(F.count("*"), 0).alias("customers"),
        F.round(F.avg("lifetime_net_sales"), 2).alias("avg_ltv"),
    ).orderBy(F.avg("lifetime_net_sales").desc()).show()

print(f"\n{'='*65}")
print(f"✅ Gold complete — {datetime.now().isoformat()}")
print(f"   Auth    : workspace MSI → Immuta → OneLake → ADLS RBAC")
print(f"   Written : {GOLD_ROOT}/gold_<table>/")
print(f"   Next    : create shortcuts gold_* → Power BI Direct Lake")
