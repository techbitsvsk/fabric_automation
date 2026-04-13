"""
01_data_generation/generate_tpcds.py
=====================================
Generates synthetic TPC-DS (~5 GB) parquet data locally.
Covers all 24 TPC-DS tables with proper FK relationships.

Run:
    pip install -r requirements.txt
    python 01_data_generation/generate_tpcds.py
"""

import os
import sys
import yaml
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import date, timedelta
from tqdm import tqdm

# ─── Load config ────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
with open(ROOT / "config.yaml") as f:
    CFG = yaml.safe_load(f)

OUTPUT_DIR  = Path(CFG["data_generation"]["output_dir"])
CHUNK       = CFG["data_generation"]["chunk_size"]
RNG         = np.random.default_rng(CFG["data_generation"]["seed"])

# ─── Scale factors (rows) targeting ~5 GB parquet ───────────────────────────
# Parquet compression ~5-8x; figures below produce ~5 GB total on disk.
SCALE = {
    # Dimension tables
    "date_dim":              73_049,
    "time_dim":              86_400,
    "customer":             100_000,
    "customer_address":     100_000,
    "customer_demographics":1_920_800,
    "household_demographics":  7_200,
    "income_band":               20,
    "item":                  18_000,
    "store":                    402,
    "call_center":               30,
    "catalog_page":          11_718,
    "web_site":                  24,
    "web_page":               2_040,
    "warehouse":                 15,
    "ship_mode":                 20,
    "reason":                    35,
    "promotion":                300,
    # Fact tables
    "store_sales":        5_000_000,
    "store_returns":        500_000,
    "catalog_sales":      1_500_000,
    "catalog_returns":      145_000,
    "web_sales":            720_000,
    "web_returns":           70_000,
    "inventory":            260_000,
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

def save_parquet(df: pd.DataFrame, table_name: str) -> Path:
    out = OUTPUT_DIR / table_name
    out.mkdir(parents=True, exist_ok=True)
    path = out / "data.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path,
                   compression="snappy")
    mb = path.stat().st_size / 1024**2
    print(f"  ✓ {table_name:35s} {len(df):>10,} rows  {mb:7.1f} MB")
    return path


def rand_str(prefix: str, ids: np.ndarray) -> list:
    return [f"{prefix}_{i}" for i in ids]


def rand_date_range(n: int, start="1998-01-01", end="2003-12-31") -> np.ndarray:
    s = pd.Timestamp(start).value // 10**9
    e = pd.Timestamp(end).value   // 10**9
    return pd.to_datetime(RNG.integers(s, e, n), unit="s").date


def nullable_int(arr: np.ndarray, null_frac=0.05) -> pd.array:
    mask = RNG.random(len(arr)) < null_frac
    result = pd.array(arr.astype(object), dtype="object")
    result[mask] = None
    return pd.array(result, dtype="Int64")


# ─── Dimension table generators ──────────────────────────────────────────────

def gen_date_dim(n):
    base = date(1998, 1, 1)
    dates = [base + timedelta(days=i) for i in range(n)]
    df = pd.DataFrame({
        "d_date_sk":        np.arange(2415022, 2415022 + n, dtype="int32"),
        "d_date_id":        [d.strftime("AAAAAA%Y%m%d") for d in dates],
        "d_date":           dates,
        "d_month_seq":      [(d.year - 1900) * 12 + d.month for d in dates],
        "d_week_seq":       [(d.timetuple().tm_yday // 7) + 1 for d in dates],
        "d_quarter_seq":    [((d.month - 1) // 3) + 1 + (d.year - 1998) * 4 for d in dates],
        "d_year":           [d.year for d in dates],
        "d_dow":            [d.weekday() for d in dates],
        "d_moy":            [d.month for d in dates],
        "d_dom":            [d.day for d in dates],
        "d_qoy":            [((d.month - 1) // 3) + 1 for d in dates],
        "d_fy_year":        [d.year for d in dates],
        "d_fy_quarter_seq": [((d.month - 1) // 3) + 1 for d in dates],
        "d_fy_week_seq":    [(d.timetuple().tm_yday // 7) + 1 for d in dates],
        "d_day_name":       [d.strftime("%A") for d in dates],
        "d_quarter_name":   [f"{d.year}Q{((d.month-1)//3)+1}" for d in dates],
        "d_holiday":        RNG.choice(["Y", "N"], n, p=[0.05, 0.95]),
        "d_weekend":        ["Y" if d.weekday() >= 5 else "N" for d in dates],
        "d_following_holiday": RNG.choice(["Y", "N"], n, p=[0.05, 0.95]),
        "d_first_dom":      [date(d.year, d.month, 1).timetuple().tm_yday for d in dates],
        "d_last_dom":       [date(d.year, d.month % 12 + 1, 1).timetuple().tm_yday
                             if d.month < 12
                             else date(d.year + 1, 1, 1).timetuple().tm_yday
                             for d in dates],
        "d_same_day_ly":    np.arange(2415022, 2415022 + n, dtype="int32") - 365,
        "d_same_day_lq":    np.arange(2415022, 2415022 + n, dtype="int32") - 91,
        "d_current_day":    RNG.choice(["Y", "N"], n, p=[0.001, 0.999]),
        "d_current_week":   RNG.choice(["Y", "N"], n, p=[0.02, 0.98]),
        "d_current_month":  RNG.choice(["Y", "N"], n, p=[0.1, 0.9]),
        "d_current_quarter":RNG.choice(["Y", "N"], n, p=[0.25, 0.75]),
        "d_current_year":   RNG.choice(["Y", "N"], n, p=[0.5, 0.5]),
    })
    return df


def gen_time_dim(n):
    return pd.DataFrame({
        "t_time_sk":    np.arange(0, n, dtype="int32"),
        "t_time_id":    [f"T{i:08d}" for i in range(n)],
        "t_time":       np.arange(0, n, dtype="int32"),
        "t_hour":       np.arange(0, n, dtype="int32") // 3600,
        "t_minute":     (np.arange(0, n, dtype="int32") % 3600) // 60,
        "t_second":     np.arange(0, n, dtype="int32") % 60,
        "t_am_pm":      ["AM" if h < 12 else "PM" for h in np.arange(0, n) // 3600],
        "t_shift":      ["first" if h < 8 else ("second" if h < 16 else "third")
                         for h in np.arange(0, n) // 3600],
        "t_sub_shift":  RNG.choice(["breakfast", "lunch", "dinner", "night"], n),
        "t_meal_time":  RNG.choice([None, "breakfast", "lunch", "dinner"], n),
    })


def gen_customer(n):
    return pd.DataFrame({
        "c_customer_sk":      np.arange(1, n + 1, dtype="int32"),
        "c_customer_id":      [f"CUST{i:012d}" for i in range(1, n + 1)],
        "c_current_cdemo_sk": RNG.integers(1, 1_920_800, n),
        "c_current_hdemo_sk": RNG.integers(1, 7_200, n),
        "c_current_addr_sk":  RNG.integers(1, n, n),
        "c_first_shipto_date_sk": RNG.integers(2415022, 2415022 + 73049, n),
        "c_first_sales_date_sk":  RNG.integers(2415022, 2415022 + 73049, n),
        "c_salutation":       RNG.choice(["Mr.", "Mrs.", "Ms.", "Dr.", "Miss"], n),
        "c_first_name":       RNG.choice(["Alice","Bob","Carol","David","Eve","Frank",
                                           "Grace","Henry","Iris","Jack"], n),
        "c_last_name":        RNG.choice(["Smith","Jones","Williams","Brown","Davis",
                                           "Miller","Wilson","Moore","Taylor","Anderson"], n),
        "c_preferred_cust_flag": RNG.choice(["Y", "N"], n),
        "c_birth_day":        RNG.integers(1, 29, n),
        "c_birth_month":      RNG.integers(1, 13, n),
        "c_birth_year":       RNG.integers(1924, 1992, n),
        "c_birth_country":    RNG.choice(["UNITED STATES","CANADA","MEXICO","UK","GERMANY"], n),
        "c_login":            [f"login_{i}" for i in range(1, n + 1)],
        "c_email_address":    [f"user{i}@example.com" for i in range(1, n + 1)],
        "c_last_review_date_sk": nullable_int(RNG.integers(2415022, 2415022 + 73049, n)),
    })


def gen_customer_address(n):
    states = ["CA","TX","NY","FL","IL","PA","OH","GA","NC","MI"]
    return pd.DataFrame({
        "ca_address_sk":    np.arange(1, n + 1, dtype="int32"),
        "ca_address_id":    [f"ADDR{i:012d}" for i in range(1, n + 1)],
        "ca_street_number": [str(RNG.integers(1, 9999)) for _ in range(n)],
        "ca_street_name":   RNG.choice(["Main St","Oak Ave","Maple Dr","Pine Rd","Cedar Ln"], n),
        "ca_street_type":   RNG.choice(["Street","Avenue","Drive","Road","Lane","Blvd"], n),
        "ca_suite_number":  RNG.choice(["Suite 100","Apt 1","Unit A",None, None], n),
        "ca_city":          RNG.choice(["Los Angeles","Houston","Chicago","Phoenix","Dallas",
                                         "San Antonio","San Diego","Jacksonville","Austin"], n),
        "ca_county":        RNG.choice(["Los Angeles County","Harris County","Cook County"], n),
        "ca_state":         RNG.choice(states, n),
        "ca_zip":           [f"{RNG.integers(10000, 99999):05d}" for _ in range(n)],
        "ca_country":       ["United States"] * n,
        "ca_gmt_offset":    RNG.choice([-5, -6, -7, -8], n).astype("float32"),
        "ca_location_type": RNG.choice(["apartment","house","condo","unknown"], n),
    })


def gen_customer_demographics(n):
    genders = ["M", "F"]
    marital = ["S", "M", "D", "W", "U"]
    education = ["Advanced Degree","College","2 yr Degree","4 yr Degree",
                  "Unknown","Primary","Secondary"]
    return pd.DataFrame({
        "cd_demo_sk":          np.arange(1, n + 1, dtype="int32"),
        "cd_gender":           RNG.choice(genders, n),
        "cd_marital_status":   RNG.choice(marital, n),
        "cd_education_status": RNG.choice(education, n),
        "cd_purchase_estimate":RNG.integers(500, 10001, n, dtype="int32"),
        "cd_credit_rating":    RNG.choice(["Good","High Risk","Low Risk","Unknown"], n),
        "cd_dep_count":        RNG.integers(0, 7, n, dtype="int32"),
        "cd_dep_employed_count":RNG.integers(0, 4, n, dtype="int32"),
        "cd_dep_college_count": RNG.integers(0, 4, n, dtype="int32"),
    })


def gen_household_demographics(n):
    return pd.DataFrame({
        "hd_demo_sk":        np.arange(1, n + 1, dtype="int32"),
        "hd_income_band_sk": RNG.integers(1, 21, n, dtype="int32"),
        "hd_buy_potential":  RNG.choice(["Unknown","1001-5000","0-500","5001-10000",
                                          "10001-20000","501-1000",">10000"], n),
        "hd_dep_count":      RNG.integers(0, 7, n, dtype="int32"),
        "hd_vehicle_count":  RNG.integers(-1, 5, n, dtype="int32"),
    })


def gen_income_band(n):
    return pd.DataFrame({
        "ib_income_band_sk": np.arange(1, n + 1, dtype="int32"),
        "ib_lower_bound":    np.arange(0, n * 10000, 10000, dtype="int32")[:n],
        "ib_upper_bound":    np.arange(10000, (n + 1) * 10000, 10000, dtype="int32")[:n],
    })


def gen_item(n):
    categories = ["Books","Electronics","Music","Sports","Men","Women","Children","Shoes","Jewelry","Home"]
    return pd.DataFrame({
        "i_item_sk":          np.arange(1, n + 1, dtype="int32"),
        "i_item_id":          [f"ITEM{i:012d}" for i in range(1, n + 1)],
        "i_rec_start_date":   rand_date_range(n, "1997-01-01", "2001-01-01"),
        "i_rec_end_date":     rand_date_range(n, "2001-01-02", "2003-12-31"),
        "i_item_desc":        [f"Description for item {i}" for i in range(1, n + 1)],
        "i_current_price":    RNG.uniform(0.99, 299.99, n).round(2),
        "i_wholesale_cost":   RNG.uniform(0.50, 150.00, n).round(2),
        "i_brand_id":         RNG.integers(1, 1001, n, dtype="int32"),
        "i_brand":            RNG.choice(["Brand A","Brand B","Brand C","Brand D","Brand E"], n),
        "i_class_id":         RNG.integers(1, 101, n, dtype="int32"),
        "i_class":            RNG.choice(["class1","class2","class3","class4","class5"], n),
        "i_category_id":      RNG.integers(1, 11, n, dtype="int32"),
        "i_category":         RNG.choice(categories, n),
        "i_manufact_id":      RNG.integers(1, 1001, n, dtype="int32"),
        "i_manufact":         RNG.choice(["Manufact A","Manufact B","Manufact C","Manufact D"], n),
        "i_size":             RNG.choice(["small","medium","large","extra large","N/A"], n),
        "i_formulation":      [f"form_{i}" for i in range(1, n + 1)],
        "i_color":            RNG.choice(["red","blue","green","black","white","yellow","orange"], n),
        "i_units":            RNG.choice(["Ounce","Lb","Gram","Dozen","Each","Tbl","Cup"], n),
        "i_container":        RNG.choice(["Bag","Box","Can","Cup","Jar","Unknown"], n),
        "i_manager_id":       RNG.integers(1, 101, n, dtype="int32"),
        "i_product_name":     [f"Product_{i:06d}" for i in range(1, n + 1)],
    })


def gen_store(n):
    return pd.DataFrame({
        "s_store_sk":          np.arange(1, n + 1, dtype="int32"),
        "s_store_id":          [f"STORE{i:012d}" for i in range(1, n + 1)],
        "s_rec_start_date":    rand_date_range(n, "1997-01-01", "2001-01-01"),
        "s_rec_end_date":      rand_date_range(n, "2001-01-02", "2003-12-31"),
        "s_closed_date_sk":    nullable_int(RNG.integers(2415022, 2415022 + 73049, n)),
        "s_store_name":        [f"Store {i}" for i in range(1, n + 1)],
        "s_number_employees":  RNG.integers(10, 300, n, dtype="int32"),
        "s_floor_space":       RNG.integers(5000, 50001, n, dtype="int32"),
        "s_hours":             RNG.choice(["8AM-4PM","8AM-12AM","Unknown"], n),
        "s_manager":           RNG.choice(["John Smith","Jane Doe","Bob Brown","Alice White"], n),
        "s_market_id":         RNG.integers(1, 11, n, dtype="int32"),
        "s_geography_class":   RNG.choice(["Metropolitan","Unknown","Rural","Suburban"], n),
        "s_market_desc":       [f"Market description {i}" for i in range(1, n + 1)],
        "s_market_manager":    RNG.choice(["Manager A","Manager B","Manager C"], n),
        "s_division_id":       RNG.integers(1, 11, n, dtype="int32"),
        "s_division_name":     RNG.choice(["Division A","Division B","Division C"], n),
        "s_company_id":        RNG.integers(1, 6, n, dtype="int32"),
        "s_company_name":      RNG.choice(["Company A","Company B","Company C"], n),
        "s_street_number":     [str(RNG.integers(1, 9999)) for _ in range(n)],
        "s_street_name":       RNG.choice(["Main St","Oak Ave","Pine Rd"], n),
        "s_street_type":       RNG.choice(["Street","Avenue","Drive"], n),
        "s_suite_number":      RNG.choice(["Suite 100","Unit A","N/A"], n),
        "s_city":              RNG.choice(["Chicago","Dallas","LA","NYC","Houston"], n),
        "s_county":            RNG.choice(["Cook County","Dallas County","LA County"], n),
        "s_state":             RNG.choice(["IL","TX","CA","NY","FL"], n),
        "s_zip":               [f"{RNG.integers(10000, 99999):05d}" for _ in range(n)],
        "s_country":           ["United States"] * n,
        "s_gmt_offset":        RNG.choice([-5.0, -6.0, -7.0, -8.0], n),
        "s_tax_precentage":    RNG.uniform(0.00, 0.12, n).round(4),
    })


def gen_call_center(n):
    return pd.DataFrame({
        "cc_call_center_sk": np.arange(1, n + 1, dtype="int32"),
        "cc_call_center_id": [f"CC{i:08d}" for i in range(1, n + 1)],
        "cc_rec_start_date": rand_date_range(n, "1997-01-01", "2001-01-01"),
        "cc_rec_end_date":   rand_date_range(n, "2001-01-02", "2003-12-31"),
        "cc_closed_date_sk": nullable_int(np.zeros(n, dtype="int64")),
        "cc_open_date_sk":   RNG.integers(2415022, 2415022 + 73049, n),
        "cc_name":           [f"Call Center {i}" for i in range(1, n + 1)],
        "cc_class":          RNG.choice(["large","medium","small","mktg"], n),
        "cc_employees":      RNG.integers(100, 10001, n, dtype="int32"),
        "cc_sq_ft":          RNG.integers(1000, 100001, n, dtype="int32"),
        "cc_hours":          RNG.choice(["8AM-12AM","8AM-4PM","8AM-8AM"], n),
        "cc_manager":        RNG.choice(["Manager A","Manager B","Manager C"], n),
        "cc_mkt_id":         RNG.integers(1, 11, n, dtype="int32"),
        "cc_mkt_class":      RNG.choice(["More than 100 empl","Class A","Class B"], n),
        "cc_mkt_desc":       [f"Market {i}" for i in range(1, n + 1)],
        "cc_market_manager": RNG.choice(["Mkt Mgr A","Mkt Mgr B"], n),
        "cc_division":       RNG.integers(1, 6, n, dtype="int32"),
        "cc_division_name":  RNG.choice(["Division A","Division B","Division C"], n),
        "cc_company":        RNG.integers(1, 6, n, dtype="int32"),
        "cc_company_name":   RNG.choice(["Company A","Company B","Company C"], n),
        "cc_street_number":  [str(RNG.integers(1, 9999)) for _ in range(n)],
        "cc_street_name":    RNG.choice(["Main St","Oak Ave","Pine Rd"], n),
        "cc_street_type":    RNG.choice(["Street","Avenue","Drive"], n),
        "cc_suite_number":   RNG.choice(["Suite 100","Suite 200","Unit A"], n),
        "cc_city":           RNG.choice(["Chicago","Dallas","LA","NYC"], n),
        "cc_county":         RNG.choice(["Cook County","Dallas County"], n),
        "cc_state":          RNG.choice(["IL","TX","CA","NY","FL"], n),
        "cc_zip":            [f"{RNG.integers(10000, 99999):05d}" for _ in range(n)],
        "cc_country":        ["United States"] * n,
        "cc_gmt_offset":     RNG.choice([-5.0, -6.0, -7.0, -8.0], n),
        "cc_tax_percentage": RNG.uniform(0.00, 0.12, n).round(4),
    })


def gen_catalog_page(n):
    return pd.DataFrame({
        "cp_catalog_page_sk":  np.arange(1, n + 1, dtype="int32"),
        "cp_catalog_page_id":  [f"CP{i:012d}" for i in range(1, n + 1)],
        "cp_start_date_sk":    nullable_int(RNG.integers(2415022, 2415022 + 73049, n)),
        "cp_end_date_sk":      nullable_int(RNG.integers(2415022, 2415022 + 73049, n)),
        "cp_department":       RNG.choice(["Sports","Books","Electronics","Home","Clothing"], n),
        "cp_catalog_number":   RNG.integers(1, 51, n, dtype="int32"),
        "cp_catalog_page_number": RNG.integers(1, 201, n, dtype="int32"),
        "cp_description":      [f"Catalog page description {i}" for i in range(1, n + 1)],
        "cp_type":             RNG.choice(["catalog page","catalog"], n),
    })


def gen_web_site(n):
    return pd.DataFrame({
        "web_site_sk":         np.arange(1, n + 1, dtype="int32"),
        "web_site_id":         [f"WEB{i:012d}" for i in range(1, n + 1)],
        "web_rec_start_date":  rand_date_range(n, "1997-01-01", "2001-01-01"),
        "web_rec_end_date":    rand_date_range(n, "2001-01-02", "2003-12-31"),
        "web_name":            [f"site_{i}" for i in range(1, n + 1)],
        "web_open_date_sk":    RNG.integers(2415022, 2415022 + 73049, n),
        "web_close_date_sk":   nullable_int(np.zeros(n, dtype="int64")),
        "web_class":           RNG.choice(["Unknown","Large","Small","Medium"], n),
        "web_manager":         RNG.choice(["Manager A","Manager B","Manager C"], n),
        "web_mkt_id":          RNG.integers(1, 11, n, dtype="int32"),
        "web_mkt_class":       RNG.choice(["Class A","Class B","Class C"], n),
        "web_mkt_desc":        [f"Market {i}" for i in range(1, n + 1)],
        "web_market_manager":  RNG.choice(["Mkt Mgr A","Mkt Mgr B"], n),
        "web_company_id":      RNG.integers(1, 6, n, dtype="int32"),
        "web_company_name":    RNG.choice(["Company A","Company B","Company C"], n),
        "web_street_number":   [str(RNG.integers(1, 9999)) for _ in range(n)],
        "web_street_name":     RNG.choice(["Main St","Oak Ave","Pine Rd"], n),
        "web_street_type":     RNG.choice(["Street","Avenue","Drive"], n),
        "web_suite_number":    RNG.choice(["Suite 100","Suite 200"], n),
        "web_city":            RNG.choice(["Chicago","Dallas","LA","NYC"], n),
        "web_county":          RNG.choice(["Cook County","Dallas County"], n),
        "web_state":           RNG.choice(["IL","TX","CA","NY","FL"], n),
        "web_zip":             [f"{RNG.integers(10000, 99999):05d}" for _ in range(n)],
        "web_country":         ["United States"] * n,
        "web_gmt_offset":      RNG.choice([-5.0, -6.0, -7.0, -8.0], n),
        "web_tax_percentage":  RNG.uniform(0.00, 0.12, n).round(4),
    })


def gen_web_page(n):
    return pd.DataFrame({
        "wp_web_page_sk":     np.arange(1, n + 1, dtype="int32"),
        "wp_web_page_id":     [f"WP{i:012d}" for i in range(1, n + 1)],
        "wp_rec_start_date":  rand_date_range(n, "1997-01-01", "2001-01-01"),
        "wp_rec_end_date":    rand_date_range(n, "2001-01-02", "2003-12-31"),
        "wp_creation_date_sk": RNG.integers(2415022, 2415022 + 73049, n),
        "wp_access_date_sk":  RNG.integers(2415022, 2415022 + 73049, n),
        "wp_autogen_flag":    RNG.choice(["Y", "N"], n),
        "wp_customer_sk":     nullable_int(RNG.integers(1, 100_001, n)),
        "wp_url":             [f"http://www.site.com/page_{i}" for i in range(1, n + 1)],
        "wp_type":            RNG.choice(["welcome","order","browse","shipping","coupon",
                                           "checkout","search","feedback"], n),
        "wp_char_count":      RNG.integers(100, 10001, n, dtype="int32"),
        "wp_link_count":      RNG.integers(1, 26, n, dtype="int32"),
        "wp_image_count":     RNG.integers(0, 8, n, dtype="int32"),
        "wp_max_ad_count":    RNG.integers(0, 5, n, dtype="int32"),
    })


def gen_warehouse(n):
    return pd.DataFrame({
        "w_warehouse_sk":   np.arange(1, n + 1, dtype="int32"),
        "w_warehouse_id":   [f"WH{i:012d}" for i in range(1, n + 1)],
        "w_warehouse_name": [f"Warehouse {i}" for i in range(1, n + 1)],
        "w_warehouse_sq_ft":RNG.integers(50000, 1000001, n, dtype="int32"),
        "w_street_number":  [str(RNG.integers(1, 9999)) for _ in range(n)],
        "w_street_name":    RNG.choice(["Main St","Oak Ave","Pine Rd"], n),
        "w_street_type":    RNG.choice(["Street","Avenue","Drive"], n),
        "w_suite_number":   RNG.choice(["Suite 100","Suite 200"], n),
        "w_city":           RNG.choice(["Chicago","Dallas","LA","NYC","Houston"], n),
        "w_county":         RNG.choice(["Cook County","Dallas County","LA County"], n),
        "w_state":          RNG.choice(["IL","TX","CA","NY","FL"], n),
        "w_zip":            [f"{RNG.integers(10000, 99999):05d}" for _ in range(n)],
        "w_country":        ["United States"] * n,
        "w_gmt_offset":     RNG.choice([-5.0, -6.0, -7.0, -8.0], n),
    })


def gen_ship_mode(n):
    return pd.DataFrame({
        "sm_ship_mode_sk": np.arange(1, n + 1, dtype="int32"),
        "sm_ship_mode_id": [f"SM{i:012d}" for i in range(1, n + 1)],
        "sm_type":         RNG.choice(["EXPRESS","NEXT DAY","SECOND","NEXT DAY AIR",
                                        "OVERNIGHT","TWO DAY","STANDARD","LIBRARY"], n),
        "sm_code":         RNG.choice(["AIR","SEA","SURFACE","RAIL","TRUCK"], n),
        "sm_carrier":      RNG.choice(["FedEx","UPS","DHL","USPS","Amazon Logistics"], n),
        "sm_contract":     [f"contract_{i}" for i in range(1, n + 1)],
    })


def gen_reason(n):
    reasons = ["Did not like","Found cheaper","Wrong size","Wrong color",
               "Not as described","Damaged","Too late","Ordered by mistake",
               "Better option found","No longer needed","Gift return","Other"]
    return pd.DataFrame({
        "r_reason_sk":   np.arange(1, n + 1, dtype="int32"),
        "r_reason_id":   [f"R{i:012d}" for i in range(1, n + 1)],
        "r_reason_desc": RNG.choice(reasons, n),
    })


def gen_promotion(n):
    return pd.DataFrame({
        "p_promo_sk":         np.arange(1, n + 1, dtype="int32"),
        "p_promo_id":         [f"PROMO{i:012d}" for i in range(1, n + 1)],
        "p_start_date_sk":    nullable_int(RNG.integers(2415022, 2415022 + 73049, n)),
        "p_end_date_sk":      nullable_int(RNG.integers(2415022, 2415022 + 73049, n)),
        "p_item_sk":          nullable_int(RNG.integers(1, 18001, n)),
        "p_cost":             RNG.uniform(100, 10000, n).round(2),
        "p_response_target":  RNG.integers(1, 6, n, dtype="int32"),
        "p_promo_name":       [f"Promo {i}" for i in range(1, n + 1)],
        "p_channel_dmail":    RNG.choice(["Y", "N"], n),
        "p_channel_email":    RNG.choice(["Y", "N"], n),
        "p_channel_catalog":  RNG.choice(["Y", "N"], n),
        "p_channel_tv":       RNG.choice(["Y", "N"], n),
        "p_channel_radio":    RNG.choice(["Y", "N"], n),
        "p_channel_press":    RNG.choice(["Y", "N"], n),
        "p_channel_event":    RNG.choice(["Y", "N"], n),
        "p_channel_demo":     RNG.choice(["Y", "N"], n),
        "p_channel_details":  [f"Details {i}" for i in range(1, n + 1)],
        "p_purpose":          RNG.choice(["Unknown","Cross-sell","Upsell","Retention"], n),
        "p_discount_active":  RNG.choice(["Y", "N"], n),
    })


# ─── Fact table generators ────────────────────────────────────────────────────

def gen_store_sales(n, date_sk_range, store_range, item_range, cust_range):
    return pd.DataFrame({
        "ss_sold_date_sk":     nullable_int(RNG.integers(*date_sk_range, n)),
        "ss_sold_time_sk":     nullable_int(RNG.integers(0, 86400, n)),
        "ss_item_sk":          RNG.integers(*item_range, n, dtype="int32"),
        "ss_customer_sk":      nullable_int(RNG.integers(*cust_range, n)),
        "ss_cdemo_sk":         nullable_int(RNG.integers(1, 1_920_800, n)),
        "ss_hdemo_sk":         nullable_int(RNG.integers(1, 7_200, n)),
        "ss_addr_sk":          nullable_int(RNG.integers(1, 100_001, n)),
        "ss_store_sk":         nullable_int(RNG.integers(*store_range, n)),
        "ss_promo_sk":         nullable_int(RNG.integers(1, 301, n)),
        "ss_ticket_number":    RNG.integers(1, 100_000_001, n, dtype="int64"),
        "ss_quantity":         nullable_int(RNG.integers(1, 101, n)),
        "ss_wholesale_cost":   RNG.uniform(1, 100, n).round(2),
        "ss_list_price":       RNG.uniform(1, 300, n).round(2),
        "ss_sales_price":      RNG.uniform(1, 300, n).round(2),
        "ss_ext_discount_amt": RNG.uniform(0, 50, n).round(2),
        "ss_ext_sales_price":  RNG.uniform(1, 1000, n).round(2),
        "ss_ext_wholesale_cost":RNG.uniform(1, 500, n).round(2),
        "ss_ext_list_price":   RNG.uniform(1, 1500, n).round(2),
        "ss_ext_tax":          RNG.uniform(0, 100, n).round(2),
        "ss_coupon_amt":       RNG.uniform(0, 50, n).round(2),
        "ss_net_paid":         RNG.uniform(1, 1000, n).round(2),
        "ss_net_paid_inc_tax": RNG.uniform(1, 1100, n).round(2),
        "ss_net_profit":       RNG.uniform(-100, 500, n).round(2),
    })


def gen_store_returns(n, ss_ticket_numbers, date_sk_range, store_range, item_range):
    return pd.DataFrame({
        "sr_returned_date_sk":  nullable_int(RNG.integers(*date_sk_range, n)),
        "sr_return_time_sk":    nullable_int(RNG.integers(0, 86400, n)),
        "sr_item_sk":           RNG.integers(*item_range, n, dtype="int32"),
        "sr_customer_sk":       nullable_int(RNG.integers(1, 100_001, n)),
        "sr_cdemo_sk":          nullable_int(RNG.integers(1, 1_920_800, n)),
        "sr_hdemo_sk":          nullable_int(RNG.integers(1, 7_200, n)),
        "sr_addr_sk":           nullable_int(RNG.integers(1, 100_001, n)),
        "sr_store_sk":          nullable_int(RNG.integers(*store_range, n)),
        "sr_reason_sk":         nullable_int(RNG.integers(1, 36, n)),
        "sr_ticket_number":     RNG.choice(ss_ticket_numbers, n),
        "sr_return_quantity":   nullable_int(RNG.integers(1, 101, n)),
        "sr_return_amt":        RNG.uniform(1, 500, n).round(2),
        "sr_return_tax":        RNG.uniform(0, 50, n).round(2),
        "sr_return_amt_inc_tax":RNG.uniform(1, 550, n).round(2),
        "sr_fee":               RNG.uniform(0.5, 100, n).round(2),
        "sr_return_ship_cost":  RNG.uniform(0, 50, n).round(2),
        "sr_refunded_cash":     RNG.uniform(0, 500, n).round(2),
        "sr_reversed_charge":   RNG.uniform(0, 100, n).round(2),
        "sr_store_credit":      RNG.uniform(0, 100, n).round(2),
        "sr_net_loss":          RNG.uniform(0, 500, n).round(2),
    })


def gen_catalog_sales(n, date_sk_range, item_range, cust_range):
    return pd.DataFrame({
        "cs_sold_date_sk":      nullable_int(RNG.integers(*date_sk_range, n)),
        "cs_sold_time_sk":      nullable_int(RNG.integers(0, 86400, n)),
        "cs_ship_date_sk":      nullable_int(RNG.integers(*date_sk_range, n)),
        "cs_bill_customer_sk":  nullable_int(RNG.integers(*cust_range, n)),
        "cs_bill_cdemo_sk":     nullable_int(RNG.integers(1, 1_920_800, n)),
        "cs_bill_hdemo_sk":     nullable_int(RNG.integers(1, 7_200, n)),
        "cs_bill_addr_sk":      nullable_int(RNG.integers(1, 100_001, n)),
        "cs_ship_customer_sk":  nullable_int(RNG.integers(*cust_range, n)),
        "cs_ship_cdemo_sk":     nullable_int(RNG.integers(1, 1_920_800, n)),
        "cs_ship_hdemo_sk":     nullable_int(RNG.integers(1, 7_200, n)),
        "cs_ship_addr_sk":      nullable_int(RNG.integers(1, 100_001, n)),
        "cs_call_center_sk":    nullable_int(RNG.integers(1, 31, n)),
        "cs_catalog_page_sk":   nullable_int(RNG.integers(1, 11_719, n)),
        "cs_ship_mode_sk":      nullable_int(RNG.integers(1, 21, n)),
        "cs_warehouse_sk":      nullable_int(RNG.integers(1, 16, n)),
        "cs_item_sk":           RNG.integers(*item_range, n, dtype="int32"),
        "cs_promo_sk":          nullable_int(RNG.integers(1, 301, n)),
        "cs_order_number":      RNG.integers(1, 100_000_001, n, dtype="int64"),
        "cs_quantity":          nullable_int(RNG.integers(1, 101, n)),
        "cs_wholesale_cost":    RNG.uniform(1, 100, n).round(2),
        "cs_list_price":        RNG.uniform(1, 300, n).round(2),
        "cs_sales_price":       RNG.uniform(1, 300, n).round(2),
        "cs_ext_discount_amt":  RNG.uniform(0, 50, n).round(2),
        "cs_ext_sales_price":   RNG.uniform(1, 1000, n).round(2),
        "cs_ext_wholesale_cost":RNG.uniform(1, 500, n).round(2),
        "cs_ext_list_price":    RNG.uniform(1, 1500, n).round(2),
        "cs_ext_tax":           RNG.uniform(0, 100, n).round(2),
        "cs_coupon_amt":        RNG.uniform(0, 50, n).round(2),
        "cs_ext_ship_cost":     RNG.uniform(0, 100, n).round(2),
        "cs_net_paid":          RNG.uniform(1, 1000, n).round(2),
        "cs_net_paid_inc_tax":  RNG.uniform(1, 1100, n).round(2),
        "cs_net_paid_inc_ship": RNG.uniform(1, 1200, n).round(2),
        "cs_net_paid_inc_ship_tax":RNG.uniform(1, 1300, n).round(2),
        "cs_net_profit":        RNG.uniform(-100, 500, n).round(2),
    })


def gen_catalog_returns(n, cs_order_numbers, date_sk_range, item_range):
    return pd.DataFrame({
        "cr_returned_date_sk":    nullable_int(RNG.integers(*date_sk_range, n)),
        "cr_returned_time_sk":    nullable_int(RNG.integers(0, 86400, n)),
        "cr_item_sk":             RNG.integers(*item_range, n, dtype="int32"),
        "cr_refunded_customer_sk":nullable_int(RNG.integers(1, 100_001, n)),
        "cr_refunded_cdemo_sk":   nullable_int(RNG.integers(1, 1_920_800, n)),
        "cr_refunded_hdemo_sk":   nullable_int(RNG.integers(1, 7_200, n)),
        "cr_refunded_addr_sk":    nullable_int(RNG.integers(1, 100_001, n)),
        "cr_returning_customer_sk":nullable_int(RNG.integers(1, 100_001, n)),
        "cr_returning_cdemo_sk":  nullable_int(RNG.integers(1, 1_920_800, n)),
        "cr_returning_hdemo_sk":  nullable_int(RNG.integers(1, 7_200, n)),
        "cr_returning_addr_sk":   nullable_int(RNG.integers(1, 100_001, n)),
        "cr_call_center_sk":      nullable_int(RNG.integers(1, 31, n)),
        "cr_catalog_page_sk":     nullable_int(RNG.integers(1, 11_719, n)),
        "cr_ship_mode_sk":        nullable_int(RNG.integers(1, 21, n)),
        "cr_warehouse_sk":        nullable_int(RNG.integers(1, 16, n)),
        "cr_reason_sk":           nullable_int(RNG.integers(1, 36, n)),
        "cr_order_number":        RNG.choice(cs_order_numbers, n),
        "cr_return_quantity":     nullable_int(RNG.integers(1, 101, n)),
        "cr_return_amount":       RNG.uniform(1, 500, n).round(2),
        "cr_return_tax":          RNG.uniform(0, 50, n).round(2),
        "cr_return_amt_inc_tax":  RNG.uniform(1, 550, n).round(2),
        "cr_fee":                 RNG.uniform(0.5, 100, n).round(2),
        "cr_return_ship_cost":    RNG.uniform(0, 50, n).round(2),
        "cr_refunded_cash":       RNG.uniform(0, 500, n).round(2),
        "cr_reversed_charge":     RNG.uniform(0, 100, n).round(2),
        "cr_store_credit":        RNG.uniform(0, 100, n).round(2),
        "cr_net_loss":            RNG.uniform(0, 500, n).round(2),
    })


def gen_web_sales(n, date_sk_range, item_range, cust_range):
    return pd.DataFrame({
        "ws_sold_date_sk":        nullable_int(RNG.integers(*date_sk_range, n)),
        "ws_sold_time_sk":        nullable_int(RNG.integers(0, 86400, n)),
        "ws_ship_date_sk":        nullable_int(RNG.integers(*date_sk_range, n)),
        "ws_item_sk":             RNG.integers(*item_range, n, dtype="int32"),
        "ws_bill_customer_sk":    nullable_int(RNG.integers(*cust_range, n)),
        "ws_bill_cdemo_sk":       nullable_int(RNG.integers(1, 1_920_800, n)),
        "ws_bill_hdemo_sk":       nullable_int(RNG.integers(1, 7_200, n)),
        "ws_bill_addr_sk":        nullable_int(RNG.integers(1, 100_001, n)),
        "ws_ship_customer_sk":    nullable_int(RNG.integers(*cust_range, n)),
        "ws_ship_cdemo_sk":       nullable_int(RNG.integers(1, 1_920_800, n)),
        "ws_ship_hdemo_sk":       nullable_int(RNG.integers(1, 7_200, n)),
        "ws_ship_addr_sk":        nullable_int(RNG.integers(1, 100_001, n)),
        "ws_web_page_sk":         nullable_int(RNG.integers(1, 2041, n)),
        "ws_web_site_sk":         nullable_int(RNG.integers(1, 25, n)),
        "ws_ship_mode_sk":        nullable_int(RNG.integers(1, 21, n)),
        "ws_warehouse_sk":        nullable_int(RNG.integers(1, 16, n)),
        "ws_promo_sk":            nullable_int(RNG.integers(1, 301, n)),
        "ws_order_number":        RNG.integers(1, 100_000_001, n, dtype="int64"),
        "ws_quantity":            nullable_int(RNG.integers(1, 101, n)),
        "ws_wholesale_cost":      RNG.uniform(1, 100, n).round(2),
        "ws_list_price":          RNG.uniform(1, 300, n).round(2),
        "ws_sales_price":         RNG.uniform(1, 300, n).round(2),
        "ws_ext_discount_amt":    RNG.uniform(0, 50, n).round(2),
        "ws_ext_sales_price":     RNG.uniform(1, 1000, n).round(2),
        "ws_ext_wholesale_cost":  RNG.uniform(1, 500, n).round(2),
        "ws_ext_list_price":      RNG.uniform(1, 1500, n).round(2),
        "ws_ext_tax":             RNG.uniform(0, 100, n).round(2),
        "ws_coupon_amt":          RNG.uniform(0, 50, n).round(2),
        "ws_ext_ship_cost":       RNG.uniform(0, 100, n).round(2),
        "ws_net_paid":            RNG.uniform(1, 1000, n).round(2),
        "ws_net_paid_inc_tax":    RNG.uniform(1, 1100, n).round(2),
        "ws_net_paid_inc_ship":   RNG.uniform(1, 1200, n).round(2),
        "ws_net_paid_inc_ship_tax":RNG.uniform(1, 1300, n).round(2),
        "ws_net_profit":          RNG.uniform(-100, 500, n).round(2),
    })


def gen_web_returns(n, ws_order_numbers, date_sk_range, item_range):
    return pd.DataFrame({
        "wr_returned_date_sk":     nullable_int(RNG.integers(*date_sk_range, n)),
        "wr_returned_time_sk":     nullable_int(RNG.integers(0, 86400, n)),
        "wr_item_sk":              RNG.integers(*item_range, n, dtype="int32"),
        "wr_refunded_customer_sk": nullable_int(RNG.integers(1, 100_001, n)),
        "wr_refunded_cdemo_sk":    nullable_int(RNG.integers(1, 1_920_800, n)),
        "wr_refunded_hdemo_sk":    nullable_int(RNG.integers(1, 7_200, n)),
        "wr_refunded_addr_sk":     nullable_int(RNG.integers(1, 100_001, n)),
        "wr_returning_customer_sk":nullable_int(RNG.integers(1, 100_001, n)),
        "wr_returning_cdemo_sk":   nullable_int(RNG.integers(1, 1_920_800, n)),
        "wr_returning_hdemo_sk":   nullable_int(RNG.integers(1, 7_200, n)),
        "wr_returning_addr_sk":    nullable_int(RNG.integers(1, 100_001, n)),
        "wr_web_page_sk":          nullable_int(RNG.integers(1, 2041, n)),
        "wr_reason_sk":            nullable_int(RNG.integers(1, 36, n)),
        "wr_order_number":         RNG.choice(ws_order_numbers, n),
        "wr_return_quantity":      nullable_int(RNG.integers(1, 101, n)),
        "wr_return_amt":           RNG.uniform(1, 500, n).round(2),
        "wr_return_tax":           RNG.uniform(0, 50, n).round(2),
        "wr_return_amt_inc_tax":   RNG.uniform(1, 550, n).round(2),
        "wr_fee":                  RNG.uniform(0.5, 100, n).round(2),
        "wr_return_ship_cost":     RNG.uniform(0, 50, n).round(2),
        "wr_refunded_cash":        RNG.uniform(0, 500, n).round(2),
        "wr_reversed_charge":      RNG.uniform(0, 100, n).round(2),
        "wr_account_credit":       RNG.uniform(0, 100, n).round(2),
        "wr_net_loss":             RNG.uniform(0, 500, n).round(2),
    })


def gen_inventory(n, date_sk_range, item_range):
    return pd.DataFrame({
        "inv_date_sk":      RNG.integers(*date_sk_range, n, dtype="int32"),
        "inv_item_sk":      RNG.integers(*item_range, n, dtype="int32"),
        "inv_warehouse_sk": RNG.integers(1, 16, n, dtype="int32"),
        "inv_quantity_on_hand": nullable_int(RNG.integers(0, 1001, n)),
    })


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    DATE_SK = (2415022, 2415022 + 73049)  # date_sk range
    ITEM_SK  = (1, 18001)
    CUST_SK  = (1, 100_001)
    STORE_SK = (1, 403)

    print("\n🗓  Generating TPC-DS synthetic data (~5 GB)\n")
    print(f"   Output: {OUTPUT_DIR}\n")
    print("─" * 60)

    # ── Dimension tables ────────────────────────────────────────
    print("\n[1/2] Dimension tables")
    save_parquet(gen_date_dim(SCALE["date_dim"]),              "date_dim")
    save_parquet(gen_time_dim(SCALE["time_dim"]),              "time_dim")
    save_parquet(gen_customer(SCALE["customer"]),              "customer")
    save_parquet(gen_customer_address(SCALE["customer_address"]), "customer_address")

    # customer_demographics is large — chunk it
    n_cd  = SCALE["customer_demographics"]
    chunk = CFG["data_generation"]["chunk_size"]
    out   = OUTPUT_DIR / "customer_demographics"
    out.mkdir(parents=True, exist_ok=True)
    writer = None
    for start in tqdm(range(0, n_cd, chunk), desc="  customer_demographics"):
        end = min(start + chunk, n_cd)
        df  = gen_customer_demographics(end - start)
        df["cd_demo_sk"] = np.arange(start + 1, end + 1, dtype="int32")
        tbl = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(out / "data.parquet", tbl.schema, compression="snappy")
        writer.write_table(tbl)
    if writer:
        writer.close()
    mb = (out / "data.parquet").stat().st_size / 1024**2
    print(f"  ✓ {'customer_demographics':35s} {n_cd:>10,} rows  {mb:7.1f} MB")

    save_parquet(gen_household_demographics(SCALE["household_demographics"]), "household_demographics")
    save_parquet(gen_income_band(SCALE["income_band"]),         "income_band")
    save_parquet(gen_item(SCALE["item"]),                       "item")
    save_parquet(gen_store(SCALE["store"]),                     "store")
    save_parquet(gen_call_center(SCALE["call_center"]),         "call_center")
    save_parquet(gen_catalog_page(SCALE["catalog_page"]),       "catalog_page")
    save_parquet(gen_web_site(SCALE["web_site"]),               "web_site")
    save_parquet(gen_web_page(SCALE["web_page"]),               "web_page")
    save_parquet(gen_warehouse(SCALE["warehouse"]),             "warehouse")
    save_parquet(gen_ship_mode(SCALE["ship_mode"]),             "ship_mode")
    save_parquet(gen_reason(SCALE["reason"]),                   "reason")
    save_parquet(gen_promotion(SCALE["promotion"]),             "promotion")

    # ── Fact tables ─────────────────────────────────────────────
    print("\n[2/2] Fact tables")

    # store_sales (chunked — largest table)
    n_ss = SCALE["store_sales"]
    out  = OUTPUT_DIR / "store_sales"
    out.mkdir(parents=True, exist_ok=True)
    writer = None
    all_ticket_numbers = []
    for start in tqdm(range(0, n_ss, chunk), desc="  store_sales"):
        end = min(start + chunk, n_ss)
        df  = gen_store_sales(end - start, DATE_SK, STORE_SK, ITEM_SK, CUST_SK)
        all_ticket_numbers.extend(df["ss_ticket_number"].tolist())
        tbl = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(out / "data.parquet", tbl.schema, compression="snappy")
        writer.write_table(tbl)
    if writer:
        writer.close()
    mb = (out / "data.parquet").stat().st_size / 1024**2
    print(f"  ✓ {'store_sales':35s} {n_ss:>10,} rows  {mb:7.1f} MB")

    ticket_sample = RNG.choice(all_ticket_numbers, min(50000, len(all_ticket_numbers)), replace=False)
    save_parquet(gen_store_returns(SCALE["store_returns"], ticket_sample, DATE_SK, STORE_SK, ITEM_SK),
                 "store_returns")

    # catalog_sales
    n_cs = SCALE["catalog_sales"]
    cs_df = gen_catalog_sales(n_cs, DATE_SK, ITEM_SK, CUST_SK)
    save_parquet(cs_df, "catalog_sales")
    cs_order_sample = cs_df["cs_order_number"].sample(min(50000, n_cs), random_state=42).values

    save_parquet(gen_catalog_returns(SCALE["catalog_returns"], cs_order_sample, DATE_SK, ITEM_SK),
                 "catalog_returns")

    # web_sales
    n_ws = SCALE["web_sales"]
    ws_df = gen_web_sales(n_ws, DATE_SK, ITEM_SK, CUST_SK)
    save_parquet(ws_df, "web_sales")
    ws_order_sample = ws_df["ws_order_number"].sample(min(30000, n_ws), random_state=42).values

    save_parquet(gen_web_returns(SCALE["web_returns"], ws_order_sample, DATE_SK, ITEM_SK),
                 "web_returns")
    save_parquet(gen_inventory(SCALE["inventory"], DATE_SK, ITEM_SK), "inventory")

    # ── Summary ─────────────────────────────────────────────────
    total = sum((OUTPUT_DIR / t / "data.parquet").stat().st_size
                for t in SCALE if (OUTPUT_DIR / t / "data.parquet").exists())
    print(f"\n{'─'*60}")
    print(f"  Total on disk: {total / 1024**3:.2f} GB  ({len(SCALE)} tables)")
    print(f"  ✅ Generation complete. Run upload_to_adls.py next.\n")


if __name__ == "__main__":
    main()
