import pandas as pd
import logging

log = logging.getLogger(__name__)

RENAME = {
    "sales": {
        "SaleID": "sale_id",
        "ProductID": "product_id",
        "CustomerID": "customer_id",
        "Quantity": "quantity",
        "TotalAmount": "total_amount",
        "SaleDate": "sale_date",
    },
    "customers": {
        "CustomerID": "customer_id",
        "Name": "name",
        "Region": "region",
        "SignUpDate": "signup_date",
        "Email": "email",
        "LoyaltyPoints": "loyalty_points",
    },
    "products": {
        "ProductID": "product_id",
        "ProductName": "product_name",
        "Category": "category",
        "Price": "price",
        "Supplier": "supplier",
    },
}

PRIMARY_KEYS = {"sales": "sale_id", "customers": "customer_id", "products": "product_id"}

# (ASSUMPTION) Placeholder values the source system uses for unknown references.
SENTINEL_IDS = {"product_id": 99999, "customer_id": 99989}


def _read_bronze(bronze_dir, name):
    path = bronze_dir / f"{name}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"missing bronze input: {path}")

    df = pd.read_parquet(path)
    if df.empty:
        # We should have an alert here, slack or gmail.
        raise ValueError(f"bronze.{name} is empty")

    # Check if there are any rows that failed to decode (full of None values). This can happen if the source file is malformed or has unexpected structure.
    undecoded = (~df["_decoded"]).sum()
    if undecoded:
        log.warning("silver.%s: dropping %d undecodable rows", name, undecoded)

    return df[df["_decoded"]].rename(columns=RENAME[name]).drop(columns="_decoded")


def _cast(df, table, col, kind):
    before = df[col].notna().sum()

    if kind == "int":
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    elif kind == "float":
        df[col] = pd.to_numeric(df[col], errors="coerce")
    elif kind == "date":
        df[col] = pd.to_datetime(df[col], errors="coerce")

    lost = before - df[col].notna().sum()
    if lost:
        log.warning("silver.%s: %d values in '%s' failed to cast to %s", table, lost, col, kind)


def _null_sentinels(df, table):
    for col, sentinel in SENTINEL_IDS.items():
        if col not in df.columns:
            continue
        mask = df[col] == sentinel
        if mask.any():
            log.info("silver.%s: %d '%s' sentinels (%s) set to null",
                     table, mask.sum(), col, sentinel)
            df.loc[mask, col] = pd.NA


def clean_sales(bronze_dir):
    df = _read_bronze(bronze_dir, "sales")
    initial_count = len(df)

    for col, kind in [("sale_id", "int"), ("product_id", "int"), ("customer_id", "int"),
                      ("quantity", "int"), ("total_amount", "float"), ("sale_date", "date")]:
        _cast(df, "sales", col, kind)

    _null_sentinels(df, "sales")

    df = df.drop_duplicates(subset="sale_id", keep="first")

    # Silver marks; gold decides what to exclude.
    df["has_product"] = df["product_id"].notna()
    df["has_customer"] = df["customer_id"].notna()
    df["amount_is_valid"] = df["total_amount"] > 0

    final_count = len(df)
    log.info("silver.sales: From %d to %d rows after cleaning", initial_count, final_count)
    return df


def clean_customers(bronze_dir):
    df = _read_bronze(bronze_dir, "customers")
    initial_count = len(df)

    for col, kind in [("customer_id", "int"), ("loyalty_points", "int"),
                      ("signup_date", "date")]:
        _cast(df, "customers", col, kind)

    df["name"] = df["name"].str.strip()
    df["email"] = df["email"].str.strip().str.lower()
    df["region"] = df["region"].str.strip().str.title()

    final_count = len(df)
    log.info("silver.customers: From %d to %d rows after cleaning", initial_count, final_count)
    return df.drop_duplicates(subset="customer_id", keep="first")


def clean_products(bronze_dir):
    df = _read_bronze(bronze_dir, "products")
    initial_count = len(df)

    _cast(df, "products", "product_id", "int")
    _cast(df, "products", "price", "float")

    df["product_name"] = df["product_name"].str.strip().replace("", pd.NA)
    df["supplier"] = df["supplier"].str.strip()

    # We could check if values in the category column are in a known set of categories, and if not, we could log a warning or set them to null.
    # Minor normalization of category names: lowercase, remove punctuation, trim whitespace. (given that I don't have a list of valid categories)
    df["category"] = (df["category"].str.lower()
                  .str.replace(r"[^a-z0-9]+", " ", regex=True)
                  .str.strip())

    # Log how many duplicate product_id rows were dropped.
    dups = df["product_id"].duplicated().sum()
    if dups:
        log.warning("silver.products: dropped %d duplicate product_id rows", dups)

    # Return without duplicates, keeping the first occurrence of each product_id.
    # We should have an alert here (assuming product ids are unique).
    final_count = len(df)
    log.info("silver.products: From %d to %d rows after cleaning", initial_count, final_count)
    return df.drop_duplicates(subset="product_id", keep="first")


CLEANERS = {"sales": clean_sales, "customers": clean_customers, "products": clean_products}


"""Clean every bronze table into silver_dir. Returns run stats."""
def build_silver(bronze_dir, silver_dir):

    silver_dir.mkdir(parents=True, exist_ok=True)
    stats = []

    for name, cleaner in CLEANERS.items():

        print()
        log.info("Applying transformations to silver.%s", name)
        df = cleaner(bronze_dir)

        pk = PRIMARY_KEYS[name]
        if df[pk].isna().any():
            raise ValueError(f"silver.{name}: '{pk}' is not a valid primary key, contains nulls")
        if df[pk].duplicated().any():
            raise ValueError(f"silver.{name}: '{pk}' is not a valid primary key, is not unique")

        df.to_parquet(silver_dir / f"{name}.parquet", index=False)
        stats.append({"table": name, "rows": len(df)})

    return pd.DataFrame(stats)