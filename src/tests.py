import pandas as pd
import json
import logging

log = logging.getLogger(__name__)

CATEGORIES = ["action figure", "board game", "doll", "puzzle", "rc toy"]
REGIONS = ["North", "South", "East", "West"]


def unique(df, column, table):
    n = df[column].duplicated().sum()
    return None if n == 0 else f"{table}.{column}: {n} duplicates"


def not_null(df, column, table):
    n = df[column].isna().sum()
    return None if n == 0 else f"{table}.{column}: {n} nulls"


def accepted_values(df, column, values, table):
    found = set(df[column].dropna().unique()) - set(values)
    return None if not found else f"{table}.{column}: unexpected {sorted(found)}"


def relationships(child, column, parent, parent_column, table):
    n = (~child[column].dropna().isin(parent[parent_column])).sum()
    return None if n == 0 else f"{table}.{column}: {n} orphan references"


def _report(results, layer):
    failures = [r for r in results if r]
    log.info("%s tests: %d passed, %d failed", layer, len(results) - len(failures), len(failures))

    for f in failures:
        log.error("FAIL %s", f)

    if failures:
        raise ValueError(f"{len(failures)} {layer} tests failed")

def test_bronze(bronze_dir, raw_dir, sources):
    results = []

    for name, cfg in sources.items():
        df = pd.read_parquet(bronze_dir / f"{name}.parquet")
        path = raw_dir / cfg["file"]

        if path.suffix == ".json":
            with open(path, encoding="utf-8-sig") as f:
                expected = len(json.load(f))
        else:
            expected = sum(1 for _ in open(path, encoding="utf-8-sig")) - 1

        results.append(
            None if len(df) == expected
            else f"bronze.{name}: {len(df)} rows vs {expected} in source"
        )
        results.append(unique(df, "_row_num", f"bronze.{name}"))

    _report(results, "bronze")

def test_silver(silver_dir):
    sales = pd.read_parquet(silver_dir / "sales.parquet")
    products = pd.read_parquet(silver_dir / "products.parquet")
    customers = pd.read_parquet(silver_dir / "customers.parquet")

    _report([
        unique(sales, "sale_id", "stg_sales"),
        not_null(sales, "sale_id", "stg_sales"),
        not_null(sales, "sale_date", "stg_sales"),
        not_null(sales, "quantity", "stg_sales"),

        unique(products, "product_id", "stg_products"),
        not_null(products, "price", "stg_products"),
        accepted_values(products, "category", CATEGORIES, "stg_products"),

        unique(customers, "customer_id", "stg_customers"),
        accepted_values(customers, "region", REGIONS, "stg_customers"),
    ], "silver")


def test_gold(gold_dir, silver_dir):
    fct = pd.read_parquet(gold_dir / "fct_sales.parquet")
    products = pd.read_parquet(gold_dir / "dim_products.parquet")
    customers = pd.read_parquet(gold_dir / "dim_customers.parquet")
    sales = pd.read_parquet(silver_dir / "sales.parquet")

    reconciled = fct["total_amount"].sum() == sales["total_amount"].sum()

    _report([
        unique(fct, "sale_id", "fct_sales"),
        not_null(fct, "sale_id", "fct_sales"),
        relationships(fct, "product_id", products, "product_id", "fct_sales"),
        relationships(fct, "customer_id", customers, "customer_id", "fct_sales"),
        None if len(fct) == len(sales) else f"fct_sales: {len(fct)} rows vs {len(sales)} in silver",
        None if reconciled else "fct_sales: total_amount does not reconcile with silver",
    ], "gold")