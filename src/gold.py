import pandas as pd
import logging

log = logging.getLogger(__name__)


def _read_silver(silver_dir, name):
    path = silver_dir / f"{name}.parquet"
    if not path.exists():
        # If silver table is missing, we should raise an error via slack or something.
        raise FileNotFoundError(f"missing silver input: {path}")

    df = pd.read_parquet(path)
    return df.loc[:, ~df.columns.str.startswith("_")]


"""Grain: one row per product."""
def build_dim_products(silver_dir):

    log.info("dim.products: No transformations applied, it's just a read from silver.products\n")
    return _read_silver(silver_dir, "products")[
        ["product_id", "product_name", "category", "price", "supplier"]
    ]


"""Grain: one row per customer."""
def build_dim_customers(silver_dir):

    log.info("dim.customers: No transformations applied, it's just a read from silver.customers\n")
    return _read_silver(silver_dir, "customers")[
        ["customer_id", "name", "region", "signup_date", "email", "loyalty_points"]
    ]


"""
    Grain: one row per sale.

    Unmatched references surface as nulls rather than dropped rows.
"""
def build_fct_sales(silver_dir, dim_products, dim_customers):

    sales = _read_silver(silver_dir, "sales")
    initial_count = len(sales)

    fct = (sales
           .merge(dim_products[["product_id", "category", "price"]],
                  on="product_id", how="left", validate="many_to_one")
           .merge(dim_customers[["customer_id", "region", "signup_date"]],
                  on="customer_id", how="left", validate="many_to_one"))

    fct["after_signup"] = fct["sale_date"] >= fct["signup_date"]
    fct["unit_price"] = fct["total_amount"] / fct["quantity"]

    log.warning("gold.fct_sales: %d rows, %d without category, %d without region",
             len(fct), fct["category"].isna().sum(), fct["region"].isna().sum())

    final_count = len(fct)
    log.info("gold.fct_sales: From %d to %d rows after building\n", initial_count, final_count)
    return fct


def build_gold(silver_dir, gold_dir):
    """Build the dimensional model into gold_dir. Returns run stats."""
    gold_dir.mkdir(parents=True, exist_ok=True)

    dim_products = build_dim_products(silver_dir)
    dim_customers = build_dim_customers(silver_dir)
    fct_sales = build_fct_sales(silver_dir, dim_products, dim_customers)

    tables = {
        "dim_products": dim_products,
        "dim_customers": dim_customers,
        "fct_sales": fct_sales,
    }

    stats = []
    for name, df in tables.items():
        df.to_parquet(gold_dir / f"{name}.parquet", index=False)
        stats.append({"table": name, "rows": len(df)})

    return pd.DataFrame(stats)