"""Bronze layer: land raw sources as text, with lineage. No casting, no dedup."""

import json
from pathlib import Path

import pandas as pd

SALES_FIELDS = ["SaleID", "ProductID", "CustomerID", "Quantity", "TotalAmount", "SaleDate"]
ANOMALY_COLUMNS = ["row_num", "source", "issue", "payload"]


def _unwrap(item):
    """Undo transport-level malformations. Returns (obj, issue)."""
    if isinstance(item, str):
        try:
            return json.loads(item), "double_serialised"
        except json.JSONDecodeError:
            return None, "invalid_json"

    if isinstance(item, dict) and len(item) == 1:
        key, value = next(iter(item.items()))
        if isinstance(value, dict):
            return value, f"wrapped_in:{key}"

    if isinstance(item, dict):
        return item, None

    return None, f"unexpected_type:{type(item).__name__}"


def load_json(path, fields, source):
    with open(path, encoding="utf-8-sig") as f:
        raw = json.load(f)

    ingested_at = pd.Timestamp.now(tz="UTC")
    rows, anomalies = [], []

    for i, item in enumerate(raw):
        obj, issue = _unwrap(item)
        if issue:
            anomalies.append({
                "row_num": i,
                "source": source,
                "issue": issue,
                "payload": json.dumps(item)[:300],
            })

        rows.append({
            **{c: str(obj[c]) if obj and c in obj else None for c in fields},
            "_row_num": i,
            "_source_file": path.name,
            "_ingested_at": ingested_at,
        })

    return pd.DataFrame(rows), pd.DataFrame(anomalies, columns=ANOMALY_COLUMNS)


def load_csv(path, source):
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    df["_row_num"] = range(len(df))
    df["_source_file"] = path.name
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC")
    return df, pd.DataFrame(columns=ANOMALY_COLUMNS)


def build_bronze(raw_dir):
    sales, sales_anom = load_json(raw_dir / "sales_data.json", SALES_FIELDS, "sales")
    # customers, cust_anom = load_json(raw_dir / "customers.json", CUSTOMER_FIELDS, "customers")
    # products, prod_anom = load_csv(raw_dir / "products.csv", "products")

    anomalies = pd.concat([sales_anom], ignore_index=True)
    return {"sales": sales}, anomalies