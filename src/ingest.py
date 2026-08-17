import pandas as pd
import json
import logging

log = logging.getLogger(__name__)

ANOMALY_COLUMNS = ["batch_id", "row_num", "source", "issue", "detail", "payload"]


MAX_UNWRAP_DEPTH = 5

def _unwrap(item):
    """Undo transport-level malformations. Returns (obj, issue, detail)."""
    issues, keys = [], []

    for _ in range(MAX_UNWRAP_DEPTH):
        if isinstance(item, str):
            try:
                item = json.loads(item)
            except json.JSONDecodeError:
                return None, "invalid_json", None
            issues.append("double_serialised")

        elif isinstance(item, dict) and len(item) == 1:
            key, value = next(iter(item.items()))
            if not isinstance(value, (dict, str)):
                break
            keys.append(key)
            issues.append("wrapped_in_key")
            item = value

        else:
            break

    if not isinstance(item, dict):
        return None, f"unexpected_type:{type(item).__name__}", None

    if not issues:
        return item, None, None

    return item, "+".join(dict.fromkeys(issues)), ".".join(keys) or None


def load_json(path, source, fields, batch_id):
    with open(path, encoding="utf-8-sig") as f:
        raw = json.load(f)

    ingested_at = pd.Timestamp.now(tz="UTC")
    rows, anomalies = [], []

    for i, item in enumerate(raw):
        obj, issue, detail = _unwrap(item)

        if issue:
            anomalies.append({
                "batch_id": batch_id,
                "row_num": i,
                "source": source,
                "issue": issue,
                "detail": detail,
                "payload": json.dumps(item)[:300],
            })

        rows.append({
            **{c: str(obj[c]) if obj and c in obj else None for c in fields}, # Fill missing fields with None for each key in case the object is missing that key
            "_batch_id": batch_id,
            "_row_num": i,
            "_source_file": path.name,
            "_ingested_at": ingested_at,
            "_decoded": obj is not None,
        })

    return pd.DataFrame(rows), pd.DataFrame(anomalies, columns=ANOMALY_COLUMNS)


def load_csv(path, source, fields, batch_id):
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()

    if fields:
        df = df.reindex(columns=fields)

    df["_batch_id"] = batch_id
    df["_row_num"] = range(len(df))
    df["_source_file"] = path.name
    df["_ingested_at"] = pd.Timestamp.now(tz="UTC")
    df["_decoded"] = True

    return df, pd.DataFrame(columns=ANOMALY_COLUMNS)


LOADERS = {".json": load_json, ".csv": load_csv}

"""Land every configured source to bronze_dir. Returns run stats."""
def build_bronze(raw_dir, bronze_dir, sources):

    # Create the bronze directory if it doesn't exist
    bronze_dir.mkdir(parents=True, exist_ok=True)
    batch_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    anomalies, stats = [], []

    for name, cfg in sources.items():
        path = raw_dir / cfg["file"]
        loader = LOADERS.get(path.suffix)
        if loader is None:
            raise ValueError(f"{name}: unsupported format '{path.suffix}'")

        df, anom = loader(path, name, cfg.get("fields"), batch_id)
        df.to_parquet(bronze_dir / f"{name}.parquet", index=False)

        anomalies.append(anom)
        stats.append({
            "source": name,
            "rows": len(df),
            "anomalies": len(anom),
            "anomaly_rate": len(anom) / len(df) if len(df) else 0.0,
        })
        log.info("bronze.%s: %d rows, %d anomalies", name, len(df), len(anom))

    pd.concat(anomalies, ignore_index=True).to_parquet(
        bronze_dir / "quarantine.parquet", index=False
    )

    return pd.DataFrame(stats)