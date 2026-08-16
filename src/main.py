from pathlib import Path

from src.ingest import build_bronze

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
OUT = ROOT / "output"


def main():
    OUT.mkdir(exist_ok=True)

    bronze, anomalies = build_bronze(RAW)

    for name, df in bronze.items():
        print(f"{name}: {len(df)} rows")

    if not anomalies.empty:
        print(anomalies.groupby("issue").size().to_string())
        anomalies.groupby("issue").head(3).to_csv(OUT / "anomaly_samples.csv", index=False)


if __name__ == "__main__":
    main()