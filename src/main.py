import logging
from src.config import BRONZE_DIR, RAW_DIR, SOURCES, THRESHOLDS
from src.ingest import build_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

log = logging.getLogger(__name__)


def main():

    ###### STEP 1 ######
    # Build bronze layer from raw sources
    stats = build_bronze(RAW_DIR, BRONZE_DIR, SOURCES)

    # Alert if anomaly rate exceeds threshold
    limit = THRESHOLDS["anomaly_rate"]
    breached = stats[stats["anomaly_rate"] > limit]
    if not breached.empty:
        raise ValueError(
            f"Anomaly rate above {limit:.0%}"
        ) # We stop the pipeline and should also send an alert to Slack or email instead of raising an exception.


    ###### STEP 2 ######
    # Build silver layer from bronze sources

if __name__ == "__main__":
    main()