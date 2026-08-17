import logging
from src.config import BRONZE_DIR, SILVER_DIR, GOLD_DIR, RAW_DIR, SOURCES, THRESHOLDS
from src.ingest import build_bronze
from src.silver import build_silver
from src.gold import build_gold
from src.tests import test_gold, test_silver
from src.tests import test_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
)

log = logging.getLogger(__name__)


def main():

    ############# STEP 1 #############
    # Build bronze layer from raw sources
    log.info("====== Starting bronze layer step ======\n")
    bronze_stats = build_bronze(RAW_DIR, BRONZE_DIR, SOURCES)

    # Alert if anomaly rate exceeds threshold
    limit = THRESHOLDS["anomaly_rate"]
    breached = bronze_stats[bronze_stats["anomaly_rate"] > limit]
    if not breached.empty:
        raise ValueError(
            f"Anomaly rate above {limit:.0%}"
        ) # We stop the pipeline and should also send an alert to Slack or email instead of raising an exception.
    test_bronze(BRONZE_DIR, RAW_DIR, SOURCES)

    ############# STEP 2 #############
    # Build silver layer from bronze sources
    print()
    log.info("====== Starting silver layer step ======")
    build_silver(BRONZE_DIR, SILVER_DIR)
    test_silver(SILVER_DIR)

    print()
    log.info("====== Starting gold layer step ======")
    build_gold(SILVER_DIR, GOLD_DIR)
    test_gold(GOLD_DIR, SILVER_DIR)

if __name__ == "__main__":
    main()