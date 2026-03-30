from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

from config import BRONZE_SCHEMA, RETENTION_DAYS, VACUUM_RETAIN_HOURS


def main():
    spark = (
        SparkSession.builder
        .appName("BronzePrune")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Use UTC consistently for timestamp literals & comparisons
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    except Exception:
        pass

    # timezone-aware now in UTC (lint-safe)
    now_utc = datetime.now(timezone.utc)
    cutoff_utc = now_utc - timedelta(days=RETENTION_DAYS)
    cutoff_str = cutoff_utc.strftime("%Y-%m-%d %H:%M:%S")  # interpreted as UTC by Spark (session tz = UTC)

    tables = [
        r.tableName
        for r in spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").collect()
        if getattr(r, "isTemporary", False) is False
    ]

    print(
        f"[PRUNE] Bronze={BRONZE_SCHEMA} retention={RETENTION_DAYS}d vacuum={VACUUM_RETAIN_HOURS}h cutoff(UTC)={cutoff_str}")

    for t in tables:
        fq = f"{BRONZE_SCHEMA}.{t}"
        cols = [r.col_name for r in spark.sql(f"DESCRIBE {fq}").collect() if getattr(r, "col_name", None)]
        if "__ingested_at" not in cols:
            print(f"[PRUNE][skip] {fq} has no __ingested_at; skipping")
            continue

        print(f"[PRUNE][delete] {fq} WHERE __ingested_at < TIMESTAMP '{cutoff_str}'")
        spark.sql(f"DELETE FROM {fq} WHERE __ingested_at < TIMESTAMP '{cutoff_str}'")

        print(f"[PRUNE][vacuum] {fq} RETAIN {VACUUM_RETAIN_HOURS} HOURS")
        spark.sql(f"VACUUM {fq} RETAIN {VACUUM_RETAIN_HOURS} HOURS")

    print("[PRUNE] done.")
    spark.stop()


if __name__ == "__main__":
    main()
