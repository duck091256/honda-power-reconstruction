from contextlib import contextmanager
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from .run import read_json
from .source import BIN, MINUTE


@contextmanager
def spark_session(run):
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(f"Honda-{run.task}")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.dir", str(run.path / "scratch"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    jvm = spark.sparkContext._jvm
    run.info["spark"] = {
        "version": spark.version, "master": spark.sparkContext.master,
        "java_version": jvm.java.lang.System.getProperty("java.runtime.version"),
        "heap_max_bytes": jvm.java.lang.Runtime.getRuntime().maxMemory(),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "timezone": spark.conf.get("spark.sql.session.timeZone"),
    }
    try:
        yield spark
    finally:
        spark.stop()


def csv_table(spark, paths, schema):
    paths = [str(path) for path in paths]
    return spark.read.csv(paths, schema=schema, header=True, mode="FAILFAST")


def transform(spark, staged):
    source = read_json(staged / "source_manifest.json")
    sources = [(m["meter_id"], m["source_id"]) for m in source["meters"]]
    meters = spark.createDataFrame(sources, "meter_id STRING, source_id STRING")
    grid = spark.range(source["start_us"], source["end_us"], BIN).withColumnRenamed("id", "bin_us")
    minute = csv_table(spark, sorted(staged.glob("source_*.csv")),
                       "meter_id STRING, epoch_us LONG, power_w DOUBLE, source_id STRING")
    issues = csv_table(spark, [staged / "issue_bins.csv"], "meter_id STRING, bin_us LONG")
    finite = (F.col("power_w").isNotNull() & ~F.isnan("power_w")
              & (F.abs("power_w") < float("inf")))
    normalized = minute.withColumn("finite_w", F.when(finite, F.col("power_w")))
    by_tick = normalized.groupBy("meter_id", "epoch_us").agg(
        F.count("*").alias("rows_at_tick"),
        F.count("finite_w").alias("finite_rows"),
        F.min("finite_w").alias("min_w"),
        F.max("finite_w").alias("max_w"),
    )
    at_minute = F.pmod("epoch_us", F.lit(MINUTE)) == 0
    by_bin = by_tick.withColumn(
        "bin_us", F.col("epoch_us") - F.pmod("epoch_us", F.lit(BIN))
    ).groupBy("meter_id", "bin_us").agg(
        F.count(F.when(at_minute, 1)).alias("minute_count"),
        F.sum("rows_at_tick").alias("row_count"),
        F.max((~at_minute).cast("int")).alias("off_grid"),
        F.max((F.col("finite_rows") != F.col("rows_at_tick")).cast("int")).alias("nonfinite"),
        F.max(F.coalesce((F.col("min_w") != F.col("max_w")).cast("int"), F.lit(0))).alias("conflict"),
        F.sum(F.when(at_minute & (F.col("min_w") == F.col("max_w"))
                     & (F.col("finite_rows") == F.col("rows_at_tick")),
                     F.col("rows_at_tick") - 1).otherwise(0)).alias("identical_duplicates"),
        F.avg("min_w").alias("mean_w"),
    )
    panel = (
        grid.crossJoin(F.broadcast(meters))
        .join(by_bin, ["meter_id", "bin_us"], "left")
        .join(issues.withColumn("issue", F.lit(True)), ["meter_id", "bin_us"], "left")
        .fillna(0, subset=["minute_count", "row_count", "identical_duplicates"])
        .withColumn("reason", F.when(F.col("issue"), "issue_interval")
                    .when(F.col("off_grid") == 1, "off_minute_grid")
                    .when(F.col("nonfinite") == 1, "nonfinite")
                    .when(F.col("conflict") == 1, "conflicting_duplicate")
                    .when(F.col("minute_count") != 15, "missing_minute")
                    .otherwise("valid"))
        .withColumn("valid_ref", F.col("reason") == "valid")
    )
    return panel.select(
        "meter_id", "bin_us", F.expr("timestamp_micros(bin_us)").alias("bin_start_utc"),
        F.when(F.col("valid_ref"), F.col("mean_w")).alias("power_w"),
        "valid_ref", "minute_count", "row_count", "identical_duplicates", "reason", "source_id",
    )


def save_panel(spark, run, staged: Path):
    source = read_json(staged / "source_manifest.json")
    output = run.path / "panel_15m"
    transform(spark, staged).write.option("compression", "snappy").parquet(str(output))
    panel = spark.read.parquet(str(output))
    coverage = (panel.withColumn("month_utc", F.date_format("bin_start_utc", "yyyy-MM"))
                .groupBy("meter_id", "month_utc", "reason")
                .agg(F.count("*").alias("bins"), F.sum("row_count").alias("input_rows"))
                .orderBy("meter_id", "month_utc", "reason"))
    rows = coverage.collect()
    expected_bins = (source["end_us"] - source["start_us"]) // BIN * len(source["meters"])
    total = sum(row.bins for row in rows)
    if (total != expected_bins
            or panel.select("meter_id", "bin_us").distinct().count() != total
            or sum(row.input_rows for row in rows) != sum(
                m["row_counts"].get("selected", 0) for m in source["meters"])):
        raise ValueError("Panel keys, grid size, or minute counts do not reconcile")
    from .source import csv_rows
    csv_rows(run.path / "coverage.csv", coverage.columns, rows)
    run.info.update(panel_rows=total,
                    valid_bins=sum(row.bins for row in rows if row.reason == "valid"),
                    parquet_bytes=sum(p.stat().st_size for p in output.glob("*.parquet")))
    return panel
