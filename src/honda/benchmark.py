import hashlib
import json
from statistics import median
from pathlib import Path
from datetime import UTC, datetime
from time import perf_counter

from pyspark.sql import functions as F

from .panel import spark_session
from .run import completed_input, read_json, sha256, write_json
from .source import micros


def rows_and_digest(rows):
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(tuple(row), ensure_ascii=False, allow_nan=False,
                                 separators=(",", ":")).encode() + b"\n")
    return {"rows": len(rows), "digest_sha256": digest.hexdigest()}


def ordered_rows(frame, columns):
    # Render the Spark timestamp in the Spark session timezone (UTC).
    return (frame.withColumn("bin_start_utc", F.date_format("bin_start_utc", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
            .select(*columns).orderBy("meter_id", "bin_us").collect())


def run_benchmark(run, panel_run, eval_run):
    dataset = completed_input(run, panel_run, "T05", ["panel_15m", "source_manifest.json"])
    evaluation = completed_input(run, eval_run, "T06", ["evaluation_config.json"])
    dependency = read_json(evaluation / "run_manifest.json")["inputs"]
    if str(dataset) not in dependency or dependency[str(dataset)] != run.info["inputs"][str(dataset)]:
        raise ValueError("Benchmark must read the same T05 panel as T06")
    locked = evaluation / "evaluation_config.json"
    cfg = read_json(locked)["benchmark"]
    first, after = (micros(cfg[key]) for key in ("start_utc", "end_utc"))
    sources = read_json(dataset / "source_manifest.json")
    if (not sources["start_us"] <= first < after <= sources["end_us"]
            or cfg["meter_id"] not in {m["meter_id"] for m in sources["meters"]}
            or cfg["warmups_per_layout"] != 1 or cfg["timed_per_layout"] != 3):
        raise ValueError("Benchmark config does not match the panel or the 1+3 protocol")
    run.info["config_sha256"] = sha256(locked)
    cost = {
        "query": cfg,
        "scope": [sources["start_us"], sources["end_us"]],
        "source_compressed_bytes": sum(m["bytes_power_gzip"] for m in sources["meters"]),
        "source_rows_in_scope": sum(m["row_counts"]["selected"] for m in sources["meters"]),
        "cache": "No Spark persist; fresh DataFrame per query; JVM/OS cache uncontrolled after one warmup",
        "bytes_read": "[chưa đo]",
        "layouts": {},
    }
    meminfo = Path("/proc/meminfo")
    if meminfo.is_file():
        cost["host_memory"] = {line.split(":")[0]: line.split(":")[1].strip()
                               for line in meminfo.read_text().splitlines()
                               if line.startswith(("MemTotal:", "MemAvailable:"))}
    with spark_session(run) as spark:
        cost["spark"] = run.info["spark"]
        cost["cpu_count"] = run.info["cpu_count"]
        base = (spark.read.parquet(str(dataset / "panel_15m"))
                .withColumn("year", F.year("bin_start_utc"))
                .withColumn("month", F.month("bin_start_utc")))
        names = base.columns
        shape = [(f.name, f.dataType) for f in base.schema]
        base_result = rows_and_digest(ordered_rows(base, names))
        layouts = ("plain", "partitioned")
        for layout in layouts:
            location = run.path / layout
            writer = (base.repartition(cfg["write_partitions"]).write
                      .option("compression", cfg["codec"]))
            if layout == "partitioned":
                writer = writer.partitionBy("year", "month")
            started = perf_counter()
            writer.parquet(str(location))
            created = perf_counter() - started
            files = list(location.rglob("*.parquet"))
            cost["layouts"][layout] = {"create_s": created, "files": len(files),
                                       "bytes": sum(f.stat().st_size for f in files), "runs": []}

        beginning, last = (datetime.fromtimestamp(t // 1_000_000, UTC) for t in (first, after - 1))
        month_start, month_end = (t.year * 100 + t.month for t in (beginning, last))
        for i in range(cfg["warmups_per_layout"] + cfg["timed_per_layout"]):
            for layout in layouts if i % 2 == 0 else layouts[::-1]:
                started = perf_counter()
                frame = spark.read.parquet(str(run.path / layout))
                month_key = F.col("year") * 100 + F.col("month")
                filtered = frame.where(
                    (F.col("meter_id") == cfg["meter_id"]) & (F.col("bin_us") >= first)
                    & (F.col("bin_us") < after) & month_key.between(month_start, month_end)
                )
                measured = ordered_rows(filtered, names)
                seconds = perf_counter() - started
                cost["layouts"][layout]["runs"].append({
                    "phase": "warmup" if i == 0 else "timed", "seconds": seconds,
                    **rows_and_digest(measured),
                })
        outcomes = set()
        for layout in layouts:
            frame = spark.read.parquet(str(run.path / layout)).select(*names)
            item = cost["layouts"][layout]
            item["whole_panel"] = rows_and_digest(ordered_rows(frame, names))
            if [(f.name, f.dataType) for f in frame.schema] != shape or item["whole_panel"] != base_result:
                raise ValueError(f"{layout} panel rows or logical schema differ")
            item["median_query_s"] = median(r["seconds"] for r in item["runs"] if r["phase"] == "timed")
            outcomes.update((r["rows"], r["digest_sha256"]) for r in item["runs"])
        if len(outcomes) != 1 or not next(iter(outcomes))[0]:
            raise ValueError(f"Filtered queries are empty or different: {outcomes}")
        cost["panel"] = base_result
    write_json(run.path / "cost.json", cost)
    run.info.update(panel_rows=base_result["rows"], panel_digest=base_result["digest_sha256"],
                    query_rows=next(iter(outcomes))[0])
