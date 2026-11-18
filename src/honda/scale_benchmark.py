"""Matched workloads for local Spark workers and DuckDB."""
from statistics import median
from threading import Event, Thread
from time import perf_counter

import duckdb
import psutil
import pyarrow as pa
import pyarrow.parquet as pq

from .reproduce import compare
from .run import read_json, sha256, write_json
from .scale import PILOT, build_panels, exclusion_bins, jobs_for
from .source import BIN, micros


class ProcessMemory:
    def __init__(self, include_children):
        self.include_children = include_children

    def __enter__(self):
        self.close = Event()
        self.peak_bytes = 0
        self.worker = Thread(target=self.measure, daemon=True)
        self.worker.start()
        return self

    def measure(self):
        proc = psutil.Process()
        while not self.close.is_set():
            total = 0
            children = proc.children(recursive=True) if self.include_children else []
            for child in [proc, *children]:
                try:
                    total += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            self.peak_bytes = max(total, self.peak_bytes)
            self.close.wait(0.05)

    def __exit__(self, *_):
        self.close.set()
        self.worker.join()


def duck_panel(conn, job, folder):
    start = perf_counter()
    if sha256(job["path"]) != job["sha256"]:
        raise ValueError(f"Registered checksum mismatch: {job['path']}")
    conn.execute("""
        CREATE OR REPLACE TEMP TABLE selected AS
        SELECT epoch_us(datetime_utc) AS tick, power_w FROM read_csv(?, header=true,
          auto_detect=false, compression='gzip',
          columns={'datetime_utc':'TIMESTAMPTZ','power_w':'DOUBLE'})
        WHERE tick >= ? AND tick < ?
    """, [job["path"], job["start"], job["end"]])
    ingest = perf_counter()
    conn.register("issue_flag", pa.table({"issue": exclusion_bins(
        job, (job["end"] - job["start"]) // BIN)}))
    values = conn.execute("""
        WITH ticks AS (
          SELECT tick, count(*) AS copies, count(*) FILTER (WHERE isfinite(power_w)) AS finite_count,
                 min(power_w) FILTER (WHERE isfinite(power_w)) AS lo,
                 max(power_w) FILTER (WHERE isfinite(power_w)) AS hi
          FROM selected GROUP BY tick
        ), bins AS (
          SELECT tick - tick%900000000 AS bin_us, sum(copies)::BIGINT AS row_count,
                 count(*) FILTER (WHERE tick%60000000=0) AS minute_count,
                 sum(CASE WHEN tick%60000000=0 AND lo=hi AND copies=finite_count
                          THEN copies-1 ELSE 0 END)::BIGINT AS identical_duplicates,
                 avg(lo) AS avg_w, bool_or(tick%60000000<>0) AS off_grid,
                 bool_or(copies<>finite_count) AS nonfinite,
                 bool_or(lo<>hi) AS conflict
          FROM ticks GROUP BY 1
        ), grid AS (
          SELECT range AS bin_us, ordinality AS i FROM range(?, ?, 900000000) WITH ORDINALITY
        ), flags AS (
          SELECT row_number() OVER () AS i, issue FROM issue_flag
        ), quality AS (
          SELECT g.bin_us, avg_w, coalesce(minute_count,0) AS minute_count,
                 coalesce(row_count,0) AS row_count, coalesce(identical_duplicates,0) AS identical_duplicates,
                 CASE WHEN issue THEN 'issue_interval' WHEN off_grid THEN 'off_minute_grid'
                      WHEN nonfinite THEN 'nonfinite' WHEN conflict THEN 'conflicting_duplicate'
                      WHEN minute_count=15 THEN 'valid' ELSE 'missing_minute' END AS reason
          FROM grid g JOIN flags f USING(i) LEFT JOIN bins b USING(bin_us)
        )
        SELECT ? AS meter_id, bin_us, make_timestamptz(bin_us) AS bin_start_utc,
               CASE WHEN reason='valid' THEN avg_w END AS power_w, reason='valid' AS valid_ref,
               minute_count, row_count, identical_duplicates, reason, ? AS source_id
        FROM quality ORDER BY bin_us
    """, [job["start"], job["end"], job["meter"], job["meter"]+":"+job["sha256"][:16]]).fetch_arrow_table()
    conn.unregister("issue_flag")
    pq.write_table(values, folder / f"{job['meter']}.parquet", compression="snappy")
    return dict(ingest_s=ingest-start, etl_s=perf_counter()-ingest,
                selected=sum(values["row_count"].to_pylist()), scanned=job["rows"])


QUERY = """SELECT meter_id, bin_us, power_w, reason FROM bench_panel
           WHERE meter_id=? AND bin_us>=? AND bin_us<? ORDER BY bin_us"""


def run_workload(engine, run, spark, jobs, folder, query):
    start = perf_counter()
    with ProcessMemory(include_children=engine == "Spark") as monitor:
        if engine == "Spark":
            stats = build_panels(run, spark, jobs, folder)
        else:
            folder.mkdir()
            with duckdb.connect(config={"threads":2, "memory_limit":"512MB",
                       "temp_directory":str(run.path / "scratch" / "duck_spill")}) as conn:
                stats = [duck_panel(conn, job, folder) for job in jobs]
        done_etl = perf_counter()
        if engine == "Spark":
            spark.read.parquet(str(folder)).createOrReplaceTempView("bench_panel")
            rows = [tuple(row) for row in spark.sql(QUERY, list(query)).collect()]
        else:
            with duckdb.connect(config={"threads":2}) as conn:
                conn.read_parquet(str(folder / "*.parquet")).create_view("bench_panel")
                rows = conn.execute(QUERY, query).fetchall()
        done_query = perf_counter()
    return dict(engine=engine, wall_s=done_etl-start, query_s=done_query-done_etl,
                peak_tree_rss_bytes=monitor.peak_bytes, query_rows=len(rows),
                selected_rows=sum(s["selected"] for s in stats),
                scanned_rows=sum(s["scanned"] for s in stats),
                gzip_bytes=sum(job["file_bytes"] for job in jobs),
                ingest_worker_s=sum(s["ingest_s"] for s in stats),
                etl_worker_s=sum(s["etl_s"] for s in stats),
                parquet_bytes=sum(p.stat().st_size for p in folder.glob("*.parquet"))), rows


def check_outputs(folders, jobs):
    for job in jobs:
        name = f"{job['meter']}.parquet"
        columns = ["meter_id","bin_us","power_w","valid_ref","minute_count",
                   "row_count","identical_duplicates","reason","source_id"]
        a,b = (pq.read_table(folders[engine] / name, columns=columns).to_pylist()
               for engine in ("Spark","DuckDB"))
        compare(a,b,{"relative":1e-12,"absolute":1e-8},name)


def benchmark(run, spark, catalog):
    cfg = read_json("configs/scaling_v2.json")
    run.info["config_sha256"] = sha256("configs/scaling_v2.json")
    run.info["duckdb_version"] = duckdb.__version__
    run.info["memory_metric"] = "50ms sampled RSS of controller, plus JVM and child workers for Spark"
    start = micros(cfg["start"])
    ends = [micros(end) for end in cfg["ends"]]
    extras = sorted(m for m, spec in catalog["channels"].items() if m not in PILOT
                    and spec["model"] == "ABB-B24" and micros(spec["first"]) <= start
                    and micros(spec["last"]) >= ends[-1] - 86_400_000_000)
    meters = list(PILOT) + extras
    query = (cfg["query"][0], *(micros(t) for t in cfg["query"][1:]))
    workloads = [(4,end) for end in ends] + [(n,ends[-1]) for n in cfg["counts"] if n>4]
    results = []
    for step, (count,end) in enumerate(workloads, 1):
        if len(meters) < count:
            raise ValueError(f"Only {len(meters)} meters span the scaling period")
        jobs = list(jobs_for(catalog, meters[:count], (start,end)))
        for repeat in range(cfg["repeats"]):
            folders, selected = {}, {}
            engines = ("Spark","DuckDB") if repeat % 2 == 0 else ("DuckDB","Spark")
            for engine in engines:
                folder = run.path / f"w{step}_r{repeat}_{engine.lower()}"
                folders[engine] = folder
                item, selected[engine] = run_workload(engine, run, spark, jobs, folder, query)
                results.append(dict(workload=step, repeat=repeat+1, channels=count,
                                    start_us=start, end_us=end, **item))
            check_outputs(folders, jobs)
            compare(selected["Spark"], selected["DuckDB"],
                    {"relative":1e-12,"absolute":1e-8}, "benchmark query")
            write_json(run.path / "measurements.json", results)
    summary = []
    for step, (count,end) in enumerate(workloads, 1):
        for engine in ("Spark","DuckDB"):
            subset = [r for r in results if r["workload"]==step and r["engine"]==engine]
            item = {name:median(r[name] for r in subset) for name in (
                "wall_s","query_s","peak_tree_rss_bytes","selected_rows","scanned_rows",
                "gzip_bytes","ingest_worker_s","etl_worker_s","parquet_bytes","query_rows")}
            summary.append(dict(workload=step,channels=count,engine=engine,
                                start_us=start,end_us=end,selected_rows_per_s=item["selected_rows"]/item["wall_s"], **item))
    write_json(run.path / "scaling.json", summary)
    run.info.update(workloads=len(workloads), measurements=len(results), compared=True)
