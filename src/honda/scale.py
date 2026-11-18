"""Per-meter Spark tasks for bounded local processing of whole gzip files."""
import argparse
import os
from functools import partial
from pathlib import Path
from shutil import copyfile
from time import perf_counter

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as ac
import pyarrow.parquet as pq

from .panel import spark_session
from .run import Run, completed_input, read_json, sha256, write_json
from .source import BIN, MINUTE, micros

PILOT = ("H1.Z11", "H1.Z12", "H1.Z20", "H1.Z24")
CLOCK_CUTOFF = micros("2020-04-01T00:00:00Z")


def read_power(job):
    name = job["meter"] + ".P"
    reader = ac.open_csv(job["path"], read_options=ac.ReadOptions(use_threads=False, block_size=2**20),
                         convert_options=ac.ConvertOptions(column_types={
                             "datetime_utc": pa.timestamp("us", tz="UTC"), name: pa.float64()}))
    if reader.schema.names != ["datetime_utc", name]:
        raise ValueError(f"Unexpected CSV schema: {job['path']}")
    dates, power, scanned = [], [], 0
    for block in reader:
        stamp = block.column(0)
        if stamp.null_count:
            raise ValueError(f"Missing timestamp: {job['path']}")
        tick = stamp.cast(pa.int64()).to_numpy(zero_copy_only=False)
        values = block.column(1).to_numpy(zero_copy_only=False)
        within = (tick >= job["start"]) & (tick < job["end"])
        scanned += len(tick)
        if within.any():
            dates.append(tick[within])
            power.append(values[within])
    return (np.concatenate(dates), np.concatenate(power), scanned) if dates else (
        np.array([], dtype="int64"), np.array([], dtype="float64"), scanned)


def exclusion_bins(job, count):
    affected = np.zeros(count + 1, dtype="int32")
    start = job["start"]
    for left, right in job["intervals"]:
        a = max(0, (left * 1_000_000 - start) // BIN)
        b = min(count, (right * 1_000_000 - start) // BIN + 1)
        if a < b:
            affected[a] += 1
            affected[b] -= 1
    return affected[:-1].cumsum() > 0


def aggregate(job, ticks, power):
    count = (job["end"] - job["start"]) // BIN
    grid = job["start"] + np.arange(count, dtype="int64") * BIN
    sizes = np.zeros(count, dtype="int64")
    minutes = sizes.copy()
    duplicates = sizes.copy()
    mean = np.zeros(count, dtype="float64")
    labels = np.full(count, "missing_minute", dtype="U24")
    if len(ticks):
        if np.any(ticks[1:] < ticks[:-1]):
            order = ticks.argsort(kind="stable")
            ticks, power = ticks[order], power[order]
        unique, starts, copies = np.unique(ticks, return_index=True, return_counts=True)
        slots = (unique - job["start"]) // BIN
        finite = np.isfinite(power)
        invalid = np.logical_or.reduceat(~finite, starts)
        sums = np.add.reduceat(np.where(finite, power, 0), starts)
        minima = np.minimum.reduceat(np.where(finite, power, np.inf), starts)
        maxima = np.maximum.reduceat(np.where(finite, power, -np.inf), starts)
        conflict = minima != maxima
        exact = unique % MINUTE == 0
        # Sum only one value per timestamp, so identical duplicates carry no extra weight.
        np.add.at(mean, slots, sums / copies)
        np.add.at(sizes, slots, copies)
        np.add.at(minutes, slots[exact], 1)
        same = exact & ~invalid & ~conflict
        np.add.at(duplicates, slots[same], copies[same] - 1)
        labels[minutes == 15] = "valid"
        labels[slots[conflict]] = "conflicting_duplicate"
        labels[slots[invalid]] = "nonfinite"
        labels[slots[~exact]] = "off_minute_grid"
    if job["model"] == "ABB-B24":
        labels[grid < CLOCK_CUTOFF] = "clock_unverified"
    labels[exclusion_bins(job, count)] = "issue_interval"
    valid = labels == "valid"
    return pa.table({
        "meter_id": [job["meter"]] * count, "bin_us": grid,
        "bin_start_utc": pa.array(grid, type=pa.timestamp("us", tz="UTC")),
        "power_w": pa.array(mean / 15, mask=~valid), "valid_ref": valid,
        "minute_count": minutes, "row_count": sizes, "identical_duplicates": duplicates,
        "reason": labels.tolist(), "source_id": [job["meter"] + ":" + job["sha256"][:16]] * count,
    })


def panel_job(job, directory):
    start = perf_counter()
    path = Path(directory) / f"{job['meter']}.parquet"
    if sha256(job["path"]) != job["sha256"]:
        raise ValueError(f"Registered checksum mismatch: {job['path']}")
    ticks, power, scanned = read_power(job)
    ingested = perf_counter()
    panel = aggregate(job, ticks, power)
    if scanned != job["rows"] or pc.sum(panel["row_count"]).as_py() != len(ticks):
        raise ValueError(f"Source counts do not reconcile: {job['meter']}")
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    pq.write_table(panel, temporary, compression="snappy")
    temporary.replace(path)
    return dict(meter=job["meter"], bins=len(panel), selected=len(ticks), scanned=scanned,
                valid=pc.sum(panel["valid_ref"]).as_py(), panel_sha256=sha256(path),
                ingest_s=ingested-start, etl_s=perf_counter()-ingested)


def jobs_for(sources, meters, bounds=None):
    for meter in meters:
        src = sources["channels"][meter]
        if sha256(src["automatic"]) != src["log_sha256"]:
            raise ValueError(f"Registered issue checksum mismatch: {meter}")
        start = micros(src["first"]) // BIN * BIN
        end = (micros(src["last"]) // BIN + 1) * BIN
        if bounds:
            start, end = bounds
        yield dict(src, meter=meter, start=start, end=end)


def build_panels(run, spark, jobs, folder, resume=None):
    folder.mkdir()
    measurements = []
    if resume:
        previous = read_json(resume / "run_manifest.json")
        key = "src/honda/scale.py"
        if (previous["task"] != "T14" or previous["inputs"] != run.info["inputs"]
                or previous["code_sha256"][key] != run.info["code_sha256"][key]):
            raise ValueError("Resume source or panel code differs")
        expected = {job["meter"]: job for job in jobs}
        for item in read_json(resume / "panel_checkpoint.json"):
            meter = item["meter"]
            old_panel = resume / "panel" / f"{meter}.parquet"
            if (meter not in expected or sha256(old_panel) != item["panel_sha256"]
                    or sha256(expected[meter]["path"]) != expected[meter]["sha256"]):
                raise ValueError(f"Invalid checkpoint for {meter}")
            copyfile(old_panel, folder / old_panel.name)
            measurements.append(item)
        resumed = {item["meter"] for item in measurements}
        jobs = [job for job in jobs if job["meter"] not in resumed]
    checkpoint = folder.parent / f"{folder.name}_checkpoint.json"
    temporary = checkpoint.with_suffix(".tmp")
    for start in range(0, len(jobs), 8):
        batch = jobs[start:start+8]
        results = (spark.sparkContext.parallelize(batch, len(batch))
                   .map(partial(panel_job, directory=str(folder))).collect())
        measurements.extend(results)
        write_json(temporary, measurements)
        temporary.replace(checkpoint)
    if not checkpoint.exists():
        write_json(checkpoint, measurements)
    return measurements


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("task", choices=["T12", "T13", "T14", "T15"])
    parser.add_argument("run", type=Path)
    parser.add_argument("--inventory", type=Path, default=Path("runs/t11_inventory"))
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--benchmark", type=Path)
    parser.add_argument("--prior")
    parser.add_argument("--resume-from", type=Path)
    args = parser.parse_args()
    with Run(args.run, args.task, prior=args.prior) as run:
        import sys
        run.info["command"] = [sys.executable, "-m", "honda.scale", *sys.argv[1:]]
        source = completed_input(run, args.inventory, "T11", ["sources.json"])
        catalog = read_json(source / "sources.json")
        for path, digest in catalog["manual"].items():
            if sha256(path) != digest:
                raise ValueError(f"Registered manual issue checksum mismatch: {path}")
        if args.task == "T12":
            from .scale_eval import pilot
            with spark_session(run) as spark:
                pilot(run, spark, catalog)
        elif args.task == "T13":
            from .scale_benchmark import benchmark
            with spark_session(run) as spark:
                benchmark(run, spark, catalog)
        elif args.task == "T14":
            from .scale_eval import full_run
            with spark_session(run) as spark:
                full_run(run, spark, catalog, args.resume_from)
        else:
            from .scale_report import report
            report(run, source, args.previous, args.benchmark)


if __name__ == "__main__":
    main()
