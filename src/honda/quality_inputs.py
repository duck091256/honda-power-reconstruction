"""Input contract and Q01 lock for the natural-quality map."""
import csv
import os
import platform
import socket
import subprocess
import sys
from collections import Counter
from importlib.metadata import version
from pathlib import Path
from shutil import copyfile, disk_usage

import duckdb
import psutil

from .run import completed_input, read_json, sha256, write_json
from .source import BIN, micros


class Checks:
    def __init__(self, path):
        self.path = path
        self.rows = []

    def check(self, name, expected, actual, evidence):
        passed = actual == expected
        self.rows.append(dict(name=name, expected=expected, actual=actual,
                              status="PASS" if passed else "FAIL", evidence=str(evidence)))
        write_json(self.path, self.rows)
        if not passed:
            raise ValueError(f"{name}: expected {expected!r}, got {actual!r}")


def machine():
    java = Path(os.environ["JAVA_HOME"]) / "bin/java"
    cpu = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")), platform.processor())
    return dict(host=socket.gethostname(), cpu=cpu, logical_cpus=os.cpu_count(),
                cpu_affinity=sorted(os.sched_getaffinity(0)), platform=platform.platform(),
                ram_bytes=psutil.virtual_memory().total, python=sys.version,
                java=subprocess.check_output([java, "-version"], stderr=subprocess.STDOUT, text=True).splitlines(),
                pyspark=version("pyspark"), duckdb=version("duckdb"),
                pyarrow=version("pyarrow"))


def sources(run, checks, inventory, full):
    try:
        inventory = completed_input(run, inventory, "T11", ["sources.json"])
        full = completed_input(run, full, "T14", ["panel", "coverage.csv"])
    except (ValueError, OSError) as exc:
        checks.check("registered T11/T14 inputs", "verified", str(exc), [str(inventory), str(full)])
    checks.check("registered T11/T14 inputs", "verified", "verified", "run_manifest.json")
    return inventory, full


def read_coverage(full):
    with (full / "coverage.csv").open() as stream:
        return {(r["meter_id"], r["month_utc"], r["reason"]): (int(r["bins"]), int(r["input_rows"]))
                for r in csv.DictReader(stream)}


def lock(run, checks, inventory, full, config_path):
    config = read_json(config_path)
    copyfile(config_path, run.path / "quality_config.json")
    run.info["config_sha256"] = sha256(run.path / "quality_config.json")
    inventory, full = sources(run, checks, inventory, full)
    catalog = read_json(inventory / "sources.json")["channels"]
    files = {p.stem: p for p in (full / "panel").glob("*.parquet")}
    checks.check("panel files and catalog", sorted(catalog), sorted(files), full / "panel")
    checks.check("registered channel count", 71, len(files), full / "panel")
    expected = read_coverage(full)
    observed, bounds = {}, {}
    with duckdb.connect(config={"threads": str(config["threads"]),
                                "memory_limit": config["duckdb_memory_limit"],
                                "temp_directory": str(run.path / "scratch")}) as db:
        for meter, path in sorted(files.items()):
            first = micros(catalog[meter]["first"]) // BIN * BIN
            end = (micros(catalog[meter]["last"]) // BIN + 1) * BIN
            cells = (end - first) // BIN
            proof = db.execute(f"""
                SELECT count(*), count(DISTINCT bin_us), min(bin_us), max(bin_us),
                       count(*) FILTER (WHERE (meter_id=? AND bin_us%{BIN}=0
                           AND bin_us=epoch_us(bin_start_utc) AND reason IS NOT NULL
                           AND valid_ref=(reason='valid') AND row_count>=0) IS NOT TRUE)
                FROM read_parquet(?)
            """, [meter, str(path)]).fetchone()
            checks.check(f"{meter} unique, aligned, contiguous, bounds and flags",
                         [cells, cells, first, end - BIN, 0], list(proof), path)
            counts = db.execute("""
                SELECT strftime(make_timestamp(bin_us), '%Y-%m'), reason, count(*), sum(row_count)
                FROM read_parquet(?) GROUP BY 1, 2 ORDER BY 1, 2
            """, [str(path)]).fetchall()
            observed.update({(meter, month, reason): (bins, rows) for month, reason, bins, rows in counts})
            bounds[meter] = dict(bins=cells, start_us=first, end_us=end, panel_bytes=path.stat().st_size,
                                 log_intervals=len(catalog[meter]["intervals"]))
    differences = sorted(key for key in expected.keys() | observed.keys()
                         if expected.get(key) != observed.get(key))
    if differences:
        key = differences[0]
        write_json(run.path / "first_mismatch.json",
                   dict(key=key, expected=expected.get(key), actual=observed.get(key)))
    with (run.path / "coverage_rebuilt.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["meter_id", "month_utc", "reason", "bins", "input_rows"])
        writer.writerows((*key, *value) for key, value in sorted(observed.items()))
    checks.check("coverage groups and source row counts", 0, len(differences), "coverage_rebuilt.csv")
    by_reason = Counter()
    for (_, _, reason), (bins, _) in observed.items():
        by_reason[reason] += bins
    checks.check("known T14 counts by reason", config["bins_by_reason"], dict(by_reason),
                 "coverage_rebuilt.csv")
    checks.check("total T14 grid cells", 11_906_542, sum(by_reason.values()), "coverage_rebuilt.csv")
    ordered = sorted(config["pilot"]) + sorted(catalog.keys() - set(config["pilot"]))
    levels = {}
    for count in config["levels"]:
        meters = sorted(ordered[:count])
        levels[str(count)] = dict(meters=meters, bins=sum(bounds[m]["bins"] for m in meters),
                                  panel_bytes=sum(bounds[m]["panel_bytes"] for m in meters),
                                  log_intervals=sum(bounds[m]["log_intervals"] for m in meters))
    resources = dict(available_ram_bytes=psutil.virtual_memory().available,
                     free_workspace_disk_bytes=disk_usage(run.path).free)
    checks.check("capacity for locked worker tree and scratch", [True, True],
                 [resources["available_ram_bytes"] >= config["tree_rss_limit_bytes"],
                  resources["free_workspace_disk_bytes"] >= config["scratch_limit_bytes"]],
                 "lock.json")
    write_json(run.path / "lock.json", dict(machine=machine(), resources=resources,
                                            config_sha256=run.info["config_sha256"],
                                            levels=levels, meters=bounds))
    run.info.update(levels=levels, rows=sum(by_reason.values()))


def locked_inputs(run, checks, lock_path, inventory, full):
    base = completed_input(run, lock_path, "Q01", ["lock.json", "quality_config.json"])
    config = read_json(base / "quality_config.json")
    locked = read_json(base / "lock.json")
    config_sha = sha256(base / "quality_config.json")
    run.info["config_sha256"] = config_sha
    checks.check("locked config SHA-256", locked["config_sha256"], config_sha, base / "lock.json")
    inventory, full = sources(run, checks, inventory, full)
    registered = read_json(base / "run_manifest.json")["inputs"]
    actual = {name: run.info["inputs"][str(p)] for name, p in (("T11", inventory), ("T14", full))}
    # Resolve by artifact name so a verified input may be relocated.
    expected = {("T11" if "sources.json" in record["artifacts_sha256"] else "T14"): record
                for record in registered.values()}
    checks.check("T11/T14 identities match Q01", True, actual == expected, base / "run_manifest.json")
    return config, locked, inventory / "sources.json", full / "panel"


def log_rows(catalog_path, meters):
    channels = read_json(catalog_path)["channels"]
    return [(meter, index, int(a) * 1_000_000, int(b) * 1_000_000)
            for meter in meters for index, (a, b) in enumerate(channels[meter]["intervals"])]
