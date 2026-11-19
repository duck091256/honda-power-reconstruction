"""Q03 full-scale build and Q04 fair, serial paired measurements."""
import os
import signal
import subprocess
import sys
from collections import Counter
from pathlib import Path
from shutil import disk_usage
from statistics import median
from time import monotonic, sleep

import psutil

from .quality_audit import PanelAudit, compare_maps, signature, verify_map
from .quality_inputs import locked_inputs, machine, read_coverage
from .run import completed_input, read_json, sha256, timestamp, write_json


def temp_bytes(path):
    total = 0
    try:
        with os.scandir(path) as files:
            for file in files:
                if file.is_dir(follow_symlinks=False):
                    total += temp_bytes(file.path)
                else:
                    try:
                        total += file.stat(follow_symlinks=False).st_blocks * 512
                    except FileNotFoundError:
                        continue
    except FileNotFoundError:
        pass
    return total


def tree_rss(process):
    rss = 0
    try:
        tree = [process, *process.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0
    for proc in tree:
        try:
            rss += proc.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return rss


def measure(folder, engine, panel, catalog, scope, config, query_keys=(), prior=None, audit_keys=()):
    folder.mkdir(parents=True)
    scratch = folder / "scratch"
    scratch.mkdir()
    job = dict(engine=engine, level=len(scope["meters"]), panel=str(panel), catalog=str(catalog),
               meters=scope["meters"], config=config, queries=query_keys, audit_keys=audit_keys)
    write_json(folder / "job.json", job)
    environment = os.environ.copy()
    environment.update(TMPDIR=str(scratch), OMP_NUM_THREADS="2", OPENBLAS_NUM_THREADS="2",
                       ARROW_NUM_THREADS="2")
    command = [sys.executable, "-m", "honda.quality_worker", str(folder)]
    phase_path = folder / "phase.json"
    started, clock = timestamp(), monotonic()
    last_phase = dict(name="startup", started=clock)
    phase_limits = {"startup": config["startup_seconds"], "shutdown": config["startup_seconds"],
                    "build": config["build_seconds"]}
    peaks = {}
    hit = None
    with (folder / "worker.log").open("wb") as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                 env=environment, start_new_session=True)
        proc = psutil.Process(child.pid)
        try:
            while child.poll() is None:
                if phase_path.exists():
                    last_phase = read_json(phase_path)
                phase = last_phase["name"]
                stats = peaks.setdefault(phase, dict(peak_tree_rss_bytes=0, peak_scratch_bytes=0, samples=0))
                rss = tree_rss(proc)
                temporary = temp_bytes(scratch)
                stats["peak_tree_rss_bytes"] = max(stats["peak_tree_rss_bytes"], rss)
                stats["peak_scratch_bytes"] = max(stats["peak_scratch_bytes"], temporary)
                stats["samples"] += 1
                resources = (
                    ("tree_rss_bytes", rss, config["tree_rss_limit_bytes"]),
                    ("scratch_bytes", temporary, config["scratch_limit_bytes"]),
                    ("wall_s", monotonic() - last_phase["started"],
                     phase_limits.get(phase, config["query_seconds"])),
                )
                hit = next((dict(resource=name, observed=value, limit=limit, phase=phase)
                            for name, value, limit in resources if value > limit), None)
                if hit:
                    os.killpg(child.pid, signal.SIGKILL)
                    break
                sleep(config["sample_seconds"])
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
            child.wait()

    raw_path = folder / "worker_result.json"
    raw = read_json(raw_path) if raw_path.exists() else {}
    failed = bool(hit or child.returncode)
    if failed and not hit:
        log_text = (folder / "worker.log").read_text(errors="replace")
        if engine == "duckdb" and "Out of Memory Error:" in log_text:
            hit = dict(resource="engine_memory", limit=config["duckdb_memory_limit"],
                       phase=last_phase["name"])
        elif engine == "spark" and "java.lang.OutOfMemoryError: Java heap space" in log_text:
            hit = dict(resource="engine_memory", limit=config["spark_heap"], phase=last_phase["name"])

    record = dict(engine=engine, status="FAILED" if failed else "OK", budget_hit=hit,
                  exit_code=child.returncode, started_utc=started, ended_utc=timestamp(),
                  command=command, overall_s=monotonic()-clock, sample_seconds=config["sample_seconds"],
                  peaks=peaks, source_rows=scope["bins"], panel_bytes=scope["panel_bytes"],
                  catalog_bytes=catalog.stat().st_size, expected_intervals=scope["log_intervals"],
                  prior_trial=prior, **raw)
    write_json(folder / "measurement.json", record)
    return record


def handoff_inputs(run, checks, args):
    config, locked, catalog, panel = locked_inputs(run, checks, args.lock, args.inventory, args.full)
    checks.check("machine and engine versions match Q01", locked["machine"], machine(), args.lock / "lock.json")
    fixture = completed_input(run, args.fixture, "Q02", ["checks.json", "expected.json"])
    fixture_config = read_json(fixture / "run_manifest.json")["config_sha256"]
    checks.check("fixture used locked configuration", run.info["config_sha256"], fixture_config, fixture)
    return config, locked, catalog, panel


def build_maps(run, checks, args):
    config, locked, catalog, panel = handoff_inputs(run, checks, args)
    coverage = read_coverage(args.full)
    summary = {}
    for level in config["levels"]:
        scope = locked["levels"][str(level)]
        home = run.path / f"level_{level}"
        counts = Counter()
        meters = set(scope["meters"])
        for (meter, _, reason), (bins, _) in coverage.items():
            if meter in meters:
                counts[reason] += bins
        specs = {m: locked["meters"][m] for m in scope["meters"]}
        audit = PanelAudit(panel, specs, catalog, counts)
        result = measure(home, "duckdb", panel, catalog, scope, config, audit_keys=audit.audit_keys())
        checks.check(f"{level} channels build completed", "OK", result["status"], home / "worker.log")
        checks.check(f"{level} channels log input count", scope["log_intervals"],
                     result["selected_log_intervals"], home / "measurement.json")
        stats = verify_map(run, checks, str(level), home / "tables", audit, counts)
        names = sorted([*episode["key"], meter] for episode in stats["crosschecks"]
                       for meter, bins in episode["channels"] if bins == episode["support"][0])
        if names != result["audit_channels"]:
            write_json(run.path / "first_mismatch.json",
                       dict(expected=names, actual=result["audit_channels"]))
        checks.check(f"{level} independently verified identities of all sampled continuous channels",
                     True, names == result["audit_channels"], home / "worker_result.json")
        stats.update(scope=scope, engine="duckdb")
        checks.check(f"{level} channels all input cells accounted for", scope["bins"],
                     stats["valid_bins"] + stats["invalid_bins"], home / "tables")
        write_json(home / "audit.json", stats)
        summary[str(level)] = {k:v for k,v in stats.items() if k not in ("crosschecks", "duration_bins")}
        write_json(run.path / "summary.json", summary)
    run.info.update(levels=config["levels"], engines=["duckdb"], rows=scope["bins"])


def verified_map(run, checks, path, levels):
    required = ["summary.json", "checks.json"]
    for level in levels:
        required += [f"level_{level}/tables", f"level_{level}/audit.json"]
    path = completed_input(run, path, "Q03", required)
    manifest = read_json(path / "run_manifest.json")
    checks.check("map used locked configuration", run.info["config_sha256"], manifest["config_sha256"], path)
    expected = [record for record in manifest["inputs"].values()
                if "sources.json" in record["artifacts_sha256"] or "coverage.csv" in record["artifacts_sha256"]]
    current = list(run.info["inputs"].values())
    checks.check("map upstream checksums match", True, len(expected) == 2 and all(r in current for r in expected), path)
    return path


def benchmark(run, checks, args):
    config, locked, catalog, panel = handoff_inputs(run, checks, args)
    baseline = verified_map(run, checks, args.map, config["levels"])
    reference = read_json(baseline / "summary.json")
    queries = {level: stats["queries"] for level, stats in reference.items()}
    write_json(run.path / "query_schedule.json", queries)
    run.info["query_schedule_sha256"] = sha256(run.path / "query_schedule.json")
    run.info["cache"] = config["cache"]
    run.info["resource_sampling"] = ("Sum of RSS in worker tree at 100 ms intervals may count shared pages "
                                     "multiple times; scratch is allocated blocks under the trial scratch only. "
                                     "Transient peaks between samples may be missed.")
    run.save()
    measurements, summary = [], {}
    for offset, level in enumerate(config["levels"]):
        level_key = str(level)
        scope = locked["levels"][level_key]
        expected = reference[level_key]
        query_keys = [d["key"] for d in queries[level_key]]
        confirmed = {}
        for round_number in range(config["warmups"] + config["repeats"]):
            engines = ("duckdb", "spark") if (round_number + offset) % 2 == 0 else ("spark", "duckdb")
            for engine in engines:
                if engine in confirmed:
                    continue
                trial = f"level_{level}/r{round_number}_{engine}"
                folder = run.path / trial
                for repeat in (0, 1):
                    result = measure(folder, engine, panel, catalog, scope, config, query_keys,
                                     prior=trial if repeat else None)
                    result.update(level=level, round=round_number, warmup=round_number < config["warmups"],
                                  trial=str(folder.relative_to(run.path)), confirmation_attempt=bool(repeat))
                    measurements.append(result)
                    write_json(run.path / "measurements.json", measurements)
                    if result["status"] == "OK":
                        break
                    hit = result["budget_hit"]
                    evidence = folder / "measurement.json"
                    checks.check(f"{result['trial']} failed within a recorded budget condition",
                                 True, hit is not None, evidence)
                    if repeat:
                        prior = measurements[-2]["budget_hit"]
                        same_limit = all(hit.get(k) == prior.get(k) for k in ("resource", "phase", "limit"))
                        checks.check(f"{trial} independent bound confirmation", True, same_limit, evidence)
                        confirmed[engine] = dict(trial=trial, confirmation=result["trial"], budget=hit)
                        break
                    safe = (psutil.virtual_memory().available >= config["tree_rss_limit_bytes"]
                            and disk_usage(run.path).free >= config["scratch_limit_bytes"])
                    checks.check(f"{trial} independent retry is safe", True, safe, evidence)
                    folder = run.path / (trial + "_confirm")

                if result["status"] != "OK":
                    continue
                tables = folder / "tables"
                compare_maps(run, checks, f"{result['trial']} all logical rows",
                             baseline / f"level_{level}" / "tables", tables,
                             expected["signature"], signature(tables))
                observed_details = [item["detail"] for item in result["queries"]]
                if observed_details != queries[level_key]:
                    first = next((i for i, (a, b) in enumerate(zip(observed_details, queries[level_key]))
                                  if a != b), min(len(observed_details), len(queries[level_key])))
                    write_json(run.path / "first_mismatch.json",
                               dict(trial=result["trial"], query=first,
                                    expected=queries[level_key][first:first+1],
                                    actual=observed_details[first:first+1]))
                checks.check(f"{result['trial']} query detail matched direct panel scan",
                             True, observed_details == queries[level_key], folder / "worker_result.json")
                checks.check(f"{result['trial']} same log input count", scope["log_intervals"],
                             result["selected_log_intervals"], folder / "measurement.json")

        stats = {}
        for engine in ("duckdb", "spark"):
            if engine in confirmed:
                stats[engine] = dict(status="CONFIRMED_LIMIT", **confirmed[engine])
                continue
            all_rounds = [m for m in measurements if m["level"] == level and m["engine"] == engine
                          and m["status"] == "OK"]
            timed = [m for m in all_rounds if not m["warmup"]]
            checks.check(f"{level}/{engine} successful warmup and timed rounds",
                         [config["warmups"], config["repeats"]], [len(all_rounds)-len(timed), len(timed)],
                         "measurements.json")
            build_peaks = [m["peaks"]["build"] for m in timed]
            stats[engine] = dict(status="OK", pairing="all map hashes and all query details matched Q03",
                                 startup_median_s=median(m["startup_s"] for m in timed),
                                 build_median_s=median(m["build_s"] for m in timed),
                                 build_max_tree_rss_bytes=max(p["peak_tree_rss_bytes"] for p in build_peaks),
                                 build_max_scratch_bytes=max(p["peak_scratch_bytes"] for p in build_peaks),
                                 query_median_s=[median(m["queries"][i]["wall_s"] for m in timed)
                                                 for i in range(len(query_keys))])
        completed = [name for name, item in stats.items() if item["status"] == "OK"]
        checks.check(f"{level} has required completed tools", 2 if level == 4 else True,
                     len(completed) if level == 4 else bool(completed) or level != 71, "measurements.json")
        if len(completed) == 2:
            stats["spark_over_duckdb_build"] = stats["spark"]["build_median_s"] / stats["duckdb"]["build_median_s"]
            stats["spark_over_duckdb_query"] = [a/b for a, b in
                                               zip(stats["spark"]["query_median_s"], stats["duckdb"]["query_median_s"])]
        summary[level_key] = dict(scope=scope, query_keys=query_keys, engines=stats)
        write_json(run.path / "benchmark.json", summary)
    run.info.update(levels=config["levels"], measurements=len(measurements))
