"""Independent recomputation using CSV, datetime, statistics and PyArrow."""
import csv
import gzip
import hashlib
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import mean, median

import pyarrow.parquet as parquet
import yaml


WORKSPACE = Path.cwd()
OUTPUT = Path(__file__).resolve().parent
ROOT = Path(sys.argv[1])
RUNS = WORKSPACE / "runs"
SOURCE_CONFIG = WORKSPACE / "configs/sources_v1.json"
LOCK = RUNS / "t06_locked"
SCORES = RUNS / "t07_honda"
PANEL = RUNS / "t05_pilot"
start_date = "2023-01-01"
end_date = "2023-04-01"
floor = datetime.fromisoformat(start_date).replace(tzinfo=UTC)
stop = datetime.fromisoformat(end_date).replace(tzinfo=UTC)
quarter = timedelta(minutes=15)
n_bins = (stop - floor) // quarter
norm = lambda date: datetime.fromisoformat(date).astimezone(UTC)
timestamp = lambda us: datetime.fromtimestamp(us / 1_000_000, UTC)
iso = lambda date: date.isoformat().replace("+00:00", "Z")


def digest(path):
    with path.open("rb") as data:
        return hashlib.file_digest(data, "sha256").hexdigest()


def jread(path):
    return json.loads(path.read_text())


def rows(path):
    with path.open(newline="") as data:
        yield from csv.DictReader(data)


def equal_number(actual, expected):
    return ((actual is None and expected is None)
            or (actual is not None and expected is not None
                and math.isclose(actual, expected, abs_tol=1e-8, rel_tol=1e-12)))


def require(condition, description):
    if not condition:
        raise AssertionError(description)


def read_channel(path, meter):
    slots = defaultdict(list)
    scanned = 0
    with gzip.open(path, "rt", newline="") as stream:
        table = csv.reader(stream)
        require(next(table) == ["datetime_utc", meter + ".P"], path.name)
        for date, text in table:
            scanned += 1
            if start_date <= date[:10] < end_date:
                require(date.endswith(("+00:00", "Z")), date)
                slots[norm(date)].append(float(text) if text else None)
    return slots, scanned


def load_issues(paths, meter, buffer):
    ranges = []
    for log in paths:
        entries = yaml.safe_load(log.read_text())
        for issue in entries.values():
            if issue["reference"] in (meter, meter + ".P"):
                begin = datetime.fromtimestamp(issue["time_start"], UTC) - buffer
                end = datetime.fromtimestamp(issue["time_end"], UTC) + buffer
                if begin < stop and end >= floor:
                    ranges.append((begin, end))
    return ranges


def rebuild_panel(result):
    config = jread(SOURCE_CONFIG)
    stage_info = jread(PANEL / "source_manifest.json")
    original = {item["meter_id"]: item for item in stage_info["meters"]}
    require(stage_info["start_us"] == int(floor.timestamp()) * 1_000_000, "source start")
    require(stage_info["end_us"] == int(stop.timestamp()) * 1_000_000, "source end")
    files = dict(config["manual_catalog"])
    for group in config["meters"].values():
        files.update(group)
    for path, sha in files.items():
        require(digest(ROOT / path) == sha, f"source checksum {path}")
    result["input_hashes"] = files
    reference = {}
    diagnostics = {}
    for meter, paths in sorted(config["meters"].items()):
        harmonized, = (ROOT / name for name in paths if name.endswith(".gz"))
        issue_file, = (ROOT / name for name in paths if name.endswith(".yaml"))
        issues = load_issues([issue_file, *(ROOT / p for p in config["manual_catalog"])],
                             meter, timedelta(seconds=config["issue_buffer_seconds"]))
        by_stamp, scanned = read_channel(harmonized, meter)
        raw_path = harmonized.parent / harmonized.name.replace("_harmonized", "_raw")
        raw, _ = read_channel(raw_path, meter)
        result["input_hashes"][str(raw_path.relative_to(ROOT))] = digest(raw_path)
        lineage = Counter()
        for date in set(raw) | set(by_stamp):
            harmonized_w = by_stamp.get(date, [])
            raw_w = raw.get(date, [])
            if Counter(harmonized_w) == Counter(raw_w):
                lineage["raw_same"] += len(harmonized_w)
            elif Counter(harmonized_w) == Counter(-v if v is not None else None for v in raw_w):
                lineage["raw_sign_reversed"] += len(harmonized_w)
            else:
                lineage["timestamp_or_value_unmatched"] += 1
        assigned = defaultdict(dict)
        for date, group in by_stamp.items():
            slot = floor + quarter * ((date - floor) // quarter)
            assigned[slot][date] = group
        valid = Counter()
        for idx in range(n_bins):
            slot = floor + quarter * idx
            items = assigned.get(slot, {})
            distinct_minute_ticks = sum(when.second == when.microsecond == 0 for when in items)
            duplicate_count = sum(
                len(values) - 1
                for when, values in items.items()
                if when.second == when.microsecond == 0
                and len(set(values)) == 1 and values[0] is not None and math.isfinite(values[0])
            )
            if any(a < slot + quarter and b >= slot for a, b in issues):
                reason = "issue_interval"
            elif any(when.second or when.microsecond for when in items):
                reason = "off_minute_grid"
            elif any(v is None or not math.isfinite(v) for values in items.values() for v in values):
                reason = "nonfinite"
            elif any(len(set(values)) > 1 for values in items.values()):
                reason = "conflicting_duplicate"
            elif distinct_minute_ticks != 15:
                reason = "missing_minute"
            else:
                reason = "valid"
            val = mean(values[0] for values in items.values()) if reason == "valid" else None
            reference[meter, slot] = (
                val, reason, distinct_minute_ticks, sum(map(len, items.values())), duplicate_count)
            valid[reason] += 1
        selected = sum(map(len, by_stamp.values()))
        require(original[meter]["row_counts"]["scanned"] == scanned, meter)
        require(original[meter]["row_counts"]["selected"] == selected, meter)
        diagnostics[meter] = {
            "scanned": scanned, "selected": selected, "raw_lineage": dict(lineage),
            "panel_reasons": dict(valid),
        }
    result["source_cross_check"] = diagnostics
    result["checked_panel_rows"] = len(reference)
    panel = parquet.read_table(PANEL / "panel_15m").to_pylist()
    seen = set()
    for item in panel:
        key = item["meter_id"], timestamp(item["bin_us"])
        require(key not in seen, f"Duplicate {key}")
        seen.add(key)
        expected, reason, minutes, count, duplicates = reference[key]
        require(equal_number(item["power_w"], expected), f"Power {key}")
        require(item["bin_start_utc"].replace(tzinfo=UTC) == key[1], f"Timestamp {key}")
        actual = (item["reason"], item["minute_count"], item["row_count"],
                  item["identical_duplicates"], item["valid_ref"])
        require(actual == (reason, minutes, count, duplicates, expected is not None),
                f"Quality {key}")
    require(seen == set(reference), "panel coverage")
    return {key: values[0] for key, values in reference.items()}


def check_cases(result, panel):
    config = jread(LOCK / "evaluation_config.json")
    hist_start, hist_end, test_start, test_end = [norm(config[name + "_utc"])
        for name in ("history_start", "history_end", "test_start", "test_end")]
    history = {(m, dt): w for (m, dt), w in panel.items()
               if hist_start <= dt < hist_end and w is not None}
    actual_history = list(rows(LOCK / "history.csv"))
    require(len(actual_history) == len(history), "history count")
    seen = set()
    for item in actual_history:
        key = item["meter_id"], timestamp(int(item["bin_us"]))
        require(key not in seen and equal_number(float(item["observed_w"]), history[key]), str(key))
        seen.add(key)
    selected = {}
    for meter in sorted({m for m, _ in panel}):
        for count in config["gap_bins"]:
            eligible = {}
            for j in range(1, (test_end - test_start) // quarter - count):
                gap = test_start + j * quarter
                all_slots = [gap + k * quarter for k in range(-1, count + 1)]
                if all(panel[meter, dt] is not None for dt in all_slots):
                    key = hashlib.sha256(f"{config['seed']}|{meter}|{iso(gap)}|{count}".encode()).hexdigest()
                    eligible[key] = all_slots[1:-1]
            used = set()
            picked = 0
            for key, slots in sorted(eligible.items()):
                if picked == config["cap_per_meter_per_duration"]:
                    break
                if used.isdisjoint(slots):
                    selected[key] = (meter, slots)
                    used.update(slots)
                    picked += 1

    candidates = jread(LOCK / "masked.json")
    require(len(candidates) == len(selected) == len({c["case_id"] for c in candidates}), "case count")
    for case in candidates:
        meter, slots = selected[case["case_id"]]
        require(case["meter_id"] == meter and case["n_bins"] == len(slots)
                and timestamp(case["start_us"]) == slots[0], "case metadata")
        actual = {}
        for micros, value, state in case["context"]:
            date = timestamp(micros)
            require(date not in actual and test_start <= date < test_end, "context bounds")
            actual[date] = value, state
        require({date for date, (_, state) in actual.items() if state == "hidden"} == set(slots),
                "mask coverage")
        require(all(value is None if dt in slots else
                    equal_number(value, panel[meter, dt])
                    and state == ("missing" if value is None else "observed")
                    for dt, (value, state) in actual.items()), "mask values")

    profiles = defaultdict(list)
    for (meter, date), w in history.items():
        clock = date.time()
        for key in ((meter, date.weekday(), clock), (meter, clock), (meter,)):
            profiles[key].append(w)
    profiles = {key: median(points) for key, points in profiles.items()}
    predictions = {}
    truth = {}
    scores = {}
    by_length = defaultdict(list)
    for case, (m, slots) in selected.items():
        lo, hi = panel[m, slots[0] - quarter], panel[m, slots[-1] + quarter]
        absolute, signed = defaultdict(list), defaultdict(list)
        for idx, dt in enumerate(slots, 1):
            ref = panel[m, dt]
            truth[case, dt] = ref
            candidates_by_level = [((m, dt.weekday(), dt.time()), "weekday_slot"),
                                   ((m, dt.time()), "slot"), ((m,), "meter")]
            seasonal, fallback = next((profiles[key], level)
                                      for key, level in candidates_by_level if key in profiles)
            for method, power, level in [
                ("Linear", lo + (hi - lo) * idx / (len(slots) + 1), ""),
                ("Seasonal", seasonal, fallback),
            ]:
                predictions[case, dt, method] = (power, level)
                signed[method].append(power - ref)
                absolute[method].append(abs(power - ref))
        for method in signed:
            result_pair = mean(absolute[method]), abs(sum(signed[method])) / 4000
            scores[case, method] = result_pair
            by_length[len(slots), method].append(result_pair)

    def check_csv(path, expected, decode):
        seen = set()
        for row in rows(path):
            key, values = decode(row)
            require(key not in seen and key in expected, f"{path.name}: {key}")
            seen.add(key)
            desired = expected[key]
            require(len(values) == len(desired) and all(
                equal_number(v, d) if isinstance(d, float) else v == d
                for v, d in zip(values, desired)), f"{path.name}: {key} {values} vs {desired}")
        require(seen == set(expected), f"{path.name}: keys")

    check_csv(LOCK / "truth.csv", {key: (value,) for key, value in truth.items()},
              lambda r: ((r["case_id"], timestamp(int(r["bin_us"]))), (float(r["ref_w"]),)))
    check_csv(SCORES / "predictions.csv", predictions,
              lambda r: ((r["case_id"], timestamp(int(r["bin_us"])), r["method"]),
                         (float(r["pred_w"]), r["fallback"])))
    check_csv(SCORES / "case_scores.csv", scores,
              lambda r: ((r["case_id"], r["method"]),
                         (float(r["mae_w"]), float(r["abs_energy_error_kwh"]),)))
    summary_expected = {key: (len(values), mean(a for a, _ in values), mean(b for _, b in values))
                        for key, values in by_length.items()}
    check_csv(SCORES / "summary.csv", summary_expected,
              lambda r: ((int(r["n_bins"]), r["method"]),
                         (int(r["paired_cases"]), float(r["mean_paired_mae_w"]),
                          float(r["mean_paired_abs_energy_error_kwh"]))))
    result.update(checked_cases=len(selected), checked_truth=len(truth),
                  checked_predictions=len(predictions), checked_case_scores=len(scores),
                  checked_summary_rows=len(summary_expected))


started = datetime.now(UTC)
result = {"status": "RUNNING", "started_utc": started.isoformat(),
          "python": sys.version, "source": str(ROOT),
          "command": [sys.executable, *sys.argv],
          "checksums": {str(p.relative_to(WORKSPACE)): digest(p) for p in [
              SOURCE_CONFIG, LOCK / "evaluation_config.json", LOCK / "run_manifest.json",
              SCORES / "run_manifest.json", PANEL / "run_manifest.json", Path(__file__),
          ]}}
try:
    check_cases(result, rebuild_panel(result))
    result.update(status="OK")
except Exception as exc:
    result.update(status="FAILED", error=repr(exc))
    raise
finally:
    result["ended_utc"] = datetime.now(UTC).isoformat()
    (OUTPUT / "run_manifest.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
print(json.dumps(result, indent=2))
