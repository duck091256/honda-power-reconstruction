import csv
import gzip
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta

import yaml

from .run import read_json, sha256, write_json

MINUTE = 60_000_000
BIN = 15 * MINUTE
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MINUTE_COLUMNS = ("meter_id", "epoch_us", "power_w", "source_id")


def micros(value):
    when = datetime.fromisoformat(value)
    if when.utcoffset() is None:
        raise ValueError(f"Missing UTC offset: {value}")
    return (when - EPOCH) // timedelta(microseconds=1)


def utc(value):
    return (EPOCH + timedelta(microseconds=value)).isoformat().replace("+00:00", "Z")


def csv_rows(path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(columns)
        writer.writerows(rows)


def checked_files(root, expected):
    files = {}
    for name, digest in expected.items():
        path = root / name
        if sha256(path) != digest:
            raise ValueError(f"Registered checksum mismatch: {path}")
        files[name] = path
    return files


def issues_for(meter, files, buffer_seconds, start, end):
    for path in files:
        with path.open(encoding="utf-8") as stream:
            entries = yaml.safe_load(stream)
        if not isinstance(entries, dict):
            raise ValueError(f"Expected keyed issue records: {path}")
        for issue_id, entry in entries.items():
            reference = entry["reference"]
            # The publisher's meter-level gaps apply to all measurements.
            # A signal-specific entry about W or reactive power does not flag P.
            if reference not in (meter, f"{meter}.P"):
                continue
            left, right = entry["time_start"], entry["time_end"]
            if type(left) is not int or type(right) is not int or right < left:
                raise ValueError(f"Invalid issue interval {issue_id} in {path}")
            padded_left = (left - buffer_seconds) * 1_000_000
            # Source endpoints are inclusive and specified to the second.
            padded_right = (right + buffer_seconds) * 1_000_000 + 1
            if padded_left < end and padded_right > start:
                yield issue_id, entry["reason"], padded_left, padded_right, path.name


def power_rows(path, meter, source_id, start, end, counters):
    with gzip.open(path, "rt", newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream)
        if next(reader) != ["datetime_utc", f"{meter}.P"]:
            raise ValueError(f"Unexpected power header in {path}")
        for time, text in reader:
            counters["scanned"] += 1
            if not time.endswith(("+00:00", "Z")):
                raise ValueError(f"Non-UTC timestamp in registered Honda input: {time}")
            epoch = micros(time)
            if not start <= epoch < end:
                continue
            value = float(text) if text else None
            if value is None or not math.isfinite(value):
                value = None
                counters["nonfinite"] += 1
            if epoch % MINUTE:
                counters["off_grid"] += 1
            counters["selected"] += 1
            yield meter, epoch, value, source_id


def stage(run, config_path, root, meters, start, end):
    config = read_json(config_path)
    if not (micros(config["start_utc"]) <= start < end <= micros(config["end_utc"])):
        raise ValueError("Requested scope is outside T01")
    if start % BIN or end % BIN:
        raise ValueError("Expected quarter-hour UTC bounds")
    run.info.update(config_sha256=sha256(config_path), scope=[utc(start), utc(end)])
    manual = checked_files(root, config["manual_catalog"])
    found = {str(p.relative_to(root)) for p in (root / "issues/manual_issues").glob("*.yaml")
             if p.name != "1_issues_manual_template.yaml"}
    if found != set(manual):
        raise ValueError("Manual catalog inventory changed")
    manifests = []
    excluded_bins = []
    issue_rows = []
    for meter in meters:
        expected = config["meters"][meter]
        files = checked_files(root, expected)
        power = next(path for path in files.values() if path.suffix == ".gz")
        automatic = next(path for path in files.values() if path.suffix == ".yaml")
        source_id = f"{meter}:{expected[str(power.relative_to(root))][:16]}"
        issues = list(issues_for(meter, [automatic, *manual.values()],
                                config["issue_buffer_seconds"], start, end))
        bins = set()
        for issue_id, reason, left, right, filename in issues:
            issue_rows.append((meter, issue_id, reason, utc(left), utc(right), filename))
            bins.update(range(max(start, left // BIN * BIN), min(end, right), BIN))
        excluded_bins.extend((meter, slot) for slot in sorted(bins))
        counters = Counter()
        csv_rows(run.path / f"source_{meter}.csv", MINUTE_COLUMNS,
                 power_rows(power, meter, source_id, start, end, counters))
        manifests.append({
            **{k: config[k] for k in ("uri", "version", "license", "metadata", "measurement",
                                     "unit", "timezone", "source_stage")},
            "meter_id": meter, "source_id": source_id,
            "paths": {str(path): expected[name] for name, path in files.items()},
            "bytes_power_gzip": power.stat().st_size, "row_counts": dict(counters),
            "issues_in_scope": len(issues), "issue_bins": len(bins),
        })
    write_json(run.path / "source_manifest.json", {
        "start_us": start, "end_us": end, "kind": "honda", "meters": manifests,
        "manual_catalog": {str(p): config["manual_catalog"][name] for name, p in manual.items()},
    })
    csv_rows(run.path / "issue_bins.csv", ("meter_id", "bin_us"), excluded_bins)
    csv_rows(run.path / "issues.csv",
             ("meter_id", "issue_id", "reason", "padded_start_utc", "padded_end_exclusive_utc", "file"),
             issue_rows)
    run.info["source_manifest_sha256"] = sha256(run.path / "source_manifest.json")
    run.info["row_counts"] = {m["meter_id"]: m["row_counts"] for m in manifests}
    return set(excluded_bins), manifests


def audit_sample(run, config_path, root):
    meter = "H1.Z20"
    start, end = map(micros, ("2023-03-03T00:00:00Z", "2023-03-05T00:00:00Z"))
    excluded, _ = stage(run, config_path, root, [meter], start, end)
    by_minute = defaultdict(list)
    offgrid_bins = set()
    with (run.path / f"source_{meter}.csv").open(newline="") as stream:
        for row in csv.DictReader(stream):
            epoch = int(row["epoch_us"])
            if epoch % MINUTE:
                offgrid_bins.add(epoch // BIN * BIN)
                continue
            by_minute[epoch].append(row["power_w"])
    minute_rows = []
    counts = Counter()
    run_start = longest_start = start
    current = longest = 0
    for slot in range(start, end, BIN):
        reasons = set()
        issue = (meter, slot) in excluded
        for minute in range(slot, slot + BIN, MINUTE):
            values = by_minute.get(minute, [])
            if not values:
                status = "missing_minute"
            elif "" in values:
                status = "nonfinite"
            elif len({float(value) for value in values}) > 1:
                status = "conflicting_duplicate"
            elif len(values) > 1:
                status = "identical_duplicate"
            else:
                status = "observed"
            if status not in ("observed", "identical_duplicate"):
                reasons.add(status)
            counts[status] += 1
            minute_rows.append((meter, utc(minute), utc(slot), len(values), "|".join(values),
                                status, int(issue), int(slot in offgrid_bins)))
        if reasons or issue or slot in offgrid_bins:
            current = 0
        else:
            if not current:
                run_start = slot
            current += 1
            if current > longest:
                longest, longest_start = current, run_start
    csv_rows(run.path / "minute_audit.csv",
             ("meter_id", "minute_utc", "bin_utc", "row_count", "observed_w",
              "minute_reason", "issue_bin", "off_grid_bin"), minute_rows)
    write_json(run.path / "audit.json", {
        "minute_counts": dict(counts), "excluded_issue_bins": len(excluded),
        "longest_valid_bins": longest, "longest_start_utc": utc(longest_start),
        "longest_end_exclusive_utc": utc(longest_start + longest * BIN),
        "supports_6h_with_edges": longest >= 26,
    })
