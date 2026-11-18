"""Inventory of the extracted Dryad copy. Device models follow Engel et al., Table 2."""
import gzip
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as ac
import yaml

from .run import Run, read_json, sha256, write_json
from .source import csv_rows

ABB = """H2.Z35 H2.Z36 H1.Z20 H3.Z43 H3.Z44 H3.Z46 H3.Z71 H2.Z61 H2.Z62 H2.Z63
H2.Z64 H2.Z65 H2.Z66 H2.Z67 H2.T.Z31 H3.Z42 H2.Z68 H2.Z69 H2.Z70 H2.T.Z34 H1.Z10
H1.Z13 H1.Z14 H1.Z16 H1.Z11 H1.Z12 H1.Z24 H1.Z25 H1.Z19 H1.Z23 H1.Z18 H1.Z21
H1.Z22 H2.T.Z30 H2.T.Z32 H4.Z50 H4.Z51 H2.T.Z33 H3.Z40 H3.Z41 H3.Z45 H3.Z47 H3.Z48 H3.Z49"""
JANITZA = """V.Z81 V.Z82 H2.Z351 H2.Z361 H1.ZE20 V.ZE84 H1.Z310 H2.Z311 H3.Z312
H3.ZE43 H3.ZE44 H2.ZE64 H2.ZE65 H2.ZE66 H2.ZE67 H2.ZE74 H1.Z15 H1.Z28 H1.Z17
H1.Z29 H1.Z26 H1.Z27 H4.ZE50 H4.ZE51 H3.ZE40 H3.ZE41"""
THERMAL = "V.K21 H1.K11 H1.K15 H1.K12 H1.K14 H2.K21 H1.K16 H1.W11 H1.W12"
MODELS = {meter: model for model, meters in [
    ("ABB-B24", ABB), ("Janitza", JANITZA), ("Socomec", "V.Z84"),
    ("SensorStar", THERMAL), ("Lufft", "WeatherStation.Weather")
] for meter in meters.split()}


def scan(path, m):
    data = ac.open_csv(path, convert_options=ac.ConvertOptions(
        column_types={"datetime_utc": pa.timestamp("us", tz="UTC"), m + ".P": pa.float64()}))
    if data.schema.names != ["datetime_utc", m + ".P"]:
        raise ValueError(f"Unexpected header {path}")
    low, high, rows = [], [], 0
    for batch in data:
        dates = batch.column(0)
        if dates.null_count:
            raise ValueError(f"Null timestamp in {path}")
        low.append(pc.min(dates).as_py())
        high.append(pc.max(dates).as_py())
        rows += batch.num_rows
    return min(low).isoformat(), max(high).isoformat(), rows


def epoch(value):
    if isinstance(value, int):
        return value
    return int(datetime.strptime(value, "%Y/%m/%d %H:%M:%S%z").timestamp())


def inventory(run, root):
    pilot = read_json("configs/sources_v1.json")
    for group in [*pilot["meters"].values(), pilot["manual_catalog"]]:
        for name, registered in group.items():
            if sha256(root / name) != registered:
                raise ValueError(f"Registered checksum mismatch: {name}")
    docs = root.parent / "README.md"
    if sha256(docs) != "7433ae4227aa9253c3a4c3f007486f5567d7fec2fdcf33b1600799217ff7cdee":
        raise ValueError("Companion README checksum mismatch")
    manual = sorted(p for p in (root / "issues/manual_issues").glob("*.yaml") if "template" not in p.name)
    ledger, channels = [], {}
    for directory in sorted((root / "data").iterdir()):
        if not directory.is_dir():
            continue
        m = directory.name
        model = MODELS.get(m, "unknown")
        path = directory / f"{m}.P_harmonized.csv.gz"
        log = root / "issues/automatic_issues" / f"{m}_issues_automatic.yaml"
        status = "eligible"
        if model in ("SensorStar", "Lufft", "unknown"):
            status = model
        elif not path.exists() or not log.exists():
            status = "missing input"
        first, last, rows = scan(path, m) if path.exists() else ("", "", 0)
        size = path.stat().st_size if path.exists() else 0
        ledger.append((m, model, status, first, last, rows, size))
        if status != "eligible":
            continue
        intervals = []
        for file in [log, *manual]:
            with file.open() as f:
                entries = yaml.load(f, Loader=getattr(yaml, "CSafeLoader", yaml.SafeLoader)) or {}
            for key, issue in entries.items():
                ref = issue["reference"]
                if file in manual:
                    # Source paper Methods assigns the mislabeled manual events to these meters.
                    ref = {"H1.Z19_issues_manual.yaml": "H1.Z19",
                           "H3.ZE50_issues_manual.yaml": "H4.ZE50",
                           "H3.ZE51_issues_manual.yaml": "H4.ZE51"}.get(file.name, ref)
                if ref in (m, m + ".P"):
                    a, b = epoch(issue["time_start"]), epoch(issue["time_end"])
                    if b < a:
                        raise ValueError(f"Inverted interval {file} {key}")
                    intervals.append([a - 900, b + 900])
        channels[m] = dict(path=str(path), sha256=sha256(path), file_bytes=size,
                           automatic=str(log), log_sha256=sha256(log), model=model,
                           first=first, last=last, rows=rows, intervals=intervals)
    csv_rows(run.path / "inventory.csv", ["meter","device","status","first_utc","last_utc","rows","gzip_bytes"], ledger)
    write_json(run.path / "sources.json", dict(channels=channels, manual={str(p):sha256(p) for p in manual},
               metadata="https://arxiv.org/html/2503.11469v1", unit="W", timezone="UTC",
               doi=pilot["uri"], stage="harmonized", root=str(root)))
    run.info.update(inventory_rows=len(ledger), eligible=len(channels),
                    selected_bytes=sum(c["file_bytes"] for c in channels.values()),
                    selected_rows=sum(c["rows"] for c in channels.values()),
                    status_counts=dict(Counter(row[2] for row in ledger)))


if __name__ == "__main__":
    with Run(sys.argv[1], "T11", prior=sys.argv[3] if len(sys.argv)>3 else None) as current:
        current.info["command"] = [sys.executable,"-m","honda.inventory",*sys.argv[1:]]
        inventory(current, Path(sys.argv[2]).resolve())
