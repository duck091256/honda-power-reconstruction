import csv
import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import fmean, median

import pyarrow.parquet as pq

from honda.run import Run, read_json, sha256


def table(path):
    with open(path, newline="") as stream:
        return list(csv.DictReader(stream))


def timestamp(cfg, field):
    return int(datetime.fromisoformat(cfg[field]).timestamp()) * 1_000_000


def match(a, b):
    return math.isclose(float(a), b, abs_tol=1e-8, rel_tol=1e-12)


def audit(run, source):
    source_manifest = read_json(source / "run_manifest.json")
    assert source_manifest["status"] == "OK"
    cfg = read_json(source / "evaluation_config.json")
    history_left = timestamp(cfg, "history_start_utc")
    history_right = timestamp(cfg, "history_end_utc")
    test_left = timestamp(cfg, "test_start_utc")
    test_right = timestamp(cfg, "test_end_utc")
    cases = defaultdict(list)
    case_lookup = {}
    for case in read_json(source / "masked.json"):
        assert case["case_id"] not in case_lookup
        case_lookup[case["case_id"]] = case
        cases[case["meter_id"]].append(case)
    forecast = {}
    for line in table(source / "predictions.csv"):
        key = (line["case_id"], int(line["bin_us"]), line["method"])
        assert key not in forecast
        forecast[key] = line
    truth = {}
    for line in table(source / "truth.csv"):
        key = (line["case_id"], int(line["bin_us"]))
        assert key not in truth
        truth[key] = float(line["ref_w"])

    step = 900_000_000
    deltas = defaultdict(list)
    n_bins, n_rows, n_valid = 0, 0, 0
    for file in sorted((source / "panel").glob("*.parquet")):
        m = file.stem
        records = pq.read_table(file, columns=["meter_id", "bin_us", "power_w", "row_count", "valid_ref"]).to_pylist()
        values, hist, groups = {}, defaultdict(list), {}
        for index, r in enumerate(records):
            bin_us = r["bin_us"]
            power = r["power_w"]
            assert r["meter_id"] == m and bin_us == records[0]["bin_us"] + index * step
            assert r["valid_ref"] == (power is not None and math.isfinite(power))
            n_rows += r["row_count"]
            n_valid += r["valid_ref"]
            if history_left <= bin_us < history_right and power is not None:
                slot = bin_us // step % 96
                day = (bin_us // (96 * step) + 3) % 7
                for key in ((day,slot), (slot,), ()):
                    hist[key].append(power)
            if test_left <= bin_us < test_right:
                values[bin_us] = power
        groups = {k: median(v) for k,v in hist.items()}
        n_bins += len(records)
        for c in cases[m]:
            cid, left, length = c["case_id"], c["start_us"], c["n_bins"]
            right = left + length * step
            lo, hi = values[left-step], values[right]
            context = {bin_us:(v,state) for bin_us,v,state in c["context"]}
            assert lo is not None and hi is not None
            assert set(bin_us for bin_us, (_, state) in context.items() if state == "hidden") == set(range(left,right,step))
            for i, bin_us in enumerate(range(left,right,step),1):
                assert context[bin_us] == (None, "hidden")
                actual = truth.pop((cid,bin_us))
                assert match(actual, values[bin_us])
                slot = bin_us // step % 96
                day = (bin_us // (96 * step) + 3) % 7
                estimates = {
                    "Linear": lo + (hi-lo) * (i/(length+1)),
                    "Seasonal": groups.get((day,slot), groups.get((slot,),groups.get(()))),
                }
                for method, power in estimates.items():
                    estimate = forecast.pop((cid,bin_us,method))["pred_w"]
                    assert (not estimate) if power is None else match(estimate,power)
                    deltas[cid,method].append(None if power is None else power-actual)
    assert not forecast and not truth
    assert (n_bins,n_rows,n_valid) == tuple(source_manifest[k] for k in ("bins","rows","valid"))
    scores = table(source / "case_scores.csv")
    for score in scores:
        differences = deltas.pop((score["case_id"], score["method"]))
        if None in differences:
            assert not score["mae_w"] and not score["abs_energy_error_kwh"]
        else:
            assert match(score["mae_w"],fmean(map(abs,differences)))
            assert match(score["abs_energy_error_kwh"],abs(math.fsum(differences))*0.00025)
    assert not deltas
    return dict(panel_bins=n_bins, input_rows=n_rows, valid_bins=n_valid,
                cases=len(case_lookup), scores=len(scores), predictions=source_manifest["predictions"])


if __name__ == "__main__":
    with Run(sys.argv[1], "audit_T14") as run:
        run.info["command"]=[sys.executable,*sys.argv]
        source = Path(sys.argv[2])
        run.info["t14_manifest_sha256"] = sha256(source / "run_manifest.json")
        run.info["checks"] = audit(run,source)
        (run.path / "verify.py").write_bytes(Path(__file__).read_bytes())
