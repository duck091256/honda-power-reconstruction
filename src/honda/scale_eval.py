"""Evaluation from one meter at a time, independently of the global panel size."""
from collections import Counter
from functools import partial
from pathlib import Path
from shutil import copyfile

import pyarrow.parquet as pq

from .cases import CASE_COLUMNS, select_cases, time_bounds
from .evaluate import predict, score
from .reproduce import compare, csv_contents
from .run import read_json, sha256, write_json
from .scale import PILOT, aggregate, build_panels, jobs_for
from .scoring import PREDICTION_COLUMNS, SCORE_COLUMNS, TRUTH_COLUMNS, summarize
from .source import BIN, csv_rows, utc


def evaluate_meter(meter, folder, cfg, run_id):
    columns = ["bin_us", "power_w"]
    h_start, _, t_start, t_end = time_bounds(cfg)
    stored = pq.read_table(Path(folder) / f"{meter}.parquet", columns=columns,
                           filters=[("bin_us", ">=", h_start), ("bin_us", "<", t_end)])
    values = {(meter, row["bin_us"]): row["power_w"] for row in stored.to_pylist()}
    for slot in range(t_start, t_end, BIN):
        values.setdefault((meter, slot), None)
    cases, truth, history, candidates = select_cases(values, [meter], cfg)
    methods = cfg["methods"]
    predictions = list(predict(cases, history, methods))
    scores = score(cases, predictions, truth, run_id, methods)
    shifted_truth = [(cid, slot, value + 100_000) for cid, slot, value in truth]
    changed_scores = score(cases, predictions, shifted_truth, run_id, methods)
    if predictions != list(predict(cases, history, methods)) or (scores and scores == changed_scores):
        raise AssertionError("Truth isolation failed")
    counts = Counter((m,n,reason) for m,n,_,_,reason in candidates)
    return meter, cases, truth, predictions, scores, dict(counts), len(history)


def evaluate(run, spark, folder, meters, cfg):
    cap = cfg["cap_per_meter_per_duration"]
    lengths = cfg["gap_bins"]
    grouped = (spark.sparkContext.parallelize(meters, len(meters))
               .map(partial(evaluate_meter, folder=str(folder), cfg=cfg, run_id=run.path.name))
               .collect())
    cases, truth, predictions, scores, groups, per_meter, candidates = [], [], [], [], [], [], []
    for meter, selected, target, estimated, points, counts, n_history in sorted(grouped):
        cases.extend(selected)
        truth.extend(target)
        predictions.extend(estimated)
        scores.extend(points)
        for n in lengths:
            total = counts.get((meter, n, "selected"), 0)
            groups.append((meter,n,total,cap,cap-total))
        candidates.extend((*key,value) for key,value in sorted(counts.items()))
        per_meter.extend(dict(meter_id=meter, observed_history_bins=n_history, **summary)
                         for summary in summarize(selected, points, cfg, dict.fromkeys(lengths,cap)))
    write_json(run.path / "masked.json", cases)
    csv_rows(run.path / "cases.csv", CASE_COLUMNS,
             ((c["case_id"], c["meter_id"], utc(c["start_us"]), c["start_us"], c["n_bins"],
               c["split"]) for c in cases))
    csv_rows(run.path / "truth.csv", TRUTH_COLUMNS, truth)
    csv_rows(run.path / "predictions.csv", PREDICTION_COLUMNS,predictions)
    csv_rows(run.path / "case_scores.csv", SCORE_COLUMNS,scores)
    csv_rows(run.path / "selected_counts.csv", ("meter_id","n_bins","selected","cap","below_cap"),groups)
    csv_rows(run.path / "candidate_counts.csv", ("meter_id","n_bins","reason","starts"),candidates)
    write_json(run.path / "summary.json", list(summarize(
        cases, scores, cfg, dict.fromkeys(lengths, cap * len(meters)))))
    write_json(run.path / "metrics_by_meter.json", per_meter)
    run.info.update(selected_cases=len(cases), scenario_meters=len({c["meter_id"] for c in cases}),
                    predictions=len(predictions), truth_rows=len(truth))


def pilot(run, spark, catalog):
    import numpy as np
    start = time_bounds(read_json("configs/evaluation_v1.json"))[0]
    fixture = dict(meter="fixture", start=start, end=start + 3 * BIN,
                   intervals=[[start // 1_000_000 + 1800] * 2], model="ABB-B24", sha256="0"*64)
    ticks = start + np.arange(45) * (BIN // 15)
    tick_values = np.append(ticks, ticks[0])
    result = aggregate(fixture, tick_values, np.append(np.full(45, -2.0), -2.0)).to_pylist()
    assert [(b["reason"], b["power_w"], b["identical_duplicates"]) for b in result] == [
        ("valid", -2.0, 1), ("valid", -2.0, 0), ("issue_interval", None, 0)]
    cfg = read_json("configs/evaluation_v1.json")
    run.info["config_sha256"] = sha256("configs/evaluation_v1.json")
    h_start, _, _, t_end = time_bounds(cfg)
    jobs = list(jobs_for(catalog, PILOT, (h_start, t_end)))
    folder = run.path / "panel"
    build_panels(run, spark, jobs, folder)
    old = Path("runs/t05_pilot")
    reference = {(r["meter_id"], r["bin_us"]):r for r in pq.read_table(old / "panel_15m").to_pylist()}
    checked = 0
    tol = cfg["tolerance"]
    for meter in PILOT:
        for row in pq.read_table(folder / f"{meter}.parquet").to_pylist():
            expected = reference.pop((meter,row["bin_us"]))
            expected["bin_start_utc"] = expected["bin_start_utc"].isoformat().split("+")[0]
            row["bin_start_utc"] = row["bin_start_utc"].isoformat().split("+")[0]
            compare(expected,row,tol,"pilot panel")
            checked += 1
    assert not reference
    evaluate(run, spark, folder, list(PILOT), cfg)
    for upstream, file in [("t06_locked", "cases.csv"), ("t07_honda", "case_scores.csv"),
                           ("t07_honda", "predictions.csv")]:
        compare(csv_contents(Path("runs") / upstream / file), csv_contents(run.path / file),
                tol, f"pilot {file}")
    run.info["pilot_panel_matches"] = checked


def full_run(run, spark, catalog, resume):
    cfg_path = Path("configs/evaluation_v2.json")
    cfg = read_json(cfg_path)
    copyfile(cfg_path, run.path / "evaluation_config.json")
    run.info["config_sha256"] = sha256(cfg_path)
    meters = sorted(catalog["channels"])
    jobs = list(jobs_for(catalog, meters))
    folder = run.path / "panel"
    stats = build_panels(run, spark, jobs, folder, resume=resume)
    if sum(s["selected"] for s in stats) != sum(c["rows"] for c in catalog["channels"].values()):
        raise ValueError("Full scan did not account for all registered source rows")
    spark.read.parquet(str(folder)).createOrReplaceTempView("scale_panel")
    coverage = spark.sql("""
        SELECT meter_id, date_format(bin_start_utc,'yyyy-MM') AS month_utc, reason,
               count(*) AS bins, sum(row_count) AS input_rows
        FROM scale_panel GROUP BY 1,2,3 ORDER BY 1,2,3
    """).collect()
    csv_rows(run.path / "coverage.csv",
             ("meter_id","month_utc","reason","bins","input_rows"),coverage)
    evaluate(run, spark, folder, meters, cfg)
    run.info.update(channels=len(stats), channels_with_reference=sum(s["valid"]>0 for s in stats),
                    rows=sum(s["selected"] for s in stats), bins=sum(s["bins"] for s in stats),
                    valid=sum(s["valid"] for s in stats))
