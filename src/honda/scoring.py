import csv
import math
from collections import Counter
from pathlib import Path
from statistics import mean
from tempfile import TemporaryDirectory
from shutil import copyfile

from .evaluate import predict, score
from .run import completed_input, read_json, sha256, write_json
from .source import csv_rows, micros

PREDICTION_COLUMNS = ("case_id", "bin_us", "method", "pred_w", "fallback", "reason")
SCORE_COLUMNS = ("run_id", "case_id", "method", "mae_w", "abs_energy_error_kwh", "reason")
TRUTH_COLUMNS = ("case_id", "bin_us", "ref_w")


def records(path):
    with path.open(newline="", encoding="utf-8") as stream:
        yield from csv.DictReader(stream)


def read_truth(path):
    return [(r["case_id"], int(r["bin_us"]), float(r["ref_w"])) for r in records(path)]


def predict_files(masked, history_file, config):
    cases = read_json(masked)
    history = [(r["meter_id"], int(r["bin_us"]), float(r["observed_w"]))
               for r in records(history_file)]
    history_start = micros(config["history_start_utc"])
    history_end = micros(config["history_end_utc"])
    keys = {(meter, time) for meter, time, _ in history}
    if len(keys) != len(history) or any(
        not history_start <= time < history_end or not math.isfinite(value)
        for _, time, value in history
    ):
        raise ValueError("Invalid or out-of-split historical values")
    test_start, test_end = (micros(config[k]) for k in ("test_start_utc", "test_end_utc"))
    if any(not test_start <= time < test_end for case in cases for time, _, _ in case["context"]):
        raise ValueError("Masked context crosses the test boundary")
    return cases, list(predict(cases, history, config["methods"]))


def summarize(cases, scores, config, planned):
    index = {(cid, method): (mae, energy, reason)
             for _, cid, method, mae, energy, reason in scores}
    for n_bins in config["gap_bins"]:
        group = [case for case in cases if case["n_bins"] == n_bins]
        methods = config["methods"]
        paired = [case for case in group
                  if all(index[case["case_id"], m][0] is not None for m in methods)]
        for method in methods:
            available = sum(index[case["case_id"], method][0] is not None for case in group)
            metrics = [index[case["case_id"], method] for case in paired]
            reasons = Counter(index[case["case_id"], method][2]
                              for case in group if index[case["case_id"], method][2])
            yield {
                "n_bins": n_bins,
                "method": method,
                "planned_cap": planned[n_bins],
                "selected_cases": len(group),
                "available_cases": available,
                "available_fraction": available / len(group) if group else None,
                "paired_cases": len(paired),
                "selected_meters": len({case["meter_id"] for case in group}),
                "paired_meters": len({case["meter_id"] for case in paired}),
                "mean_paired_mae_w": mean(m[0] for m in metrics) if metrics else None,
                "mean_paired_abs_energy_error_kwh": mean(m[1] for m in metrics) if metrics else None,
                "missing_case_reasons": dict(reasons),
            }


def run_scoring(run, evaluations):
    names = ("evaluation_config.json", "masked.json", "history.csv", "selected_counts.csv")
    upstream = completed_input(run, evaluations, "T06", names)
    config_file = upstream / "evaluation_config.json"
    config = read_json(config_file)
    run.info.update(config_sha256=sha256(config_file), seed=config["seed"])
    cases, predictions = predict_files(upstream / "masked.json", upstream / "history.csv", config)
    csv_rows(run.path / "predictions.csv", PREDICTION_COLUMNS, predictions)

    # This is the first stage that opens the truth file.
    completed_input(run, evaluations, "T06", [*names, "truth.csv"])
    truth = read_truth(upstream / "truth.csv")
    scores = score(cases, predictions, truth, run.path.name, config["methods"])
    csv_rows(run.path / "case_scores.csv", SCORE_COLUMNS, scores)
    planned = Counter()
    for row in records(upstream / "selected_counts.csv"):
        planned[int(row["n_bins"])] += int(row["cap"])
    summary = list(summarize(cases, scores, config, planned))
    write_json(run.path / "summary.json", summary)
    csv_rows(run.path / "summary.csv", tuple(summary[0]), (row.values() for row in summary))
    case_lengths = {case["case_id"]: case["n_bins"] for case in cases}
    levels = Counter((case_lengths[cid], method, fallback or reason or "not_applicable")
                     for cid, _, method, _, fallback, reason in predictions)
    csv_rows(run.path / "fallback_counts.csv", ("n_bins", "method", "level_or_reason", "bins"),
             ((*key, count) for key, count in sorted(levels.items())))

    with TemporaryDirectory(dir=run.path) as directory:
        check = Path(directory)
        for filename in ("masked.json", "history.csv", "truth.csv"):
            copyfile(upstream / filename, check / filename)
        changed = [(cid, slot, power + 1000000) for cid, slot, power in truth]
        csv_rows(check / "truth.csv", TRUTH_COLUMNS, changed)
        _, after = predict_files(check / "masked.json", check / "history.csv", config)
        after_score = score(cases, after, read_truth(check / "truth.csv"),
                            run.path.name, config["methods"])
        if after != predictions or (cases and scores == after_score):
            raise AssertionError("Truth isolation check failed")
    run.info.update(case_count=len(cases), prediction_rows=len(predictions),
                    case_score_rows=len(scores), changed_truth_leaves_predictions_identical=True)
