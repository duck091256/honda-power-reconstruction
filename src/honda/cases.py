from collections import Counter
from pathlib import Path
from shutil import copyfile

from .evaluate import panel_values, rank, window
from .panel import spark_session
from .run import read_json, sha256, write_json
from .source import BIN, csv_rows, micros, utc

CASE_COLUMNS = ("case_id", "meter_id", "start_utc", "start_us", "n_bins", "split")
TRUTH_COLUMNS = ("case_id", "bin_us", "ref_w")
HISTORY_COLUMNS = ("meter_id", "bin_us", "observed_w")


def time_bounds(config):
    days = ("history_start", "history_end", "test_start", "test_end")
    hist_start, hist_end, test_start, test_end = (
        micros(config[f"{day}_utc"]) for day in days)
    bounds = hist_start, hist_end, test_start, test_end
    if (not hist_start < hist_end <= test_start < test_end
            or any(slot % BIN for slot in bounds)):
        raise ValueError("Expected disjoint, chronological and aligned time ranges")
    return bounds


def eligible_start(values, meter, start, n_bins, left, right):
    end = start + n_bins * BIN
    if start - BIN < left or end >= right:
        return "outside_test_edges"
    if any(values.get((meter, slot)) is None for slot in range(start, end, BIN)):
        return "invalid_reference"
    if values.get((meter, start - BIN)) is None or values.get((meter, end)) is None:
        return "invalid_edge"
    return "eligible"


def select_cases(values, meters, config):
    h_start, h_end, t_start, t_end = time_bounds(config)
    history = [(m, t, value) for (m, t), value in sorted(values.items())
               if h_start <= t < h_end and value is not None]
    cases, truth, candidates = [], [], []
    for meter in meters:
        for n_bins in config["gap_bins"]:
            ranked = sorted((rank(config["seed"], meter, start, n_bins), start)
                            for start in range(t_start, t_end, BIN))
            used = set()
            selected = 0
            for case_id, start in ranked:
                reason = eligible_start(values, meter, start, n_bins, t_start, t_end)
                if reason == "eligible":
                    hidden = set(range(start, start + n_bins * BIN, BIN))
                    if selected >= config["cap_per_meter_per_duration"]:
                        reason = "cap_reached"
                    elif hidden & used:
                        reason = "overlap_selected"
                    else:
                        used.update(hidden)
                        selected += 1
                        reason = "selected"
                        case, refs = window(meter, start, n_bins, t_start, t_end,
                                            values, case_id, config["context_each_side_bins"])
                        cases.append(case)
                        truth.extend(refs)
                candidates.append((meter, n_bins, utc(start), case_id, reason))
    return cases, truth, history, candidates


def prepare_evaluation(run, upstream, config_path):
    cfg = read_json(config_path)
    h_start, _, _, t_end = time_bounds(cfg)
    source = read_json(upstream / "source_manifest.json")
    if (h_start < source["start_us"] or t_end > source["end_us"]
            or cfg["gap_bins"] != [1, 4, 24]
            or cfg["methods"] != ["Linear", "Seasonal"]
            or cfg["seasonal_fallback"] != ["weekday_slot", "slot", "meter"]
            or type(cfg["cap_per_meter_per_duration"]) is not int
            or cfg["cap_per_meter_per_duration"] < 1
            or cfg["context_each_side_bins"] < 1):
        raise ValueError("Configuration does not match evaluation contract and panel scope")
    snapshot = Path(copyfile(config_path, run.path / "evaluation_config.json"))
    run.info.update(config_sha256=sha256(snapshot), seed=cfg["seed"],
                    scope={k: v for k, v in cfg.items() if k.endswith("_utc")})
    run.save()
    meters = sorted(m["meter_id"] for m in source["meters"])
    with spark_session(run) as spark:
        values = panel_values(spark.read.parquet(str(upstream / "panel_15m")))
    cases, truth, history, candidates = select_cases(values, meters, cfg)
    csv_rows(run.path / "cases.csv", CASE_COLUMNS,
             ((c["case_id"], c["meter_id"], utc(c["start_us"]), c["start_us"],
               c["n_bins"], c["split"]) for c in cases))
    write_json(run.path / "masked.json", cases)
    csv_rows(run.path / "truth.csv", TRUTH_COLUMNS, truth)
    csv_rows(run.path / "history.csv", HISTORY_COLUMNS, history)
    csv_rows(run.path / "candidate_audit.csv",
             ("meter_id", "n_bins", "start_utc", "case_id", "reason"), candidates)
    counts = Counter((meter, n, reason) for meter, n, _, _, reason in candidates)
    csv_rows(run.path / "candidate_counts.csv", ("meter_id", "n_bins", "reason", "starts"),
             ((*key, count) for key, count in sorted(counts.items())))
    csv_rows(run.path / "selected_counts.csv", ("meter_id", "n_bins", "selected", "cap", "below_cap"),
             ((meter, n, counts[meter, n, "selected"], cfg["cap_per_meter_per_duration"],
               cfg["cap_per_meter_per_duration"] - counts[meter, n, "selected"])
              for meter in meters for n in cfg["gap_bins"]))
    run.info.update(case_count=len(cases), truth_rows=len(truth), history_rows=len(history),
                    candidate_rows=len(candidates))
