from .evaluate import errors, panel_values, predict, rank, score, window
from .panel import save_panel, spark_session
from .run import write_json
from .scoring import PREDICTION_COLUMNS, SCORE_COLUMNS, TRUTH_COLUMNS
from .source import BIN, csv_rows, micros, stage


def development(run, config_path, source_root):
    meter = "H1.Z20"
    start, end = map(micros, ("2023-03-03T00:00:00Z", "2023-03-05T00:00:00Z"))
    stage(run, config_path, source_root, [meter], start, end)
    with spark_session(run) as spark:
        values = panel_values(save_panel(spark, run, run.path))
    n_bins = 4
    gap = next(t for t in range(start + BIN, end - n_bins * BIN, BIN)
               if all(values[(meter, slot)] is not None
                      for slot in range(t - BIN, t + (n_bins + 1) * BIN, BIN)))
    case, truth = window(meter, gap, n_bins, start, end, values, rank(42, meter, gap, n_bins))
    write_json(run.path / "masked.json", [case])
    csv_rows(run.path / "truth.csv", TRUTH_COLUMNS, truth)
    predictions = list(predict([case], [], ("Linear",)))
    csv_rows(run.path / "predictions.csv", PREDICTION_COLUMNS, predictions)
    scores = score([case], predictions, truth, run.path.name, ("Linear",))
    csv_rows(run.path / "case_scores.csv", SCORE_COLUMNS, scores)
    if errors([(1000.0, 0.0)] * 4) != (1000.0, 1.0):
        raise AssertionError("Four quarter hours at +1000 W must yield 1 kWh")
    run.info.update(case_count=1, fixture_unit_check="1000 W for 1 h = 1 kWh")
