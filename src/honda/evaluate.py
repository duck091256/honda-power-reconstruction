import math
from datetime import UTC, datetime
from hashlib import sha256
from statistics import median

from .source import BIN, utc


def rank(seed, meter, start, n_bins):
    return sha256(f"{seed}|{meter}|{utc(start)}|{n_bins}".encode()).hexdigest()


def panel_values(panel):
    return {(row.meter_id, row.bin_us): row.power_w
            for row in panel.select("meter_id", "bin_us", "power_w").collect()}


def window(meter, start, n_bins, left, right, values, case_id, context_bins=4):
    end = start + n_bins * BIN
    masked = []
    truth = []
    for slot in range(max(left, start - context_bins * BIN),
                      min(right, end + context_bins * BIN), BIN):
        value = values[(meter, slot)]
        hidden = start <= slot < end
        if hidden:
            if value is None:
                raise ValueError(f"Missing reference in {case_id} at {utc(slot)}")
            truth.append((case_id, slot, value))
        masked.append((slot, None if hidden else value,
                       "hidden" if hidden else "missing" if value is None else "observed"))
    case = {"case_id": case_id, "meter_id": meter, "start_us": start,
            "n_bins": n_bins, "split": "evaluation", "context": masked}
    return case, truth


def calendar(slot):
    when = datetime.fromtimestamp(slot // 1_000_000, UTC)
    return when.weekday(), when.hour * 4 + when.minute // 15


def seasonal_profile(history):
    groups = {}
    for meter, slot, power in history:
        weekday, quarter = calendar(slot)
        for key in ((meter, weekday, quarter), (meter, quarter), (meter,)):
            groups.setdefault(key, []).append(power)
    return {key: median(values) for key, values in groups.items()}


def predict(cases, history, methods=("Linear", "Seasonal")):
    profile = seasonal_profile(history) if "Seasonal" in methods else {}
    for case in cases:
        meter = case["meter_id"]
        case_id = case["case_id"]
        context = {slot: (power, state) for slot, power, state in case["context"]}
        start = case["start_us"]
        n_bins = case["n_bins"]
        expected = set(range(start, start + n_bins * BIN, BIN))
        if (len(context) != len(case["context"])
                or set(slot for slot, (_, state) in context.items() if state == "hidden") != expected
                or any(context[slot][0] is not None for slot in expected)):
            raise ValueError(f"Invalid masked context for {case_id}")
        left = context.get(start - BIN, (None, "missing"))[0]
        right = context.get(start + n_bins * BIN, (None, "missing"))[0]
        for idx, slot in enumerate(sorted(expected), 1):
            if "Linear" in methods:
                power = None if left is None or right is None else (
                    left * (n_bins + 1 - idx) + right * idx) / (n_bins + 1)
                yield case_id, slot, "Linear", power, "", "missing_edge" if power is None else ""
            if "Seasonal" in methods:
                weekday, quarter = calendar(slot)
                lookups = [
                    ((meter, weekday, quarter), "weekday_slot"),
                    ((meter, quarter), "slot"),
                    ((meter,), "meter"),
                ]
                for key, level in lookups:
                    if key in profile:
                        yield case_id, slot, "Seasonal", profile[key], level, ""
                        break
                else:
                    yield case_id, slot, "Seasonal", None, "", "no_history"


def errors(pairs):
    differences = [estimate - reference for estimate, reference in pairs]
    return (math.fsum(map(abs, differences)) / len(differences),
            abs(math.fsum(differences)) * 0.25 / 1000)


def score(cases, predictions, truth, run_id, methods):
    refs = {(case_id, slot): power for case_id, slot, power in truth}
    if len(refs) != len(truth):
        raise ValueError("Duplicate truth keys")
    expected = {(c["case_id"], slot) for c in cases for slot, _, state in c["context"]
                if state == "hidden"}
    if set(refs) != expected or any(not math.isfinite(v) for v in refs.values()):
        raise ValueError("Truth must cover exactly the hidden keys")
    estimates = {(case_id, slot, method): (power, reason)
                 for case_id, slot, method, power, _, reason in predictions}
    wanted = {(key, slot, m) for key, slot in expected for m in methods}
    if len(estimates) != len(predictions) or set(estimates) != wanted:
        raise ValueError("Prediction keys must match hidden keys and methods")
    scores = []
    for case in cases:
        case_id = case["case_id"]
        for method in methods:
            pairs = []
            missing = set()
            for slot in range(case["start_us"], case["start_us"] + case["n_bins"] * BIN, BIN):
                power, reason = estimates[(case_id, slot, method)]
                if power is None:
                    missing.add(reason or "missing_prediction")
                elif math.isfinite(power):
                    pairs.append((power, refs[(case_id, slot)]))
                else:
                    raise ValueError(f"Nonfinite prediction in {case_id}")
            mae, energy = (None, None) if missing else errors(pairs)
            scores.append((run_id, case_id, method, mae, energy, "|".join(sorted(missing))))
    return scores
