import unittest

from honda.cases import eligible_start, select_cases, time_bounds
from honda.evaluate import errors, predict, score
from honda.source import BIN, micros


class EvaluationContract(unittest.TestCase):
    def setUp(self):
        self.start = micros("2023-01-01T00:00:00Z")
        self.end = self.start + 8 * BIN
        self.values = {("m", self.start + idx * BIN): float(idx) for idx in range(8)}

    def test_both_edges_must_stay_in_test(self):
        left, right = self.start, self.end
        self.assertEqual(eligible_start(self.values, "m", left, 4, left, right), "outside_test_edges")
        self.assertEqual(eligible_start(self.values, "m", left + 4 * BIN, 4, left, right),
                         "outside_test_edges")
        self.assertEqual(eligible_start(self.values, "m", left + 3 * BIN, 4, left, right), "eligible")

    def test_no_historical_overlap_and_hidden_values(self):
        config = {
            "history_start_utc": "2023-01-01T00:00:00Z",
            "history_end_utc": "2023-01-01T00:30:00Z",
            "test_start_utc": "2023-01-01T00:30:00Z",
            "test_end_utc": "2023-01-01T02:00:00Z",
            "gap_bins": [4], "cap_per_meter_per_duration": 30, "seed": 42,
            "context_each_side_bins": 4,
        }
        cases, truth, history, _ = select_cases(self.values, ["m"], config)
        self.assertEqual(len(cases), 1)
        self.assertEqual(len(truth), 4)
        self.assertTrue(all(t < self.start + 2 * BIN for _, t, _ in history))
        self.assertTrue(all(t >= self.start + 2 * BIN for t, _, _ in cases[0]["context"]))
        self.assertTrue(all(v is None for _, v, state in cases[0]["context"] if state == "hidden"))
        estimates = list(predict(cases, history, ("Linear",)))
        scores = score(cases, estimates, truth, "test", ("Linear",))
        self.assertEqual(scores[0][3:5], (0.0, 0.0))
        new_truth = [(cid, time, value + 1000) for cid, time, value in truth]
        self.assertEqual(estimates, list(predict(cases, history, ("Linear",))))
        self.assertEqual(score(cases, estimates, new_truth, "test", ("Linear",))[0][3:5],
                         (1000.0, 1.0))
        with self.assertRaises(ValueError):
            score(cases, estimates, [*truth, truth[-1]], "test", ("Linear",))
        config["test_start_utc"] = "2023-01-01T00:15:00Z"
        with self.assertRaises(ValueError):
            time_bounds(config)

    def test_energy_signed_sum(self):
        self.assertEqual(errors([(1000.0, 0.0)] * 4), (1000.0, 1.0))
        self.assertEqual(errors([(1.0, 0.0), (-1.0, 0.0)]), (1.0, 0.0))

    def test_seasonal_fallback_and_missing_history(self):
        slot = self.start + BIN
        case = {"meter_id": "m", "case_id": "f", "n_bins": 1, "start_us": slot,
                "context": [(slot, None, "hidden")]}
        preceding_day = 96 * BIN
        for history, expected in [
            ([("m", slot - 7 * preceding_day, 9.0)], (9.0, "weekday_slot", "")),
            ([("m", slot - preceding_day, 8.0)], (8.0, "slot", "")),
            ([("m", slot - preceding_day + BIN, 7.0)], (7.0, "meter", "")),
            ([], (None, "", "no_history")),
        ]:
            predicted = list(predict([case], history, ("Seasonal",)))
            self.assertEqual(predicted[0][3:], expected)
