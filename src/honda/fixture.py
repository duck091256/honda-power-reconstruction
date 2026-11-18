from .run import write_json
from .source import BIN, MINUTE, MINUTE_COLUMNS, csv_rows, micros

EXPECTED = [
    (0, "valid", 15, -7.0, 1),
    (1, "missing_minute", 14, None, 0),
    (2, "conflicting_duplicate", 15, None, 0),
    (3, "off_minute_grid", 15, None, 0),
    (4, "issue_interval", 15, None, 0),
    (5, "nonfinite", 15, None, 0),
    (6, "valid", 15, 0.0, 0),
    (7, "missing_minute", 0, None, 0),
]


def prepare(path):
    start = micros("2023-01-01T00:00:00Z")
    assert start == micros("2023-01-01T07:00:00+07:00")
    # First point is identical after normalizing its explicit +07:00 offset.
    points = [(micros("2023-01-01T07:00:00+07:00"), -14)]
    for i in range(7):
        for j in range(15):
            if (i, j) == (1, 14):
                continue
            value = j - 14 if i == 0 else 0
            points.append((start + BIN * i + MINUTE * j, None if (i, j) == (5, 1) else value))
    points.extend([(start + 2 * BIN, 100), (start + 4 * BIN - 1, 10)])
    csv_rows(path / "source_fixture.csv", MINUTE_COLUMNS,
             (("fixture", epoch, value, "synthetic") for epoch, value in points))
    csv_rows(path / "issue_bins.csv", ("meter_id", "bin_us"), [("fixture", start + 4 * BIN)])
    write_json(path / "source_manifest.json", {
        "start_us": start, "end_us": start + 8 * BIN, "kind": "fixture_mo_phong",
        "meters": [{"meter_id": "fixture", "source_id": "synthetic",
                    "row_counts": {"selected": len(points)}}],
    })


def verify(panel):
    rows = panel.orderBy("bin_us").collect()
    start = micros("2023-01-01T00:00:00Z")
    actual = [((r.bin_us - start) // BIN, r.reason, r.minute_count,
               r.power_w, r.identical_duplicates) for r in rows]
    if actual != EXPECTED or any(r.valid_ref != (r.power_w is not None) for r in rows):
        raise AssertionError(f"Fixture differs from hand calculation: {actual}")
