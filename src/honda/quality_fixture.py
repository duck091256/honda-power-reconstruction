"""Hand-calculated synthetic maps, with order and boundary regression checks."""
import pyarrow as pa
import pyarrow.parquet as pq

from .quality_engine import PANEL_COLUMNS, SORT, TABLES, build, detail, engine, load_map
from .run import completed_input, read_json, sha256, write_json
from .source import BIN, micros

ORIGIN = micros("2023-01-31T23:30:00+00:00")
METERS = ["A", "B", "C"]
MISSING = "missing_minute"
CLOCK = "clock_unverified"


def tick(cell):
    return ORIGIN + cell * BIN


def fixture():
    panel = {
        "A": [(0, MISSING), (1, MISSING), (2, CLOCK), (3, CLOCK),
              (96, MISSING), (97, "valid"), (98, MISSING)],
        "B": [(0, "valid"), (1, "valid"), (2, MISSING), (3, MISSING),
              (96, MISSING), (97, "valid"), (98, "valid")],
        "C": [(2, "valid"), (3, "valid"), (96, MISSING), (97, "valid"), (98, "valid")],
    }
    catalog = {"channels": {
        "A": {"intervals": [[tick(start)//1_000_000, tick(end)//1_000_000]
                            for start, end in [(-1, 0), (-100, 3), (4, 4)]]},
        "B": {"intervals": []},
        "C": {"intervals": [[tick(0)//1_000_000, tick(1)//1_000_000]]},
    }}
    a = ("A", tick(0), tick(4))
    b = ("B", tick(2), tick(4))
    extra = [(m, tick(96), tick(97)) for m in METERS] + [("A", tick(98), tick(99))]
    expected = {
        "episodes": [(*a, 4), (*b, 2), *[(*key, 1) for key in extra]],
        "episode_reasons": [(*a, MISSING, 2), (*a, CLOCK, 2), (*b, MISSING, 2),
                            *[(*key, MISSING, 1) for key in extra]],
        "timeline": [(tick(i), *counts) for i, counts in
                     [(0, (2, 1, 1)), (1, (2, 1, 1)), (2, (3, 1, 2)), (3, (3, 1, 2)),
                      (96, (3, 0, 3)), (97, (3, 3, 0)), (98, (3, 2, 1))]],
        "support": [(*a, 4, 4, 0), (*b, 2, 2, 1),
                    *[(*key, 1, 0, 0) for key in extra[:3]], (*extra[-1], 1, 1, 2)],
        "log_links": [(*a, 0, tick(-1), tick(0)), (*a, 1, tick(-100), tick(3))],
    }
    a_detail = dict(key=list(a), support=[4, 4, 0], reasons=[[CLOCK, 2], [MISSING, 2]],
                    channels=[["B", 2], ["C", 2]],
                    logs=[[0, tick(-1), tick(0)], [1, tick(-100), tick(3)]])
    return panel, catalog, {name: sorted(rows) for name, rows in expected.items()}, a_detail


def exercise(run, checks, args):
    locked = completed_input(run, args.lock, "Q01", ["quality_config.json", "lock.json"])
    config_path = locked / "quality_config.json"
    config = read_json(config_path)
    run.info["config_sha256"] = sha256(config_path)
    panel, catalog, expected, a_detail = fixture()
    catalog_path = run.path / "fixture_intervals.json"
    write_json(catalog_path, catalog)
    write_json(run.path / "expected.json", dict(tables=expected, example=a_detail, synthetic=True))
    for reverse in (False, True):
        folder = run.path / ("fixture_reversed" if reverse else "fixture")
        folder.mkdir()
        for meter, values in panel.items():
            rows = [(meter, tick(cell), reason == "valid", reason) for cell, reason in values]
            if reverse:
                rows.reverse()
            pq.write_table(pa.Table.from_pylist([dict(zip(PANEL_COLUMNS, row)) for row in rows]),
                           folder / f"{meter}.parquet")
    for name in ("duckdb", "spark"):
        home = run.path / name
        with engine(name, config, run.path / "scratch" / name) as db:
            for ordering in ("fixture", "fixture_reversed"):
                folder = home / ordering
                scratch = run.path / "scratch" / name / ordering
                scratch.mkdir()
                if name == "spark":
                    db.scratch = scratch
                build(db, run.path / ordering, METERS, catalog_path, folder, scratch)
                load_map(db, folder)
                for table, columns in TABLES.items():
                    result = db.rows(f"SELECT {', '.join(columns)} FROM {table} ORDER BY {', '.join(SORT[table])}")
                    checks.check(f"{name}/{ordering}/{table}", expected[table], result, folder / f"{table}.parquet")
                checks.check(f"{name}/{ordering}/cross-month episode detail", a_detail,
                             detail(db, a_detail["key"]), folder)
    run.info.update(synthetic=True, rows=sum(map(len, panel.values())), engines=["duckdb", "spark"])
