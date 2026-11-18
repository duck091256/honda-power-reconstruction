import math
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile

from .run import completed_input, read_json, sha256, write_json
from .scoring import records

POWER_COLUMNS = {"ref_w", "observed_w", "pred_w", "mae_w", "abs_energy_error_kwh"}


def compare(a, b, tolerance, label):
    if isinstance(a, float) or isinstance(b, float):
        same = (isinstance(a, (float, int)) and isinstance(b, (float, int))
                and math.isclose(a, b, rel_tol=tolerance["relative"], abs_tol=tolerance["absolute"]))
    elif type(a) is not type(b):
        same = False
    elif isinstance(a, dict):
        same = a.keys() == b.keys()
        if same:
            for key in a:
                compare(a[key], b[key], tolerance, f"{label}/{key}")
    elif isinstance(a, (list, tuple)):
        same = len(a) == len(b)
        if same:
            for i, (first, second) in enumerate(zip(a, b)):
                compare(first, second, tolerance, f"{label}/{i}")
    else:
        same = a == b
    if not same:
        raise AssertionError(f"Reproduction mismatch: {label}")


def csv_contents(path):
    rows = []
    for record in records(path):
        rows.append({key: float(value) if key in POWER_COLUMNS and value else value
                     for key, value in record.items() if key != "run_id"})
    return rows


def parquet_contents(path):
    import pyarrow.parquet as pq

    return sorted(pq.read_table(path).to_pylist(), key=lambda r: (r["meter_id"], r["bin_us"]))


def verify_reproduction(run, baseline, repeat):
    run_paths = [completed_input(run, path, "T09", ["bundle"]) for path in (baseline, repeat)]
    bundle_paths = [path / "bundle" for path in run_paths]
    manifests = [read_json(path / "bundle_manifest.json") for path in bundle_paths]
    config_hashes = [sha256(path / "evaluation_config.json") for path in bundle_paths]
    if len(set(config_hashes)) != 1:
        raise AssertionError("Evaluation configuration changed")
    run.info["config_sha256"] = config_hashes[0]
    config = read_json(bundle_paths[0] / "evaluation_config.json")
    tolerance = config["tolerance"]
    tolerance_files = ("masked.json", "summary.json", "truth.csv", "predictions.csv", "case_scores.csv")
    exact_files = ("source_manifest.json", "coverage.csv", "selected_counts.csv",
                   "candidate_counts.csv", "fallback_counts.csv")
    for file in tolerance_files:
        load = read_json if file.endswith(".json") else csv_contents
        compare(*(load(path / file) for path in bundle_paths), tolerance, file)
    for file in exact_files:
        if len({sha256(path / file) for path in bundle_paths}) != 1:
            raise AssertionError(f"Reproduction mismatch: {file}")

    eval_runs = [completed_input(run, info["upstream_runs"]["T06"], "T06",
                                 ["history.csv", "cases.csv", "candidate_audit.csv"])
                 for info in manifests]
    compare(*(csv_contents(path / "history.csv") for path in eval_runs), tolerance, "history")
    for file in ("cases.csv", "candidate_audit.csv"):
        if len({sha256(path / file) for path in eval_runs}) != 1:
            raise AssertionError(f"Reproduction mismatch: {file}")
    panel_runs = [completed_input(run, info["upstream_runs"]["T05"], "T05", ["panel_15m"])
                  for info in manifests]
    panels = [parquet_contents(path / "panel_15m") for path in panel_runs]
    compare(*panels, tolerance, "panel")
    cost = [read_json(path / "cost.json") for path in bundle_paths]
    compare(cost[0]["query"], cost[1]["query"], tolerance, "benchmark query")
    query_counts = {item["rows"] for result in cost
                    for layout in result["layouts"].values() for item in layout["runs"]}
    if len(query_counts) != 1:
        raise AssertionError("Benchmark query row count changed")
    evidence = {
        "baseline": str(run_paths[0]),
        "repeat": str(run_paths[1]),
        "evaluation_hash": config_hashes[0],
        "float_tolerance": tolerance,
        "panel_rows": len(panels[0]),
        "matching_exact": [*exact_files, "cases.csv", "candidate_audit.csv"],
        "matching_with_tolerance": [*tolerance_files, "panel_15m", "history.csv"],
        "panel_digest_exact": cost[0]["panel"] == cost[1]["panel"],
        "query_rows": next(iter(query_counts)),
        "query_digest_exact": cost[0]["layouts"]["plain"]["runs"][0]["digest_sha256"]
        == cost[1]["layouts"]["plain"]["runs"][0]["digest_sha256"],
    }
    return bundle_paths[0], evidence


def package(run, bundle, evidence):
    import sys
    from streamlit.testing.v1 import AppTest

    archive = run.path / "honda-power-reconstruction.zip"
    workspace = Path(__file__).resolve().parents[2]
    evidence_file = run.path / "verification.json"
    write_json(evidence_file, evidence)
    deliverables = [workspace / p for p in ("README.md", "SPEC.md", "PLAN.md", "requirements.txt",
                                           "requirements-demo.txt", "app/main.py")]
    for folder in ("src", "configs", "docs", "tests"):
        deliverables.extend(p for p in (workspace / folder).rglob("*")
                            if p.is_file() and p.suffix in (".py", ".md", ".json"))
    with ZipFile(archive, "w", ZIP_DEFLATED) as zip_file:
        for p in sorted(deliverables):
            zip_file.write(p, p.relative_to(workspace))
        for p in sorted(bundle.iterdir()):
            zip_file.write(p, Path("bundle") / p.name)
        zip_file.write(evidence_file, evidence_file.name)
    with TemporaryDirectory(dir=run.path) as isolated:
        with ZipFile(archive) as zip_file:
            if zip_file.testzip() is not None:
                raise ValueError("ZIP failed CRC check")
            zip_file.extractall(isolated)
        folder = Path(isolated).resolve()
        app = folder / "app/main.py"
        argv = sys.argv
        try:
            sys.argv = [str(app), "--bundle", str(folder / "bundle")]
            demo = AppTest.from_file(app, default_timeout=20).run()
            if demo.exception or not demo.get("arrow_vega_lite_chart"):
                raise AssertionError(f"Extracted demo failed: {demo.exception}")
            demo.checkbox(key="truth").check().run()
            if demo.exception or not demo.checkbox(key="truth").value:
                raise AssertionError("Extracted demo reference toggle failed")
        finally:
            sys.argv = argv
    run.info.update(zip_demo_verified=True, zip_bytes=archive.stat().st_size)
