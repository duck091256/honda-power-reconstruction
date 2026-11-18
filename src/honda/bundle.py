from pathlib import Path
from shutil import copyfile

from .run import completed_input, read_json, sha256, write_json


def build_bundle(run, panel_run, eval_run, score_run, cost_run):
    sources = [
        (panel_run, "T05", ["source_manifest.json", "coverage.csv"]),
        (eval_run, "T06", ["evaluation_config.json", "masked.json", "truth.csv",
                          "selected_counts.csv", "candidate_counts.csv"]),
        (score_run, "T07", ["predictions.csv", "case_scores.csv", "summary.json", "fallback_counts.csv"]),
        (cost_run, "T08", ["cost.json"]),
    ]
    bundle = run.path / "bundle"
    bundle.mkdir()
    origin = {}
    for path, task, names in sources:
        upstream = completed_input(run, path, task, names)
        origin[task] = str(upstream)
        for name in names:
            copyfile(upstream / name, bundle / name)
    # Require the selected scores and benchmark to have been generated from this evaluation.
    for task in ("T07", "T08"):
        entry = read_json(Path(origin[task]) / "run_manifest.json")
        if (origin["T06"] not in entry["inputs"]
                or entry["config_sha256"] != sha256(bundle / "evaluation_config.json")):
            raise ValueError(f"{task} did not use this evaluation")
    if origin["T05"] not in read_json(Path(origin["T06"]) / "run_manifest.json")["inputs"]:
        raise ValueError("Evaluation and source manifest must come from the same panel")
    write_json(bundle / "bundle_manifest.json", {
        "run_id": run.path.name, "upstream_runs": origin,
        "hashes": {p.name: sha256(p) for p in sorted(bundle.iterdir())},
        "kind": read_json(bundle / "source_manifest.json")["kind"],
    })
    run.info["case_count"] = len(read_json(bundle / "masked.json"))
