"""Isolated, resource-monitored worker for one engine and one scale."""
import os
import sys
from pathlib import Path
from time import monotonic

from .quality_engine import build, detail, engine, load_map, load_panel, streak_channel_names
from .run import read_json, write_json


def work(folder):
    job = read_json(folder / "job.json")
    scratch = folder / "scratch"
    results = dict(queries=[])

    def phase(name):
        now = monotonic()
        temp = folder / "phase.tmp"
        write_json(temp, dict(name=name, started=now))
        temp.replace(folder / "phase.json")
        return now

    start = phase("startup")
    with engine(job["engine"], job["config"], scratch) as db:
        db.rows("SELECT 1")
        end = monotonic()
        panel, catalog, meters = Path(job["panel"]), Path(job["catalog"]), job["meters"]
        output = folder / "tables"
        results["startup_s"] = end - start
        start = phase("build")
        intervals = build(db, panel, meters, catalog, output, scratch)
        results["build_s"] = monotonic() - start
        results["selected_log_intervals"] = intervals
        results["output_bytes"] = sum(p.stat().st_size for p in output.glob("*.parquet"))
        write_json(folder / "worker_result.json", results)

        if job["audit_keys"]:
            phase("query_setup")
            results["audit_channels"] = streak_channel_names(db, job["audit_keys"], scratch)
            write_json(folder / "worker_result.json", results)
        if job["queries"]:
            phase("query_setup")
            load_panel(db, panel, meters)
            load_map(db, output)
            for index, key in enumerate(job["queries"]):
                start = phase(f"query_{index}")
                data = detail(db, key)
                results["queries"].append(dict(wall_s=monotonic() - start, detail=data))
                write_json(folder / "worker_result.json", results)
        phase("shutdown")


if __name__ == "__main__":
    # Parquet staging and any Python/Java subprocess temp files belong to this round.
    os.environ["TZ"] = "UTC"
    work(Path(sys.argv[1]).resolve())
