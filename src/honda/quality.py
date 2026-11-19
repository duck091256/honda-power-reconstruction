"""Run the Q01–Q05 quality-map workflow."""
import argparse
import sys
from pathlib import Path

from .quality_inputs import Checks, lock
from .run import Run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["Q01", "Q02", "Q03", "Q04", "Q05"])
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--prior")
    parser.add_argument("--inventory", type=Path, default=Path("runs/t11_inventory"))
    parser.add_argument("--full", type=Path, default=Path("runs/t14_full"))
    parser.add_argument("--config", type=Path, default=Path("configs/quality_v1.json"))
    parser.add_argument("--lock", type=Path, default=Path("runs/q01_lock_r2"))
    parser.add_argument("--fixture", type=Path, default=Path("runs/q02_fixture_r6"))
    parser.add_argument("--map", type=Path, default=Path("runs/q03_map_r2"))
    parser.add_argument("--benchmark", type=Path, default=Path("runs/q04_benchmark"))
    args = parser.parse_args()
    with Run(args.run, args.task, prior=args.prior) as run:
        run.info["command"] = [sys.executable, "-m", "honda.quality", *sys.argv[1:]]
        run.save()
        checks = Checks(run.path / "checks.json")
        if args.task == "Q01":
            lock(run, checks, args.inventory, args.full, args.config)
        elif args.task == "Q02":
            from .quality_fixture import exercise
            exercise(run, checks, args)
        elif args.task == "Q03":
            from .quality_workflow import build_maps
            build_maps(run, checks, args)
        elif args.task == "Q04":
            from .quality_workflow import benchmark
            benchmark(run, checks, args)
        else:
            from .quality_delivery import deliver
            deliver(run, checks, args)


if __name__ == "__main__":
    main()
