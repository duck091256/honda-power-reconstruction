import argparse
from pathlib import Path

from .run import Run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("task", choices=["T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10"])
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--sources", type=Path, default=Path("configs/sources_v1.json"))
    parser.add_argument("--prior")
    parser.add_argument("--panel-run", type=Path)
    parser.add_argument("--eval-run", type=Path)
    parser.add_argument("--score-run", type=Path)
    parser.add_argument("--cost-run", type=Path)
    parser.add_argument("--demo-run", type=Path)
    parser.add_argument("--repeat-demo-run", type=Path)
    parser.add_argument("--evaluation", type=Path, default=Path("configs/evaluation_v1.json"))
    args = parser.parse_args()
    required = {
        "T02": ["source_root"], "T04": ["source_root"], "T05": ["source_root"],
        "T06": ["panel_run"], "T07": ["eval_run"], "T08": ["panel_run", "eval_run"],
        "T09": ["panel_run", "eval_run", "score_run", "cost_run"],
        "T10": ["demo_run", "repeat_demo_run"],
    }
    for name in required.get(args.task, []):
        if getattr(args, name) is None:
            parser.error(f"{args.task} requires --{name.replace('_', '-')}")
    with Run(args.run, args.task, prior=args.prior) as run:
        if args.task == "T02":
            from .source import audit_sample
            audit_sample(run, args.sources, args.source_root.resolve())
        elif args.task == "T03":
            from .fixture import prepare, verify
            from .panel import save_panel, spark_session
            prepare(run.path)
            with spark_session(run) as spark:
                verify(save_panel(spark, run, run.path))
        elif args.task == "T04":
            from .development import development
            development(run, args.sources, args.source_root.resolve())
        elif args.task == "T05":
            from .run import read_json
            from .source import micros, stage
            from .panel import save_panel, spark_session
            cfg = read_json(args.sources)
            stage(run, args.sources, args.source_root.resolve(), sorted(cfg["meters"]),
                  micros(cfg["start_utc"]), micros(cfg["end_utc"]))
            with spark_session(run) as spark:
                save_panel(spark, run, run.path)
        elif args.task == "T06":
            from .cases import prepare_evaluation
            from .run import completed_input
            panel = completed_input(run, args.panel_run, "T05", ["panel_15m", "source_manifest.json"])
            prepare_evaluation(run, panel, args.evaluation)
        elif args.task == "T07":
            from .scoring import run_scoring
            run_scoring(run, args.eval_run)
        elif args.task == "T08":
            from .benchmark import run_benchmark
            run_benchmark(run, args.panel_run, args.eval_run)
        elif args.task == "T09":
            from .bundle import build_bundle
            build_bundle(run, args.panel_run, args.eval_run, args.score_run, args.cost_run)
        else:
            from .reproduce import package, verify_reproduction
            bundle, evidence = verify_reproduction(run, args.demo_run, args.repeat_demo_run)
            package(run, bundle, evidence)


if __name__ == "__main__":
    main()
