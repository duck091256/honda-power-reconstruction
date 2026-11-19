"""Q05 replay, portable Streamlit bundle, and evidence-backed report."""
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from shutil import copy2, copytree
from zipfile import ZIP_STORED, ZipFile

import duckdb
import pyarrow.parquet as pq

from .quality_audit import PanelAudit, compare_maps, signature
from .quality_engine import PANEL_COLUMNS, Duck, detail, load_map, load_panel
from .quality_workflow import handoff_inputs, measure, verified_map
from .run import completed_input, read_json, sha256, write_json

ROOT = Path(__file__).resolve().parents[2]


def months_between(start, end):
    date = datetime.fromtimestamp(start / 1_000_000, UTC).replace(day=1, hour=0, minute=0, second=0)
    months = {}
    while date.timestamp() * 1_000_000 < end:
        next_month = date.replace(year=date.year + 1, month=1) if date.month == 12 else date.replace(month=date.month + 1)
        months[date.strftime("%Y-%m")] = [int(date.timestamp()) * 1_000_000, int(next_month.timestamp()) * 1_000_000]
        date = next_month
    return months


def compact_panel(panel, target, meters):
    target.mkdir()
    for meter in meters:
        name = f"{meter}.parquet"
        table = pq.read_table(panel / name, columns=list(PANEL_COLUMNS))
        pq.write_table(table, target / name, compression="snappy", use_dictionary=["meter_id", "reason"])


def examples(db, bundle, months, meters, specs):
    load_map(db, bundle / "tables")
    representative = db.rows("""
        SELECT s.meter_id, s.start_us, s.end_us
        FROM support s WHERE bins=24 AND continuous_channels>0
        ORDER BY CASE WHEN EXISTS (SELECT 1 FROM log_links l WHERE l.meter_id=s.meter_id
                    AND l.start_us=s.start_us AND l.end_us=s.end_us) THEN 0 ELSE 1 END,
                 s.meter_id, s.start_us, s.end_us LIMIT 1
    """)
    if len(representative) != 1:
        raise ValueError("Expected a checked six-hour real episode with continuous support")
    key = representative[0]
    meter, start, _ = key
    month = datetime.fromtimestamp(start / 1_000_000, UTC).strftime("%Y-%m")
    load_panel(db, bundle / "panel", meters)
    observed = detail(db, key)
    episodes = db.rows("SELECT meter_id, start_us, end_us FROM episodes ORDER BY meter_id, start_us, end_us")
    occupied = {(m, name) for m, a, b in episodes for name in months_between(a, b)}
    empty = next(([name, m] for name, (a, b) in months.items() for m in meters
                  if (m, name) not in occupied and a >= specs[m]["start_us"] and b <= specs[m]["end_us"]),
                 None)
    if empty is None:
        empty = next([name, m] for name in months for m in meters if (m, name) not in occupied)
    return [month, meter, key], empty, observed


def num(value, decimals=0):
    return f"{value:,.{decimals}f}".replace(",", "_").replace(".", ",").replace("_", ".")


def report(path, run, audited, stats, locked, build_choice, query_choice):
    total, supported = audited["episodes"], audited["episodes_with_continuous_support"]
    heading = """# Bản đồ chất lượng công suất Honda

"""
    overview = (
        f"**{num(supported)}/{num(total)} đợt ({num(100*supported/total, 2)}%)** "
        "có ít nhất một kênh khác hợp lệ suốt đợt. "
        f"Trên 71 kênh có {num(audited['scope']['bins'])} ô trong phạm vi, "
        f"gồm {num(audited['valid_bins'])} ô hợp lệ và {num(audited['invalid_bins'])} ô không hợp lệ. "
        "Mỗi đợt nằm trong một công tơ và được xác định trên toàn lịch sử trước khi lọc tháng. "
        "Một kênh hợp lệ ở tất cả các ô là điều kiện về độ phủ để chọn ca cho thí nghiệm đa công tơ tiếp theo.\n\n"
        f"Trên {locked['machine']['cpu']} với hai luồng, dùng **{build_choice} để dựng bảng** "
        f"và **{query_choice} để tra chi tiết** dựa trên thời gian trung vị ở mức 71 kênh. "
        "Chi phí khởi động được ghi riêng. Tra cứu dùng cùng đợt, cùng danh sách kênh và cùng liên kết nhật ký.\n\n"
    )
    rows = ["| Kênh | Ô đầu vào | Byte Parquet | Công cụ | Khởi động (s) | Dựng (s) | RSS đỉnh khi dựng (MiB) | Tạm đỉnh khi dựng (MiB) | Tra ngắn / dài / gần trung vị (s) |",
            "|---:|---:|---:|---|---:|---:|---:|---:|---|"]
    for level in sorted(stats, key=int):
        item = stats[level]
        scope = item["scope"]
        for name, label in (("duckdb", "DuckDB"), ("spark", "Spark local")):
            values = item["engines"][name]
            prefix = f"| {level} | {num(scope['bins'])} | {num(scope['panel_bytes'])} | {label} |"
            if values["status"] == "OK":
                query = " / ".join(num(s, 3) for s in values["query_median_s"])
                rows.append(f"{prefix} {num(values['startup_median_s'], 3)} | {num(values['build_median_s'], 3)} | "
                            f"{num(values['build_max_tree_rss_bytes']/2**20, 1)} | "
                            f"{num(values['build_max_scratch_bytes']/2**20, 1)} | {query} |")
            else:
                bound = values["budget"]
                rows.append(f"{prefix} — | Chạm {bound['resource']} ({bound['limit']}) | — | — | — |")
    ratios = []
    for level in sorted(stats, key=int):
        values = stats[level]["engines"]
        if "spark_over_duckdb_build" in values:
            ratios.append(f"{level} kênh {num(values['spark_over_duckdb_build'], 2)}×")
    ratio_line = ("\n\nTỷ số thời gian dựng Spark/DuckDB lần lượt là " + ", ".join(ratios) + ".") if ratios else ""
    notes = (
        "\n\nMỗi mức dùng lịch sử riêng của từng kênh. Byte Parquet là kích thước các tệp T14 đã chọn. "
        "Hai công cụ còn đọc cùng danh mục khoảng T11. Mỗi vòng mở một tiến trình mới. "
        "Mỗi mức và công cụ có một lượt làm nóng và ba lượt đo. Các lượt chạy tuần tự, "
        "đổi lượt đi trước và giữ cache hệ điều hành tự nhiên. RSS là tổng trên cây tiến trình, "
        "có thể đếm trùng trang dùng chung. RSS và block đĩa tạm được lấy mẫu mỗi 100 ms. "
        "Mỗi truy vấn có thời gian riêng trong dữ liệu phép đo. Nếu cách chọn cho cùng một khóa, "
        "chỉ đo khóa đó một lần trong mỗi lượt. Bảng dùng giới hạn đĩa và RAM khóa ở Q01.\n\n"
        "Mỗi liên kết nhật ký xác nhận giao thời gian với một khoảng T11 đã đệm của đúng công tơ. "
        "Chỉ số khoảng bắt đầu từ 0 trong danh mục T11. Bản đồ hiển thị lý do chính theo T14.\n\n"
        f"Nguồn chứng cứ theo thư mục run gồm bản đồ `{run.info['map_run']}`, benchmark "
        f"`{run.info['benchmark_run']}` và lượt dựng lại trong `{run.path.name}/repro_71`. "
        "Kiểm tra nội dung dùng SHA-256 Arrow IPC sau khi chuẩn hóa schema và sắp xếp khóa.\n"
    )
    path.write_text(heading + overview + "\n".join(rows) + ratio_line + notes, encoding="utf-8")


def setup_bundle(run, checks, args, catalog, panel, config, locked, baseline, stats, official):
    bundle = run.path / "bundle"
    bundle.mkdir()
    scope = locked["levels"]["71"]
    meters = scope["meters"]
    specs = locked["meters"]
    months = months_between(min(s["start_us"] for s in specs.values()), max(s["end_us"] for s in specs.values()))
    copytree(baseline / "level_71" / "tables", bundle / "tables")
    compact_panel(panel, bundle / "panel", meters)
    with duckdb.connect(config={"threads": "2"}) as conn:
        example, empty, observed = examples(Duck(conn, "snappy"), bundle, months, meters, specs)
    audit = PanelAudit(panel, specs, catalog, config["bins_by_reason"])
    independent = audit.detail(tuple(example[2]))
    checks.check("real six-hour example matches the original panel and Q03 tables", independent, observed,
                 baseline / "level_71/tables")
    body = dict(example=example, empty_example=empty, months=months, meters=meters,
                expected_detail=independent, map_run=baseline.name, panel_run=args.full.name,
                inventory_run=args.inventory.name, delivery_run=run.path.name, benchmark_run=official.name,
                config_sha256=run.info["config_sha256"], table_hashes=stats["signature"])
    copy2(ROOT / "app/quality.py", bundle / "app.py")
    copy2(ROOT / "tests/quality_demo_smoke.py", bundle / "verify_demo.py")
    copy2(run.path / "report.md", bundle / "REPORT.md")
    module_dir = bundle / "src/honda"
    module_dir.mkdir(parents=True)
    for name in ("__init__", "quality_engine", "quality_inputs", "run", "source"):
        copy2(ROOT / f"src/honda/{name}.py", module_dir / f"{name}.py")
    (bundle / "requirements.txt").write_text(
        "streamlit==1.50.0\nduckdb==1.4.3\npyarrow==26.0.1\npsutil==7.2.2\nPyYAML==6.0.3\n", encoding="utf-8")
    (bundle / "README.md").write_text("""# Demo Honda Quality Map

Từ thư mục giải nén, dùng Python 3.12 để tạo môi trường rồi mở bản đồ.

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python verify_demo.py
.venv/bin/python -m streamlit run app.py --server.address 127.0.0.1
```

Chọn tháng UTC, công tơ và một đợt giao với tháng đó. Mỗi hàng bản đồ là một ngày.
Đặt con trỏ lên ô để xem số kênh hợp lệ và mẫu số trong phạm vi.
Chi tiết giữ nguyên hai biên UTC của đợt, kể cả khi đợt qua tháng.

Số liệu và điều kiện thử nghiệm ở REPORT.md. Gói chứa phép chiếu trạng thái và các bảng
đã kiểm của Q03. bundle_manifest.json lưu mã run và SHA-256.
""", encoding="utf-8")
    body["sha256"] = {str(p.relative_to(bundle)): sha256(p) for p in sorted(bundle.rglob("*")) if p.is_file()}
    write_json(bundle / "bundle_manifest.json", body)
    return bundle


def deliver(run, checks, args):
    config, locked, catalog, panel = handoff_inputs(run, checks, args)
    baseline = verified_map(run, checks, args.map, config["levels"])
    benchmark = completed_input(run, args.benchmark, "Q04",
                                ["benchmark.json", "checks.json", "measurements.json", "query_schedule.json"])
    bench_manifest = read_json(benchmark / "run_manifest.json")
    checks.check("benchmark used locked configuration", run.info["config_sha256"],
                 bench_manifest["config_sha256"], benchmark)
    registered_map = next((r for r in bench_manifest["inputs"].values()
                           if "summary.json" in r["artifacts_sha256"]), None)
    checks.check("benchmark used exact Q03 map", run.info["inputs"][str(baseline)], registered_map, benchmark)
    all_stats = read_json(benchmark / "benchmark.json")
    engines = {name: stats for name, stats in all_stats["71"]["engines"].items()
               if isinstance(stats, dict) and stats["status"] == "OK"}
    build_name = min(engines, key=lambda name: engines[name]["build_median_s"])
    query_name = min(engines, key=lambda name: sum(engines[name]["query_median_s"]))
    display = {"duckdb": "DuckDB", "spark": "Spark local"}
    run.info.update(map_run=baseline.name, benchmark_run=benchmark.name,
                    recommended_build=build_name, recommended_query=query_name)

    audited = read_json(baseline / "level_71" / "audit.json")
    rebuild = run.path / "repro_71"
    result = measure(rebuild, build_name, panel, catalog, locked["levels"]["71"], config)
    checks.check("fresh 71-channel run status", "OK", result["status"], rebuild / "worker.log")
    checks.check("fresh 71-channel log count", locked["levels"]["71"]["log_intervals"],
                 result["selected_log_intervals"], rebuild / "measurement.json")
    recreated = signature(rebuild / "tables")
    compare_maps(run, checks, "all five normalized map signatures reproduced",
                 baseline / "level_71" / "tables", rebuild / "tables", audited["signature"], recreated)
    write_json(run.path / "reproduction.json", dict(engine=build_name, original=audited["signature"], replay=recreated))
    report(run.path / "report.md", run, audited, all_stats, locked, display[build_name], display[query_name])
    bundle = setup_bundle(run, checks, args, catalog, panel, config, locked, baseline, audited, benchmark)
    archive = run.path / "honda-quality-map.zip"
    with ZipFile(archive, "w", compression=ZIP_STORED) as package:
        for path in sorted(bundle.rglob("*")):
            if path.is_file():
                package.write(path, path.relative_to(bundle))
    target = run.path / "scratch" / "extracted"
    with ZipFile(archive) as package:
        package.extractall(target)
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("HONDA_QUALITY_BUNDLE", None)
    with (run.path / "demo_smoke.log").open("wb") as proof:
        process = subprocess.run([sys.executable, "verify_demo.py"], cwd=target, env=env,
                                 stdout=proof, stderr=subprocess.STDOUT, timeout=120)
    checks.check("archive opens from new directory and real/empty examples match UI", 0,
                 process.returncode, "demo_smoke.log")
    run.info.update(episodes=audited["episodes"], supported_episodes=audited["episodes_with_continuous_support"],
                    archive_bytes=archive.stat().st_size)
