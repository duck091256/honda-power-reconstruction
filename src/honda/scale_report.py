"""Export the expanded results as a standalone Streamlit bundle."""
import sys
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile

from .run import completed_input, read_json, sha256, write_json

OUTPUTS = [
    "evaluation_config.json", "masked.json", "truth.csv", "predictions.csv",
    "case_scores.csv", "summary.json", "selected_counts.csv", "candidate_counts.csv",
    "metrics_by_meter.json", "coverage.csv",
]


def create_report(path, inventory, full, benchmark, summary):
    view = read_json(inventory / "run_manifest.json")
    run = read_json(full / "run_manifest.json")
    lines = [
        "# Khôi phục công suất điện Honda trên toàn lịch sử",
        "",
        f"T11 xác minh {view['eligible']} nguồn P trong {view['inventory_rows']} thư mục URN.",
        f"T14 đọc {run['rows']:,} dòng từ {run['channels']} nguồn, tổng {view['selected_bytes']:,} byte gzip.",
        f"Panel theo phạm vi thời gian riêng của từng công tơ có {run['bins']:,} ô.",
        f"Có {run['valid']:,} ô đạt quy tắc tham chiếu trên {run['channels_with_reference']} công tơ.",
        f"Quý IV/2023 chọn {run['selected_cases']:,} ca trên {run['scenario_meters']} công tơ.",
        "Bài chính §3.1 báo 57 kênh trong panel kiểm định theo giao thức nghiên cứu, gồm điều kiện tối thiểu 30 ngày quan sát trong fit.",
        "Phạm vi T14 dùng quy tắc 15 vị trí phút UTC, lọc nhật ký, loại rủi ro đồng hồ và yêu cầu hai biên trong ca kiểm.",
        "Lịch sử xây Seasonal từ tháng 4/2020 đến hết tháng 9/2023.",
        "",
        "## Sai số theo độ dài",
        "",
        "| Khoảng che | Phương pháp | Ca chọn | Ca ghép cặp | MAE (W) | Sai số điện năng (kWh) |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in summary:
        lines.append(f"| {row['n_bins'] * 15} phút | {row['method']} | {row['selected_cases']} |"
                     f" {row['paired_cases']} | {row['mean_paired_mae_w']:.2f} |"
                     f" {row['mean_paired_abs_energy_error_kwh']:.3f} |")
    per_meter = read_json(full / "metrics_by_meter.json")
    lines.extend(["", "Ảnh hưởng của H1.Z20 được kiểm bằng trung bình trên cùng ca ghép cặp sau khi bỏ công tơ này.", ""])
    for row in summary:
        subset = [r for r in per_meter if r["meter_id"] != "H1.Z20" and r["n_bins"] == row["n_bins"]
                  and r["method"] == row["method"] and r["paired_cases"]]
        n = sum(r["paired_cases"] for r in subset)
        if n:
            mae = sum(r["paired_cases"] * r["mean_paired_mae_w"] for r in subset) / n
            lines.append(f"- {row['n_bins']*15} phút · {row['method']} · {mae:.2f} W trên {n} ca ghép cặp.")
    lines.extend([
        "", "## Chi phí tại các quy mô đã đo", "",
        "| Công tơ × tháng | Công cụ | Dòng trong kỳ | Dựng panel (s) | Truy vấn (s) | RSS đỉnh (MiB) |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ])
    for item in benchmark:
        lines.append(f"| {item['labels']} | {item['engine']} | {item['selected_rows']:,} |"
                     f" {item['wall_s']:.3f} | {item['query_s']:.3f} |"
                     f" {item['peak_tree_rss_bytes'] / 2**20:.0f} |")
    lines.extend([
        "", "Mỗi ô thời gian là trung vị ba lượt. Đọc gzip bao gồm quét toàn tệp đã chọn để lọc kỳ.",
        "Cả hai công cụ lấy cùng tập tệp tại mỗi mức. Spark chạy local[2] với worker Arrow/NumPy, DuckDB chạy hai luồng.",
        "Thời gian gồm dựng và ghi panel. Bảng chi tiết ghi tổng thời gian các tác vụ nhập và ETL.",
        "RSS của Spark cộng cả JVM và các worker Python nên có thể tính lặp trang bộ nhớ chia sẻ.",
        "Cache hệ điều hành được giữ tự nhiên. Lượng byte thiết bị thực đọc chưa đo.",
        "So sánh chỉ phản ánh cấu hình máy cục bộ và hai cách triển khai đã đo.",
        "", "Mở app bằng `streamlit run app/main.py -- --bundle bundle`.", "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def report(run, inventory, full_path, benchmark_path):
    from streamlit.testing.v1 import AppTest

    full = completed_input(run, full_path, "T14", OUTPUTS)
    benchmark = completed_input(run, benchmark_path, "T13", ["scaling.json", "measurements.json"])
    scaling = read_json(benchmark / "scaling.json")
    months = {1:3,2:6,3:12,4:12,5:12}
    for row in scaling:
        row["labels"] = f"{row['channels']} × {months[row['workload']]}"
    bundle = run.path / "bundle"
    bundle.mkdir()
    for name in OUTPUTS:
        copyfile(full / name, bundle / name)
    write_json(bundle / "cost.json", {"scaling": scaling})
    write_json(bundle / "bundle_manifest.json", {
        "kind": "honda", "run_id": full.name, "hashes": {p.name: sha256(p) for p in bundle.iterdir()},
    })
    create_report(run.path / "report.md", inventory, full, scaling, read_json(full / "summary.json"))
    archive = run.path / "honda-scale-demo.zip"
    with ZipFile(archive, "w", ZIP_DEFLATED) as zip_file:
        for name in ("app/main.py", "requirements-demo.txt"):
            zip_file.write(name)
        zip_file.write(run.path / "report.md", "report.md")
        zip_file.write(inventory / "inventory.csv", "inventory.csv")
        zip_file.write(benchmark / "measurements.json", "measurements.json")
        for p in bundle.iterdir():
            zip_file.write(p, Path("bundle") / p.name)
    with TemporaryDirectory(dir=run.path) as temp:
        with ZipFile(archive) as zip_file:
            assert zip_file.testzip() is None
            zip_file.extractall(temp)
        app = Path(temp) / "app/main.py"
        argv = sys.argv
        try:
            sys.argv = [str(app), "--bundle", str(Path(temp) / "bundle")]
            ui = AppTest.from_file(app, default_timeout=30).run()
            assert not ui.exception and ui.selectbox(key="month")
            if ui.get("checkbox"):
                ui.checkbox(key="truth").check().run()
                assert not ui.exception and ui.checkbox(key="truth").value
            ui.selectbox(key="duration").select(24).run()
            assert not ui.exception
        finally:
            sys.argv = argv
    run.info.update(zip_demo_verified=True, zip_bytes=archive.stat().st_size)
