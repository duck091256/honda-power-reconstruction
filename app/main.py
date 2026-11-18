import argparse
import json
import sys
from hashlib import file_digest
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

DURATIONS = {1: "15 phút", 4: "1 giờ", 24: "6 giờ"}
BIN = 900_000_000
SERIES = ["Quan sát", "Tham chiếu", "Linear", "Seasonal"]
COLORS = ["#17536A", "#30333C", "#7546AB", "#B54A17"]


@st.cache_data(show_spinner=False)
def load_bundle(path, manifest_bytes):
    manifest = json.loads(manifest_bytes)
    for name, expected in manifest["hashes"].items():
        with (path / name).open("rb") as stream:
            if file_digest(stream, "sha256").hexdigest() != expected:
                raise ValueError(f"Checksum không khớp cho {name}")
    return {
        "manifest": manifest,
        "cases": json.loads((path / "masked.json").read_text()),
        "summary": pd.read_json(path / "summary.json"),
        "cost": json.loads((path / "cost.json").read_text()),
        "truth": pd.read_csv(path / "truth.csv"),
        "predictions": pd.read_csv(path / "predictions.csv"),
        "scores": pd.read_csv(path / "case_scores.csv"),
        "groups": pd.read_csv(path / "selected_counts.csv"),
        "by_meter": pd.read_json(path / "metrics_by_meter.json")
        if (path / "metrics_by_meter.json").exists() else None,
    }


def time(value):
    return pd.to_datetime(value, unit="us", utc=True)


def chart(case, truth, predictions, show_truth, methods):
    start = case["start_us"]
    end = start + case["n_bins"] * BIN
    shaded = [{"start": time(start), "end": time(end), "state": "Chủ động che"}]
    points = []
    segment = 0
    for slot, value, state in case["context"]:
        if state == "observed":
            points.append((time(slot), value, "Quan sát", str(segment)))
        else:
            segment += 1
            if state == "missing":
                shaded.append({"start": time(slot), "end": time(slot + BIN),
                               "state": "Thiếu tự nhiên / không hợp lệ"})
    if show_truth:
        points.extend((time(row.bin_us), row.ref_w, "Tham chiếu", "truth")
                      for row in truth.itertuples())
    for method in methods:
        segment = 0
        estimates = predictions[predictions.method == method].sort_values("bin_us")
        for row in estimates.itertuples():
            if pd.isna(row.pred_w):
                segment += 1
            else:
                points.append((time(row.bin_us), row.pred_w, method, f"{method}-{segment}"))
    frame = pd.DataFrame(points, columns=["utc", "watts", "series", "segment"])
    domain = [time(case["context"][0][0]).isoformat(),
              time(case["context"][-1][0] + BIN).isoformat()]
    frame["utc_label"] = frame["utc"].dt.strftime("%Y-%m-%d %H:%M")
    def x(field):
        return alt.X(field, scale=alt.Scale(type="utc", domain=domain), title="Thời gian UTC",
                     axis=alt.Axis(format="%d/%m %H:%M", labelOverlap=True))
    areas = alt.Chart(pd.DataFrame(shaded)).mark_rect(opacity=0.24).encode(
        x=x("start:T"), x2="end:T",
        color=alt.Color("state:N", title="Trạng thái",
                        scale=alt.Scale(domain=["Chủ động che", "Thiếu tự nhiên / không hợp lệ"],
                                        range=["#C4B0E8", "#94A3B8"])),
        tooltip=[alt.Tooltip("state:N", title="Trạng thái")],
    )
    curves = alt.Chart(frame).mark_line(point=alt.OverlayMarkDef(size=34), strokeWidth=2.2).encode(
        x=x("utc:T"),
        y=alt.Y("watts:Q", title="Công suất tác dụng (W)", scale=alt.Scale(zero=False)),
        color=alt.Color("series:N", title="Đường công suất", sort=SERIES,
                        scale=alt.Scale(domain=SERIES, range=COLORS)),
        detail="segment:N",
        strokeDash=alt.StrokeDash("series:N", legend=None,
                                  scale=alt.Scale(domain=SERIES, range=[[1, 0], [5, 3], [1, 0], [1, 0]])),
        tooltip=[
            alt.Tooltip("utc_label:N", title="UTC"),
            alt.Tooltip("series:N", title="Đường"),
            alt.Tooltip("watts:Q", title="Công suất (W)", format=",.2f"),
        ],
    )
    return (areas + curves).resolve_scale(color="independent", x="shared").properties(height=390)


def render(directory):
    path = directory.resolve()
    try:
        data = load_bundle(path, (path / "bundle_manifest.json").read_bytes())
    except (ValueError, OSError) as exc:
        st.error(f"Không đọc được gói demo tại {path}. {exc}")
        return
    st.title("Khôi phục công suất điện Honda")
    if data["manifest"]["kind"] != "honda":
        st.warning("Dữ liệu mô phỏng · Kết quả Honda [chưa đo]")
    st.caption(f"Run {data['manifest']['run_id']} · {path}")

    groups = data["groups"]
    meters = sorted(groups.meter_id.unique())
    expanded = "scaling" in data["cost"]
    columns = st.columns([1, 1, 1, 2] if expanded else [1, 1, 2])
    meter_col, duration_col, case_col = columns[0], columns[1], columns[-1]
    meter = meter_col.selectbox("Công tơ", meters, key="meter")
    n_bins = duration_col.selectbox("Độ dài khoảng che", list(DURATIONS),
                                    format_func=DURATIONS.get, key="duration")
    options = sorted((c for c in data["cases"] if c["meter_id"] == meter and c["n_bins"] == n_bins),
                     key=lambda case: case["start_us"])
    if expanded:
        months = sorted({time(c["start_us"]).strftime("%Y-%m") for c in data["cases"]})
        month = columns[2].selectbox("Tháng UTC", ["Cả kỳ", *months], key="month")
        if month != "Cả kỳ":
            options = [case for case in options if time(case["start_us"]).strftime("%Y-%m") == month]
    if not options:
        st.info(f"{meter} · {DURATIONS[n_bins]} · 0 ca đủ điều kiện.")
    else:
        by_label = {time(c["start_us"]).strftime("%d/%m/%Y %H:%M"): c for c in options}
        selected = case_col.selectbox("Ca theo thời điểm UTC", list(by_label),
                                      key=f"case_{meter}_{n_bins}")
        case = by_label[selected]
        toggles = st.columns(3)
        show_truth = toggles[0].checkbox("Hiện tham chiếu", value=False, key="truth")
        methods = [method for col, method in zip(toggles[1:], ("Linear", "Seasonal"))
                   if col.checkbox(method, value=True, key=method)]
        cid = case["case_id"]
        st.altair_chart(chart(case, data["truth"].query("case_id == @cid"),
                              data["predictions"].query("case_id == @cid"), show_truth, methods),
                        use_container_width=True)
        st.caption(f"Ca {cid[:12]} · {DURATIONS[n_bins]} · "
                   f"{len(options)} ca của công tơ trong nhóm")
        st.dataframe(
            data["scores"].query("case_id == @cid")[["method", "mae_w", "abs_energy_error_kwh"]],
            hide_index=True,
            column_config={
                "method": "Phương pháp", "mae_w": st.column_config.NumberColumn("MAE (W)", format="%.2f"),
                "abs_energy_error_kwh": st.column_config.NumberColumn(
                    "Sai số điện năng (kWh)", format="%.3f"),
            },
        )

    st.subheader("Kết quả theo độ dài")
    summary = data["summary"].assign(
        duration=data["summary"]["n_bins"].map(DURATIONS)
    )
    st.dataframe(summary[["duration", "method", "selected_cases", "available_cases", "paired_cases",
                          "paired_meters", "mean_paired_mae_w", "mean_paired_abs_energy_error_kwh"]],
                 hide_index=True,
                 column_config={
                     "duration": "Khoảng che", "method": "Phương pháp", "selected_cases": "Ca đã chọn",
                     "available_cases": "Có dự đoán", "paired_cases": "Ca ghép cặp", "paired_meters": "Công tơ",
                     "mean_paired_mae_w": st.column_config.NumberColumn("MAE (W)", format="%.2f"),
                     "mean_paired_abs_energy_error_kwh": st.column_config.NumberColumn(
                         "Sai số điện năng (kWh)", format="%.3f"),
                 })
    if expanded:
        metrics = data["by_meter"].query("meter_id == @meter").copy()
        metrics["duration"] = metrics["n_bins"].map(DURATIONS)
        with st.expander(f"Kết quả riêng cho {meter}"):
            st.dataframe(metrics[["duration", "method", "selected_cases", "paired_cases",
                                  "mean_paired_mae_w", "mean_paired_abs_energy_error_kwh"]],
                         hide_index=True,
                         column_config={"duration":"Khoảng che","method":"Phương pháp",
                                        "selected_cases":"Ca đã chọn","paired_cases":"Ca ghép cặp",
                                        "mean_paired_mae_w":"MAE (W)",
                                        "mean_paired_abs_energy_error_kwh":"Sai số điện năng (kWh)"})
        with st.expander("Chi phí Spark và DuckDB"):
            scaling = pd.DataFrame(data["cost"]["scaling"])
            scaling["quy_mo"] = scaling["labels"]
            st.altair_chart(alt.Chart(scaling).mark_bar().encode(
                x=alt.X("quy_mo:N", title="Phạm vi", sort=scaling["quy_mo"].unique().tolist()),
                xOffset="engine:N", color=alt.Color("engine:N", title="Công cụ"),
                y=alt.Y("wall_s:Q", title="Trung vị thời gian dựng panel (s)"),
                tooltip=["quy_mo", "engine", "selected_rows", "wall_s", "query_s"],
            ), use_container_width=True)
        return

    with st.expander("Chi phí truy vấn Spark"):
        layouts = data["cost"]["layouts"]
        st.dataframe(pd.DataFrame([
            {"Bố cục": label, "Trung vị (s)": layouts[name]["median_query_s"],
             "Tệp": layouts[name]["files"], "Dung lượng (byte)": layouts[name]["bytes"]}
            for name, label in (("plain", "Parquet thường"), ("partitioned", "Phân vùng năm/tháng"))
        ]), hide_index=True)


def demo_directory(requested):
    root = Path(__file__).resolve().parents[1]
    packaged = root / "bundle"
    if requested is not None:
        path = requested.resolve()
        if path != packaged or path.exists():
            return path
    for candidate in (packaged, root / "runs/t15_delivery/bundle",
                      root / "runs/t09_demo/bundle"):
        if (candidate / "bundle_manifest.json").is_file():
            return candidate
    return packaged


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path)
    args = parser.parse_args(sys.argv[1:])
    st.set_page_config(page_title="Khôi phục công suất Honda", layout="wide")
    render(demo_directory(args.bundle))


if __name__ == "__main__":
    main()
