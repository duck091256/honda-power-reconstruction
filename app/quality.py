"""Local Streamlit explorer for the checked Honda quality map."""
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

APP_ROOT = Path(__file__).resolve().parent
if (APP_ROOT / "src").is_dir():
    sys.path.insert(0, str(APP_ROOT / "src"))

from honda.quality_engine import Duck, detail, load_map, load_panel
from honda.run import read_json
from honda.source import BIN

STATES = {
    "valid": ("Hợp lệ", "#198071"),
    "missing_minute": ("Thiếu phút", "#d4a02d"),
    "clock_unverified": ("Đồng hồ chưa xác minh", "#7960b1"),
    "off_minute_grid": ("Lệch lưới phút", "#bf5d69"),
    "issue_interval": ("Khoảng nhật ký có đệm", "#5276b0"),
    "nonfinite": ("Giá trị không hữu hạn", "#b26b3f"),
    "conflicting_duplicate": ("Bản ghi trùng xung đột", "#7e4242"),
    "out_of_scope": ("Ngoài phạm vi", "#e3e7ec"),
}


def utc(tick):
    return datetime.fromtimestamp(tick / 1_000_000, UTC).strftime("%Y-%m-%d %H:%M:%S")


def heatmap(month_data):
    times = pd.to_datetime(month_data["bin_us"], unit="us", utc=True)
    month_data = month_data.assign(
        day=times.dt.day, time=times.dt.strftime("%H:%M"),
        label=times.dt.strftime("%d/%m/%Y %H:%M UTC"),
        state=month_data["reason"].map({key: caption for key, (caption, _) in STATES.items()}),
    )
    slots = [f"{hour:02}:{minute:02}" for hour in range(24) for minute in range(0, 60, 15)]
    active = [(caption, color) for caption, color in STATES.values() if caption in set(month_data["state"])]
    return alt.Chart(month_data).mark_rect(stroke=None).encode(
        x=alt.X("time:O", sort=slots, title="Giờ UTC",
                axis=alt.Axis(values=slots[::12], labelAngle=0)),
        y=alt.Y("day:O", sort="ascending", title="Ngày"),
        color=alt.Color("state:N", scale=alt.Scale(domain=[caption for caption, _ in active],
                                                   range=[color for _, color in active]),
                        legend=alt.Legend(title=None, orient="bottom", direction="horizontal",
                                          columns=3, labelLimit=190)),
        tooltip=[alt.Tooltip("label:N", title="Đầu ô"), alt.Tooltip("state:N", title="Trạng thái"),
                 alt.Tooltip("valid:Q", title="Kênh hợp lệ"),
                 alt.Tooltip("in_scope:Q", title="Kênh trong phạm vi")],
    ).properties(height=360).configure_view(strokeWidth=0)


def episode_detail(db, bundle, manifest, key):
    load_panel(db, bundle / "panel", manifest["meters"])
    row = detail(db, key)
    bins, supported, continuous = row["support"]
    st.subheader("Chi tiết đợt")
    st.caption(f"{key[0]} · [{utc(key[1])}, {utc(key[2])}) UTC · {manifest['map_run']}")
    first, second, third = st.columns(3)
    first.metric("Thời lượng", f"{bins * 15:,} phút")
    second.metric("Ô có ≥1 kênh khác hợp lệ", f"{supported:,}/{bins:,}",
                  help=f"{100 * supported / bins:.1f}% số ô trong đợt")
    third.metric("Kênh khác hợp lệ suốt đợt", continuous)

    left, right = st.columns([1, 1.2])
    with left:
        st.markdown("**Lý do loại ô**")
        st.dataframe(pd.DataFrame([(STATES[r][0], n) for r, n in row["reasons"]],
                                  columns=["Lý do chính", "Số ô"]),
                     hide_index=True, width="stretch")
    with right:
        st.markdown("**Kênh còn quan sát được**")
        if row["channels"]:
            channels = sorted(row["channels"], key=lambda item: (-item[1], item[0]))
            st.dataframe(pd.DataFrame([(m, n, bins, n == bins) for m, n in channels],
                                     columns=["Công tơ", "Ô hợp lệ", "Ô trong đợt", "Hợp lệ suốt đợt"]),
                         hide_index=True, width="stretch")
        else:
            st.info("Không có kênh khác hợp lệ ở bất kỳ ô nào của đợt.")

    st.markdown("**Khoảng nhật ký T11 giao với đợt**")
    if row["logs"]:
        st.dataframe(pd.DataFrame([(i, utc(a), utc(b)) for i, a, b in row["logs"]],
                                 columns=["Vị trí T11 (từ 0)", "Đầu khoảng UTC", "Cuối khoảng UTC"]),
                     hide_index=True, width="stretch")
        st.caption("Các liên kết thể hiện giao thời gian với khoảng của chính công tơ đã đệm trong T11.")
    else:
        st.info("Không có khoảng nhật ký T11 của công tơ giao với đợt.")


def main():
    st.set_page_config(page_title="Chất lượng dữ liệu Honda", layout="wide")
    st.title("Bản đồ chất lượng công suất Honda")
    st.caption("Dữ liệu công suất tác dụng P · Ô 15 phút · Toàn bộ thời gian hiển thị theo UTC")
    default = APP_ROOT if (APP_ROOT / "bundle_manifest.json").is_file() else (
        APP_ROOT.parent / "runs/q05_delivery_r2/bundle")
    bundle = Path(os.environ.get("HONDA_QUALITY_BUNDLE", default)).resolve()
    manifest_path = bundle / "bundle_manifest.json"
    if not manifest_path.is_file():
        st.error("Chưa có gói bản đồ. Chạy Q05 hoặc đặt HONDA_QUALITY_BUNDLE đến thư mục gói đã giải nén.")
        return
    manifest = read_json(manifest_path)
    df_months = manifest["months"]
    start_month, start_meter, start_episode = manifest["example"]
    month_col, meter_col = st.columns([1, 1])
    month = month_col.selectbox("Tháng UTC", list(df_months), index=list(df_months).index(start_month),
                                key="month")
    meters = manifest["meters"]
    meter = meter_col.selectbox("Công tơ", meters, index=meters.index(start_meter), key="meter")
    start, end = df_months[month]

    with duckdb.connect(config={"threads": "2"}) as conn:
        db = Duck(conn, "snappy")
        load_panel(db, bundle / "panel", [meter])
        load_map(db, bundle / "tables")
        frame = conn.execute("""
            SELECT slots.range AS bin_us, coalesce(p.reason, 'out_of_scope') AS reason,
                   coalesce(t.in_scope, 0) AS in_scope, coalesce(t.valid, 0) AS valid
            FROM range(?, ?, ?) slots LEFT JOIN panel p ON p.bin_us=slots.range
            LEFT JOIN timeline t ON t.bin_us=slots.range ORDER BY slots.range
        """, [start, end, BIN]).df()
        scoped = frame["reason"] != "out_of_scope"
        valid = frame["reason"] == "valid"
        valid_count = int(valid.sum())
        scope_count = int(scoped.sum())
        first, second, third = st.columns(3)
        first.metric("Ô hợp lệ", f"{valid_count:,}")
        second.metric("Ô không hợp lệ", f"{scope_count - valid_count:,}")
        third.metric("Ô ngoài phạm vi", f"{len(frame) - scope_count:,}")
        st.altair_chart(heatmap(frame), use_container_width=True)
        matches = conn.execute("""
            SELECT meter_id, start_us, end_us, bins FROM episodes
            WHERE meter_id=? AND start_us<? AND end_us>? ORDER BY start_us, end_us
        """, [meter, end, start]).fetchall()
        if not matches:
            st.info("Công tơ không có đợt chất lượng giao với tháng này.")
        else:
            options = {f"{utc(a)} UTC · {n * 15:,} phút": (m, a, b) for m, a, b, n in matches}
            label = next((name for name, key in options.items() if list(key) == start_episode), None)
            labels = list(options)
            choice = st.selectbox("Đợt giao với tháng đã chọn", labels,
                                  index=labels.index(label) if label else 0, key="episode")
            episode_detail(db, bundle, manifest, options[choice])

    st.caption(f"Bản đồ {manifest['map_run']} · Panel {manifest['panel_run']} · "
               f"Danh mục nhật ký {manifest['inventory_run']} · Gói {manifest['delivery_run']}")


if __name__ == "__main__":
    main()
