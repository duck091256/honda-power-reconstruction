"""Run the delivered Streamlit app from the unpacked bundle."""
import json
from pathlib import Path

from streamlit.testing.v1 import AppTest

from app import STATES, utc
from honda.run import sha256

ROOT = Path(__file__).resolve().parent


def verify():
    bundle = json.loads((ROOT / "bundle_manifest.json").read_text())
    for path, expected in bundle["sha256"].items():
        assert sha256(ROOT / path) == expected, path

    app = AppTest.from_file(str(ROOT / "app.py"), default_timeout=60).run()
    assert not app.exception, app.exception
    month, meter, key = bundle["example"]
    assert app.selectbox(key="month").value == month
    assert app.selectbox(key="meter").value == meter
    assert len(app.get("arrow_vega_lite_chart")) == 1
    expected = bundle["expected_detail"]
    bins, supported, continuous = expected["support"]
    assert any(f"[{utc(key[1])}, {utc(key[2])}) UTC" in view.value for view in app.caption)

    metrics = {metric.label: metric.value for metric in app.metric}
    assert metrics["Thời lượng"] == f"{bins * 15:,} phút"
    assert metrics["Ô có ≥1 kênh khác hợp lệ"] == f"{supported:,}/{bins:,}"
    assert metrics["Kênh khác hợp lệ suốt đợt"] == str(continuous)
    assert [(STATES[r][0], n) for r, n in expected["reasons"]] == list(app.dataframe[0].value.itertuples(index=False, name=None))
    channels = sorted(expected["channels"], key=lambda row: (-row[1], row[0]))
    assert [(m, n, bins, n == bins) for m, n in channels] == list(app.dataframe[1].value.itertuples(index=False, name=None))
    if expected["logs"]:
        assert [(i, utc(a), utc(b)) for i, a, b in expected["logs"]] == list(app.dataframe[2].value.itertuples(index=False, name=None))
    else:
        assert any("Không có khoảng nhật ký" in info.value for info in app.info)

    month, meter = bundle["empty_example"]
    app.selectbox(key="month").select(month).run()
    assert not app.exception, app.exception
    app.selectbox(key="meter").select(meter).run()
    assert not app.exception, app.exception
    assert any("không có đợt" in message.value for message in app.info)
    assert len(app.get("arrow_vega_lite_chart")) == 1
    assert len(app.selectbox) == 2
    print(json.dumps(dict(
        real_episode=key, valid_other_channels=len(channels), continuous_channels=continuous,
        supported_bins=supported, episode_bins=bins, journal_links=len(expected["logs"]),
        empty_example=[month, meter], chart_rendered=True, files_verified=len(bundle["sha256"]),
    ), ensure_ascii=False))


if __name__ == "__main__":
    verify()
