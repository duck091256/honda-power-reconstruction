# Khôi phục công suất tác dụng Honda

Pilot Spark local với mốc UTC. Dùng Python 3.12, Java 17 hoặc 21 và môi trường ảo riêng.

```bash
export UV_CACHE_DIR="$PWD/.runtime/uv-cache"
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt
export JAVA_HOME=/path/to/java-21
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYTHONPATH="$PWD/src"
export HONDA_SOURCE='/run/media/duck/New Volume/Vice - Download/Honda_Dataset'
.venv/bin/python -m honda T02 --run runs/sample --source-root "$HONDA_SOURCE"
.venv/bin/python -m honda T03 --run runs/fixture
```

Spark local cần loopback TCP. Dùng tên run mới mỗi lần chạy. Có thể thêm `--prior <run_id>` khi thử lại.

```bash
.venv/bin/python -m honda T04 --run runs/one_meter --source-root "$HONDA_SOURCE"
.venv/bin/python -m honda T05 --run runs/pilot_panel --source-root "$HONDA_SOURCE"
```

```bash
.venv/bin/python -m honda T06 --run runs/locked --panel-run runs/pilot_panel
.venv/bin/python -m honda T07 --run runs/evaluated --eval-run runs/locked
.venv/bin/python -m honda T08 --run runs/parquet_cost --panel-run runs/pilot_panel --eval-run runs/locked
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
```

## Demo từ kết quả đã lưu

```bash
uv pip install --python .venv/bin/python -r requirements-demo.txt
.venv/bin/python -m honda T09 --run runs/demo --panel-run runs/pilot_panel --eval-run runs/locked --score-run runs/evaluated --cost-run runs/parquet_cost
.venv/bin/python -m streamlit run app/main.py --server.address=127.0.0.1 --browser.gatherUsageStats=false -- --bundle runs/demo/bundle
```

Chạy `.venv/bin/python -m streamlit run app/main.py` từ workspace để mở gói mở rộng đã bàn giao tại `runs/t15_delivery/bundle`. Có thể chọn một run cụ thể bằng `-- --bundle <đường-dẫn>`. Trong thư mục giải nén ZIP, chạy `streamlit run app/main.py` để mở thư mục `bundle` đi kèm. Máy mở demo chỉ cần phụ thuộc trong `requirements-demo.txt`. Dữ liệu Honda gốc cần cho quy trình dòng lệnh T02–T08 của dự án.

Biểu đồ phân biệt ô che và thiếu tự nhiên. Chọn công tơ, độ dài và ca theo UTC. Dùng ba ô chọn để hiện tham chiếu và từng phương pháp. Xem sai số từng ca và thống kê ghép cặp theo độ dài bên dưới.

## Đối chiếu hai lần chạy

T10 so khóa, bộ ca, giá trị trong dung sai và số hàng truy vấn. Thời gian đo phụ thuộc trạng thái máy và được ghi riêng theo lượt. Ví dụ dưới đây tiếp nối các tên run dùng trong hướng dẫn trên.

```bash
.venv/bin/python -m honda T05 --run runs/repeat_panel --prior pilot_panel --source-root "$HONDA_SOURCE"
.venv/bin/python -m honda T06 --run runs/repeat_eval --prior locked --panel-run runs/repeat_panel --evaluation runs/locked/evaluation_config.json
.venv/bin/python -m honda T07 --run runs/repeat_score --prior evaluated --eval-run runs/repeat_eval
.venv/bin/python -m honda T08 --run runs/repeat_cost --prior parquet_cost --panel-run runs/repeat_panel --eval-run runs/repeat_eval
.venv/bin/python -m honda T09 --run runs/repeat_demo --prior demo --panel-run runs/repeat_panel --eval-run runs/repeat_eval --score-run runs/repeat_score --cost-run runs/repeat_cost
.venv/bin/python -m honda T10 --run runs/handoff --demo-run runs/demo --repeat-demo-run runs/repeat_demo
```

[Lượt đo và phân tích](docs/results.md). T10 ghi `verification.json` cùng ZIP bàn giao. ZIP có mã nguồn, cấu hình, báo cáo và một gói demo đã xuất.

## Giai đoạn mở rộng · T11–T15

Dùng môi trường riêng với `uv pip install --python .venv/bin/python -r requirements-scale.txt -r requirements-demo.txt`.
Giữ các biến `JAVA_HOME`, `PYSPARK_PYTHON`, `PYTHONPATH`, `SPARK_LOCAL_IP` ở phần chuẩn bị.

```bash
.venv/bin/python -m honda.inventory runs/t11_local "$HONDA_SOURCE"
.venv/bin/python -m honda.scale T12 runs/t12_local --inventory runs/t11_local
.venv/bin/python -m honda.scale T13 runs/t13_local --inventory runs/t11_local
.venv/bin/python -m honda.scale T14 runs/t14_local --inventory runs/t11_local
.venv/bin/python -m honda.scale T15 runs/t15_local --inventory runs/t11_local --previous runs/t14_local --benchmark runs/t13_local
.venv/bin/python -m streamlit run app/main.py -- --bundle runs/t15_local/bundle
```

T12 đối chiếu các run pilot đã lưu. T14 có thể dùng `--resume-from runs/<lượt_trước> --prior <lượt_trước>` với thư mục đích mới,
cùng manifest nguồn và mã dựng panel. Điểm tiếp tục được ghi sau mỗi lô và kiểm hash khi nạp.
T15 xuất ZIP chứa kết quả, app và báo cáo. Máy mở ZIP chỉ cần cài `requirements-demo.txt`.

Trên workspace mới, tạo ba run đối chứng trước T12 nếu chúng chưa có.

```bash
.venv/bin/python -m honda T05 --run runs/t05_pilot --source-root "$HONDA_SOURCE"
.venv/bin/python -m honda T06 --run runs/t06_locked --panel-run runs/t05_pilot
.venv/bin/python -m honda T07 --run runs/t07_honda --eval-run runs/t06_locked
```
