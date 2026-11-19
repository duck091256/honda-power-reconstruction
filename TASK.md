# Sổ công việc

**Hiện tại** — T01–T15 và Q01–Q05 hoàn tất. Bản đồ chất lượng có báo cáo và gói demo tại runs/q05_delivery_r2.

| ID | Phụ thuộc | Trạng thái | Việc cần làm | Bằng chứng để hoàn tất |
| --- | --- | --- | --- | --- |
| T01 | — | DONE | Kiểm kê nguồn Honda có thể đọc, metadata, nhật ký, giấy phép; đối chiếu yêu cầu môn và ngân sách laptop. | docs/feasibility.md ghi nguồn/phiên bản, tệp mẫu, bằng chứng đại lượng điện, đơn vị, timezone, quyền truy cập, RAM/đĩa và các mục chưa xác minh. Ghi rõ có thể thử Honda hay chỉ làm fixture. |
| T02 | T01 | DONE | Lấy một lát dữ liệu phút theo phạm vi đã xác minh. Truy nguyên kênh/tệp, đối chiếu nhật ký và chốt cách tính ô tham chiếu. | source_manifest và bảng mẫu phút chỉ ra lặp, xung đột, vùng bị loại, lý do, ít nhất một đoạn liên tục gồm 6 giờ và hai ô biên nếu có. Ghi độ dài khả dụng nếu ngắn hơn. |
| T03 | T02 | DONE | Viết bộ kiểm thử tổng hợp và Spark ETL tối thiểu theo hợp đồng panel. | Fixture bắt được phút khuyết, bản sao trùng, xung đột, sai lệch múi giờ, lỗi sát biên ô, vùng nhật ký, giá trị âm và 0. Panel khớp bảng tính tay. Khi thiếu dữ liệu thật có thể chuẩn bị phần fixture độc lập. |
| T04 | T03 | DONE | Chạy thử một công tơ trên dữ liệu phát triển. Che một đoạn, nội suy và nối lại với truth bằng khóa. | Một run mẫu có trace phút → ô → ca → dự đoán → sai số. Test riêng 4 ô với sai lệch đều +1000 W cho sai số điện năng 1 kWh. |
| T05 | T04 | DONE | Mở rộng ETL trên tập kênh đủ điều kiện, ghi Parquet và thống kê theo công tơ/tháng. | Panel duy nhất theo khóa, số dòng hợp lệ/bị loại và nguyên nhân đối soát được với nguồn. Ghi thời gian, số kênh, kỳ đo và dung lượng thực tế. |
| T06 | T05 | DONE | Chốt khoảng lịch sử/test, độ dài, quy tắc chọn và hạn mức ca, seed, dự phòng, dung sai và ngân sách. Sinh cùng bộ ca cho cả hai phương pháp. | configs/evaluation_v1.json, hash cấu hình, cases, truth và dữ liệu đã che. Test chống rò rỉ mốc thời gian và truth. Ghi số ca đủ/thiếu theo độ dài. |
| T07 | T06 | DONE | Chạy Linear/Seasonal, so từng cặp trên cùng case_id và ghi tỷ lệ dự đoán được. | predictions, case_scores, summary kèm đơn vị, số ca và số công tơ. Thay truth sau bước che giữ nguyên dự đoán. Báo cả ca bị bỏ và lượt dự phòng. |
| T08 | T06 | DONE | Benchmark truy vấn lọc trên Parquet thường và phân vùng cùng một panel. | cost ghi chi phí tạo, 1+3 lượt truy vấn mỗi bản, trung vị, số tệp/byte lưu, cấu hình máy, trạng thái cache, digest kết quả. Byte đọc thực tế chưa lấy được thì ghi [chưa đo]. |
| T09 | T07, T08 | DONE | Làm demo Streamlit dùng bảng nhỏ đã xuất. Chọn công tơ, độ dài và ca. Cho bật/tắt đường thật và hai đường khôi phục. | Chạy cục bộ từ run cố định. Biểu đồ có thời gian UTC, W, vùng che, tên phương pháp và sai số kWh. Hiển thị được cả nhóm không có ca và đường dẫn run. |
| T10 | T09 | DONE | Chạy lại từ cấu hình đã lưu trong thư mục mới, so kết quả logic, đóng gói hướng dẫn và báo cáo. | README có lệnh cài/chạy thật đã thử, phiên bản, ví dụ; báo cáo ghi quy mô, sai số, chi phí và giới hạn. Demo hoạt động từ gói kết quả, kiểm riêng quy trình có nguồn để tái tạo. |

Khi hoàn tất một task, thêm một dòng dưới đây với ngày, ID, lệnh đã chạy, kết quả, đường dẫn run/bằng chứng và việc còn mở. Nếu bị chặn, ghi điều kiện mở lại. Trạng thái tích hợp của task dùng bốn giá trị TODO, IN_PROGRESS, BLOCKED, DONE.

## Nhật ký triển khai

- 2026-11-18 · T01 · Kiểm kê ổ ngoài bằng `find`, `gzip -cd`, `sha256sum` và đo tài nguyên bằng `free`, `df`, `nproc`. Đủ căn cứ lấy mẫu Honda theo [feasibility](docs/feasibility.md). Phiên bản chính xác của toàn bộ bản giải nén và rubric chính thức còn mở.

- 2026-11-18 · T02 · `PYTHONPATH=src python3 -m honda T02 --run runs/t02_honda_sample --source-root '<Honda_Dataset>'`. [Run mẫu](runs/t02_honda_sample/audit.json): H1.Z20 có 2.867/2.880 phút quan sát, 13 phút khuyết, 3 ô nhật ký, chuỗi hợp lệ dài nhất 137 ô. Khóa tệp nguồn trong [sources_v1](configs/sources_v1.json).

- 2026-11-18 · T03 · `python -m honda T03 --run runs/t03_fixture` (Spark local[2], Temurin 21, PySpark 4.0.1). [Run](runs/t03_fixture/run_manifest.json) qua 8 ô tính tay, gồm trùng/lệch lưới, giờ UTC, nhật ký, giá trị âm và 0.

- 2026-11-18 · T04 · `python -m honda T04 --run runs/t04_one_meter --source-root '<Honda_Dataset>'`. [Run](runs/t04_one_meter/run_manifest.json) truy được một ca H1.Z20 1 giờ qua panel, dữ liệu đã che, truth, Linear và scores. Kiểm riêng +1.000 W trong 1 giờ cho 1 kWh. Lát phát triển 03–05/03 UTC sẽ nằm ngoài kỳ chấm chính.

- 2026-11-18 · T05 · `python -m honda T05 --run runs/t05_pilot --source-root '<Honda_Dataset>'`. [Panel pilot](runs/t05_pilot/run_manifest.json) có 34.560 ô duy nhất, 33.266 ô hợp lệ trên 4 công tơ quý I/2023 UTC. Đối soát hàng nguồn theo [độ phủ](runs/t05_pilot/coverage.csv).

- 2026-11-18 · T06 · `python -m honda T06 --run runs/t06_locked --panel-run runs/t05_pilot`, 3 test hợp đồng qua. [Run khóa](runs/t06_locked/run_manifest.json) ghi hash cấu hình `23e20146c0854549db87128906e7d7500651b7c96c685b58474e4ba41dce4878`, bộ ca, lịch sử, truth, mặt nạ và lý do loại; [mẫu số](runs/t06_locked/selected_counts.csv).

- 2026-11-18 · T07 · `python -m honda T07 --run runs/t07_honda --eval-run runs/t06_locked`. [Run sai số](runs/t07_honda/summary.csv) có 120/120 ca ghép cặp trên 4 công tơ mỗi độ dài, 6.960 dự đoán theo ô và phương pháp, Seasonal sử dụng đủ lịch sử cùng thứ/ô. Thử thay truth giữ nguyên dự đoán. Bốn test hợp đồng qua.

- 2026-11-18 · T08 · `python -m honda T08 --run runs/t08_cost --panel-run runs/t05_pilot --eval-run runs/t06_locked`. [Benchmark](runs/t08_cost/cost.json) khớp schema, digest toàn panel và digest 1.344 hàng truy vấn ở cả 8 lượt; byte đọc [chưa đo].

- 2026-11-18 · T09 · `python -m honda T09 --run runs/t09_demo --panel-run runs/t05_pilot --eval-run runs/t06_locked --score-run runs/t07_honda --cost-run runs/t08_cost`. [Gói cố định](runs/t09_demo/bundle/bundle_manifest.json) qua AppTest đổi công tơ/độ dài/ca, toggle và nhóm trống mô phỏng. Chrome hiển thị biểu đồ và bật được đường tham chiếu, [ảnh demo](output/playwright/honda-demo.png).

- 2026-11-18 · T10 · Lặp T05–T09 ở `runs/t10_repeat_*` từ nguồn gốc và cấu hình đã lưu. Cài sạch bằng `uv pip install -r requirements.txt -r requirements-demo.txt` với wheel Spark cục bộ. Bốn test Python và fixture Spark qua. `python -m honda T10 --run runs/t10_handoff --demo-run runs/t09_demo --repeat-demo-run runs/t10_repeat_demo` khớp 34.560 ô, ca, mặt nạ, dự đoán, điểm, digest panel và digest truy vấn. [Bằng chứng](runs/t10_handoff/verification.json) · [ZIP đã giải nén và thử demo](runs/t10_handoff/honda-power-reconstruction.zip).

## Giai đoạn mở rộng
| ID | Phụ thuộc | Trạng thái | Đầu ra |
| --- | --- | --- | --- |
| T11 | T10 | DONE | Danh mục URN, loại thiết bị, nguồn P, thời kỳ, nhật ký và dung lượng. |
| T12 | T11 | DONE | Spark đọc gzip, sinh ca theo công tơ và đối chiếu pilot. |
| T13 | T12 | DONE | Benchmark Spark và DuckDB theo số tháng và số kênh. |
| T14 | T13 | DONE | Panel lịch sử, độ phủ và đánh giá theo công tơ, có checkpoint. |
| T15 | T14 | DONE | Demo, báo cáo quy mô và hướng dẫn tái tạo. |

- 2026-11-18 · T11 · Kiểm kê 81 URN. Có 71 nguồn P điện đã đăng ký SHA-256, 208.746.721 hàng và 1.484.219.373 byte gzip. Bài chính §3.1 báo 57 kênh sau kiểm định theo giao thức của bài. [Kiểm kê](runs/t11_inventory/inventory.csv) và [manifest](runs/t11_inventory/run_manifest.json).

- 2026-11-18 · T12 · Spark phân công theo tệp gzip và tạo ca theo công tơ. [Run đối chiếu](runs/t12_verified_r2/run_manifest.json) khớp 34.560 ô, 360 ca, dự đoán và điểm pilot. Hai lần thử trước được giữ lại để truy lỗi tương thích timestamp và API Arrow.

- 2026-11-18 · T13 · [Benchmark](runs/t13_scaling_r3/scaling.json) gồm 5 mức và 30 lượt đo. Spark và DuckDB khớp panel và truy vấn ở từng cặp lượt. Hai thử nghiệm trước lưu trạng thái FAILED do phiên bản API DuckDB và biên độ phủ cuối năm, đã điều chỉnh trước lượt chính.
- 2026-11-18 · T14 · [Run đầy đủ](runs/t14_full/run_manifest.json) đối soát 208.746.721 dòng từ 71 nguồn. Panel có 11.906.542 ô, gồm 4.717.365 ô tham chiếu trên 51 kênh. Quý IV/2023 có 3.487 ca ghép cặp từ 39 kênh. [Kiểm toán độc lập](runs/t14_audit/run_manifest.json) tính lại toàn bộ dự đoán và điểm. [Lượt chạy từ checkpoint](runs/t14_resume_check/run_manifest.json) khớp coverage, predictions và summary từng byte.
- 2026-11-18 · T15 · [Báo cáo](runs/t15_delivery/report.md) và [ZIP demo](runs/t15_delivery/honda-scale-demo.zip). Giải nén và kiểm qua Streamlit AppTest việc chọn H1.Z20, khoảng che 6 giờ, tháng 11/2023 và bật tham chiếu.

## Bản đồ chất lượng

| ID | Phụ thuộc | Trạng thái | Đầu ra và điều kiện qua |
| --- | --- | --- | --- |
| Q01 | T11, T14 | DONE | Khóa nguồn, ba mức toàn lịch sử và ngân sách; đối soát trực tiếp khóa, biên và coverage. |
| Q02 | Q01 | DONE | Fixture và đáp án tính tay, kiểm cả Spark và DuckDB ở hai thứ tự đầu vào. |
| Q03 | Q02 | DONE | Bản đồ Parquet trên 4 rồi 71 kênh; bất biến toàn panel và đối chứng trực tiếp theo đợt mỗi kênh. |
| Q04 | Q03 | DONE | Đo 4, 16, 71 kênh với hai công cụ, xác minh tương đương hoặc bằng chứng chạm ngân sách. |
| Q05 | Q04 | DONE | Gói demo xác minh từ bản giải nén, báo cáo, dựng lại 71 kênh và so hash nội dung. |

- 2026-11-19 · Q01 · `python -m honda.quality Q01 --run runs/q01_lock_r2 --prior q01_lock`. [Kiểm](runs/q01_lock_r2/checks.json) và [khóa](runs/q01_lock_r2/lock.json) xác nhận 71 tệp, 11.906.542 ô, từng khóa, biên và nhóm coverage. Lượt đầu giữ trạng thái FAILED vì DuckDB không tải được ICU. Lượt hai lấy tháng trực tiếp từ timestamp microsecond UTC.

- 2026-11-19 · Q02 · `python -m honda.quality Q02 --run runs/q02_fixture_r4 --prior q02_fixture_r3`. [Đáp án](runs/q02_fixture_r4/expected.json) và [24 phép kiểm](runs/q02_fixture_r4/checks.json) khớp ở hai công cụ và hai thứ tự dòng. Các lượt trước lưu lỗi tạo thư mục, thứ tự phép toán của nhóm ngày UTC và giới hạn cổng loopback trong sandbox. Spark local chạy qua khi được phép dùng loopback.
- 2026-11-19 · Q03 lượt 1 · [Log 71 kênh](runs/q03_map/level_71/worker.log) ghi DuckDB chạm giới hạn nội bộ 1 GiB khi gán đợt bằng hai cửa sổ. Mức 4 và 16 qua. Giữ nguyên ngân sách. Thay cách gán đợt bằng một cửa sổ dùng số thứ tự trong từng trạng thái, bỏ bảo toàn thứ tự chèn của DuckDB và kiểm lại Q02 trên cả hai công cụ trước khi chạy lại Q03.
- 2026-11-19 · Q02 kiểm lại · `python -m honda.quality Q02 --run runs/q02_fixture_r6 --prior q02_fixture_r5`. [24 phép kiểm](runs/q02_fixture_r6/checks.json) khớp sau đổi truy vấn. Lượt r5 phát hiện phép nhân số thứ tự kiểu 32 bit của Spark; sửa sang 64 bit trước khi chạy dữ liệu Honda.
- 2026-11-19 · Q03 · `python -m honda.quality Q03 --run runs/q03_map_r2 --prior q03_map`. [Đối chiếu 71 kênh](runs/q03_map_r2/level_71/audit.json) khớp 7.189.177 ô loại trong 136,514 đợt và 125 khóa mẫu kiểm trực tiếp. 103,239/136,514 đợt có ít nhất một kênh khác hợp lệ suốt đợt. Mức 4 và 16 cũng đạt.
- 2026-11-19 · Q04 · `python -m honda.quality Q04 --run runs/q04_benchmark`. [24 lượt](runs/q04_benchmark/measurements.json) trên 4, 16, 71 kênh đều qua. Tất cả hash nội dung và kết quả tra khớp Q03. Ở 71 kênh, dựng bảng trung vị DuckDB 2.958 s và Spark local 17.453 s. [Tổng hợp](runs/q04_benchmark/benchmark.json).
- 2026-11-19 · Q05 · `python -m honda.quality Q05 --run runs/q05_delivery_r2 --prior q05_delivery`. [Dựng lại 71 kênh](runs/q05_delivery_r2/reproduction.json) khớp cả năm SHA-256 nội dung. [Kiểm gói giải nén](runs/q05_delivery_r2/demo_smoke.log) qua đợt sáu giờ H1.Z11 và tháng không có đợt của H2.T.Z33. Chrome kiểm thêm H1.Z21 ngày 29/04/2023, 04:00–10:00 UTC, đúng 1/24 ô có kênh khác hợp lệ và một liên kết T11 ở chỉ số 36. [Ảnh Chrome](output/playwright/honda-quality-journal.png) · [Báo cáo](runs/q05_delivery_r2/report.md) · [ZIP](runs/q05_delivery_r2/honda-quality-map.zip). Lượt Q05 đầu đã dựng lại đúng nhưng dừng khi PyArrow từ chối danh sách cột dạng tuple, đã sửa trước lượt bàn giao.
