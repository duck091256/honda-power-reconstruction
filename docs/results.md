# Kết quả pilot khôi phục công suất Honda

Dữ liệu quan sát trước bước sửa và điền từ bốn công tơ điện H1.Z11, H1.Z12, H1.Z20, H1.Z24. Pilot đọc quý I/2023 UTC trên bản Honda cục bộ. Bộ đánh giá dùng lịch sử tháng 1–2 và các ca che ngày 06–31/03. Lát phát triển ngày 03–04/03 được giữ ngoài kỳ chấm. Nguồn và giấy phép ghi trong [kiểm kê](feasibility.md).

## Dữ liệu đủ điều kiện

[T05, t05_pilot](../runs/t05_pilot/run_manifest.json) quét 12.163.371 bản ghi ở bốn tệp gzip và chọn 517.238 bản ghi trong phạm vi. Panel có 34.560 khóa công tơ–thời điểm duy nhất. Có 33.266 ô hợp lệ (96,26%), 1.066 ô thiếu phút, 216 ô có mẫu lệch lưới thời gian và 12 ô giao vùng nhật ký. Mỗi ô được thống kê theo lý do ưu tiên. Parquet panel có 455.890 byte dữ liệu.

## Sai số trên cùng ca

[T07, t07_honda](../runs/t07_honda/summary.csv) chấm 120/120 ca ghép cặp từ bốn công tơ ở mỗi độ dài, 30 ca mỗi công tơ. Mỗi phương pháp dự đoán được 100% ca đã chọn. Seasonal dùng hồ sơ cùng thứ và ô giờ cho cả 3.480 ô che. Bảng dùng trung bình theo ca.

| Khoảng che | Ca ghép cặp | Linear MAE (W) | Seasonal MAE (W) | Linear sai số điện năng (kWh) | Seasonal sai số điện năng (kWh) |
| --- | ---: | ---: | ---: | ---: | ---: |
| 15 phút | 120 | 818,31 | 25.475,02 | 0,205 | 6,369 |
| 1 giờ | 120 | 1.720,71 | 21.414,87 | 1,323 | 21,123 |
| 6 giờ | 120 | 3.928,37 | 22.351,11 | 18,206 | 129,070 |

Linear có sai số trung bình thấp hơn ở cả ba độ dài. Chênh lệch chịu ảnh hưởng lớn của H1.Z20, công tơ CHP. Chẳng hạn với ca 6 giờ, MAE theo công tơ này là 12.987,3 W cho Linear và 85.657,3 W cho Seasonal, mỗi phương pháp trên 30 ca. Trên H1.Z12, Seasonal có MAE thấp hơn Linear ở 1 giờ và 6 giờ. Các ca được lấy từ đoạn có quan sát đủ và hai biên hợp lệ, trong cùng một tòa nhà và một quý. Sai số pilot chỉ bao phủ điều kiện đó. Điện năng tính từ công suất trung bình có dấu của từng ô.

## Chi phí Spark

[T08, t08_cost](../runs/t08_cost/cost.json) đo trên Spark 4.0.1, Temurin 21.0.13, local[2], heap 512 MiB. Hai bản có cùng 34.560 hàng, schema logic, codec Snappy và digest toàn panel. Truy vấn lọc H1.Z20 từ 06/03 đến trước 20/03 UTC trả 1.344 hàng với digest trùng trong cả tám lượt. Mỗi bố cục có một lượt làm nóng và ba lượt đo, xen kẽ thứ tự. Không persist Spark. Cache hệ điều hành và JVM không được đặt lại giữa các lượt.

| Bố cục | Tạo bản (s) | Tệp | Dung lượng (byte) | Trung vị truy vấn (s) |
| --- | ---: | ---: | ---: | ---: |
| Thường | 0,744 | 2 | 558.422 | 0,302 |
| Phân vùng năm/tháng | 0,605 | 6 | 559.414 | 0,214 |

Lượt đo này quan sát trung vị thấp hơn với bản phân vùng. Máy đo có tải nền và pilot nhỏ. Byte đọc thực tế **[chưa đo]**. Ba lượt đo mỗi bản mô tả phép thử cục bộ, chưa đủ để suy ra mức tăng tốc ổn định khi mở rộng dữ liệu.

## Kiểm tra và tái tạo

- [T03](../runs/t03_fixture/run_manifest.json) kiểm tra tám ô theo bảng tính tay. [T04](../runs/t04_one_meter/run_manifest.json) truy vết ca Honda phát triển và kiểm công thức +1.000 W trong một giờ cho 1 kWh.
- [T06](../runs/t06_locked/run_manifest.json) khóa SHA-256 trước chấm. [T07](../runs/t07_honda/run_manifest.json) ghi 6.960 dự đoán và kiểm sự độc lập giữa dự đoán và truth.
- [T09](../runs/t09_demo/bundle/bundle_manifest.json) lưu gói nhỏ mở bằng Streamlit. Xem [ảnh kiểm tra Chrome](../output/playwright/honda-demo.png) và [hướng dẫn](../README.md).
- Lượt lặp đo trung vị 0,234 s (thường) và 0,199 s (phân vùng). Digest toàn panel và digest truy vấn trùng lượt đầu. Các lượt đều xử lý tuần tự với cùng cấu hình Spark.
- Trong ZIP bàn giao, `bundle/` chứa coverage, nguồn, cấu hình, mặt nạ, dự đoán, truth, số liệu và benchmark của lượt đầu. `verification.json` ghi đối chiếu hai lần chạy. Các liên kết vào `runs/` và ảnh Chrome trong báo cáo dành cho workspace triển khai.
