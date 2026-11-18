# Khôi phục công suất điện Honda trên toàn lịch sử

T11 xác minh 71 nguồn P trong 81 thư mục URN.
T14 đọc 208,746,721 dòng từ 71 nguồn, tổng 1,484,219,373 byte gzip.
Panel theo phạm vi thời gian riêng của từng công tơ có 11,906,542 ô.
Có 4,717,365 ô đạt quy tắc tham chiếu trên 51 công tơ.
Quý IV/2023 chọn 3,487 ca trên 39 công tơ.
Bài chính §3.1 báo 57 kênh trong panel kiểm định theo giao thức nghiên cứu, gồm điều kiện tối thiểu 30 ngày quan sát trong fit.
Phạm vi T14 dùng quy tắc 15 vị trí phút UTC, lọc nhật ký, loại rủi ro đồng hồ và yêu cầu hai biên trong ca kiểm.
Lịch sử xây Seasonal từ tháng 4/2020 đến hết tháng 9/2023.

## Sai số theo độ dài

| Khoảng che | Phương pháp | Ca chọn | Ca ghép cặp | MAE (W) | Sai số điện năng (kWh) |
| --- | --- | ---: | ---: | ---: | ---: |
| 15 phút | Linear | 1170 | 1170 | 1487.00 | 0.372 |
| 15 phút | Seasonal | 1170 | 1170 | 5347.23 | 1.337 |
| 60 phút | Linear | 1170 | 1170 | 1859.09 | 1.292 |
| 60 phút | Seasonal | 1170 | 1170 | 5298.67 | 5.162 |
| 360 phút | Linear | 1147 | 1147 | 2424.52 | 10.365 |
| 360 phút | Seasonal | 1147 | 1147 | 6242.91 | 36.246 |

Ảnh hưởng của H1.Z20 được kiểm bằng trung bình trên cùng ca ghép cặp sau khi bỏ công tơ này.

- 15 phút · Linear · 762.95 W trên 1140 ca ghép cặp.
- 15 phút · Seasonal · 2603.70 W trên 1140 ca ghép cặp.
- 60 phút · Linear · 799.62 W trên 1140 ca ghép cặp.
- 60 phút · Seasonal · 2516.91 W trên 1140 ca ghép cặp.
- 360 phút · Linear · 1186.66 W trên 1117 ca ghép cặp.
- 360 phút · Seasonal · 2754.04 W trên 1117 ca ghép cặp.

## Chi phí tại các quy mô đã đo

| Công tơ × tháng | Công cụ | Dòng trong kỳ | Dựng panel (s) | Truy vấn (s) | RSS đỉnh (MiB) |
| --- | --- | ---: | ---: | ---: | ---: |
| 4 × 3 | Spark | 517,238 | 0.630 | 0.236 | 1329 |
| 4 × 3 | DuckDB | 517,238 | 1.588 | 0.011 | 392 |
| 4 × 6 | Spark | 1,038,215 | 0.646 | 0.179 | 1446 |
| 4 × 6 | DuckDB | 1,038,215 | 1.715 | 0.012 | 434 |
| 4 × 12 | Spark | 1,952,480 | 0.698 | 0.171 | 1615 |
| 4 × 12 | DuckDB | 1,952,480 | 1.821 | 0.011 | 514 |
| 8 × 12 | Spark | 3,905,157 | 1.413 | 0.163 | 1670 |
| 8 × 12 | DuckDB | 3,905,157 | 3.734 | 0.012 | 530 |
| 16 × 12 | Spark | 7,145,599 | 2.815 | 0.164 | 1699 |
| 16 × 12 | DuckDB | 7,145,599 | 7.262 | 0.012 | 541 |

Mỗi ô thời gian là trung vị ba lượt. Đọc gzip bao gồm quét toàn tệp đã chọn để lọc kỳ.
Cả hai công cụ lấy cùng tập tệp tại mỗi mức. Spark chạy local[2] với worker Arrow/NumPy, DuckDB chạy hai luồng.
Thời gian gồm dựng và ghi panel. Bảng chi tiết ghi tổng thời gian các tác vụ nhập và ETL.
RSS của Spark cộng cả JVM và các worker Python nên có thể tính lặp trang bộ nhớ chia sẻ.
Cache hệ điều hành được giữ tự nhiên. Lượng byte thiết bị thực đọc chưa đo.
So sánh chỉ phản ánh cấu hình máy cục bộ và hai cách triển khai đã đo.

Mở app bằng `streamlit run app/main.py -- --bundle bundle`.
