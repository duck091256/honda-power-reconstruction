# Bản đồ chất lượng công suất Honda

**103.239/136.514 đợt (75,63%)** có ít nhất một kênh khác hợp lệ suốt đợt. Trên 71 kênh có 11.906.542 ô trong phạm vi, gồm 4.717.365 ô hợp lệ và 7.189.177 ô không hợp lệ. Mỗi đợt nằm trong một công tơ và được xác định trên toàn lịch sử trước khi lọc tháng. Một kênh hợp lệ ở tất cả các ô là điều kiện về độ phủ để chọn ca cho thí nghiệm đa công tơ tiếp theo.

Trên AMD Ryzen 7 7735HS with Radeon Graphics với hai luồng, dùng **DuckDB để dựng bảng** và **DuckDB để tra chi tiết** dựa trên thời gian trung vị ở mức 71 kênh. Chi phí khởi động được ghi riêng. Tra cứu dùng cùng đợt, cùng danh sách kênh và cùng liên kết nhật ký.

| Kênh | Ô đầu vào | Byte Parquet | Công cụ | Khởi động (s) | Dựng (s) | RSS đỉnh khi dựng (MiB) | Tạm đỉnh khi dựng (MiB) | Tra ngắn / dài / gần trung vị (s) |
|---:|---:|---:|---|---:|---:|---:|---:|---|
| 4 | 841.344 | 16.496.292 | DuckDB | 0,008 | 0,488 | 267,5 | 0,1 | 0,012 / 0,011 |
| 4 | 841.344 | 16.496.292 | Spark local | 4,938 | 7,232 | 1.175,9 | 12,2 | 0,596 / 0,423 |
| 16 | 3.365.376 | 63.725.972 | DuckDB | 0,008 | 1,113 | 508,1 | 0,3 | 0,029 / 0,040 |
| 16 | 3.365.376 | 63.725.972 | Spark local | 4,766 | 9,410 | 1.607,9 | 34,8 | 0,622 / 0,648 |
| 71 | 11.906.542 | 223.906.273 | DuckDB | 0,009 | 2,958 | 1.335,6 | 7,0 | 0,077 / 0,109 |
| 71 | 11.906.542 | 223.906.273 | Spark local | 4,727 | 17,453 | 1.788,7 | 155,3 | 1,064 / 1,089 |

Tỷ số thời gian dựng Spark/DuckDB lần lượt là 4 kênh 14,82×, 16 kênh 8,45×, 71 kênh 5,90×.

Mỗi mức dùng lịch sử riêng của từng kênh. Byte Parquet là kích thước các tệp T14 đã chọn. Hai công cụ còn đọc cùng danh mục khoảng T11. Mỗi vòng mở một tiến trình mới. Mỗi mức và công cụ có một lượt làm nóng và ba lượt đo. Các lượt chạy tuần tự, đổi lượt đi trước và giữ cache hệ điều hành tự nhiên. RSS là tổng trên cây tiến trình, có thể đếm trùng trang dùng chung. RSS và block đĩa tạm được lấy mẫu mỗi 100 ms. Mỗi truy vấn có thời gian riêng trong dữ liệu phép đo. Nếu cách chọn cho cùng một khóa, chỉ đo khóa đó một lần trong mỗi lượt. Bảng dùng giới hạn đĩa và RAM khóa ở Q01.

Mỗi liên kết nhật ký xác nhận giao thời gian với một khoảng T11 đã đệm của đúng công tơ. Chỉ số khoảng bắt đầu từ 0 trong danh mục T11. Bản đồ hiển thị lý do chính theo T14.

Nguồn chứng cứ theo thư mục run gồm bản đồ `q03_map_r2`, benchmark `q04_benchmark` và lượt dựng lại trong `q05_delivery_r2/repro_71`. Kiểm tra nội dung dùng SHA-256 Arrow IPC sau khi chuẩn hóa schema và sắp xếp khóa.
