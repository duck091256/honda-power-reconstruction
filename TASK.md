# Sổ công việc

**Hiện tại:** kế hoạch đã soạn, T01 là việc đầu tiên. Tất cả task kỹ thuật đang ở TODO. Tên tệp trong bảng là đầu ra dự kiến. Cách chạy task được quy định trong [AGENTS.md](AGENTS.md).

| ID | Phụ thuộc | Trạng thái | Việc cần làm | Bằng chứng để hoàn tất |
| --- | --- | --- | --- | --- |
| T01 | — | TODO | Kiểm kê nguồn Honda có thể đọc, metadata, nhật ký, giấy phép; đối chiếu yêu cầu môn và ngân sách laptop. | docs/feasibility.md ghi nguồn/phiên bản, tệp mẫu, bằng chứng đại lượng điện, đơn vị, timezone, quyền truy cập, RAM/đĩa và các mục chưa xác minh. Ghi rõ có thể thử Honda hay chỉ làm fixture. |
| T02 | T01 | TODO | Lấy một lát dữ liệu phút theo phạm vi đã xác minh. Truy nguyên kênh/tệp, đối chiếu nhật ký và chốt cách tính ô tham chiếu. | source_manifest và bảng mẫu phút chỉ ra lặp, xung đột, vùng bị loại, lý do, ít nhất một đoạn liên tục gồm 6 giờ và hai ô biên nếu có. Ghi độ dài khả dụng nếu ngắn hơn. |
| T03 | T02 | TODO | Viết bộ kiểm thử tổng hợp và Spark ETL tối thiểu theo hợp đồng panel. | Fixture bắt được phút khuyết, bản sao trùng, xung đột, sai lệch múi giờ, lỗi sát biên ô, vùng nhật ký, giá trị âm và 0. Panel khớp bảng tính tay. Khi thiếu dữ liệu thật có thể chuẩn bị phần fixture độc lập. |
| T04 | T03 | TODO | Chạy thử một công tơ trên dữ liệu phát triển. Che một đoạn, nội suy và nối lại với truth bằng khóa. | Một run mẫu có trace phút → ô → ca → dự đoán → sai số. Test riêng 4 ô với sai lệch đều +1000 W cho sai số điện năng 1 kWh. |
| T05 | T04 | TODO | Mở rộng ETL trên tập kênh đủ điều kiện, ghi Parquet và thống kê theo công tơ/tháng. | Panel duy nhất theo khóa, số dòng hợp lệ/bị loại và nguyên nhân đối soát được với nguồn. Ghi thời gian, số kênh, kỳ đo và dung lượng thực tế. |
| T06 | T05 | TODO | Chốt khoảng lịch sử/test, độ dài, quy tắc chọn và hạn mức ca, seed, dự phòng, dung sai và ngân sách. Sinh cùng bộ ca cho cả hai phương pháp. | configs/evaluation_v1.json, hash cấu hình, cases, truth và dữ liệu đã che. Test chống rò rỉ mốc thời gian và truth. Ghi số ca đủ/thiếu theo độ dài. |
| T07 | T06 | TODO | Chạy Linear/Seasonal, so từng cặp trên cùng case_id và ghi tỷ lệ dự đoán được. | predictions, case_scores, summary kèm đơn vị, số ca và số công tơ. Thay truth sau bước che giữ nguyên dự đoán. Báo cả ca bị bỏ và lượt dự phòng. |
| T08 | T06 | TODO | Benchmark truy vấn lọc trên Parquet thường và phân vùng cùng một panel. | cost ghi chi phí tạo, 1+3 lượt truy vấn mỗi bản, trung vị, số tệp/byte lưu, cấu hình máy, trạng thái cache, digest kết quả. Byte đọc thực tế chưa lấy được thì ghi [chưa đo]. |
| T09 | T07, T08 | TODO | Làm demo Streamlit dùng bảng nhỏ đã xuất. Chọn công tơ, độ dài và ca. Cho bật/tắt đường thật và hai đường khôi phục. | Chạy cục bộ từ run cố định. Biểu đồ có thời gian UTC, W, vùng che, tên phương pháp và sai số kWh. Hiển thị được cả nhóm không có ca và đường dẫn run. |
| T10 | T09 | TODO | Chạy lại từ cấu hình đã lưu trong thư mục mới, so kết quả logic, đóng gói hướng dẫn và báo cáo. | README có lệnh cài/chạy thật đã thử, phiên bản, ví dụ; báo cáo ghi quy mô, sai số, chi phí và giới hạn. Demo hoạt động từ gói kết quả, kiểm riêng quy trình có nguồn để tái tạo. |

Khi hoàn tất một task, thêm một dòng dưới đây với ngày, ID, lệnh đã chạy, kết quả, đường dẫn run/bằng chứng và việc còn mở. Nếu bị chặn, ghi điều kiện mở lại. Trạng thái tích hợp của task dùng bốn giá trị TODO, IN_PROGRESS, BLOCKED, DONE.

## Nhật ký triển khai

Chưa có run trong workspace này.
