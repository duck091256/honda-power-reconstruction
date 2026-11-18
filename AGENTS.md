# Hướng dẫn làm việc trong workspace khôi phục công suất Honda

## Bắt đầu một task

1. Đọc [SPEC.md](SPEC.md) để biết hợp đồng dữ liệu và phép đo, [PLAN.md](PLAN.md) để biết mục tiêu giai đoạn, [TASK.md](TASK.md) để xác định đúng ID và phụ thuộc.
2. Khi được giao “thực hiện Tnn”, làm trọn dòng đó và dừng ở đầu ra đã ghi. Khi được giao cả giai đoạn, làm các task trong giai đoạn theo thứ tự phụ thuộc. Cập nhật trạng thái trong TASK.md khi bắt đầu và sau kiểm tra.
3. Đây là workspace mới cho công suất tác dụng điện P. Chỉ đọc các nguồn ngoài workspace. Đầu ra của dự án, môi trường Python và tệp tạm phải có vị trí riêng. Chỉ dùng dữ liệu hoặc mã từ dự án trước nếu có căn cứ về quyền, ngữ nghĩa và nguồn gốc phù hợp với task.

## Khi triển khai

- Tạo src/, tests/, configs/, runs/ và app/ khi task đầu tiên thực sự cần chúng. Dùng fixture nhỏ để phát triển trước khi chạy Spark trên dữ liệu thật. Dữ liệu thật chỉ được dùng trong giới hạn nguồn và tài nguyên đã kiểm ở T01–T02.
- Giữ đầu vào bất biến. Tạo thư mục runs/<run_id>/ mới cho mỗi lần chạy cần lưu bằng chứng. Lưu lệnh, hash đầu vào/cấu hình, mốc thời gian, môi trường, số hàng, phạm vi và trạng thái thành công/thất bại. Khi thử lại, liên kết run mới tới run trước.
- Áp dụng điều kiện tham chiếu trong SPEC.md. Trường W và tên công tơ từ đồ án trước không xác nhận đơn vị watt. Khi nguồn chưa đủ điều kiện, vẫn có thể phát triển bằng fixture có nhãn mô phỏng. Đánh dấu kết quả Honda là [chưa đo].
- Bộ dự đoán chỉ đọc dữ liệu đã che và lịch sử được phép. Bộ chấm mới nối truth vào predictions. Kiểm tra schema, khóa, biên thời gian và bất biến này bằng test. So sánh trên cùng case_id và ghi lý do cho mọi ca thiếu.
- Ghi cấu hình và SHA-256 trước lượt đánh giá chính. Nếu phát hiện lỗi sau khi xem kết quả, giữ run cũ, ghi rõ ảnh hưởng, sửa và chạy phiên bản mới. Nêu rõ mọi thay đổi về phạm vi hoặc phương pháp trong SPEC.md/PLAN.md và nhật ký task.
- Đưa lệnh tái tạo thực sự chạy được vào hướng dẫn khi code xuất hiện. Khi đánh dấu DONE, đối chiếu tiêu chí của đúng task với log hoặc artifact. Số liệu bài báo giữ nhãn “bài báo báo cáo”. Số liệu đồ án cần có run, mẫu số và đơn vị.
- Gặp vấn đề kỹ thuật cục bộ thì sửa và kiểm lại. Dừng phần phụ thuộc nếu cần sửa nguồn, nếu checksum đã đăng ký bị lệch hoặc nếu quyết định vượt phạm vi đã giao. Ghi rõ bước nào còn làm được, thông tin nào còn thiếu và điều kiện chạy tiếp.
