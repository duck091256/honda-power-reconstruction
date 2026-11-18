# Kế hoạch phát triển theo giai đoạn

Bản đầu hướng tới demo chạy trên laptop bằng Spark local và giao diện Streamlit đọc bảng đã xuất. Lịch 4 tuần dưới đây là ước lượng để sắp việc. Hạn nộp, tài nguyên máy đích, dữ liệu thực có và yêu cầu công nghệ của học phần còn cần xác minh. [Đặc tả](SPEC.md) · [Task theo thứ tự](TASK.md)

| Giai đoạn | Thời điểm dự kiến | Sản phẩm để xem hoặc chạy | Điều kiện chuyển bước |
| --- | --- | --- | --- |
| 1. Kiểm tra tính khả thi · T01–T02 | Tuần 1 | Danh mục nguồn và một đoạn công suất phút đọc được, có thể truy về tệp | Xác minh đây là kênh điện P, đơn vị, thời gian, quyền sử dụng và cách xử lý lỗi nguồn. Có ít nhất một đoạn đủ dài cho ca thử. Ghi quy mô khả dụng thực tế. |
| 2. Làm luồng nhỏ từ đầu đến cuối · T03–T04 | Tuần 1–2 | Fixture và panel 15 phút trên một lát Honda. Một ca che, nội suy và chấm tay | Đối chiếu đúng từ phút → ô → ca → watt/kWh. Lưu run mẫu để phát triển tiếp mà không đọc lại toàn nguồn mỗi lần. |
| 3. Mở rộng và đóng băng bộ đánh giá · T05–T06 | Tuần 2 | Panel Parquet trên các kênh đủ điều kiện, bảng độ phủ, cấu hình và danh sách ca cố định | Đối soát khóa/số dòng, lịch sử và test tách theo thời gian. Chốt ngưỡng, ngân sách và dung sai trước lượt chấm chính. |
| 4. Đo sai số và chi phí · T07–T08 | Tuần 3 | Bảng sai số ghép cặp theo độ dài và bảng chi phí Spark | T07 và T08 có thể làm song song sau T06. Chạy benchmark một mình trên máy đo. Kết quả truy vấn giữa hai cách lưu phải tương đương. |
| 5. Triển khai demo cục bộ và bàn giao · T09–T10 | Tuần 4 | Demo chọn ca, báo cáo và hướng dẫn chạy trên máy chấm | Tái tạo được một run, truy được ca demo về nguồn, nêu rõ phần đã đo và phần thiếu dữ liệu. |

## Luồng xử lý

Nguồn P + metadata + nhật ký → kiểm tra và chuẩn hóa UTC → Spark dựng panel Parquet → tách lịch sử/test → chọn đoạn quan sát rồi che → Linear và Seasonal → chấm trên bản tham chiếu → xuất bảng nhỏ cho demo. Nhánh đo chi phí đọc cùng panel sau khi qua đối soát.

**Ưu tiên khi thời gian ít.** Hoàn thiện lát cắt một công tơ trước, mở rộng theo số kênh thực sự qua kiểm tra. Nếu một độ dài không có ca, công bố số ca bằng 0 và lý do. Nếu chỉ có fixture, có thể hoàn thiện kiểm thử kỹ thuật và trình bày luồng thao tác với nhãn mô phỏng. Kết quả Honda vẫn để [chưa đo].

**Rủi ro ảnh hưởng quyết định.** Thiếu bằng chứng đơn vị, timezone hoặc cách tạo tham chiếu sẽ chặn tính sai số Honda. Thiếu RAM/đĩa thì chia việc đọc theo tệp hoặc kỳ thời gian, ghi quy mô đã xử lý. Nếu rubric yêu cầu cluster, ghi ở T01 rồi bổ sung bước kiểm tra triển khai trên hạ tầng được cấp. Quy mô 57 kênh trong bài báo dùng để đối chiếu thiết kế.

**Chỗ bắt đầu đọc.** Bài chính §3.1, §4.1–4.2 giải thích tham chiếu, điện năng và hai baseline. Phụ lục S1–S2, S7–S8 giải thích đơn vị, chia thời gian, cách báo chi phí và phạm vi tái tạo. Hai bản thảo trên máy chuẩn bị là [bài chính](/home/duck/Downloads/main_paper.md) và [phụ lục](/home/duck/Downloads/supplementary_material.md). Khi chuyển máy, đăng ký lại vị trí tài liệu và nguồn dữ liệu tại T01.

T01–T02 chốt pilot bốn kênh điện trong quý I/2023 UTC trên bản nguồn cục bộ. ETL xử lý tuần tự trong ngân sách Spark local hai luồng và heap 512 MiB. Benchmark báo thời gian trên đúng panel pilot này. Rubric chính thức chưa có trong workspace. Xem [kiểm kê](docs/feasibility.md).

T06 khóa giai đoạn sau 06-03-2023 UTC cho kỳ đánh giá và để riêng lát phát triển. Mỗi công tơ đóng góp tối đa 30 ca cho mỗi độ dài. Cấu hình và SHA-256 được lưu trước lượt chấm chính.

## Giai đoạn 2 · T11–T15
Kiểm kê theo bảng thiết bị Honda, mở rộng xử lý theo công tơ, đối chiếu pilot trước khi đo Spark local và DuckDB. Đo 3/6/12 tháng với 4 công tơ pilot, rồi tăng số kênh ở 12 tháng. T14 xử lý toàn bộ lịch sử P điện có metadata và nhật ký, có checkpoint theo kênh. T15 xuất demo và báo cáo số liệu thực đo.
