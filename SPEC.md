# Đặc tả đồ án · Khôi phục dữ liệu công suất điện Honda bằng Apache Spark

**Người dùng** là chuyên viên phân tích năng lượng đang xem lại lịch sử một công tơ sau khi một khoảng gián đoạn đã kết thúc. Họ chọn công tơ, ngày và khoảng trống để xem hai cách ước lượng đường công suất và lượng điện tương ứng. Nhóm phát triển cần biết sai số thay đổi ra sao khi khoảng trống dài hơn, và Spark cần bao nhiêu thời gian, dung lượng để xử lý dữ liệu.

**Sản phẩm tối thiểu** gồm pipeline Spark chạy theo lô trên một máy, bộ đánh giá có thể chạy lại và demo cục bộ đọc kết quả đã xuất. Một lượt demo hoàn chỉnh đi từ đoạn đo có thật → che có kiểm soát → khôi phục → bật đường tham chiếu → đọc sai số. Phạm vi học phần và quyền dùng nguồn cần được xác minh ở T01.

## Dữ liệu và quy tắc tham chiếu

- Nguồn ứng viên là Honda Research Institute, Offenbach, dữ liệu và nhật ký trên [Dryad](https://doi.org/10.5061/dryad.73n5tb363). Định danh kênh bằng metadata về thiết bị, đại lượng **tổng công suất tác dụng điện**, đơn vị và quy ước thời gian. Tên cột P riêng lẻ chưa đủ bằng chứng. Ghi URI/phiên bản, điều khoản sử dụng, SHA-256 và đường dẫn tệp đã đọc.
- Bài báo mô tả 57 kênh tham chiếu. Quy mô được báo cáo cho đồ án là số kênh và thời gian thực sự qua kiểm tra trong workspace này. Nguồn W cùng các kết quả P1–P3 của đồ án trước có mục tiêu và hợp đồng dữ liệu riêng.
- Quy ước đề xuất cho MVP là phút UTC và ô 15 phút dạng [t, t+15 phút), gắn nhãn bằng t. Xác minh ý nghĩa timestamp và giá trị phút từ tài liệu nguồn trước khi áp dụng. Ô hợp lệ cần đủ 15 vị trí phút phân biệt, giá trị hữu hạn và đơn vị chuẩn hóa về watt. Giữ dấu của công suất. Gộp bản sao đồng nhất, đánh dấu ô xung đột hoặc thiếu phút là không hợp lệ. Dùng trung bình số học của 15 giá trị phút khi metadata hỗ trợ cách đại diện này.
- Tham chiếu dùng bản quan sát gốc. Loại dữ liệu đã sửa/điền và khoảng bị nhật ký đánh dấu công suất không đáng tin. Đề xuất vùng đệm 15 phút mỗi phía theo bài báo, chốt cách ánh xạ nhật ký ở T02. Nếu thiếu nguồn đối chiếu, ghi mức kiểm chứng thực tế và giải quyết trước khi chấm Honda.
- Phân biệt ba trạng thái hiển thị: có quan sát hợp lệ, thiếu/không hợp lệ tự nhiên, chủ động che để đánh giá. Chỉ trạng thái cuối có giá trị thật dùng chấm điểm.

## Thiết kế đánh giá đề xuất

1. **Chia theo thời gian.** Từ thống kê độ phủ, chọn hai miền UTC rời nhau: lịch sử để xây hồ sơ mùa vụ/phát triển, giai đoạn sau để đánh giá. Ghi ngày biên trước khi xem sai số. Toàn bộ khoảng che và hai điểm biên nội suy phải nằm trong cùng miền đánh giá. Dùng thời điểm sau khoảng trống vì đây là khôi phục hồi cứu.
2. **Tạo ca thử.** Dùng ba độ dài 1, 4 và 24 ô liên tiếp (15 phút, 1 giờ và 6 giờ). Mỗi ca thuộc một công tơ, có tham chiếu hợp lệ suốt đoạn và một ô hợp lệ sát mỗi đầu. Chọn bằng độ phủ. Đề xuất sắp các ứng viên theo hash của seed 42, công tơ, thời điểm và độ dài, chốt hạn mức từng nhóm trước test. Lưu danh sách ca và lý do loại. Một ca là một tình huống mô phỏng.
3. **Hai phương pháp.** Linear nội suy theo thời gian giữa hai giá trị sát biên. Seasonal lấy trung vị trong lịch sử của cùng công tơ, cùng thứ trong tuần và cùng ô giờ UTC. Khóa thứ tự dự phòng cho Seasonal là cùng ô giờ trong lịch sử, rồi trung vị lịch sử của công tơ. Đếm lần dùng dự phòng. Thiếu cả lịch sử thì ghi ca chưa có dự đoán. Hai phương pháp nhận ngữ cảnh đã che và dùng cùng danh sách ca.
4. **Chấm theo ca.** Với tập G gồm các ô che, tính MAE công suất bằng trung bình của |P dự đoán − P tham chiếu| theo watt. Tính sai số tuyệt đối điện năng bằng 0,25/1000 × |tổng công suất dự đoán trên G − tổng công suất tham chiếu trên G| theo kWh. Công thức dùng công suất trung bình ô và bước 15 phút đã xác minh. Báo từng độ dài với số ca dự kiến, số ca có dự đoán của mỗi phương pháp, số ca ghép cặp và số công tơ. So sánh trung bình theo ca trên tập ghép cặp. Công bố cả tỷ lệ dự đoán được và lý do thiếu.
5. **Đo phần Big Data.** Spark đọc nguồn theo lô, kiểm tra, gom phút, ghi panel Parquet và tạo các bảng đánh giá. So truy vấn lọc cùng công tơ/khoảng ngày trên Parquet thường và Parquet phân vùng theo năm/tháng UTC. Cùng hàng, schema logic, codec và tài nguyên. Ghi thời gian tạo từng bản, số file, dung lượng, thời gian truy vấn (1 làm nóng + 3 lần đo mỗi bản, báo trung vị), số hàng và digest logic của kết quả. Ghi phiên bản Spark/Java, số core, RAM, quy mô đầu vào và điều kiện cache. Chấp nhận kết quả chậm hơn.

## Hợp đồng đầu ra dự kiến

| Bảng hoặc tệp | Khóa / trường thiết yếu | Ai sử dụng |
| --- | --- | --- |
| source_manifest | source_id, URI, hash, kênh, đại lượng, đơn vị, timezone, căn cứ metadata | ETL và người kiểm tra |
| panel_15m | meter_id + bin_start_utc, power_w, valid_ref, minute_count, reason, source_id hoặc liên kết tới danh sách nguồn | Spark và bộ tạo ca |
| cases và truth | case_id, meter_id, start_utc, n_bins, split; truth có thêm bin_start_utc và ref_w | Scorer đọc truth; bộ dự đoán chỉ nhận dữ liệu đã che |
| predictions | case_id + bin_start_utc + method, pred_w, fallback hoặc reason | Scorer và demo |
| case_scores và summary | run_id, case_id, method, mae_w, abs_energy_error_kwh; tổng hợp theo độ dài, kèm mẫu số | Báo cáo và demo |
| run_manifest và cost | hash nguồn/cấu hình, phiên bản mã/môi trường, seed, số dòng, thời gian, dung lượng, hash đầu ra logic | Tái tạo và benchmark |

## Tiêu chí hoàn thành

Trên fixture tính tay, pipeline cho đúng khóa, khoảng UTC, số phút, cờ hợp lệ, dự đoán và đơn vị. Trên dữ liệu thật, khóa panel duy nhất, các ô che có tham chiếu, thay giá trị truth sau khi che không làm đổi dự đoán. Hai lần chạy cùng đầu vào/cấu hình khớp khóa, mặt nạ và số liệu trong dung sai đã đăng ký. Demo mở được từ một run lưu sẵn và hiển thị quan sát, đoạn che, hai ước lượng, tham chiếu cùng mẫu số. Kết luận về độ chính xác chỉ bao phủ các ca thực sự chấm được.

Mở rộng như mô hình đa công tơ, khoảng dự báo và triển khai cluster sẽ cần câu hỏi và ngân sách riêng sau MVP. Lộ trình và sổ công việc nằm ở [PLAN.md](PLAN.md) và [TASK.md](TASK.md).

## Phạm vi pilot v1 đã chốt ở T01–T02

Nguồn là các quan sát trong tệp harmonized của H1.Z11, H1.Z12, H1.Z20, H1.Z24 trong quý I/2023 UTC. Giai đoạn harmonized chuẩn hóa đơn vị và dấu trước khi nhà cung cấp sửa lỗi hoặc điền. Giữ dấu của bản này. Một phút hợp lệ cần timestamp đúng đầu phút UTC và giá trị hữu hạn. Bất kỳ bản ghi lệch giây nào làm ô chứa nó không hợp lệ. Các ô không có hàng vẫn xuất hiện trong panel. Công suất ô là trung bình đại diện của 15 mẫu tức thời. kWh tính từ các ô là điện năng ước lượng có dấu.

Đọc cả danh mục nhật ký tự động và tay. Áp nội dung tham chiếu URN chung hoặc đúng URN.P, với thời điểm đầu/cuối bao hàm và đệm 15 phút hai phía. Loại mọi ô có giao với đoạn đệm. Nhật ký theo trường năng lượng W hoặc đại lượng khác không được quy thành lỗi riêng của P. Đăng ký tệp và biên ở [sources_v1](configs/sources_v1.json).

## Cấu hình đánh giá v1 đã khóa ở T06

Lịch sử từ 01-01 đến trước 01-03-2023 UTC. Đánh giá từ 06-03 đến trước 01-04-2023 UTC. Để riêng lát T02–T04 khỏi kỳ chấm. Chọn tối đa 30 ca mỗi công tơ mỗi độ dài theo thứ tự SHA-256 seed 42. Các ô che trong cùng công tơ và cùng độ dài không trùng nhau. Các độ dài khác nhau có thể dùng chung thời điểm. Áp quy tắc, dung sai và ngân sách trong [evaluation_v1](configs/evaluation_v1.json). Spark đọc và dựng panel. Phần sinh ca, dự đoán, chấm và xuất bảng nhỏ dùng Python trên pilot đã giới hạn.

Giai đoạn 2 dùng bảng 2 và 4 của Engel và cộng sự để xác định URN điện và đơn vị W. Nhật ký H1.Z19 ghi sai reference, nhật ký H3.ZE50/H3.ZE51 ghi sai tiền tố tòa nhà. Mục Manual specification của tác giả xác định đích tương ứng H1.Z19, H4.ZE50 và H4.ZE51; bảng nguồn chạy giữ hash các YAML gốc. Khi dùng timestamp harmonized, loại khỏi tham chiếu thời kỳ trước 01-04-2020 UTC của thiết bị ABB có rủi ro trôi đồng hồ gateway; giữ các hàng và thống kê lý do.

T14 ghi lưới UTC theo biên quan sát của từng tệp kênh, làm tròn ra ngoài đến vạch 15 phút. Lịch sử cho Seasonal là [01-04-2020, 01-10-2023), kỳ chấm là [01-10-2023, 01-01-2024). Thiết kế chỉ che các đoạn có tham chiếu và hai biên hợp lệ. Tối đa 30 ca cho mỗi công tơ và độ dài. Số 57 trong bài chính §3.1 thuộc panel đã qua kiểm định của bài, trong đó có điều kiện tối thiểu 30 ngày quan sát ở giai đoạn fit. T11 đếm nguồn P trước khi xét điều kiện ô và ca của pipeline này.
