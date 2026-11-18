# Đề xuất mở rộng đồ án Big Data Honda

**Mục tiêu.** Giúp chuyên viên năng lượng xem lại một đoạn thiếu số đo và trả lời hai câu hỏi. Đường công suất có thể khôi phục chính xác đến đâu? Khi cộng cả đoạn thành điện năng, việc tham khảo các công tơ khác có giảm sai số đủ để đáng chi phí xử lý hay không?

## Điểm xuất phát

T14 đã đọc 208.746.721 dòng từ 71 nguồn công suất điện P. Có 51 công tơ với ít nhất một ô 15 phút đạt điều kiện tham chiếu và 39 công tơ đóng góp 3.487 ca đánh giá trong quý IV/2023. Với 1.147 ca che liên tục 6 giờ, sai số điện năng tuyệt đối trung bình là 10,365 kWh cho Linear và 36,246 kWh cho Seasonal. Đây là mốc so sánh thực đo. Bài báo dùng panel đã kiểm định gồm 57 kênh và quy tắc chọn ca riêng. Mọi phương pháp bổ sung bên dưới đều ở trạng thái đề xuất. [Kết quả T15](../runs/t15_delivery/report.md) · [Bài chính, mục 3.1](/home/duck/Downloads/main_paper.md:68)

## Phương pháp nào đáng thử?

Đề xuất chạy sáu phương pháp trong đợt chính, gồm hai phương pháp đã có và bốn phương pháp mới. Cách chọn này cho phép kiểm tra lần lượt giá trị của lịch sử, diễn biến ở hai đầu khoảng trống và tín hiệu từ các công tơ khác. Bốn phương pháp còn lại có câu hỏi thử nghiệm và điều kiện mở rộng riêng. [Bài chính, bảng 3](/home/duck/Downloads/main_paper.md:122) · [Phụ lục, mục S2.1](/home/duck/Downloads/supplementary_material.md:73)

| Phương pháp | Cách ước lượng công suất bên trong khoảng trống | Vai trò trong đồ án |
| --- | --- | --- |
| Linear | Nối hai số đo hợp lệ sát hai đầu khoảng trống. | Mốc đã có, chi phí thấp. |
| Seasonal | Lấy trung vị lịch sử của chính công tơ vào cùng thứ và cùng khung 15 phút. | Mốc đã có cho phụ tải lặp theo lịch. |
| Blend | Phối hợp Linear và Seasonal bằng trọng số chọn trên tập phát triển. | Đợt chính. Kiểm tra liệu lịch sử có bổ sung thông tin cho hai điểm biên. |
| Seasonal Kalman | Lấy mức theo mùa làm nền, dùng các quan sát trước và sau khoảng trống để ước lượng phần biến động quanh nền. | Đợt chính. Thử mô hình chuỗi thời gian chỉ dùng công tơ mục tiêu. |
| Ridge | Học phần sai lệch còn lại sau nội suy từ lịch và các giá trị quan sát được ở công tơ hỗ trợ. | Đợt chính. Đo lợi ích của hiệu chỉnh tuyến tính. |
| XGBoost | Dùng cây tăng cường để ước lượng phần hiệu chỉnh cho Linear từ cùng tập đầu vào như Ridge. | Đợt chính. Kiểm tra giá trị của quan hệ phi tuyến. |
| Extra Trees | Tập hợp các cây dự đoán trực tiếp công suất đã chuẩn hóa. | Mở rộng khi muốn so dự đoán trực tiếp với hiệu chỉnh quanh Linear; đo chi phí suy luận trước. |
| Soft Impute | Điền các ô thiếu bằng cấu trúc hạng thấp của ma trận phần dư theo thời gian và công tơ. | Mở rộng khi có đủ quan sát đồng thời; công bố tỷ lệ hội tụ. |
| BRITS | Mạng hồi quy hai chiều khai thác diễn biến trước và sau khoảng trống trên nhiều kênh. | Ưu tiên trong nhóm mạng sâu nếu cần kiểm tra mức phụ thuộc vào công tơ hỗ trợ. |
| SAITS | Dùng cơ chế attention để tổng hợp thông tin từ chuỗi nhiều kênh và các vị trí còn quan sát. | Thử sau BRITS nếu ngân sách cho phép so hai cách học chuỗi trên cùng bộ ca. |

Bài báo báo cáo XGBoost được chọn trên dữ liệu phát triển. Trên nhóm tái hiện nhật ký năm 2023, sai số điện năng trung bình của Linear là 0,198 kWh và của XGBoost là 0,219 kWh. Blend và Ridge cho đúng cùng dãy dự đoán với Linear sau khi chọn trọng số. Vì vậy, tiêu chí thành công của đồ án là xác định **điều kiện mà mỗi phương pháp có ích**, kể cả khi một phương pháp đơn giản đạt kết quả tốt nhất. [Bài chính, bảng 5](/home/duck/Downloads/main_paper.md:228)

## Hai phép thử bổ sung cho nhau

1. **Tái hiện khoảng thiếu từ nhật ký.** Lấy công tơ, thời điểm bắt đầu, độ dài và mẫu thiếu đồng thời từ nhật ký đủ điều kiện. Tìm một đoạn khác có số đo tham chiếu, ưu tiên cùng mùa, loại ngày và giờ bắt đầu. Che đúng các vị trí ghi nhận rồi khôi phục. Ví dụ, nếu A thiếu 45 phút và B thiếu 15 phút cuối, giữ cả hai mẫu khi chuyển sang đoạn có tham chiếu. Lưu mã đợt nguồn, đoạn nhận, số đợt tìm được đối chứng và lý do loại. Nếu nhiều công tơ được chấm trong một đợt, tổng hợp trong đợt trước khi tính trung bình qua các đợt. [Bài chính, mục 3.2](/home/duck/Downloads/main_paper.md:75)
2. **Chủ động thay đổi lượng dữ liệu hỗ trợ.** Giữ các mức 15 phút, 1 giờ và 6 giờ của T14. Với cùng công tơ, thời điểm và đoạn che, lần lượt che thêm 0%, 50% và 100% số công tơ hỗ trợ đủ điều kiện. Chọn tối đa 10 công tơ hỗ trợ theo tương quan Spearman của phần dư sau khi bỏ mức mùa vụ, với ít nhất 1.000 cặp quan sát hợp lệ trong tập huấn luyện theo bài báo. Ghi lại số kênh thực sự còn quan sát và các ca thiếu hỗ trợ. Thử nghiệm phải cho cùng dự đoán Linear khi chỉ thay đổi dữ liệu ở kênh khác. Quan hệ giữa các công tơ ở đây được xác định bằng thống kê. [Bài chính, mục 3.3–3.4](/home/duck/Downloads/main_paper.md:84) · [Cách chọn kênh hỗ trợ](/home/duck/Downloads/main_paper.md:135)

## Cách chọn mô hình và đọc kết quả

- Dùng lịch sử trước tháng 10/2023 để huấn luyện và chọn cấu hình. Đề xuất chia lịch sử theo thứ tự thời gian 80/20 và chừa 7 ngày giữa hai phần. Chọn trọng số và tham số theo sai số điện năng tuyệt đối đã chuẩn hóa trên phần phát triển. Học hệ số chuẩn hóa, hồ sơ mùa vụ và quan hệ công tơ trên phần huấn luyện. Cả đợt nhật ký và cửa sổ nhận đều phải nằm trong kỳ được phép. Khóa cấu hình, mặt nạ, seed và ngân sách trước khi chấm.
- Quý IV/2023 đã được xem kết quả ở T14, nên ghi rõ đây là kỳ đối chiếu đã biết. Để kiểm tra độ ổn định qua thời gian, bổ sung một lượt đánh giá lùi về năm 2022 với toàn bộ tham số học lại từ lịch sử trước kỳ đó. Chốt thiết kế trước khi đọc điểm của các phương pháp mới.
- Mỗi mô hình nhận cùng dữ liệu đã che; chỉ bộ chấm đọc giá trị tham chiếu của ca đánh giá. Trên cùng mã ca, báo sai số công suất trung bình (W), sai số tuyệt đối của tổng điện năng trong đoạn (kWh), số ca dự đoán được, số lần dùng phương án dự phòng và sai số theo công tơ. Khi so các công tơ lớn nhỏ, bổ sung sai số năng lượng chia cho mức công suất tuyệt đối trung bình lịch sử nhân thời lượng đoạn và đổi sang kWh, dùng ngưỡng sàn 1 W như bài báo. [Bài chính, mục 4.1](/home/duck/Downloads/main_paper.md:108)
- Với từng phương pháp mới, báo mức thay đổi sai số trên cùng ca so với Linear, tách theo độ dài và mức còn dữ liệu hỗ trợ. Ghi số dự đoán trùng với Linear. Các mức che khác nhau của cùng một đoạn được tính là các kịch bản lặp trên đoạn ấy.

## Phần Big Data và lộ trình bàn giao đề xuất

Spark dùng bảng công suất 15 phút để ghép nhật ký, tìm các công tơ có đủ dữ liệu đồng thời, dựng cửa sổ đã che và xuất đặc trưng theo lô. Các mô hình dùng chung những bảng này. Đo riêng thời gian chuẩn bị dữ liệu, huấn luyện, suy luận, bộ nhớ đỉnh và dung lượng trung gian khi tăng số công tơ và số tháng. Sản phẩm cuối cho phép người xem cân nhắc mức giảm sai số kWh cùng với chi phí để đạt được mức giảm đó.

| Bước đề xuất | Đầu ra có thể kiểm tra |
| --- | --- |
| T16 · Dựng hai bộ ca | Một đợt nhật ký truy được đến đoạn nhận, bảng số đợt đủ/thiếu, tập ca kiểm soát và cấu hình đã khóa. |
| T17 · Hoàn thiện nhóm chỉ dùng công tơ mục tiêu | Linear, Seasonal, Blend và Seasonal Kalman chạy trên cùng ca; kiểm tra che dữ liệu ở công tơ khác vẫn giữ nguyên dự đoán. |
| T18 · Thử Ridge và XGBoost | Danh sách công tơ hỗ trợ học từ lịch sử, bảng đầu vào sau che, dự đoán và tỷ lệ dùng dự phòng. Chạy thử trên pilot bốn công tơ trước khi mở rộng. |
| T19 · Đánh giá độ chính xác và chi phí | Bảng ghép cặp sáu phương pháp, sai số theo hai phép thử, lượt đánh giá lùi, thời gian và RAM thực đo. Dựa vào chi phí pilot để xác định mô hình mở rộng khả thi. |
| T20 · Bàn giao | Demo chọn một đợt, xem công tơ hỗ trợ nào còn số đo, đối chiếu hai phương pháp và bật đường tham chiếu. Báo cáo kèm cấu hình, lệnh tái tạo và phạm vi kết luận. |

Nếu phát triển tiếp sang **khoảng ước lượng điện năng**, cần dành riêng dữ liệu hiệu chỉnh theo thời gian và báo cả tỷ lệ bao phủ lẫn độ rộng khoảng. Đề xuất hiện tại ưu tiên hoàn tất so sánh ước lượng điểm và chi phí trước khi mở nhánh này. [Bài chính, mục 4.4](/home/duck/Downloads/main_paper.md:150)
