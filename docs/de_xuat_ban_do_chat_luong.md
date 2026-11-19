# Đề xuất thực thi · Bản đồ chất lượng dữ liệu công suất Honda

**Mục tiêu.** Khi một công tơ có sáu giờ số đo thiếu hoặc kém tin cậy, chuyên viên năng lượng chọn đợt đó để xem ba điều. Vì sao các ô bị loại? Công tơ nào còn số đo hợp lệ suốt sáu giờ? Nhật ký có ghi nhận sự kiện cùng lúc không? Đồ án sẽ trả lời trên toàn bộ lịch sử và đo chi phí dựng, tra cứu bản đồ bằng Spark local và DuckDB.

**Điểm xuất phát.** T14 đã xử lý 208.746.721 dòng nguồn của 71 công tơ thành 11.906.542 ô 15 phút. Có 4.717.365 ô hợp lệ. Các nhãn chính còn lại gồm 3.148.629 ô có đồng hồ chưa được xác minh, 2.967.476 ô lệch lưới phút, 802.818 ô thuộc khoảng nhật ký có đệm và 270.254 ô thiếu phút. Đây là số ô, mỗi ô nhận một nhãn chính theo thứ tự ưu tiên của T14. Ở mức 16 công tơ × 12 tháng của T13, Spark dựng bảng trong 2,815 giây và DuckDB trong 7,262 giây. Khi lọc 1.344 dòng của một công tơ, hai thời gian tương ứng là 0,164 và 0,012 giây. Phép đo đề xuất bắt đầu từ Parquet và tính các đợt trên nhiều công tơ. [Độ phủ T14](../runs/t14_full/coverage.csv) · [Phép đo T13](../runs/t13_scaling_r3/scaling.json) · [Đặc tả nguồn](../SPEC.md)

## Quy tắc tính bản đồ

| Đại lượng | Cách tính và ý nghĩa |
| --- | --- |
| Phạm vi | Mỗi công tơ có lưới quan sát riêng theo T14. Tại một mốc, mẫu số là số công tơ có ô trong phạm vi ở mốc đó. Bản đồ có trạng thái riêng cho vùng ngoài phạm vi. |
| Đợt chất lượng | Chuỗi dài nhất có thể gồm các ô liên tiếp không hợp lệ của cùng công tơ, cách nhau đúng 15 phút. Đổi lý do giữa chừng vẫn thuộc một đợt. Một ô hợp lệ kết thúc đợt. Khóa đợt gồm công tơ và hai biên UTC dạng [đầu, cuối). Xác định biên trên toàn lịch sử trước khi lọc màn hình theo tháng. |
| Kênh còn quan sát được | Với mỗi đợt, loại chính công tơ đang xét. Báo số kênh khác hợp lệ **suốt đợt** và tỷ lệ ô có **ít nhất một** kênh khác hợp lệ. Chỉ số thứ hai có thể cao dù kênh hỗ trợ thay đổi giữa đợt. Ghi cả tử số và mẫu số. |
| Đối chiếu nhật ký | Dùng các khoảng đúng công tơ/P đã được T11 ánh xạ và đệm sẵn. Với đợt [s,e) và khoảng đã đệm có hai đầu bao hàm [l,r], có giao khi l < e và r ≥ s. Lưu công tơ, hai biên, vị trí khoảng trong danh mục T11 đã khóa. Muốn trích nội dung sự kiện, phải truy được bản ghi gốc theo hash đã đăng ký. Hai công tơ cùng thiếu số đo chỉ xác nhận sự trùng thời gian. |

## Năm bước và điều kiện qua

Q01–Q05 là mã đề xuất, dành riêng cho phần chất lượng dữ liệu. Khi được giao triển khai, ghi các bước vào [sổ công việc](../TASK.md), cập nhật phạm vi trong [kế hoạch](../PLAN.md) và quy tắc đo trong [đặc tả](../SPEC.md). Thực hiện theo thứ tự dưới đây.

### Q01 · Khóa đầu vào và phép đo

- **Làm.** Đọc sources.json của T11, panel/ và coverage.csv của T14. Kiểm trạng thái, danh sách tệp và SHA-256 theo manifest. Từ Parquet, tính lại số ô theo công tơ, tháng UTC và lý do. Kiểm khóa duy nhất, mốc cách nhau đúng 15 phút, biên riêng từng công tơ khớp danh mục, cờ hợp lệ khớp nhãn.
- **Đầu ra.** Cấu hình đã băm SHA-256, danh sách kênh cho ba mức, ngân sách bộ nhớ, đĩa tạm và thời gian mỗi lượt theo tài nguyên đo được trên máy. Cố định máy, phiên bản, số luồng, cách đo và quy tắc chọn đợt truy vấn trước khi xem kết quả.
- **Qua khi** đủ 71 tệp, mọi khóa và biên đạt, từng nhóm tính lại khớp coverage.csv, tổng từng lý do khớp mốc xuất phát. Bốn kênh pilot là H1.Z11, H1.Z12, H1.Z20, H1.Z24. Thêm 12 mã đầu theo thứ tự từ điển từ các kênh còn lại để có mức 16. Mức cuối gồm cả 71. Cả ba mức dùng toàn bộ lịch sử riêng của từng kênh và ghi số ô, số byte thực tế.

### Q02 · Chứng minh cách đếm bằng dữ liệu tính tay

Dùng bốn ô liên tiếp. Công tơ A không hợp lệ cả bốn, đổi lý do sau ô thứ hai. B hợp lệ ở hai ô đầu và không hợp lệ ở hai ô cuối. C ngoài phạm vi ở hai ô đầu và hợp lệ ở hai ô cuối.

| Kết quả bắt buộc | Đáp án tính tay |
| --- | --- |
| Đợt của A, B, C | A có một đợt 60 phút với hai ô cho mỗi lý do. B có một đợt 30 phút. C có 0 đợt. |
| Mẫu số toàn hệ thống ở bốn mốc | 2, 2, 3, 3 công tơ trong phạm vi |
| Kênh hỗ trợ A | Có kênh khác hợp lệ ở 4/4 ô. Có 0 kênh khác hợp lệ suốt đợt. |

**Đầu ra và điều kiện qua.** Lưu dữ liệu mẫu, đáp án và kiểm thử cho cả hai công cụ. Từng khóa, biên và số đếm phải khớp đáp án kể cả khi đảo thứ tự dòng. Thêm ca qua biên tháng, hai đợt tách bởi một ô hợp lệ, mọi công tơ cùng thiếu, hai nhật ký chồng nhau và nhật ký chạm từng đầu đợt. Hai nhật ký giao một đợt tạo hai liên kết nhưng mỗi ô vẫn chỉ đếm một lần. Nhật ký bắt đầu đúng cuối đợt thì không giao; kết thúc đúng đầu đợt thì có giao.

### Q03 · Dựng bản đồ và kiểm toàn bộ lịch sử

- **Làm.** Chạy pilot bốn kênh, đối soát rồi mở rộng đủ 71 bằng ít nhất một công cụ. Xuất các bảng Parquet cho đợt theo công tơ, số kênh theo trạng thái tại từng mốc, liên kết đợt–nhật ký và thống kê hỗ trợ từng đợt. Tính số kênh theo mốc một lần rồi dùng lại. Tra danh sách kênh chi tiết khi mở một đợt để tránh sinh bảng mọi cặp công tơ ở mọi thời điểm.
- **Tự kiểm.** Mỗi ô không hợp lệ thuộc đúng một đợt của công tơ đó. Tổng độ dài các đợt trên 71 kênh phải bằng **7.189.177 ô**. Số ô theo từng lý do phải khớp T14. Tại mỗi mốc, số hợp lệ cộng số không hợp lệ bằng số trong phạm vi. Với mỗi đợt, số ô có hỗ trợ nằm từ 0 đến số ô của đợt.
- **Qua khi** toàn bộ bất biến đạt; phép tính độc lập đọc trực tiếp panel cũng khớp biên, lý do, kênh hỗ trợ ở đợt đầu, cuối và dài nhất của từng công tơ có đợt. Phá hòa bằng thứ tự khóa đợt. Lưu khóa đối chứng, phân bố thời lượng, số đợt có hỗ trợ suốt đợt và mẫu số. Kết quả phân tích mới mang nhãn **[chưa đo]** cho đến khi có run đạt.

### Q04 · Đo chi phí cho cùng một câu hỏi

| Tác vụ | Phép đo | Điều kiện so sánh |
| --- | --- | --- |
| Dựng bảng theo lô | Từ khi bắt đầu đọc cùng bộ Parquet T14 và khoảng nhật ký T11 đến khi ghi xong các bảng Q03 vào thư mục mới. Ghi riêng thời gian khởi động công cụ. | Cùng đầu vào, cột, codec và cách chia tệp đầu ra. Khớp toàn bộ khóa, biên, lý do, tử số và mẫu số sau sắp xếp chuẩn. |
| Tra một đợt | Từ lúc gửi truy vấn đến lúc nhận đủ chi tiết cho cùng mã đợt trên các bảng đã dựng. Mỗi mức lấy đợt ngắn nhất, dài nhất và gần trung vị thời lượng nhất, phá hòa bằng khóa đợt và bỏ khóa trùng. | Cùng danh sách kênh, số ô hợp lệ và liên kết nhật ký. Báo thời gian từng truy vấn. |

Chạy riêng từng công cụ trên cùng máy với hai luồng ở ba mức 4, 16, 71 kênh. Mỗi phép đo có một lượt làm nóng và ba lượt tính trung vị. Luân phiên thứ tự công cụ. Lưu số đo từng lượt, số dòng và byte đầu vào, thời gian thực, RAM đỉnh của cây tiến trình, dung lượng tạm đỉnh, cách lấy mẫu và điều kiện cache. Ghi rõ RSS cộng các tiến trình có thể đếm trùng trang nhớ dùng chung. Kiểm tra tính đúng ngoài khoảng bấm giờ.

**Qua khi** mỗi mức có bảng đối soát và số đo, hoặc có bằng chứng chạm giới hạn ngân sách đã khóa. Với trường hợp chạm giới hạn, ghi điểm dừng, log và xác nhận bằng lượt độc lập cùng giới hạn nếu đủ tài nguyên để chạy an toàn; nếu chưa xác nhận được, giữ trạng thái chưa đạt ở phép đo đó. Pilot phải chạy đủ cả hai công cụ. Ít nhất một công cụ phải dựng được bản đồ đủ 71 kênh. Tính tỷ số tốc độ chỉ trên các lượt hoàn thành cùng công việc. Chọn công cụ riêng cho dựng bảng và tra cứu theo thời gian và tài nguyên thực đo. Chỉ đề xuất thử cụm máy khi chỉ ra được giới hạn cụ thể trên laptop.

### Q05 · Bàn giao, chạy lại và nghiệm thu

- **Làm.** Đóng gói bản đồ theo tháng, màn hình chi tiết, báo cáo và hướng dẫn tái tạo. Trong demo, chọn tháng → công tơ → đợt → xem kênh còn hợp lệ và nhật ký. Dùng nhãn riêng cho thiếu phút, các lý do loại khác và ngoài phạm vi. Mỗi kết quả chỉ về được công tơ, thời gian và run nguồn.
- **Tự kiểm.** Mở gói ở thư mục mới, thử một đợt Honda có thật và một lựa chọn không có đợt. Đối chiếu các giá trị hiển thị với bảng Q03. Dựng lại bản đồ đủ 71 kênh từ đầu vào đã khóa ở thư mục run mới bằng công cụ đã hoàn thành. So hash nội dung sau chuẩn hóa thứ tự và kiểu dữ liệu.
- **Hoàn thành khi** Q01–Q04 có bằng chứng đạt, demo mở được, lượt tái tạo khớp và hướng dẫn chứa các lệnh thực sự đã chạy thành công. Báo cáo trả lời bằng số đo hai câu hỏi: bao nhiêu đợt còn ít nhất một kênh khác hợp lệ suốt đợt, và nên dùng công cụ nào cho từng tác vụ trên máy đã thử. Kết quả thứ nhất giúp chọn các đợt khả thi cho thí nghiệm mô hình đa công tơ sau này.

## Vòng tự kiểm và chạy lại

1. Chọn bước đầu tiên đủ phụ thuộc và ghi IN_PROGRESS trong TASK.md. Mỗi lần thử tạo một thư mục runs/<run_id>/ mới. Tái dùng Run và completed_input để lưu lệnh, thời gian, hash mã và đầu vào. Ghi thêm hash cấu hình, phạm vi, số hàng, tài nguyên và prior_run khi thử lại. [Cơ chế run hiện có](../src/honda/run.py)
2. Mỗi bước xuất checks.json, mỗi phép kiểm có tên, giá trị mong đợi, giá trị thực, trạng thái và đường dẫn bằng chứng. Lệnh kiểm trả mã lỗi nếu bất kỳ điều kiện bắt buộc nào trượt. Trạng thái FAILED của lượt đo chạm giới hạn ở Q04 phải được giữ kể cả khi bước Q04 đạt theo quy tắc trên.
3. Khi sai kết quả, lưu khóa sai đầu tiên, thu nhỏ thành fixture, sửa nguyên nhân rồi chạy lại từ phép kiểm nhỏ nhất. Sau khi fixture qua, chạy lại bước ở đúng quy mô và kiểm lại các bước phụ thuộc với đầu ra mới. Giữ lượt cũ. Nếu đổi phương pháp sau khi xem kết quả, ghi ảnh hưởng, khóa phiên bản mới và chạy lại cả hai công cụ cùng điều kiện.
4. Với lỗi môi trường hoặc thiếu bộ nhớ có thể khắc phục trong ngân sách, tự sửa rồi thử lại. Nếu hash đã đăng ký bị lệch, cần sửa nguồn hoặc cần tài nguyên ngoài phạm vi, ghi BLOCKED cho phần phụ thuộc, kèm bằng chứng và điều kiện mở lại. Tiếp tục fixture hoặc phần độc lập. Chỉ thử lại khi đã sửa nguyên nhân hoặc điều kiện đầu vào đã thay đổi.
5. Chỉ ghi DONE khi đầu ra của đúng bước đạt và có đường dẫn run. Khi được giao cả giai đoạn, tự chuyển sang bước kế đến khi Q05 đạt hoặc gặp điều kiện chặn cụ thể. Khi được giao một bước, dừng ở đầu ra của bước đó.

## Lệnh kiểm đầu vào dùng được ngay

Từ thư mục gốc, lệnh chỉ đọc dưới đây xác nhận manifest và SHA-256 của danh mục nguồn, bảng độ phủ và toàn bộ panel. Kết quả mong đợi là 71 công tơ và 11.906.542 ô theo bảng độ phủ. Q01 còn phải đối soát trực tiếp các khóa và nội dung từng Parquet.

```bash
PYTHONPATH=src .venv/bin/python - <<'PY'
import csv
import json
from types import SimpleNamespace
from honda.run import completed_input

check = SimpleNamespace(info={})
inventory = completed_input(check, "runs/t11_inventory", "T11", ["sources.json"])
full = completed_input(check, "runs/t14_full", "T14", ["panel", "coverage.csv"])
channels = json.loads((inventory / "sources.json").read_text())["channels"]
meters = {p.stem for p in (full / "panel").glob("*.parquet")}
with (full / "coverage.csv").open() as stream:
    bins = sum(int(row["bins"]) for row in csv.DictReader(stream))
assert meters == set(channels) and (len(meters), bins) == (71, 11_906_542)
print(f"Đã kiểm {len(meters)} công tơ và {bins:,} ô theo bảng độ phủ.")
PY
```
