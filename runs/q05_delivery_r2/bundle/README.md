# Demo Honda Quality Map

Từ thư mục giải nén, dùng Python 3.12 để tạo môi trường rồi mở bản đồ.

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python verify_demo.py
.venv/bin/python -m streamlit run app.py --server.address 127.0.0.1
```

Chọn tháng UTC, công tơ và một đợt giao với tháng đó. Mỗi hàng bản đồ là một ngày.
Đặt con trỏ lên ô để xem số kênh hợp lệ và mẫu số trong phạm vi.
Chi tiết giữ nguyên hai biên UTC của đợt, kể cả khi đợt qua tháng.

Số liệu và điều kiện thử nghiệm ở REPORT.md. Gói chứa phép chiếu trạng thái và các bảng
đã kiểm của Q03. bundle_manifest.json lưu mã run và SHA-256.
