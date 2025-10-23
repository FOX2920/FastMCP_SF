# Salesforce Custom MCP for ChatGPT

Đây là một MCP (Model Context Protocol) server tùy chỉnh cho Salesforce, được thiết kế để tích hợp với ChatGPT thông qua các công cụ đặc biệt. Server này cung cấp một bộ công cụ CRUD (Create, Read, Update, Delete) đầy đủ, cũng như khả năng truy vấn SOQL, SOSL và mô tả schema.

## Tính năng chính (Tools)

- **greet(name: str)**: Gửi lời chào thân thiện đến người dùng.
- **search(query: str)**: Tìm kiếm tài liệu MCP nội bộ (demo DB).
- **fetch(id: str)**: Lấy một tài liệu MCP cụ thể bằng ID (demo DB).
- **run_soql_query(query: str)**: Thực thi một truy vấn SOQL trên Salesforce.
- **run_sosl_search(search_string: str)**: Thực thi một tìm kiếm SOSL trên Salesforce.
- **describe_object(object_name: str)**: Lấy thông tin schema chi tiết (tên, nhãn, các trường) của một đối tượng Salesforce.
- **create_record(object_name: str, data: Dict)**: Tạo một bản ghi mới cho một đối tượng.
- **update_record(object_name: str, record_id: str, data: Dict)**: Cập nhật một bản ghi hiện có bằng ID.
- **delete_record(object_name: str, record_id: str)**: Xóa một bản ghi bằng ID.

## Công nghệ sử dụng

- **FastMCP**: Framework để xây dựng MCP servers.
- **Simple Salesforce**: Thư viện Python để kết nối với Salesforce API.
- **Ngrok**: Để expose server local ra internet.

## Cài đặt và chạy

### Điều kiện tiên quyết

1.  **Python 3.8+**
2.  **Ngrok account và auth token**
3.  **Salesforce credentials** (username, password, security token)

### Các bước cài đặt

1.  **Clone repository và cài đặt dependencies**:
    ```bash
    git clone [YOUR_REPO_URL]
    cd [YOUR_REPO_NAME]
    pip install fastmcp simple-salesforce python-dotenv
    ```

2.  **Cấu hình biến môi trường**:
    Tạo file `.env` và điền credentials của bạn:

    ```env
    SF_USERNAME=your_salesforce_username
    SF_PASSWORD=your_salesforce_password
    SF_SECURITY_TOKEN=your_salesforce_security_token
    ```

3.  **Chạy server**:
    Lưu code Python (đã refactor) thành `server.py` và chạy:
    ```bash
    python server.py
    ```
    *Server sẽ chạy tại `http://localhost:8000`*

4.  **Mở terminal mới và expose server**:
    ```bash
    ngrok http 8000
    ```

5.  **Copy ngrok URL**:
    Copy URL `https://....ngrok.io` được cung cấp bởi ngrok.

6.  **Cấu hình ChatGPT Connector**:
    - Vào **Settings** -> **Connectors** trong ChatGPT.
    - Nhấn **"Create"** (Tạo mới).
    - **Name**: "Salesforce MCP"
    - **URL**: Dán URL `ngrok` của bạn vào.
    - Lưu lại và bắt đầu sử dụng.

## Bảo mật

- **Không bao giờ** hard-code credentials trong file `.py`.
- Luôn sử dụng biến môi trường và file `.env`.
- Thêm file `.env` vào `.gitignore` để tránh đưa credentials lên Git.
