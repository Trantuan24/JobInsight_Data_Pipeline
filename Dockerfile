FROM python:3.10-slim

WORKDIR /app

# Cài đặt các gói phụ thuộc hệ thống
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cài đặt Playwright và browsers
RUN playwright install chromium && \
    playwright install-deps

# Tạo thư mục cần thiết
RUN mkdir -p /app/logs /app/data /app/dags /app/sql

# Copy toàn bộ mã nguồn vào container
COPY . .

# Cấu hình biến môi trường
ENV PYTHONPATH="/app:${PYTHONPATH}"
ENV AIRFLOW_HOME="/app"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Thêm quyền thực thi cho các file và fix permissions
RUN chmod -R 777 /app

# Port cho Airflow webserver
EXPOSE 8080

# Lệnh mặc định
CMD ["airflow", "webserver"]
