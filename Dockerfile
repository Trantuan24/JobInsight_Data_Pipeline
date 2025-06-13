FROM python:3.10-slim

WORKDIR /opt/airflow

# Cài đặt các gói phụ thuộc hệ thống
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    curl \
    gcc \
    g++ \
    libpq-dev \
    libgconf-2-4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libasound2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libnss3 \
    libnspr4 \
    libgbm1 \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Cài đặt Playwright và browsers
RUN playwright install --with-deps chromium

# Tạo thư mục cần thiết
RUN mkdir -p /opt/airflow/logs /opt/airflow/data /opt/airflow/dags /opt/airflow/sql

# Copy toàn bộ mã nguồn vào container
COPY . .

# Cấu hình biến môi trường
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
ENV AIRFLOW_HOME="/opt/airflow"
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Thêm quyền thực thi cho các file và fix permissions
RUN chmod -R 777 /opt/airflow && \
    touch /opt/airflow/logs/.keep && \
    chmod -R 777 /opt/airflow/logs

# Port cho Airflow webserver
EXPOSE 8080

# Lệnh mặc định
CMD ["airflow", "webserver"]
