FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including gnupg and ODBC
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    ca-certificates \
    gnupg \
    lsb-release \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    && rm -rf /var/lib/apt/lists/*

# Copy corporate certificates and install them during build
COPY certs/*.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates && \
    echo "Corporate certificates installed during build"

# Install Microsoft ODBC Driver 18 during build
RUN curl -s https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/11/prod bullseye main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm -rf /var/lib/apt/lists/* && \
    echo "ODBC Driver 18 installed during build"

# Configure FreeTDS/SQL Server driver as fallback
RUN printf '\n[SQL Server]\nDescription = SQL Server via FreeTDS\nDriver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nSetup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so\nFileUsage = 1\nUsageCount = 1\n\n[FreeTDS]\nDescription = FreeTDS Driver\nDriver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\nSetup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so\nFileUsage = 1\n' >> /etc/odbcinst.ini

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org \
    --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/temp /app/archive /app/logs

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Verify installation during build
RUN echo "Build verification:" && \
    echo "Available ODBC drivers:" && \
    python3 -c "import pyodbc; print(pyodbc.drivers())" && \
    echo "SSL test:" && \
    curl -s -I https://packages.microsoft.com/keys/microsoft.asc | head -2

# Default command for Celery worker
CMD ["celery", "-A", "etl_processor.celery_app", "worker", "--loglevel=info", "--concurrency=2"]