# Alternative approach - use pure Windows Server Core and install Python
FROM mcr.microsoft.com/windows/servercore:ltsc2022

# Set working directory
WORKDIR C:/app

# Install Python using PowerShell (this should work in Server Core)
RUN powershell -Command \
    "Set-ExecutionPolicy Bypass -Scope Process -Force; \
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; \
    Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.11.7/python-3.11.7-amd64.exe' -OutFile 'python-installer.exe'; \
    Start-Process python-installer.exe -ArgumentList '/quiet', 'InstallAllUsers=1', 'PrependPath=1' -Wait; \
    Remove-Item python-installer.exe"

# Verify Python installation and upgrade pip
RUN python --version && python -m pip install --upgrade pip

# Copy and install requirements
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY fileserver.py .

# Create directories using Python
RUN python -c "import os; os.makedirs('logs', exist_ok=True); os.makedirs('temp', exist_ok=True)"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SHARED_PATH=C:/shared_data
ENV FLASK_ENV=production

# Expose port
EXPOSE 5000

# Health check using Python
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health', timeout=5)"

# Run the application
CMD ["python", "fileserver.py"]