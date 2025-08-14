FROM mcr.microsoft.com/windows/servercore:ltsc2022

# Install Python
RUN powershell -Command \
    Invoke-WebRequest -Uri https://www.python.org/ftp/python/3.11.0/python-3.11.0-amd64.exe -OutFile python-installer.exe; \
    Start-Process python-installer.exe -ArgumentList '/quiet', 'InstallAllUsers=1', 'PrependPath=1' -Wait; \
    Remove-Item python-installer.exe

WORKDIR C:\app

# Copy and install requirements
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy application
COPY fileserver.py .

EXPOSE 5000

CMD ["python", "fileserver.py"]