"""
Production Windows Service File Server
Run this as a Windows Service for production deployment
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from flask import Flask, send_file, jsonify, request
import pandas as pd
from waitress import serve
from dotenv import load_dotenv

# For Windows Service
try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
    import threading
    WINDOWS_SERVICE_AVAILABLE = True
except ImportError:
    WINDOWS_SERVICE_AVAILABLE = False
    print("Windows service modules not available. Running as regular application.")

load_dotenv()

# Configuration
SHARED_PATH = os.environ.get('SHARED_PATH', r'Z:\\')
MAX_FILE_SIZE = int(os.environ.get('MAX_FILE_SIZE', 100 * 1024 * 1024))  # 100MB
LOG_PATH = os.environ.get('LOG_PATH', r'.\logs\fileserver')
PORT = int(os.environ.get('PORT', 5000))

# Ensure log directory exists
os.makedirs(LOG_PATH, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_PATH, 'fileserver.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def is_allowed_file(filename):
    """Check if file type is allowed"""
    allowed_extensions = {'.xlsx', '.xls', '.xlsm', '.xlsb', '.csv'}
    return Path(filename).suffix.lower() in allowed_extensions

def get_available_paths():
    """Get all available paths for file operations"""
    paths = [SHARED_PATH]
    
    # Add fallback paths
    fallback_paths = [
        r'C:\shared_data',
        r'.\shared_data',
        os.path.join(os.getcwd(), 'shared_data')
    ]
    
    for path in fallback_paths:
        if path not in paths:
            paths.append(path)
    
    # Return first existing path
    for path in paths:
        if os.path.exists(path):
            return path
    
    # Create first fallback if none exist
    os.makedirs(fallback_paths[0], exist_ok=True)
    return fallback_paths[0]

@app.route('/health')
def health():
    """Comprehensive health check"""
    try:
        current_path = get_available_paths()
        file_count = len([f for f in os.listdir(current_path) if is_allowed_file(f)])
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service_type": "windows_service" if WINDOWS_SERVICE_AVAILABLE else "standalone",
            "shared_path": SHARED_PATH,
            "active_path": current_path,
            "shared_path_exists": os.path.exists(SHARED_PATH),
            "file_count": file_count,
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "pid": os.getpid()
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            "status": "unhealthy", 
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/list')
def list_files():
    """List all supported files"""
    try:
        current_path = get_available_paths()
        logger.info(f"Listing files from: {current_path}")
        
        files = []
        for f in os.listdir(current_path):
            if is_allowed_file(f):
                filepath = os.path.join(current_path, f)
                try:
                    stat_info = os.stat(filepath)
                    files.append({
                        "name": f,
                        "size": stat_info.st_size,
                        "size_mb": round(stat_info.st_size / (1024 * 1024), 2),
                        "modified": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                        "type": Path(f).suffix.lower(),
                        "accessible": True
                    })
                except Exception as e:
                    logger.warning(f"Could not stat file {f}: {e}")
                    files.append({
                        "name": f,
                        "error": str(e),
                        "accessible": False
                    })
        
        files.sort(key=lambda x: x.get('modified', ''), reverse=True)
        
        logger.info(f"Listed {len(files)} files")
        return jsonify({
            "files": files,
            "count": len(files),
            "path": current_path,
            "total_size_mb": round(sum(f.get('size', 0) for f in files) / (1024 * 1024), 2)
        })
        
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/download/<filename>')
def download_file(filename):
    """Download file with validation"""
    try:
        if not is_allowed_file(filename):
            return jsonify({"error": f"File type not supported: {Path(filename).suffix}"}), 400
            
        current_path = get_available_paths()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        file_size = os.path.getsize(filepath)
        if file_size > MAX_FILE_SIZE:
            return jsonify({
                "error": f"File too large: {file_size / (1024*1024):.1f}MB"
            }), 413
            
        logger.info(f"Downloading file {filename} ({file_size} bytes)")
        return send_file(filepath, as_attachment=True, download_name=filename)
        
    except Exception as e:
        logger.error(f"Error downloading {filename}: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/read_with_params', methods=['POST'])
def read_excel_with_params():
    """Read Excel/CSV with parameters"""
    try:
        data = request.json or {}
        filename = data.get('filename')
        sheet_name = data.get('sheet_name', 0)
        nrows = data.get('nrows')
        
        if not filename:
            return jsonify({"error": "filename is required"}), 400
            
        if not is_allowed_file(filename):
            return jsonify({"error": f"File type not supported"}), 400
            
        current_path = get_available_paths()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        # Read based on file type
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(filepath, nrows=nrows)
        else:
            df = pd.read_excel(filepath, sheet_name=sheet_name, nrows=nrows)
        
        logger.info(f"Read {filename}: {df.shape} rows/cols")
        
        return jsonify({
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient='records'),
            "shape": list(df.shape),
            "sheet_name": sheet_name,
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
        })
        
    except Exception as e:
        logger.error(f"Error reading with params: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/sheets/<filename>')
def get_sheet_names(filename):
    """Get sheet names from Excel file"""
    try:
        if not is_allowed_file(filename):
            return jsonify({"error": "File type not supported"}), 400
            
        current_path = get_available_paths()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        if filename.lower().endswith('.csv'):
            return jsonify({"sheets": ["Sheet1"], "note": "CSV files have only one sheet"})
            
        xl_file = pd.ExcelFile(filepath)
        sheet_names = xl_file.sheet_names
        xl_file.close()
        
        return jsonify({"sheets": sheet_names, "count": len(sheet_names)})
        
    except Exception as e:
        logger.error(f"Error getting sheets: {str(e)}")
        return jsonify({"error": str(e)}), 500

class FileServerService(win32serviceutil.ServiceFramework):
    """Windows Service wrapper for the file server"""
    _svc_name_ = "LightETLFileServer"
    _svc_display_name_ = "Light ETL File Server"
    _svc_description_ = "File server for Light ETL Windows container POC"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.server_thread = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        logger.info("Service stop requested")

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        
        logger.info("Starting Light ETL File Server as Windows Service")
        
        # Start server in a separate thread
        self.server_thread = threading.Thread(target=self.run_server)
        self.server_thread.daemon = True
        self.server_thread.start()
        
        # Wait for stop signal
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
        logger.info("Service stopped")

    def run_server(self):
        """Run the Flask server"""
        try:
            logger.info(f"Starting file server on port {PORT}")
            logger.info(f"Shared path: {SHARED_PATH}")
            serve(app, host='0.0.0.0', port=PORT, threads=4)
        except Exception as e:
            logger.error(f"Server error: {str(e)}")

def run_standalone():
    """Run as standalone application"""
    logger.info("="*60)
    logger.info("Light ETL File Server - Standalone Mode")
    logger.info(f"Shared path: {SHARED_PATH}")
    logger.info(f"Active path: {get_available_paths()}")
    logger.info(f"Server will be available at: http://localhost:{PORT}")
    logger.info("="*60)
    
    try:
        serve(app, host='0.0.0.0', port=PORT, threads=4)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")

if __name__ == '__main__':
    if len(sys.argv) == 1:
        # Run as standalone application
        run_standalone()
    elif WINDOWS_SERVICE_AVAILABLE:
        # Handle Windows service commands
        win32serviceutil.HandleCommandLine(FileServerService)
    else:
        print("Windows service modules not available")
        run_standalone()