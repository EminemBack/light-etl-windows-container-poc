import os
import json
import logging
from datetime import datetime
from io import BytesIO
from pathlib import Path
from flask import Flask, send_file, jsonify, request, make_response
import pandas as pd
from waitress import serve
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('C:/app/logs/fileserver.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
SHARED_PATH = os.environ.get('SHARED_PATH', 'C:\\shared_data')
BACKUP_PATH = os.environ.get('BACKUP_PATH', 'C:\\local_data')
MAX_FILE_SIZE = int(os.environ.get('MAX_FILE_SIZE', 100 * 1024 * 1024))  # 100MB default

def get_available_path():
    """Get the first available path for file operations"""
    for path in [SHARED_PATH, BACKUP_PATH]:
        if os.path.exists(path):
            return path
    # Create backup path if neither exists
    os.makedirs(BACKUP_PATH, exist_ok=True)
    return BACKUP_PATH

def is_allowed_file(filename):
    """Check if file type is allowed"""
    allowed_extensions = {'.xlsx', '.xls', '.xlsm', '.xlsb', '.csv'}
    return Path(filename).suffix.lower() in allowed_extensions

@app.route('/health')
def health():
    """Comprehensive health check endpoint"""
    try:
        current_path = get_available_path()
        file_count = len([f for f in os.listdir(current_path) 
                         if is_allowed_file(f)])
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "shared_path": SHARED_PATH,
            "active_path": current_path,
            "shared_path_exists": os.path.exists(SHARED_PATH),
            "backup_path_exists": os.path.exists(BACKUP_PATH),
            "file_count": file_count,
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "available_memory": get_memory_info()
        })
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

def get_memory_info():
    """Get basic memory information (Windows specific)"""
    try:
        import psutil
        memory = psutil.virtual_memory()
        return {
            "total_gb": round(memory.total / (1024**3), 2),
            "available_gb": round(memory.available / (1024**3), 2),
            "percent_used": memory.percent
        }
    except ImportError:
        return "psutil not available"

@app.route('/list')
def list_files():
    """List all supported files in the directory"""
    try:
        current_path = get_available_path()
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
        
        # Sort by modification time (newest first)
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

@app.route('/read/<filename>')
def read_excel(filename):
    """Read Excel file and return as JSON with error handling"""
    try:
        if not is_allowed_file(filename):
            return jsonify({"error": f"File type not supported: {Path(filename).suffix}"}), 400
            
        current_path = get_available_path()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found in {current_path}"}), 404
        
        # Check file size
        file_size = os.path.getsize(filepath)
        if file_size > MAX_FILE_SIZE:
            return jsonify({
                "error": f"File too large: {file_size / (1024*1024):.1f}MB > {MAX_FILE_SIZE / (1024*1024):.1f}MB"
            }), 413
        
        # Read file based on type
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            df = pd.read_excel(filepath)
            
        logger.info(f"Read file {filename}: {df.shape} rows/cols")
        
        return jsonify({
            "data": df.to_dict(orient='records'),
            "columns": df.columns.tolist(),
            "shape": list(df.shape),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "file_size_mb": round(file_size / (1024 * 1024), 2)
        })
        
    except pd.errors.EmptyDataError:
        return jsonify({"error": "File is empty or contains no data"}), 400
    except pd.errors.ParserError as e:
        return jsonify({"error": f"Failed to parse file: {str(e)}"}), 400
    except Exception as e:
        logger.error(f"Error reading {filename}: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/download/<filename>')
def download_file(filename):
    """Download raw file with validation"""
    try:
        if not is_allowed_file(filename):
            return jsonify({"error": f"File type not supported: {Path(filename).suffix}"}), 400
            
        current_path = get_available_path()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        file_size = os.path.getsize(filepath)
        if file_size > MAX_FILE_SIZE:
            return jsonify({
                "error": f"File too large for download: {file_size / (1024*1024):.1f}MB"
            }), 413
            
        logger.info(f"Downloading file {filename} ({file_size} bytes)")
        return send_file(filepath, as_attachment=True, download_name=filename)
        
    except Exception as e:
        logger.error(f"Error downloading {filename}: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/read_with_params', methods=['POST'])
def read_excel_with_params():
    """Read Excel with specific parameters and enhanced options"""
    try:
        data = request.json or {}
        filename = data.get('filename')
        sheet_name = data.get('sheet_name', 0)
        nrows = data.get('nrows')
        skiprows = data.get('skiprows', 0)
        usecols = data.get('usecols')
        
        if not filename:
            return jsonify({"error": "filename is required"}), 400
            
        if not is_allowed_file(filename):
            return jsonify({"error": f"File type not supported: {Path(filename).suffix}"}), 400
            
        current_path = get_available_path()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        # Read based on file type
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(
                filepath, 
                nrows=nrows, 
                skiprows=skiprows,
                usecols=usecols
            )
        else:
            df = pd.read_excel(
                filepath, 
                sheet_name=sheet_name, 
                nrows=nrows,
                skiprows=skiprows,
                usecols=usecols
            )
        
        logger.info(f"Read {filename} with params: sheet={sheet_name}, nrows={nrows}, skiprows={skiprows}")
        
        return jsonify({
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient='records'),
            "shape": list(df.shape),
            "sheet_name": sheet_name,
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "memory_usage_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2)
        })
        
    except Exception as e:
        logger.error(f"Error reading with params: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/sheets/<filename>')
def get_sheet_names(filename):
    """Get all sheet names from an Excel file"""
    try:
        if not is_allowed_file(filename):
            return jsonify({"error": f"File type not supported: {Path(filename).suffix}"}), 400
            
        current_path = get_available_path()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        if filename.lower().endswith('.csv'):
            return jsonify({"sheets": ["Sheet1"], "note": "CSV files have only one sheet"})
            
        xl_file = pd.ExcelFile(filepath)
        sheet_names = xl_file.sheet_names
        xl_file.close()
        
        logger.info(f"Found {len(sheet_names)} sheets in {filename}")
        return jsonify({"sheets": sheet_names, "count": len(sheet_names)})
        
    except Exception as e:
        logger.error(f"Error getting sheets from {filename}: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/stats')
def get_stats():
    """Get directory and system statistics"""
    try:
        current_path = get_available_path()
        files = [f for f in os.listdir(current_path) if is_allowed_file(f)]
        
        stats = {
            "total_files": len(files),
            "file_types": {},
            "total_size_mb": 0,
            "largest_file": None,
            "newest_file": None
        }
        
        largest_size = 0
        newest_time = 0
        
        for f in files:
            filepath = os.path.join(current_path, f)
            try:
                stat_info = os.stat(filepath)
                ext = Path(f).suffix.lower()
                
                # File type count
                stats["file_types"][ext] = stats["file_types"].get(ext, 0) + 1
                
                # Total size
                size_mb = stat_info.st_size / (1024 * 1024)
                stats["total_size_mb"] += size_mb
                
                # Largest file
                if stat_info.st_size > largest_size:
                    largest_size = stat_info.st_size
                    stats["largest_file"] = {"name": f, "size_mb": round(size_mb, 2)}
                
                # Newest file
                if stat_info.st_mtime > newest_time:
                    newest_time = stat_info.st_mtime
                    stats["newest_file"] = {
                        "name": f, 
                        "modified": datetime.fromtimestamp(stat_info.st_mtime).isoformat()
                    }
                    
            except Exception as e:
                logger.warning(f"Could not stat file {f}: {e}")
        
        stats["total_size_mb"] = round(stats["total_size_mb"], 2)
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    import sys
    
    # Ensure log directory exists
    os.makedirs('C:/app/logs', exist_ok=True)
    
    logger.info("="*50)
    logger.info("Starting Windows File Server")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Shared path: {SHARED_PATH}")
    logger.info(f"Backup path: {BACKUP_PATH}")
    logger.info(f"Max file size: {MAX_FILE_SIZE / (1024*1024):.1f}MB")
    logger.info("="*50)
    
    # Check paths
    available_path = get_available_path()
    logger.info(f"Using path: {available_path}")
    
    # Use waitress for production-ready server
    serve(app, host='0.0.0.0', port=5000, threads=4)