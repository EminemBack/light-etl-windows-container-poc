"""
Simple File Server - Run directly on Windows host
No Docker needed for this component
"""

import os
import json
from datetime import datetime
from flask import Flask, send_file, jsonify, request
import pandas as pd
from waitress import serve
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# CHANGE THIS to your actual Z:\ drive path or test folder
SHARED_PATH = r"Z:\\"  # or use r"C:\test_data" for testing

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "shared_path": SHARED_PATH,
        "path_exists": os.path.exists(SHARED_PATH),
        "host": "windows_host"
    })

@app.route('/list')
def list_files():
    """List all Excel files in the shared directory"""
    try:
        if not os.path.exists(SHARED_PATH):
            return jsonify({"error": f"Path {SHARED_PATH} does not exist"}), 404
            
        files = []
        for f in os.listdir(SHARED_PATH):
            if f.endswith(('.xlsx', '.xls', '.xlsm')):
                filepath = os.path.join(SHARED_PATH, f)
                try:
                    files.append({
                        "name": f,
                        "size": os.path.getsize(filepath),
                        "modified": datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat()
                    })
                except:
                    continue  # Skip files we can't access
        
        logger.info(f"Listed {len(files)} files")
        return jsonify({"files": files, "count": len(files)})
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/download/<filename>')
def download_file(filename):
    """Download raw Excel file"""
    try:
        filepath = os.path.join(SHARED_PATH, filename)
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
            
        logger.info(f"Downloading file {filename}")
        return send_file(filepath, as_attachment=True, download_name=filename)
    except Exception as e:
        logger.error(f"Error downloading {filename}: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/sheets/<filename>')
def get_sheet_names(filename):
    """Get all sheet names from an Excel file"""
    try:
        filepath = os.path.join(SHARED_PATH, filename)
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
            
        xl_file = pd.ExcelFile(filepath)
        sheet_names = xl_file.sheet_names
        
        logger.info(f"Found {len(sheet_names)} sheets in {filename}")
        return jsonify({"sheets": sheet_names})
    except Exception as e:
        logger.error(f"Error getting sheets from {filename}: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("="*60)
    print("Starting Simple File Server")
    print("="*60)
    print(f"Shared path: {SHARED_PATH}")
    print(f"Path exists: {os.path.exists(SHARED_PATH)}")
    
    if not os.path.exists(SHARED_PATH):
        print(f"\nWARNING: Path {SHARED_PATH} does not exist!")
        print("Please update SHARED_PATH in the script to point to your data folder")
    
    print("\nServer will be available at:")
    print("  http://localhost:5000")
    print("  http://<your-windows-ip>:5000")
    print("\nPress Ctrl+C to stop")
    print("="*60)
    
    # Use waitress for production-ready server
    serve(app, host='0.0.0.0', port=5000)