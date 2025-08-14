import os
import json
from datetime import datetime
from io import BytesIO
from flask import Flask, send_file, jsonify, request, make_response
import pandas as pd
from waitress import serve
from dotenv import load_dotenv
import logging

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Path to your mounted Z: drive
SHARED_PATH = os.environ.get('SHARED_PATH', 'C:\\shared_data')

@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "shared_path": SHARED_PATH,
        "path_exists": os.path.exists(SHARED_PATH)
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
                files.append({
                    "name": f,
                    "size": os.path.getsize(filepath),
                    "modified": datetime.fromtimestamp(os.path.getmtime(filepath)).isoformat()
                })
        
        logger.info(f"Listed {len(files)} files")
        return jsonify({"files": files, "count": len(files)})
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/read/<filename>')
def read_excel(filename):
    """Read Excel file and return as JSON"""
    try:
        filepath = os.path.join(SHARED_PATH, filename)
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
            
        df = pd.read_excel(filepath)
        logger.info(f"Read file {filename}: {df.shape} rows/cols")
        
        return jsonify({
            "data": df.to_dict(orient='records'),
            "columns": df.columns.tolist(),
            "shape": list(df.shape)
        })
    except Exception as e:
        logger.error(f"Error reading {filename}: {str(e)}")
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

@app.route('/read_with_params', methods=['POST'])
def read_excel_with_params():
    """Read Excel with specific parameters"""
    try:
        data = request.json
        filename = data.get('filename')
        sheet_name = data.get('sheet_name', 0)
        nrows = data.get('nrows', None)
        
        if not filename:
            return jsonify({"error": "filename is required"}), 400
            
        filepath = os.path.join(SHARED_PATH, filename)
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
        
        df = pd.read_excel(filepath, sheet_name=sheet_name, nrows=nrows)
        
        logger.info(f"Read {filename} with params: sheet={sheet_name}, nrows={nrows}")
        
        return jsonify({
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient='records'),
            "shape": list(df.shape),
            "sheet_name": sheet_name
        })
    except Exception as e:
        logger.error(f"Error reading with params: {str(e)}")
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
    logger.info(f"Starting file server on port 5000")
    logger.info(f"Shared path: {SHARED_PATH}")
    
    # Use waitress for production-ready server
    serve(app, host='0.0.0.0', port=5000)