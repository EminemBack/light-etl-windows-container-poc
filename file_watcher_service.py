"""
Fixed File Watcher Service - Python 3.13 Compatible
Automatically triggers ETL tasks when new files are added to shared directories
"""

import os
import sys
import json
import time
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from flask import Flask, send_file, jsonify, request
import pandas as pd
from waitress import serve
from dotenv import load_dotenv

# Alternative file watching approach for Python 3.13 compatibility
import glob
from concurrent.futures import ThreadPoolExecutor

# Celery client for triggering tasks
import requests

# Windows Service support
try:
    import win32serviceutil
    import win32service
    import win32event
    import servicemanager
    WINDOWS_SERVICE_AVAILABLE = True
except ImportError:
    WINDOWS_SERVICE_AVAILABLE = False

load_dotenv()

# Configuration
SHARED_PATH = os.environ.get('SHARED_PATH', r'Z:\\')
WATCH_PATHS = os.environ.get('WATCH_PATHS', SHARED_PATH).split(';')
MAX_FILE_SIZE = int(os.environ.get('MAX_FILE_SIZE', 100 * 1024 * 1024))
LOG_PATH = os.environ.get('LOG_PATH', r'.\logs\fileserver_watcher')
PORT = int(os.environ.get('PORT', 5000))

# ETL Configuration
CELERY_BROKER_URL_WATCHER = os.environ.get('CELERY_BROKER_URL_WATCHER', 'redis://localhost:6379/0')
ETL_TRIGGER_ENABLED = os.environ.get('ETL_TRIGGER_ENABLED', 'true').lower() == 'true'
ETL_PROCESS_DELAY = int(os.environ.get('ETL_PROCESS_DELAY', 30))
SUPPORTED_EXTENSIONS = {'.xlsx', '.xls', '.xlsm', '.xlsb', '.csv'}
POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', 5))  # Poll every 5 seconds

# Setup logging
os.makedirs(LOG_PATH, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_PATH, 'fileserver_watcher.log')),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Celery client setup
celery_available = False
if ETL_TRIGGER_ENABLED:
    try:
        # Test Redis connection
        import redis
        r = redis.Redis.from_url(CELERY_BROKER_URL_WATCHER)
        r.ping()
        celery_available = True
        logger.info("Redis connection successful - ETL triggering enabled")
    except Exception as e:
        logger.warning(f"Redis connection failed: {e} - ETL triggering disabled")
        ETL_TRIGGER_ENABLED = False

class FileProcessor:
    """Manages file processing queue and triggers ETL tasks"""
    
    def __init__(self):
        self.processing_queue = {}
        self.processed_files = set()
        self.file_timestamps = {}  # Track file modification times
        self.lock = threading.Lock()
        
    def schedule_file_processing(self, filepath: str):
        """Schedule a file for processing after a delay"""
        with self.lock:
            process_time = time.time() + ETL_PROCESS_DELAY
            self.processing_queue[filepath] = process_time
            logger.info(f"Scheduled {os.path.basename(filepath)} for processing in {ETL_PROCESS_DELAY} seconds")
    
    def process_pending_files(self):
        """Process files that are ready for ETL"""
        current_time = time.time()
        files_to_process = []
        
        with self.lock:
            for filepath, scheduled_time in list(self.processing_queue.items()):
                if current_time >= scheduled_time:
                    files_to_process.append(filepath)
                    del self.processing_queue[filepath]
        
        for filepath in files_to_process:
            try:
                self.trigger_etl_task_minimal_test(filepath)
                self.processed_files.add(filepath)
            except Exception as e:
                logger.error(f"Failed to trigger ETL for {filepath}: {e}")
    
    def trigger_etl_task(self, filepath: str):
        """Trigger ETL processing using proper Celery task calling"""
        
        if not ETL_TRIGGER_ENABLED:
            logger.info(f"ETL triggering disabled, skipping {os.path.basename(filepath)}")
            return
        sys.path.insert(0, './etl-worker')
        filename = os.path.basename(filepath)
        logger.info(f'filepath: {filepath}')
        logger.info(f"Triggering ETL task for: {filename}")
        
        try:
            # Import and call the task properly
            from etl_processor.enhanced_tasks import process_excel_file
            
            # Send task to Celery
            result = process_excel_file.delay(
                filename, 
                auto_triggered=True, 
                filepath=filepath
            )
            
            logger.info(f"ETL task queued successfully for {filename}, Task ID: {result.task_id}")
            self.log_processing_event(filepath, 'triggered', f"Task ID: {result.task_id}")
            
        except Exception as e:
            logger.error(f"Failed to trigger ETL task for {filename}: {e}")
            self.log_processing_event(filepath, 'error', str(e))

    # FIXED VERSION - Use direct Redis queue insertion instead:

    def trigger_etl_task_fixed(self, filepath: str):
        """Fixed version - uses direct Redis queue insertion"""
        if not ETL_TRIGGER_ENABLED:
            logger.info(f"ETL triggering disabled, skipping {os.path.basename(filepath)}")
            return
        
        filename = os.path.basename(filepath)
        logger.info(f"Triggering ETL task for: {filename} trigger_etl_task_fixed")
        
        try:
            # Use direct Redis connection (not Celery)
            import redis
            import json
            import time
            
            # Connect to Redis directly using localhost
            r = redis.Redis(host='localhost', port=6379, db=0)
            
            # Create task message manually
            task_data = {
                'id': f"{filename}_{int(time.time())}",
                'task': 'etl_processor.enhanced_tasks.process_excel_file',
                'args': [filename],
                'kwargs': {'auto_triggered': True, 'filepath': filepath},
                'retries': 0,
                'eta': None,
                # Add required Celery fields
                'headers': {},
                'properties': {
                    'correlation_id': f"{filename}_{int(time.time())}",
                    'reply_to': None,
                    'delivery_mode': 2,
                    'delivery_info': {'exchange': '', 'routing_key': 'celery'},
                    'priority': 0,
                    'body_encoding': 'base64',
                    'delivery_tag': None
                }
            }
            
            # Create Celery message format
            celery_message = {
                'body': json.dumps([task_data['args'], task_data['kwargs'], {}]).encode('utf-8'),
                'headers': {
                    'lang': 'py',
                    'task': task_data['task'],
                    'id': task_data['id'],
                    'shadow': None,
                    'eta': None,
                    'expires': None,
                    'group': None,
                    'group_index': None,
                    'retries': 0,
                    'timelimit': [None, None],
                    'root_id': task_data['id'],
                    'parent_id': None,
                    'argsrepr': repr(task_data['args']),
                    'kwargsrepr': repr(task_data['kwargs'])
                },
                'content-type': 'application/json',
                'content-encoding': 'utf-8'
            }

            logger.info('pushing cerlery on redis')
            # Push to Redis queue
            r.lpush('celery', json.dumps(celery_message))
            logger.info('after pushing cerlery on redis')
            
            logger.info(f"ETL task queued successfully for {filename}")
            self.log_processing_event(filepath, 'triggered', f"Task ID: {task_data['id']}")
            
        except Exception as e:
            logger.error(f"Failed to trigger ETL task for {filename}: {e}")
            self.log_processing_event(filepath, 'error', str(e))

    def trigger_etl_task_simple(self, filepath: str):
        # filename = os.path.basename(filepath)
        # logger.info(f"Triggering ETL via HTTP for: {filename}")
        
        # try:
        #     import requests
        #     response = requests.post(f"http://localhost:5000/trigger_etl/{filename}", timeout=30)
        #     if response.status_code == 200:
        #         logger.info(f'status: {response.status_code}')
        #         logger.info(f"ETL triggered successfully via HTTP")
        #     else:
        #         logger.error(f"HTTP trigger failed: {response.status_code}")
        # except Exception as e:
        #     logger.error(f"HTTP trigger failed: {e}")
        pass

    def trigger_etl_task_emergency(self, filepath: str):
        """Emergency Redis approach - very simple"""
        if not ETL_TRIGGER_ENABLED:
            return
        
        filename = os.path.basename(filepath)
        logger.info(f"Emergency ETL trigger for: {filename}")
        
        try:
            import redis
            import json
            import time
            
            r = redis.Redis(host='localhost', port=6379, db=0)
            
            # Ultra-simple message
            simple_task = {
                'task': 'etl_processor.enhanced_tasks.process_excel_file',
                'args': [filename],
                'kwargs': {'auto_triggered': True},
                'id': f"emergency_{int(time.time())}"
            }
            
            # Just push as simple JSON
            r.lpush('celery', json.dumps(simple_task))
            logger.info(f"Emergency task queued: {filename}")
            
        except Exception as e:
            logger.error(f"Emergency trigger failed: {e}")

    def trigger_etl_task_minimal_test(self, filepath: str):
        filename = os.path.basename(filepath)
        logger.info(f"Minimal test for: {filename}")
        
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0)
            
            # Test if worker can handle ANY message
            r.lpush('celery', '{"test": "message"}')
            logger.info("Pushed test message")
            
        except Exception as e:
            logger.error(f"Test failed: {e}")

    def trigger_etl_task_working_method(self, filepath: str):
        """Use Celery's send_task - should work properly"""
        filename = os.path.basename(filepath)
        logger.info(f"Using Celery send_task for: {filename}")
        
        try:
            from celery import Celery
            
            # Create Celery app configured for Windows host
            app = Celery('file_watcher')
            app.conf.update(
                broker_url='http://localhost:6379/0',
                result_backend='http://localhost:6379/1'
            )
            
            # Use send_task (doesn't require importing the actual task)
            result = app.send_task(
                'etl_processor.enhanced_tasks.process_excel_file',
                args=[filename],
                kwargs={'auto_triggered': True, 'filepath': str(filepath)}
            )
            
            logger.info(f"Task sent successfully: {result.id}")
            
        except Exception as e:
            logger.error(f"send_task failed: {e}")

    def log_processing_event(self, filepath: str, status: str, details: str):
        """Log processing events for tracking"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'filepath': filepath,
            'filename': os.path.basename(filepath),
            'status': status,
            'details': details
        }
        
        log_file = os.path.join(LOG_PATH, 'processing_events.log')
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(event) + '\n')
        except Exception as e:
            logger.error(f"Failed to write processing event: {e}")
    
    def get_processing_status(self):
        """Get current processing queue status"""
        with self.lock:
            return {
                'queued_files': len(self.processing_queue),
                'processed_files': len(self.processed_files),
                'queue_details': {
                    os.path.basename(fp): datetime.fromtimestamp(st).isoformat() 
                    for fp, st in self.processing_queue.items()
                }
            }

class PollingFileWatcher:
    """Alternative file watcher using polling (Python 3.13 compatible)"""
    
    def __init__(self, processor: FileProcessor):
        self.processor = processor
        self.running = False
        self.thread = None
        
    def start(self):
        """Start the polling watcher"""
        self.running = True
        self.thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.thread.start()
        logger.info("Polling file watcher started")
    
    def stop(self):
        """Stop the polling watcher"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("Polling file watcher stopped")
    
    def _poll_loop(self):
        """Main polling loop"""
        while self.running:
            try:
                self._check_for_new_files()
                time.sleep(POLL_INTERVAL)
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                time.sleep(POLL_INTERVAL)
    
    def _check_for_new_files(self):
        """Check for new or modified files"""
        for watch_path in WATCH_PATHS:
            if not os.path.exists(watch_path):
                continue
                
            try:
                # Find all supported files
                patterns = [f"{watch_path}/**/*{ext}" for ext in SUPPORTED_EXTENSIONS]
                
                for pattern in patterns:
                    for filepath in glob.glob(pattern, recursive=True):
                        if os.path.isfile(filepath):
                            self._check_file(filepath)
                            
            except Exception as e:
                logger.error(f"Error checking path {watch_path}: {e}")
    
    def _check_file(self, filepath: str):
        """Check if a file is new or modified"""
        try:
            stat_info = os.stat(filepath)
            current_mtime = stat_info.st_mtime
            
            # Check if this is a new file or if it has been modified
            if filepath not in self.processor.file_timestamps:
                # New file
                logger.info(f"New file detected: {os.path.basename(filepath)}")
                self.processor.file_timestamps[filepath] = current_mtime
                
                # Wait a moment and check if file is still being written
                time.sleep(2)
                new_stat = os.stat(filepath)
                if new_stat.st_mtime == current_mtime and new_stat.st_size > 0:
                    self.processor.schedule_file_processing(filepath)
                    
            elif self.processor.file_timestamps[filepath] != current_mtime:
                # Modified file
                logger.info(f"Modified file detected: {os.path.basename(filepath)}")
                self.processor.file_timestamps[filepath] = current_mtime
                
                # Check if not already processed recently
                if filepath not in self.processor.processed_files:
                    time.sleep(2)  # Wait for file write completion
                    self.processor.schedule_file_processing(filepath)
                    
        except Exception as e:
            logger.error(f"Error checking file {filepath}: {e}")

# Global instances
file_processor = FileProcessor()
file_watcher = PollingFileWatcher(file_processor)

def is_allowed_file(filename):
    """Check if file type is allowed"""
    return Path(filename).suffix.lower() in SUPPORTED_EXTENSIONS

def get_available_paths():
    """Get first available path for file operations"""
    for path in WATCH_PATHS:
        if os.path.exists(path):
            return path
    
    # Create fallback if none exist
    fallback = r'C:\shared_data'
    os.makedirs(fallback, exist_ok=True)
    return fallback

@app.route('/health')
def health():
    """Enhanced health check with file watching status"""
    try:
        current_path = get_available_paths()
        file_count = len([f for f in os.listdir(current_path) if is_allowed_file(f)])
        processing_status = file_processor.get_processing_status()
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "service_type": "polling_file_watcher",
            "shared_path": SHARED_PATH,
            "watch_paths": WATCH_PATHS,
            "active_path": current_path,
            "file_count": file_count,
            "etl_trigger_enabled": ETL_TRIGGER_ENABLED,
            "processing_delay": ETL_PROCESS_DELAY,
            "poll_interval": POLL_INTERVAL,
            "processing_status": processing_status,
            "celery_broker": CELERY_BROKER_URL_WATCHER if ETL_TRIGGER_ENABLED else None,
            "watcher_running": file_watcher.running
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
    """List all supported files with processing status"""
    try:
        current_path = get_available_paths()
        files = []
        
        for f in os.listdir(current_path):
            if is_allowed_file(f):
                filepath = os.path.join(current_path, f)
                try:
                    stat_info = os.stat(filepath)
                    file_info = {
                        "name": f,
                        "size": stat_info.st_size,
                        "size_mb": round(stat_info.st_size / (1024 * 1024), 2),
                        "modified": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
                        "type": Path(f).suffix.lower(),
                        "accessible": True,
                        "processed": filepath in file_processor.processed_files,
                        "queued": filepath in file_processor.processing_queue
                    }
                    files.append(file_info)
                except Exception as e:
                    files.append({"name": f, "error": str(e), "accessible": False})
        
        files.sort(key=lambda x: x.get('modified', ''), reverse=True)
        
        return jsonify({
            "files": files,
            "count": len(files),
            "path": current_path,
            "processing_status": file_processor.get_processing_status()
        })
        
    except Exception as e:
        logger.error(f"Error listing files: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/trigger_etl/<filename>', methods=['POST'])
def manual_trigger_etl(filename):
    # """Manually trigger ETL for a specific file"""
    # try:
    #     current_path = get_available_paths()
    #     filepath = os.path.join(current_path, filename)
        
    #     if not os.path.exists(filepath):
    #         return jsonify({"error": f"File {filename} not found"}), 404
        
    #     if not is_allowed_file(filename):
    #         return jsonify({"error": f"File type not supported"}), 400
        
    #     # Trigger immediately
    #     file_processor.trigger_etl_task_emergency(filepath)
        
    #     return jsonify({
    #         "message": f"ETL triggered for {filename}",
    #         "filepath": filepath,
    #         "timestamp": datetime.now().isoformat()
    #     })
        
    # except Exception as e:
    #     logger.error(f"Error triggering ETL for {filename}: {str(e)}")
    #     return jsonify({"error": str(e)}), 500
    pass

@app.route('/processing_complete', methods=['POST'])
def processing_complete():
    """Receive processing completion notifications from ETL workers"""
    try:
        data = request.json or {}
        filename = data.get('filename')
        status = data.get('status')
        details = data.get('details')
        worker_id = data.get('worker_id')
        
        logger.info(f"Processing complete notification: {filename} - {status}")
        
        # Log completion event
        file_processor.log_processing_event(
            filename, 
            f'completed_{status}', 
            f"Worker: {worker_id}, Details: {details}"
        )
        
        return jsonify({
            "message": "Processing completion recorded",
            "filename": filename,
            "status": status
        })
        
    except Exception as e:
        logger.error(f"Error recording processing completion: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/processing_history')
def processing_history():
    """Get processing history"""
    try:
        log_file = os.path.join(LOG_PATH, 'processing_events.log')
        events = []
        
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        events.append(json.loads(line.strip()))
                    except json.JSONDecodeError:
                        continue
        
        # Return last 50 events
        events = events[-50:]
        
        return jsonify({
            "events": events,
            "count": len(events)
        })
        
    except Exception as e:
        logger.error(f"Error getting processing history: {str(e)}")
        return jsonify({"error": str(e)}), 500

# Keep existing endpoints
@app.route('/download/<filename>')
def download_file(filename):
    """Download file with validation"""
    try:
        if not is_allowed_file(filename):
            return jsonify({"error": f"File type not supported"}), 400
            
        current_path = get_available_paths()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File {filename} not found"}), 404
        
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
        
        if not filename or not is_allowed_file(filename):
            return jsonify({"error": "Invalid filename"}), 400
            
        current_path = get_available_paths()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File not found"}), 404
        
        # Read based on file type
        if filename.lower().endswith('.csv'):
            df = pd.read_csv(filepath, nrows=nrows)
        else:
            df = pd.read_excel(filepath, sheet_name=sheet_name, nrows=nrows)
        
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
    """Get sheet names from Excel file"""
    try:
        if not is_allowed_file(filename):
            return jsonify({"error": "File type not supported"}), 400
            
        current_path = get_available_paths()
        filepath = os.path.join(current_path, filename)
        
        if not os.path.exists(filepath):
            return jsonify({"error": f"File not found"}), 404
        
        if filename.lower().endswith('.csv'):
            return jsonify({"sheets": ["Sheet1"]})
            
        xl_file = pd.ExcelFile(filepath)
        sheet_names = xl_file.sheet_names
        xl_file.close()
        
        return jsonify({"sheets": sheet_names})
        
    except Exception as e:
        logger.error(f"Error getting sheets: {str(e)}")
        return jsonify({"error": str(e)}), 500

class FileWatcherService(win32serviceutil.ServiceFramework):
    """Windows Service with polling file watching"""
    _svc_name_ = "LightETLFileWatcher"
    _svc_display_name_ = "Light ETL File Watcher Service"
    _svc_description_ = "File watcher that triggers ETL tasks for new files"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.server_thread = None
        self.processing_thread = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        
        # Stop file watcher
        file_watcher.stop()
            
        win32event.SetEvent(self.hWaitStop)
        logger.info("Service stop requested")

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        
        logger.info("Starting Light ETL File Watcher Service")
        
        # Start file watcher
        file_watcher.start()
        
        # Start Flask server
        self.server_thread = threading.Thread(target=self.run_server, daemon=True)
        self.server_thread.start()
        
        # Start processing thread
        self.processing_thread = threading.Thread(target=self.run_processor, daemon=True)
        self.processing_thread.start()
        
        # Wait for stop signal
        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
        
        # Cleanup
        file_watcher.stop()
        logger.info("Service stopped")

    def run_server(self):
        """Run the Flask server"""
        try:
            serve(app, host='0.0.0.0', port=PORT, threads=4)
        except Exception as e:
            logger.error(f"Server error: {str(e)}")
    
    def run_processor(self):
        """Run the file processor loop"""
        while True:
            try:
                file_processor.process_pending_files()
                time.sleep(5)
            except Exception as e:
                logger.error(f"Processor error: {str(e)}")
                time.sleep(10)

def run_standalone():
    """Run as standalone application with file watching"""
    logger.info("="*60)
    logger.info("Light ETL File Watcher Service - Standalone Mode")
    logger.info(f"Watch paths: {WATCH_PATHS}")
    logger.info(f"ETL triggering: {'Enabled' if ETL_TRIGGER_ENABLED else 'Disabled'}")
    logger.info(f"Poll interval: {POLL_INTERVAL} seconds")
    logger.info(f"Server: http://localhost:{PORT}")
    logger.info("="*60)
    
    # Start file watcher
    file_watcher.start()
    
    # Start processing thread
    def processor_loop():
        while True:
            try:
                file_processor.process_pending_files()
                time.sleep(5)
            except Exception as e:
                logger.error(f"Processor error: {e}")
                time.sleep(10)
    
    processor_thread = threading.Thread(target=processor_loop, daemon=True)
    processor_thread.start()
    
    try:
        # Start Flask server
        serve(app, host='0.0.0.0', port=PORT, threads=4)
    except KeyboardInterrupt:
        logger.info("Stopping file watcher...")
        file_watcher.stop()
        logger.info("Server stopped")

if __name__ == '__main__':
    if len(sys.argv) == 1:
        run_standalone()
    elif WINDOWS_SERVICE_AVAILABLE:
        win32serviceutil.HandleCommandLine(FileWatcherService)
    else:
        print("Windows service modules not available")
        run_standalone()