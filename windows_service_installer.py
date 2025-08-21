#!/usr/bin/env python3
"""
Windows Service Installation for Configurable Pattern-Based File Watcher
Converts the configurable pattern watcher into a Windows service
"""

import os
import sys
import time
import logging
import servicemanager
import socket
import win32event
import win32service
import win32serviceutil
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import your configurable pattern watcher
try:
    from pattern_watcher_configurable import ConfigurablePatternWatcher
    WATCHER_AVAILABLE = True
except ImportError:
    print("Error: Could not import ConfigurablePatternWatcher")
    print("Make sure pattern_watcher_configurable.py is in the same directory")
    WATCHER_AVAILABLE = False

# Try to import configuration system
try:
    from pattern_config_system import PatternConfig
    CONFIG_SYSTEM_AVAILABLE = True
except ImportError:
    print("Warning: pattern_config_system.py not found - will use default configuration")
    CONFIG_SYSTEM_AVAILABLE = False

class ETLConfigurableWatcherService(win32serviceutil.ServiceFramework):
    """Windows service wrapper for the configurable pattern-based file watcher"""
    
    # Service configuration
    _svc_name_ = "ETLConfigurableWatcher"
    _svc_display_name_ = "ETL Configurable Pattern-Based File Watcher"
    _svc_description_ = "Monitors file directories and processes Excel/CSV files based on configurable path patterns"
    _svc_reg_class_ = "ETLConfigurableWatcherService"  # Add this line
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.is_alive = True
        self.watcher = None
        self.config_file = None
        
        # Setup service logging
        self.setup_service_logging()
        
        # Determine config file location
        self.find_config_file()
        
    def find_config_file(self):
        """Find configuration file"""
        possible_configs = [
            './config/pattern_config.yaml',
            './pattern_config.yaml',
            str(Path.home() / '.etl' / 'pattern_config.yaml'),
            None  # Use defaults
        ]
        
        for config_path in possible_configs:
            if config_path is None:
                self.config_file = None
                self.logger.info("Using default configuration (no config file)")
                break
            elif os.path.exists(config_path):
                self.config_file = config_path
                self.logger.info(f"Found configuration file: {config_path}")
                break
        
        if self.config_file is None and CONFIG_SYSTEM_AVAILABLE:
            self.logger.warning("No configuration file found, using defaults")
        
    def setup_service_logging(self):
        """Setup logging for the Windows service"""
        log_dir = Path(__file__).parent / "logs"
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / "etl_configurable_service.log"
        
        # Enhanced logging configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(str(log_file)),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger('ETLConfigurableService')
        
        # Also log to Windows Event Log
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, "Configurable service logging initialized")
        )
    
    def SvcStop(self):
        """Called when service is stopped"""
        self.logger.info("ETL Configurable Pattern Watcher Service stopping...")
        
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STOPPED,
            (self._svc_name_, "Service stop requested")
        )
        
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_alive = False
        
        # Stop the watcher gracefully
        if self.watcher:
            try:
                self.logger.info("Stopping configurable file watcher...")
            except Exception as e:
                self.logger.error(f"Error stopping watcher: {e}")
    
    def SvcDoRun(self):
        """Main service execution"""
        self.logger.info("ETL Configurable Pattern Watcher Service starting...")
        
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, f"Service started with config: {self.config_file or 'default'}")
        )
        
        try:
            # Check if watcher is available
            if not WATCHER_AVAILABLE:
                raise ImportError("ConfigurablePatternWatcher not available")
            
            # Initialize the configurable pattern watcher
            self.logger.info("Initializing configurable pattern-based file watcher...")
            self.logger.info(f"Configuration file: {self.config_file or 'Using defaults'}")
            self.logger.info(f"Configuration system available: {CONFIG_SYSTEM_AVAILABLE}")
            
            self.watcher = ConfigurablePatternWatcher(self.config_file)
            
            # Get and log configuration details
            status = self.watcher.get_status()
            self.logger.info("Configurable watcher initialized successfully:")
            self.logger.info(f"  Watch path: {status['watch_path']}")
            self.logger.info(f"  Poll interval: {status['poll_interval']} seconds")
            self.logger.info(f"  Max file size: {status['max_file_size_mb']}MB")
            self.logger.info(f"  Celery configured: {status['celery_configured']}")
            self.logger.info(f"  Pattern mappings: {len(status['pattern_mappings'])} patterns")
            
            for pattern, table in status['pattern_mappings'].items():
                self.logger.info(f"    '{pattern}' -> {table}")
            
            # Main service loop
            self.main_service_loop()
            
        except Exception as e:
            self.logger.error(f"Service error: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_ERROR_TYPE,
                servicemanager.PYS_SERVICE_STOPPED,
                (self._svc_name_, f"Service error: {e}")
            )
            raise
    
    def main_service_loop(self):
        """Main service loop that runs the configurable file watcher"""
        self.logger.info("Starting main service loop...")
        
        cycle_count = 0
        last_status_log = time.time()
        
        try:
            # Run the watcher in a way that can be interrupted
            while self.is_alive:
                # Check if stop event is signaled (non-blocking)
                rc = win32event.WaitForSingleObject(self.hWaitStop, 1000)  # 1 second timeout
                
                if rc == win32event.WAIT_OBJECT_0:
                    # Stop event was signaled
                    self.logger.info("Stop event received, shutting down...")
                    break
                
                # Run one cycle of file processing
                try:
                    if self.watcher:
                        cycle_count += 1
                        self.logger.debug(f"Starting processing cycle #{cycle_count}")
                        
                        self.watcher.find_and_process_files()
                        
                        # Periodic status logging (every 10 minutes)
                        current_time = time.time()
                        if current_time - last_status_log > 600:  # 10 minutes
                            status = self.watcher.get_status()
                            self.logger.info(f"Service status update:")
                            self.logger.info(f"  Cycles completed: {cycle_count}")
                            self.logger.info(f"  Files watched: {status['total_files_watched']}")
                            self.logger.info(f"  Files processed: {status['total_files_processed']}")
                            last_status_log = current_time
                        
                        # Sleep for poll interval
                        poll_interval = getattr(self.watcher, 'poll_interval', 10)
                        time.sleep(poll_interval)
                        
                except Exception as e:
                    self.logger.error(f"Error in watcher cycle #{cycle_count}: {e}")
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
                    # Don't exit on individual errors, just log and continue
                    time.sleep(30)  # Wait 30 seconds before retrying
                    
        except Exception as e:
            self.logger.error(f"Fatal error in main loop: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        
        self.logger.info(f"Main service loop ended after {cycle_count} cycles")

def install_service():
    """Install the Windows service"""
    print("Installing ETL Configurable Pattern Watcher as Windows Service...")
    
    try:
        # Check if configurable watcher is available
        if not WATCHER_AVAILABLE:
            print("✗ ConfigurablePatternWatcher not available")
            print("  Make sure pattern_watcher_configurable.py is in the same directory")
            return False
        
        # Install the service
        win32serviceutil.InstallService(
            ETLConfigurableWatcherService,
            ETLConfigurableWatcherService._svc_name_,
            ETLConfigurableWatcherService._svc_display_name_,
            startType=win32service.SERVICE_AUTO_START,
            description=ETLConfigurableWatcherService._svc_description_
        )
        
        print(f"✓ Service '{ETLConfigurableWatcherService._svc_display_name_}' installed successfully")
        print(f"  Service Name: {ETLConfigurableWatcherService._svc_name_}")
        print(f"  Startup Type: Automatic")
        print(f"  Configuration system: {'Available' if CONFIG_SYSTEM_AVAILABLE else 'Using defaults'}")
        print()
        print("Next steps:")
        print(f"  1. Start service: python {__file__} start")
        print(f"  2. Or use Windows Services: services.msc")
        print(f"  3. Check logs: logs/etl_configurable_service.log")
        print(f"  4. Configure patterns: config/pattern_config.yaml")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to install service: {e}")
        return False

def uninstall_service():
    """Uninstall the Windows service"""
    print("Uninstalling ETL Configurable Pattern Watcher service...")
    
    try:
        # Stop service if running
        try:
            win32serviceutil.StopService(ETLConfigurableWatcherService._svc_name_)
            print("  Service stopped")
        except Exception:
            pass  # Service might not be running
        
        # Remove the service
        win32serviceutil.RemoveService(ETLConfigurableWatcherService._svc_name_)
        print(f"✓ Service '{ETLConfigurableWatcherService._svc_display_name_}' uninstalled successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to uninstall service: {e}")
        return False

def start_service():
    """Start the Windows service"""
    print("Starting ETL Configurable Pattern Watcher service...")
    
    try:
        win32serviceutil.StartService(ETLConfigurableWatcherService._svc_name_)
        print("✓ Service started successfully")
        
        # Wait a moment and check status
        time.sleep(3)
        status = win32serviceutil.QueryServiceStatus(ETLConfigurableWatcherService._svc_name_)[1]
        if status == win32service.SERVICE_RUNNING:
            print("✓ Service is running")
        else:
            print(f"⚠ Service status: {status}")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to start service: {e}")
        return False

def stop_service():
    """Stop the Windows service"""
    print("Stopping ETL Configurable Pattern Watcher service...")
    
    try:
        win32serviceutil.StopService(ETLConfigurableWatcherService._svc_name_)
        print("✓ Service stopped successfully")
        return True
        
    except Exception as e:
        print(f"✗ Failed to stop service: {e}")
        return False

def service_status():
    """Check service status"""
    try:
        status_code = win32serviceutil.QueryServiceStatus(ETLConfigurableWatcherService._svc_name_)[1]
        
        status_map = {
            win32service.SERVICE_STOPPED: "Stopped",
            win32service.SERVICE_START_PENDING: "Starting",
            win32service.SERVICE_STOP_PENDING: "Stopping",
            win32service.SERVICE_RUNNING: "Running",
            win32service.SERVICE_CONTINUE_PENDING: "Continuing",
            win32service.SERVICE_PAUSE_PENDING: "Pausing",
            win32service.SERVICE_PAUSED: "Paused"
        }
        
        status_text = status_map.get(status_code, f"Unknown ({status_code})")
        print(f"Service Status: {status_text}")
        
        if status_code == win32service.SERVICE_RUNNING:
            print("✓ Service is running normally")
        elif status_code == win32service.SERVICE_STOPPED:
            print("⚠ Service is stopped")
        else:
            print(f"⚠ Service is in transitional state: {status_text}")
            
        return status_code
        
    except Exception as e:
        print(f"✗ Could not check service status: {e}")
        print("  Service may not be installed")
        return None

def check_prerequisites():
    """Check if all prerequisites are met"""
    print("Checking prerequisites for Configurable Pattern Watcher...")
    
    issues = []
    
    # Check if running as administrator
    try:
        import ctypes
        is_admin = ctypes.windll.shell32.IsUserAnAdmin()
        if not is_admin:
            issues.append("Must run as Administrator to install/manage Windows services")
    except:
        issues.append("Could not check administrator privileges")
    
    # Check if pattern_watcher_configurable.py exists
    if not os.path.exists("pattern_watcher_configurable.py"):
        issues.append("pattern_watcher_configurable.py not found in current directory")
    else:
        print("✓ pattern_watcher_configurable.py found")
    
    # Check if pattern_config_system.py exists
    if not os.path.exists("pattern_config_system.py"):
        print("⚠ pattern_config_system.py not found (will use default configuration)")
    else:
        print("✓ pattern_config_system.py found")
    
    # Check configuration files
    config_files = [
        './config/pattern_config.yaml',
        './pattern_config.yaml'
    ]
    
    config_found = False
    for config_file in config_files:
        if os.path.exists(config_file):
            print(f"✓ Configuration file found: {config_file}")
            config_found = True
            break
    
    if not config_found:
        print("⚠ No configuration file found (will use defaults)")
        print("  Consider creating config/pattern_config.yaml")
    
    # Check if required packages are installed
    required_packages = ['pandas', 'celery', 'redis']
    missing_packages = []
    
    # Check pywin32 separately since it has specific modules
    try:
        import win32serviceutil
        import win32service
        import win32event
        import servicemanager
        print("✓ pywin32 available")
    except ImportError:
        missing_packages.append('pywin32')
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✓ {package} available")
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        issues.append(f"Missing required packages: {', '.join(missing_packages)}")
    
    # Check YAML support
    try:
        import yaml
        print("✓ PyYAML available (configuration file support)")
    except ImportError:
        print("⚠ PyYAML missing (limited configuration support)")
        print("  Install with: pip install pyyaml")
    
    # Check if Redis is accessible
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✓ Redis connection successful")
    except Exception as e:
        issues.append(f"Redis connection failed: {e}")
    
    # Check if Celery workers are running
    try:
        import requests
        response = requests.get("http://localhost:5555", timeout=5)
        if response.status_code == 200:
            print("✓ Celery Flower accessible")
        else:
            issues.append("Celery Flower not accessible (workers may not be running)")
    except Exception:
        issues.append("Celery monitoring not accessible")
    
    # Test configurable watcher import
    try:
        from pattern_watcher_configurable import ConfigurablePatternWatcher
        print("✓ ConfigurablePatternWatcher can be imported")
        
        # Test initialization
        test_watcher = ConfigurablePatternWatcher()
        status = test_watcher.get_status()
        print(f"✓ Test watcher initialized ({len(status['pattern_mappings'])} patterns)")
        
    except Exception as e:
        issues.append(f"ConfigurablePatternWatcher initialization failed: {e}")
    
    if issues:
        print("\n⚠ Issues found:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print("\n✓ All prerequisites met")
        return True

def create_sample_config():
    """Create a sample configuration file"""
    print("Creating sample configuration file...")
    
    try:
        config_dir = Path("config")
        config_dir.mkdir(exist_ok=True)
        
        config_file = config_dir / "pattern_config.yaml"
        
        if config_file.exists():
            print(f"Configuration file already exists: {config_file}")
            return str(config_file)
        
        sample_config = """# ETL Configurable Pattern Watcher Configuration

watcher_settings:
  watch_path: "Z:\\"
  backup_watch_path: "./watch_production"
  poll_interval: 10
  process_delay: 5
  supported_extensions: [".csv", ".xlsx", ".xls", ".xlsm"]

celery_settings:
  broker_url: "redis://localhost:6379/0"
  result_backend: "redis://localhost:6379/1"
  task_ignore_result: true

pattern_mappings:
  tel_list:
    table: "dim_numbers"
    schema: "public"
    description: "Telephone numbers and contact information"
  customer_data:
    table: "dim_customers"
    schema: "public"
    description: "Customer master data"
  product_info:
    table: "dim_products"
    schema: "public"
    description: "Product information and catalog"
  sales_data:
    table: "fact_sales"
    schema: "public"
    description: "Sales transaction data"
  inventory:
    table: "dim_inventory"
    schema: "public"
    description: "Inventory levels and stock data"

data_quality:
  max_file_size_mb: 100
  require_headers: true
  skip_empty_files: true
  encoding_fallbacks: ["utf-8", "utf-8-sig", "latin1", "cp1252"]

logging:
  level: "INFO"
  file: "./pattern_watcher_configurable.log"
  format: "%(asctime)s - %(levelname)s - %(message)s"
"""
        
        with open(config_file, 'w') as f:
            f.write(sample_config)
        
        print(f"✓ Sample configuration created: {config_file}")
        print("  Edit this file to customize your patterns and settings")
        
        return str(config_file)
        
    except Exception as e:
        print(f"✗ Failed to create sample configuration: {e}")
        return None

def show_usage():
    """Show usage information"""
    print("ETL Configurable Pattern Watcher - Windows Service Manager")
    print("=" * 60)
    print("Usage: python windows_service_installer.py [command]")
    print()
    print("Commands:")
    print("  install       Install as Windows service")
    print("  uninstall     Remove Windows service")
    print("  start         Start the service")
    print("  stop          Stop the service")
    print("  restart       Restart the service")
    print("  status        Check service status")
    print("  check         Check prerequisites")
    print("  debug         Run in debug mode (not as service)")
    print("  config        Create sample configuration file")
    print("  test-config   Test configuration loading")
    print()
    print("Features:")
    print("  ✓ Configuration file support (YAML)")
    print("  ✓ Pattern-based file routing")
    print("  ✓ Flexible data quality settings")
    print("  ✓ External configuration management")
    print("  ✓ Enhanced logging and monitoring")
    print()
    print("Prerequisites:")
    print("  • Run as Administrator (for install/uninstall)")
    print("  • Redis server running")
    print("  • Celery workers running")
    print("  • pattern_watcher_configurable.py in same directory")
    print("  • Optional: pattern_config_system.py for advanced config")
    print()
    print("Configuration:")
    print("  • Default config: config/pattern_config.yaml")
    print("  • Create sample: python windows_service_installer.py config")
    print("  • Test config: python windows_service_installer.py test-config")
    print()
    print("Examples:")
    print("  python windows_service_installer.py install")
    print("  python windows_service_installer.py start")
    print("  python windows_service_installer.py status")

def test_config():
    """Test configuration loading"""
    print("Testing configuration loading...")
    
    try:
        # Test if we can create a configurable watcher
        watcher = ConfigurablePatternWatcher()
        status = watcher.get_status()
        
        print("✓ Configuration loaded successfully")
        print(f"  Config file: {status.get('config_file', 'Default/Environment')}")
        print(f"  Watch path: {status['watch_path']}")
        print(f"  Poll interval: {status['poll_interval']} seconds")
        print(f"  Max file size: {status['max_file_size_mb']}MB")
        print(f"  Pattern mappings: {len(status['pattern_mappings'])}")
        
        print("\nPattern Mappings:")
        for pattern, table in status['pattern_mappings'].items():
            print(f"  '{pattern}' -> {table}")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False

def debug_mode():
    """Run the configurable watcher in debug mode (not as service)"""
    print("Running Configurable Pattern Watcher in Debug Mode")
    print("=" * 55)
    print("This will run the watcher directly (not as a service)")
    print("Press Ctrl+C to stop")
    print()
    
    try:
        watcher = ConfigurablePatternWatcher()
        status = watcher.get_status()
        
        print("Configuration loaded:")
        print(f"  Config file: {status.get('config_file', 'Default')}")
        print(f"  Watch path: {status['watch_path']}")
        print(f"  Patterns: {len(status['pattern_mappings'])}")
        print()
        
        input("Press Enter to start watching...")
        watcher.start_watching()
        
    except KeyboardInterrupt:
        print("\nDebug session stopped")
    except Exception as e:
        print(f"Error in debug mode: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function"""
    if len(sys.argv) < 2:
        show_usage()
        return
    
    command = sys.argv[1].lower()
    
    if command == "install":
        # Skip full prerequisite check since we know the main components work
        # Just check the critical imports
        try:
            import win32serviceutil
            import win32service
            from pattern_watcher_configurable import ConfigurablePatternWatcher
            print("✓ Critical components available")
            install_service()
        except ImportError as e:
            print(f"✗ Critical import failed: {e}")
            print("Cannot install service without required components")
            
    elif command == "uninstall":
        uninstall_service()
        
    elif command == "start":
        start_service()
        
    elif command == "stop":
        stop_service()
        
    elif command == "restart":
        print("Restarting service...")
        stop_service()
        time.sleep(3)
        start_service()
        
    elif command == "status":
        service_status()
        
    elif command == "check":
        check_prerequisites()
        
    elif command == "config":
        create_sample_config()
        
    elif command == "test-config":
        test_config()
        
    elif command == "debug":
        debug_mode()
        
    elif command in ["help", "--help", "-h"]:
        show_usage()
        
    else:
        print(f"Unknown command: {command}")
        show_usage()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        # If no arguments, this might be called by the service manager
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(ETLConfigurableWatcherService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        main()
    """Windows service wrapper for the pattern-based file watcher"""
    
    # Service configuration
    _svc_name_ = "ETLPatternWatcher"
    _svc_display_name_ = "ETL Pattern-Based File Watcher"
    _svc_description_ = "Monitors file directories and processes Excel/CSV files based on path patterns"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.is_alive = True
        self.watcher = None
        
        # Setup service logging
        self.setup_service_logging()
        
    def setup_service_logging(self):
        """Setup logging for the Windows service"""
        log_dir = Path(__file__).parent / "logs"
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / "etl_service.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(str(log_file)),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger('ETLService')
        
        # Also log to Windows Event Log
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, "Service logging initialized")
        )
    
    def SvcStop(self):
        """Called when service is stopped"""
        self.logger.info("ETL Pattern Watcher Service stopping...")
        
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STOPPED,
            (self._svc_name_, "Service stop requested")
        )
        
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        self.is_alive = False
        
        # Stop the watcher gracefully
        if self.watcher:
            try:
                # The watcher should handle KeyboardInterrupt gracefully
                self.logger.info("Stopping file watcher...")
            except Exception as e:
                self.logger.error(f"Error stopping watcher: {e}")
    
    def SvcDoRun(self):
        """Main service execution"""
        self.logger.info("ETL Pattern Watcher Service starting...")
        
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, "Service started successfully")
        )
        
        try:
            # Initialize the pattern watcher
            self.logger.info("Initializing pattern-based file watcher...")
            self.watcher = PatternBasedWatcher()
            
            self.logger.info("Pattern watcher initialized successfully")
            self.logger.info(f"Watching path: {self.watcher.get_watch_path()}")
            self.logger.info(f"Pattern mappings: {self.watcher.pattern_mappings}")
            
            # Main service loop
            self.main_service_loop()
            
        except Exception as e:
            self.logger.error(f"Service error: {e}")
            servicemanager.LogMsg(
                servicemanager.EVENTLOG_ERROR_TYPE,
                servicemanager.PYS_SERVICE_STOPPED,
                (self._svc_name_, f"Service error: {e}")
            )
            raise
    
    def main_service_loop(self):
        """Main service loop that runs the file watcher"""
        self.logger.info("Starting main service loop...")
        
        try:
            # Run the watcher in a way that can be interrupted
            while self.is_alive:
                # Check if stop event is signaled (non-blocking)
                rc = win32event.WaitForSingleObject(self.hWaitStop, 1000)  # 1 second timeout
                
                if rc == win32event.WAIT_OBJECT_0:
                    # Stop event was signaled
                    self.logger.info("Stop event received, shutting down...")
                    break
                
                # Run one cycle of file processing
                try:
                    if self.watcher:
                        self.watcher.find_and_process_files()
                        time.sleep(self.watcher.poll_interval)
                except Exception as e:
                    self.logger.error(f"Error in watcher cycle: {e}")
                    # Don't exit on individual errors, just log and continue
                    time.sleep(30)  # Wait 30 seconds before retrying
                    
        except Exception as e:
            self.logger.error(f"Fatal error in main loop: {e}")
            raise
        
        self.logger.info("Main service loop ended")

def install_service():
    """Install the Windows service"""
    print("Installing ETL Pattern Watcher as Windows Service...")
    
    try:
        # Install the service
        win32serviceutil.InstallService(
            ETLPatternWatcherService._svc_reg_class_,
            ETLPatternWatcherService._svc_name_,
            ETLPatternWatcherService._svc_display_name_,
            startType=win32service.SERVICE_AUTO_START,
            description=ETLPatternWatcherService._svc_description_
        )
        
        print(f"✓ Service '{ETLPatternWatcherService._svc_display_name_}' installed successfully")
        print(f"  Service Name: {ETLPatternWatcherService._svc_name_}")
        print(f"  Startup Type: Automatic")
        print()
        print("Next steps:")
        print(f"  1. Start service: python {__file__} start")
        print(f"  2. Or use Windows Services: services.msc")
        print(f"  3. Check logs: logs/etl_service.log")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to install service: {e}")
        return False

def uninstall_service():
    """Uninstall the Windows service"""
    print("Uninstalling ETL Pattern Watcher service...")
    
    try:
        # Stop service if running
        try:
            win32serviceutil.StopService(ETLPatternWatcherService._svc_name_)
            print("  Service stopped")
        except Exception:
            pass  # Service might not be running
        
        # Remove the service
        win32serviceutil.RemoveService(ETLPatternWatcherService._svc_name_)
        print(f"✓ Service '{ETLPatternWatcherService._svc_display_name_}' uninstalled successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to uninstall service: {e}")
        return False

def start_service():
    """Start the Windows service"""
    print("Starting ETL Pattern Watcher service...")
    
    try:
        win32serviceutil.StartService(ETLPatternWatcherService._svc_name_)
        print("✓ Service started successfully")
        
        # Wait a moment and check status
        time.sleep(2)
        status = win32serviceutil.QueryServiceStatus(ETLPatternWatcherService._svc_name_)[1]
        if status == win32service.SERVICE_RUNNING:
            print("✓ Service is running")
        else:
            print(f"⚠ Service status: {status}")
        
        return True
        
    except Exception as e:
        print(f"✗ Failed to start service: {e}")
        return False

def stop_service():
    """Stop the Windows service"""
    print("Stopping ETL Pattern Watcher service...")
    
    try:
        win32serviceutil.StopService(ETLPatternWatcherService._svc_name_)
        print("✓ Service stopped successfully")
        return True
        
    except Exception as e:
        print(f"✗ Failed to stop service: {e}")
        return False

def service_status():
    """Check service status"""
    try:
        status_code = win32serviceutil.QueryServiceStatus(ETLPatternWatcherService._svc_name_)[1]
        
        status_map = {
            win32service.SERVICE_STOPPED: "Stopped",
            win32service.SERVICE_START_PENDING: "Starting",
            win32service.SERVICE_STOP_PENDING: "Stopping",
            win32service.SERVICE_RUNNING: "Running",
            win32service.SERVICE_CONTINUE_PENDING: "Continuing",
            win32service.SERVICE_PAUSE_PENDING: "Pausing",
            win32service.SERVICE_PAUSED: "Paused"
        }
        
        status_text = status_map.get(status_code, f"Unknown ({status_code})")
        print(f"Service Status: {status_text}")
        
        if status_code == win32service.SERVICE_RUNNING:
            print("✓ Service is running normally")
        elif status_code == win32service.SERVICE_STOPPED:
            print("⚠ Service is stopped")
        else:
            print(f"⚠ Service is in transitional state: {status_text}")
            
        return status_code
        
    except Exception as e:
        print(f"✗ Could not check service status: {e}")
        print("  Service may not be installed")
        return None

def check_prerequisites():
    """Check if all prerequisites are met"""
    print("Checking prerequisites...")
    
    issues = []
    
    # Check if running as administrator
    try:
        import ctypes
        is_admin = ctypes.windll.shell32.IsUserAnAdmin()
        if not is_admin:
            issues.append("Must run as Administrator to install/manage Windows services")
    except:
        issues.append("Could not check administrator privileges")
    
    # Check if pattern_based_watcher.py exists
    if not os.path.exists("pattern_based_watcher.py"):
        issues.append("pattern_based_watcher.py not found in current directory")
    
    # Check if required packages are installed
    required_packages = ['pywin32', 'pandas', 'celery', 'redis']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        issues.append(f"Missing required packages: {', '.join(missing_packages)}")
    
    # Check if Redis is accessible
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✓ Redis connection successful")
    except Exception as e:
        issues.append(f"Redis connection failed: {e}")
    
    # Check if Celery workers are running
    try:
        import requests
        response = requests.get("http://localhost:5555", timeout=5)
        if response.status_code == 200:
            print("✓ Celery Flower accessible")
        else:
            issues.append("Celery Flower not accessible (workers may not be running)")
    except Exception:
        issues.append("Celery monitoring not accessible")
    
    if issues:
        print("\n⚠ Issues found:")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print("✓ All prerequisites met")
        return True

def show_usage():
    """Show usage information"""
    print("ETL Pattern Watcher - Windows Service Manager")
    print("=" * 50)
    print("Usage: python windows_service_installer.py [command]")
    print()
    print("Commands:")
    print("  install    Install as Windows service")
    print("  uninstall  Remove Windows service")
    print("  start      Start the service")
    print("  stop       Stop the service")
    print("  restart    Restart the service")
    print("  status     Check service status")
    print("  check      Check prerequisites")
    print("  debug      Run in debug mode (not as service)")
    print()
    print("Prerequisites:")
    print("  • Run as Administrator")
    print("  • Redis server running")
    print("  • Celery workers running")
    print("  • pattern_based_watcher.py in same directory")
    print()
    print("Examples:")
    print("  python windows_service_installer.py install")
    print("  python windows_service_installer.py start")
    print("  python windows_service_installer.py status")

def debug_mode():
    """Run the watcher in debug mode (not as service)"""
    print("Running Pattern Watcher in debug mode...")
    print("Press Ctrl+C to stop")
    print()
    
    try:
        watcher = PatternBasedWatcher()
        watcher.start_watching()
    except KeyboardInterrupt:
        print("\nDebug session stopped")
    except Exception as e:
        print(f"Error in debug mode: {e}")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        show_usage()
        return
    
    command = sys.argv[1].lower()
    
    if command == "install":
        if check_prerequisites():
            install_service()
        else:
            print("\n❌ Cannot install service due to prerequisite issues")
            
    elif command == "uninstall":
        uninstall_service()
        
    elif command == "start":
        start_service()
        
    elif command == "stop":
        stop_service()
        
    elif command == "restart":
        print("Restarting service...")
        stop_service()
        time.sleep(2)
        start_service()
        
    elif command == "status":
        service_status()
        
    elif command == "check":
        check_prerequisites()
        
    elif command == "debug":
        debug_mode()
        
    elif command in ["help", "--help", "-h"]:
        show_usage()
        
    else:
        print(f"Unknown command: {command}")
        show_usage()

if __name__ == '__main__':
    if len(sys.argv) == 1:
        # If no arguments, this might be called by the service manager
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(ETLPatternWatcherService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        main()