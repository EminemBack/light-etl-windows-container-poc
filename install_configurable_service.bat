REM ==========================================
REM install_configurable_service.bat
REM ==========================================
@echo off
echo Installing ETL Configurable Pattern Watcher Service...
echo.

REM Check if running as Administrator
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: This script must be run as Administrator
    echo Right-click and select "Run as administrator"
    pause
    exit /b 1
)

REM Check prerequisites
echo Checking prerequisites...
python windows_service_installer.py check
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Prerequisites check failed
    echo Please fix the issues above before installing
    pause
    exit /b 1
)

@REM REM Create sample configuration if needed
@REM echo.
@REM echo Creating sample configuration if needed...
@REM python windows_service_installer.py config

REM Install service
echo.
echo Installing configurable service...
python windows_service_installer.py install
if %errorlevel% neq 0 (
    echo ERROR: Service installation failed
    pause
    exit /b 1
)

REM Start service
echo.
echo Starting service...
python windows_service_installer.py start
if %errorlevel% neq 0 (
    echo ERROR: Service start failed
    pause
    exit /b 1
)

echo.
echo ================================
echo SUCCESS: Configurable Service installed and started!
echo ================================
echo.
echo Service Name: ETLConfigurableWatcher
echo Display Name: ETL Configurable Pattern-Based File Watcher
echo Configuration: config/pattern_config.yaml
echo.
echo Management commands:
echo   start_configurable_service.bat    - Start the service
echo   stop_configurable_service.bat     - Stop the service
echo   check_configurable_service.bat    - Check service status
echo   uninstall_configurable_service.bat - Remove the service
echo   edit_config.bat                   - Edit configuration
echo.
echo Monitoring:
echo   View logs: type logs\etl_configurable_service.log
echo   Celery: http://localhost:5555
echo   Services: services.msc
echo.
pause