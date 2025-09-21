# Sistema de Relatórios de Telemetria Veicular - Replit Setup

## Overview
This is a FastAPI-based vehicle telemetry reporting system that processes CSV data and generates comprehensive PDF reports with analytics, maps, and insights for fleet management.

## Recent Changes
- **Sept 20, 2025**: Successfully imported from GitHub and configured for Replit environment
- Installed Python 3.11 and all required dependencies including plotly
- Configured server to run on port 5000 with host 0.0.0.0 for Replit proxy
- Set up workflow for web application
- Initialized SQLite database successfully
- Configured deployment settings for autoscale production deployment
- **Sept 21, 2025**: Implemented enhanced adaptive PDF system with intelligent data validation
- Added comprehensive data quality rules to eliminate inconsistent telemetry data
- Implemented period-specific report strategies (Daily/Weekly ≤7 days, Medium-term 8-30 days, Monthly >30 days)
- All report generation now uses exclusively real database data with sophisticated validation
- Added intelligent highlights system for automatic identification of best/worst performing days and vehicles
- Fixed critical bug in weekly data aggregation for improved reliability

## Project Architecture
- **Backend**: FastAPI with SQLAlchemy (SQLite database)
- **Frontend**: Bootstrap 5 web interface with JavaScript
- **Analytics**: Pandas, Matplotlib, Plotly for data processing and visualization
- **Reports**: ReportLab for PDF generation, Folium for maps
- **Structure**:
  - `app/` - Main application code
    - `main.py` - FastAPI application and API endpoints
    - `models.py` - SQLAlchemy database models
    - `services.py` - TelemetryAnalyzer and ReportGenerator classes
    - `utils.py` - CSV processing utilities
    - `reports.py` - PDF report generation
  - `frontend/` - Static files and templates
  - `data/` - CSV uploads and SQLite database
  - `reports/` - Generated PDF reports

## User Preferences
- Application runs on port 5000 for Replit environment
- Uses 0.0.0.0 host binding for proxy compatibility
- SQLite database for development (can migrate to PostgreSQL for production)
- Portuguese language interface
- Responsive Bootstrap design

## Key Features
- CSV file upload and processing
- Vehicle telemetry analysis with operational periods
- Interactive maps with route visualization
- Speed analysis and alerts
- Fuel consumption estimates
- PDF report generation
- Dashboard with statistics
- Multi-vehicle fleet management

## Running the Application
The application starts automatically via the configured workflow:
- Server runs on http://0.0.0.0:5000
- Web interface accessible through Replit's webview
- Database initializes automatically on startup
- File uploads saved to data/uploads directory
- Reports generated in reports/ directory

## Deployment
Configured for autoscale deployment on Replit with:
- Command: `uvicorn app.main:app --host 0.0.0.0 --port 5000`
- Stateless web application suitable for auto-scaling
- No build step required