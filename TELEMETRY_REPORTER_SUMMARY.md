# Telemetry Reporter System Summary

## Overview
The Telemetry Reporter system is a comprehensive solution for processing vehicle telemetry data and generating professional PDF reports with data validation and consistency checks. The system implements all the requirements specified in the prompt.

## Key Features Implemented

### 1. Data Input and Filtering
- **Flexible Vehicle Selection**: Support for individual vehicles or "All" vehicles
- **Period Filtering**: Date range filtering with inclusive day calculation (correctly calculates days as `end_date - start_date + 1`)
- **Granularity Options**: Automatic report structure selection based on period length and vehicle count

### 2. Data Validation and Correction
- **Coherence Validation**: 
  - If `km_total > 0` then `max_speed > 0`
  - If `max_speed > 0` then `km_total > 0`
  - Automatic recalculation when inconsistencies are detected
- **Outlier Filtering**:
  - Speed > 220 km/h are ignored
  - GPS jumps > 500 km in short intervals are marked as outliers
  - Records with km > 0 and speed = 0 are handled appropriately
- **Data Correction Logic**:
  - Distance calculation using odometer or haversine formula
  - Speed calculation using raw data or instantaneous speed (distance/time)
  - Sensor inconsistency detection and handling

### 3. Report Generation
- **Adaptive Structure**:
  - Detailed mode for ≤ 7 days and ≤ 5 vehicles
  - Summary mode for longer periods or more vehicles
- **Multiple Output Formats**:
  - PDF reports with professional formatting
  - JSON with KPIs and processed data
  - CSV with detected anomalies
  - TXT processing logs
- **Quality Assurance**:
  - Built-in QA tests before finalizing reports
  - Limitations section in reports when data issues are detected

### 4. Technical Implementation
- **Modular Architecture**: Clean separation of concerns with dedicated modules
- **Extensible Design**: Easy to add new validation rules or report sections
- **Robust Error Handling**: Comprehensive error handling and logging
- **Command-line Interface**: Both programmatic and CLI usage supported

## Usage Examples

### Command-line Usage
```bash
python telemetry_reporter.py <csv_file> <start_date> <end_date> [output_dir] [client_name]
```

Example:
```bash
python telemetry_reporter.py data/telemetry.csv 2025-09-01 2025-09-07 reports "Client Name"
```

### Programmatic Usage
```python
from app.telemetry_reporter import TelemetryReporter

reporter = TelemetryReporter()
result = reporter.generate_report_from_csv(
    csv_file_path="data/telemetry.csv",
    output_dir="reports",
    start_date=datetime(2025, 9, 1),
    end_date=datetime(2025, 9, 7),
    vehicles="Todos",
    client_name="Client Name"
)
```

## Files Generated
1. **PDF Report**: Professional report with all required sections
2. **JSON File**: Processed data and KPIs in machine-readable format
3. **CSV Anomalies**: Detected data inconsistencies for review
4. **TXT Log**: Processing details and system information

## Validation Rules Implemented
- All data coherence rules from the specification
- Outlier detection and filtering
- Automatic data correction when possible
- Clear marking of data issues when correction isn't possible
- Never inventing values - all outputs are based on real data or clearly marked corrections

## System Benefits
- **Data Integrity**: Ensures all outputs are consistent and reliable
- **Flexibility**: Handles various input formats and filtering options
- **Transparency**: Clear documentation of data sources and processing steps
- **Professional Output**: High-quality PDF reports suitable for business use
- **Automation**: Complete end-to-end processing with minimal manual intervention