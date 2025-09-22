import datetime
import os
from app.reports import generate_consolidated_vehicle_report

# Test the consolidated report generation function
start_date = datetime.datetime(2025, 9, 1)
end_date = datetime.datetime(2025, 9, 7)
output_dir = 'reports'

# Generate a consolidated report
result = generate_consolidated_vehicle_report(
    start_date=start_date,
    end_date=end_date,
    output_dir=output_dir,
    vehicle_filter='TST-1234',
    cliente_nome='Test Client'
)

print(f"Report generation result: {result}")