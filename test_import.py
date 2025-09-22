import sys
import os

# Add the project directory to the Python path
project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_dir)

try:
    from app.reports import PDFReportGenerator, generate_consolidated_vehicle_report
    print("✅ Successfully imported reports module")
    
    # Try to create an instance of PDFReportGenerator
    generator = PDFReportGenerator()
    print("✅ Successfully created PDFReportGenerator instance")
    
    # Check if _format_distance method exists
    if hasattr(generator, '_format_distance'):
        print("✅ _format_distance method exists")
        # Test the method
        result = generator._format_distance(123.456, 2)
        print(f"✅ _format_distance test result: {result}")
    else:
        print("❌ _format_distance method is missing")
        
except Exception as e:
    print(f"❌ Error importing reports module: {e}")
    import traceback
    traceback.print_exc()