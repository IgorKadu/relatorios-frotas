#!/usr/bin/env python3
"""
Script to test the telemetry processing system
"""

import sys
import os

# Add the app directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.telemetry_system import TelemetryProcessingSystem

def main():
    print("🚀 Testing Telemetry Processing System")
    print("=" * 50)
    
    # Initialize the system
    system = TelemetryProcessingSystem()
    
    # Path to our test CSV file
    csv_file_path = os.path.join(os.path.dirname(__file__), 'data', 'comprehensive_test.csv')
    output_dir = os.path.join(os.path.dirname(__file__), 'reports')
    
    print(f"📄 Processing: {csv_file_path}")
    print(f"📂 Output directory: {output_dir}")
    print()
    
    # Check if the CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"❌ CSV file not found: {csv_file_path}")
        return
    
    # Process the CSV file and generate reports
    result = system.process_csv_and_generate_report(
        csv_file_path, 
        output_dir, 
        "Test Client"
    )
    
    if result['success']:
        print("✅ Processing completed successfully!")
        print()
        print("📤 Generated files:")
        for output_type, path in result['outputs'].items():
            print(f"   • {output_type}: {path}")
        
        # Show summary metrics
        processing_result = result['processing_result']
        distance_metrics = processing_result.get('distance_speed_metrics', {})
        trips = processing_result.get('trips', [])
        
        print()
        print("📈 Summary metrics:")
        print(f"   • Total distance: {distance_metrics.get('total_km', 0):.2f} km")
        print(f"   • Max speed: {distance_metrics.get('max_speed', 0):.2f} km/h")
        print(f"   • Number of trips: {len(trips)}")
        
        # Show QA test results
        qa_results = result['qa_results']
        print()
        print("🧪 QA Test Results:")
        passed_tests = 0
        total_tests = 0
        for test_name, test_result in qa_results.items():
            if test_name not in ['limitations', 'error']:
                total_tests += 1
                if test_result == 'passed':
                    passed_tests += 1
                    status = "✅"
                elif test_result == 'skipped':
                    status = "⏭️"
                else:
                    status = "❌"
                print(f"   {status} {test_name}: {test_result}")
        
        print(f"   Total: {passed_tests}/{total_tests} tests passed")
        
        # Show any limitations
        limitations = qa_results.get('limitations', [])
        if limitations:
            print()
            print("⚠️  Limitations identified:")
            for limitation in limitations:
                print(f"   • {limitation}")
    else:
        print(f"❌ Error during processing: {result['error']}")

if __name__ == "__main__":
    main()