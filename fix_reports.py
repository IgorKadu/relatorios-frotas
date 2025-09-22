# Script to fix the reports.py file by adding the missing _format_distance method

import os

# Read the current file
file_path = r"c:\Users\Administrator\Downloads\relatorios-frotas\relatorios-frotas\app\reports.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Check if the method already exists
if '_format_distance' not in content:
    # Find the position to insert the method
    insert_pos = content.find('    def _get_analyzer(self):')
    
    if insert_pos != -1:
        # Find the end of the _get_analyzer method
        method_end = content.find('    def setup_custom_styles(self):', insert_pos)
        
        if method_end != -1:
            # Insert the missing method
            method_to_add = '''
    
    def _format_distance(self, km_value: float, decimals: int = 1) -> str:
        """Formata distância de modo inteligente: usa metros quando < 1 km, caso contrário km."""
        try:
            if km_value is None:
                return '0 m'
            if km_value < 0:
                km_value = 0
            if km_value < 1:
                metros = round(km_value * 1000)
                return f"{metros:,} m".replace(',', '.')
            fmt = f"{{:,.{decimals}f}} km"
            return fmt.format(km_value).replace(',', 'X').replace('.', ',').replace('X', '.')
        except Exception:
            try:
                return f"{float(km_value):.{decimals}f} km"
            except Exception:
                return '0 km'
'''
            # Insert the method between _get_analyzer and setup_custom_styles
            new_content = content[:method_end] + method_to_add + '\n' + content[method_end:]
            
            # Write the fixed content back to the file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            print("✅ Successfully added _format_distance method to reports.py")
        else:
            print("❌ Could not find setup_custom_styles method")
    else:
        print("❌ Could not find _get_analyzer method")
else:
    print("✅ _format_distance method already exists in reports.py")