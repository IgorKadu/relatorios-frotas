# Script to cleanly add the missing _format_distance method to the reports.py file

import os

# Read the current file
file_path = r"c:\Users\Administrator\Downloads\relatorios-frotas\relatorios-frotas\app\reports.py"

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Check if the method already exists
if '_format_distance' not in content:
    # Find the position to insert the method after _get_analyzer
    lines = content.split('\n')
    
    # Find the line with _get_analyzer method
    insert_line = -1
    for i, line in enumerate(lines):
        if 'def _get_analyzer(self):' in line:
            # Find the end of the method
            j = i + 1
            while j < len(lines):
                if lines[j].strip() == '' and j + 1 < len(lines) and lines[j + 1].startswith('    def '):
                    insert_line = j + 1
                    break
                j += 1
            break
    
    if insert_line != -1:
        # Insert the missing method
        method_to_add = '''    def _format_distance(self, km_value: float, decimals: int = 1) -> str:
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
        # Insert the method
        lines.insert(insert_line, method_to_add)
        
        # Write the fixed content back to the file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        print("✅ Successfully added _format_distance method to reports.py")
    else:
        print("❌ Could not find the correct position to insert the method")
else:
    print("✅ _format_distance method already exists in reports.py")