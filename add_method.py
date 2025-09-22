# Script to add the missing _format_distance method to the PDFReportGenerator class

import re

# Read the file
with open(r'c:\Users\Administrator\Downloads\relatorios-frotas\relatorios-frotas\app\reports.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Define the method to add
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

# Find the exact location to insert the method (after _get_analyzer)
pattern = r'(return self\.analyzer\s+)'
replacement = f'\\1\n{method_to_add}\n    def setup_custom_styles(self):'

# Replace the content
new_content = re.sub(pattern, replacement, content, count=1)

# Write the file back
with open(r'c:\Users\Administrator\Downloads\relatorios-frotas\relatorios-frotas\app\reports.py', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("Method added successfully!")