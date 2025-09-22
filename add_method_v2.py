# Script to add the missing _format_distance method to the PDFReportGenerator class

# Read the file
with open(r'c:\Users\Administrator\Downloads\relatorios-frotas\relatorios-frotas\app\reports.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Find the exact location to insert the method (after _get_analyzer)
method_to_add = [
    '    def _format_distance(self, km_value: float, decimals: int = 1) -> str:\n',
    '        """Formata distância de modo inteligente: usa metros quando < 1 km, caso contrário km."""\n',
    '        try:\n',
    '            if km_value is None:\n',
    '                return \'0 m\'\n',
    '            if km_value < 0:\n',
    '                km_value = 0\n',
    '            if km_value < 1:\n',
    '                metros = round(km_value * 1000)\n',
    '                return f"{metros:,} m".replace(\',\', \'.\')\n',
    '            fmt = f"{{:,.{decimals}f}} km"\n',
    '            return fmt.format(km_value).replace(\',\', \'X\').replace(\'.\', \',\').replace(\'X\', \'.\')\n',
    '        except Exception:\n',
    '            try:\n',
    '                return f"{float(km_value):.{decimals}f} km"\n',
    '            except Exception:\n',
    '                return \'0 km\'\n',
    '\n'
]

# Find the line with "return self.analyzer"
for i, line in enumerate(lines):
    if 'return self.analyzer' in line and lines[i+1].strip() == 'def setup_custom_styles(self):':
        # Insert the method right after the return statement
        lines.insert(i+1, '\n')
        for j, method_line in enumerate(method_to_add):
            lines.insert(i+2+j, method_line)
        break

# Write the file back
with open(r'c:\Users\Administrator\Downloads\relatorios-frotas\relatorios-frotas\app\reports.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("Method added successfully!")