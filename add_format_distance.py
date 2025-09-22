# Script to add the missing _format_distance method to the PDFReportGenerator class

# Read the file
with open(r'c:\Users\Administrator\Downloads\relatorios-frotas\relatorios-frotas\app\reports.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Define the method to add
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

# Find the exact location to insert the method (after _get_analyzer in PDFReportGenerator class)
# Look for the return statement in the PDFReportGenerator class
in_pdf_report_generator = False
for i, line in enumerate(lines):
    # Check if we're in the PDFReportGenerator class
    if 'class PDFReportGenerator:' in line:
        in_pdf_report_generator = True
    elif in_pdf_report_generator and 'class ' in line and 'PDFReportGenerator' not in line:
        # We've moved to a different class
        in_pdf_report_generator = False
    
    # Look for the return statement in the _get_analyzer method
    if in_pdf_report_generator and 'return self.analyzer' in line:
        # Check if the next non-empty line is setup_custom_styles
        j = i + 1
        while j < len(lines) and lines[j].strip() == '':
            j += 1
        if j < len(lines) and 'def setup_custom_styles(self):' in lines[j]:
            # Insert the method right after the return statement
            lines.insert(i+1, '\n')
            for k, method_line in enumerate(method_to_add):
                lines.insert(i+2+k, method_line)
            break

# Write the file back
with open(r'c:\Users\Administrator\Downloads\relatorios-frotas\relatorios-frotas\app\reports.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print("Method added successfully to PDFReportGenerator class!")