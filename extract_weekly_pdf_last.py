import PyPDF2
import os

# Path to the weekly example PDF
pdf_path = r"utils/Apresentação/Automatizado/relatorio_TGF-3D93_20250901_20250907.pdf"

try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        print(f"Number of pages: {len(reader.pages)}")
        
        # Extract text from the last page
        if len(reader.pages) >= 4:
            print(f"\n--- Page 4 ---")
            text = reader.pages[3].extract_text()
            print(text[:2000])  # Print first 2000 characters
except Exception as e:
    print(f"Error reading PDF: {e}")