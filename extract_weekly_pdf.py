import PyPDF2
import os

# Path to the weekly example PDF
pdf_path = r"utils/Apresentação/Automatizado/relatorio_TGF-3D93_20250901_20250907.pdf"

try:
    with open(pdf_path, 'rb') as file:
        reader = PyPDF2.PdfReader(file)
        print(f"Number of pages: {len(reader.pages)}")
        
        # Extract text from first few pages
        for i in range(min(3, len(reader.pages))):
            print(f"\n--- Page {i+1} ---")
            text = reader.pages[i].extract_text()
            print(text[:2000])  # Print first 2000 characters
except Exception as e:
    print(f"Error reading PDF: {e}")