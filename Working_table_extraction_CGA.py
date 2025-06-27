#!/usr/bin/env python
# coding: utf-8

# In[5]:


import os
import re
import sys
import pandas as pd
import numpy as np
import pdfplumber
import camelot
import warnings
from datetime import datetime
from typing import Tuple, Optional

# Fix Windows console encoding
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Configuration
GS_PATH = r"C:\Program Files\gs\gs10.05.1\bin\gswin64c.exe"

# Verify Ghostscript exists
if not os.path.exists(GS_PATH):
    print(f"ERROR: Ghostscript not found at {GS_PATH}")
    print("Please install Ghostscript 10.05.1 from https://www.ghostscript.com/")
    sys.exit(1)

# Configure Camelot
try:
    camelot.__gs_path__ = GS_PATH
    print(f"\nSuccessfully configured Camelot with Ghostscript at: {GS_PATH}")
except Exception as e:
    print(f"\nERROR configuring Camelot: {str(e)}")
    sys.exit(1)

def safe_print(message):
    """Safely print Unicode characters in Windows"""
    try:
        print(message)
    except UnicodeEncodeError:
        print(message.encode('ascii', 'replace').decode('ascii'))

def find_section_boundaries(pdf_path: str) -> Tuple[Optional[int], Optional[int]]:
    filename = os.path.basename(pdf_path)
    year_match = re.search(r'(20\d{2})', filename)
    if not year_match:
        raise ValueError(f"Could not extract year from filename: {filename}")

    year = year_match.group(1)
    end_marker_pattern = re.compile(rf"TUNISIAN\s+INSURANCE\s+MARKET\s+IN\s+{year}", re.IGNORECASE)

    with pdfplumber.open(pdf_path) as pdf:
        last_annexe_page = None
        for i, page in enumerate(pdf.pages):
            text = (page.extract_text() or "").replace("\n", " ")
            if "annexe" in text.lower() or "annexes" in text.lower():
                last_annexe_page = i + 1

        if last_annexe_page is None:
            raise ValueError("No 'annexe' or 'annexes' markers found in document")

        start_page = last_annexe_page + 1
        end_page = None
        
        for i in range(start_page - 1, len(pdf.pages)):
            text = (pdf.pages[i].extract_text() or "").replace("\n", " ")
            if end_marker_pattern.search(text):
                end_page = i
                break

        if end_page is None:
            raise ValueError(f"End marker not found: 'TUNISIAN INSURANCE MARKET IN {year}'")

        return (start_page, end_page + 1)

def extract_tables(pdf_path: str, start_page: int, end_page: int, output_dir: str) -> int:
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    total_tables = 0
    
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        safe_print(f"\nStarting table extraction (pages {start_page}-{end_page} of {total_pages})")
        
        for page_num in range(start_page, end_page + 1):
            if page_num > total_pages:
                safe_print(f"Page {page_num} exceeds document length")
                continue
                
            safe_print(f"\n{'='*50}")
            safe_print(f"PROCESSING PAGE {page_num}")
            safe_print(f"{'='*50}")
            
            tables = []
            try:
                # Try lattice mode first
                lattice_tables = camelot.read_pdf(
                    pdf_path,
                    pages=str(page_num),
                    flavor='lattice',
                    strip_text='\n',
                    suppress_stdout=True
                )
                tables.extend([t.df for t in lattice_tables if not t.df.empty])
                
                # Fall back to stream mode if no tables found
                if not tables:
                    stream_tables = camelot.read_pdf(
                        pdf_path,
                        pages=str(page_num),
                        flavor='stream',
                        edge_tol=500,
                        row_tol=10,
                        suppress_stdout=True
                    )
                    tables.extend([t.df for t in stream_tables if not t.df.empty])
                
                # Fall back to pdfplumber if Camelot fails
                if not tables:
                    page = pdf.pages[page_num - 1]
                    plumber_tables = page.extract_tables()
                    if plumber_tables:
                        tables = [pd.DataFrame(table).replace(r'^\s*$', np.nan, regex=True) 
                                for table in plumber_tables]
                
                # Save extracted tables
                for i, table in enumerate(tables, 1):
                    if not table.empty:
                        # Clean the table
                        table = table.replace('', np.nan).dropna(how='all').dropna(axis=1, how='all')
                        if not table.empty:
                            filename = os.path.splitext(os.path.basename(pdf_path))[0]
                            output_file = os.path.join(output_dir, f"{filename}_page_{page_num}_table_{i}.xlsx")
                            table.to_excel(output_file, index=False, header=False)
                            safe_print(f"[OK] Extracted table {i} ({table.shape[1]} cols x {table.shape[0]} rows)")
                            safe_print(f"Saved to: {output_file}")
                            total_tables += 1
                        else:
                            safe_print(f"[SKIP] Empty table {i} after cleaning")
                    else:
                        safe_print(f"[SKIP] Empty table {i}")
                        
            except Exception as e:
                safe_print(f"[ERROR] Processing page {page_num}: {str(e)}")
                continue
                
    safe_print(f"\nExtraction complete! Total tables extracted: {total_tables}")
    return total_tables

def process_pdf(pdf_path: str, output_base_dir: str) -> None:
    filename = os.path.basename(pdf_path)
    safe_print(f"\n\n--- Processing file: {filename} ---")
    
    try:
        # Extract year and create output directory
        year_match = re.search(r'(20\d{2})', filename)
        if not year_match:
            safe_print(f"[ERROR] Could not detect year for: {filename}")
            return
            
        year = year_match.group(1)
        output_dir = os.path.join(output_base_dir, year, "raw_extracted_tables")
        os.makedirs(output_dir, exist_ok=True)
        
        # Find annexes range
        start_page, end_page = find_section_boundaries(pdf_path)
        safe_print(f"Annexes detected: pages {start_page} to {end_page}")
        
        # Extract tables
        extract_tables(pdf_path, start_page, end_page, output_dir)
        
    except Exception as e:
        safe_print(f"[ERROR] Failed to process {filename}: {str(e)}")

def main():
    # Configure paths
    pdf_dir = os.path.join("scraped_pdfs_final_3", "Rapports_CGA")
    output_base_dir = "extracted_tables_CGA"
    log_file = os.path.join(pdf_dir, "log_traitements_rapports_CGA.txt")
    
    # Create output directory if not exists
    os.makedirs(output_base_dir, exist_ok=True)

    # Get processed files
    if os.path.exists(log_file):
        with open(log_file, 'r', encoding='utf-8') as f:
            processed_files = set(line.strip().split('|')[0].strip() for line in f if line.strip())
    else:
        processed_files = set()

    # Process new files
    for filename in os.listdir(pdf_dir):
        if filename.lower().endswith(".pdf") and filename not in processed_files:
            pdf_path = os.path.join(pdf_dir, filename)
            process_pdf(pdf_path, output_base_dir)
            
            # Update log
            with open(log_file, 'a', encoding='utf-8') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{filename} | {timestamp}\n")
            safe_print(f"[LOG] Added to log: {filename} at {timestamp}")
        else:
            safe_print(f"[SKIP] Already processed: {filename}")

if __name__ == "__main__":
    main()

