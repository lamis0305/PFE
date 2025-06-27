#!/usr/bin/env python
# coding: utf-8

# In[2]:


import os
import pandas as pd
import re
import time
from datetime import datetime
from pathlib import Path

# Configuration
INPUT_DIR = os.path.join("extracted_tables_CGA")
OUTPUT_DIR = os.path.join("fully_cleaned_tables_CGA")
LOG_FILE = os.path.join(OUTPUT_DIR, "log_cleaning_CGA.txt")

def setup_directories():
    """Create necessary directories if they don't exist"""
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w') as f:
            f.write("filename|status|timestamp|output_file\n")

def get_processed_files():
    """Get dictionary of already processed files"""
    processed = {}
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            for line in f.readlines()[1:]:  # Skip header
                if line.strip():
                    parts = line.strip().split('|')
                    if len(parts) >= 3:
                        processed[parts[0]] = {
                            'status': parts[1],
                            'timestamp': parts[2],
                            'output_file': parts[3] if len(parts) > 3 else None
                        }
    return processed

def log_processing(filename, status, output_file=None):
    """Log a processing attempt"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a') as f:
        f.write(f"{filename}|{status}|{timestamp}|{output_file or ''}\n")

def needs_processing(input_file, processed_files):
    """Check if file needs processing"""
    if not os.path.exists(input_file):
        return False
        
    filename = os.path.basename(input_file)
    
    # New file needs processing
    if filename not in processed_files:
        return True
        
    # File modified since last processing
    input_mtime = os.path.getmtime(input_file)
    log_time = datetime.strptime(
        processed_files[filename]['timestamp'], 
        "%Y-%m-%d %H:%M:%S"
    ).timestamp()
    
    return input_mtime > log_time

def clean_filename(text):
    """Sanitize filename"""
    text = str(text).strip()
    text = re.sub(r'[\\/*?:"<>|]', "", text)
    text = re.sub(r'\s+', '_', text)
    return text[:100]

def process_cga_file(input_path, output_dir):
    """Process a single CGA file with enhanced error handling"""
    filename = os.path.basename(input_path)
    try:
        # Verify input file exists
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        # Read with explicit handling of potential errors
        try:
            df = pd.read_excel(input_path, header=None, engine='openpyxl')
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {str(e)}")
        
        # Step 1: Remove rows containing 'annexe'
        df = df[~df.apply(lambda row: row.astype(str).str.lower().str.contains("annexe").any(), axis=1)]
        df = df.reset_index(drop=True)

        # Step 2: Cut at first row with >15 words
        def count_words(row):
            words = []
            for cell in row:
                if isinstance(cell, str):
                    words += cell.strip().split()
            return len(words)
            
        cut_idx = df[df.apply(count_words, axis=1) > 15].index.min()
        if pd.notna(cut_idx):
            df = df.iloc[:cut_idx]
        df = df.reset_index(drop=True)

        # Step 3: Rename file using one-cell header if available
        rename_header = df.iloc[0].dropna().astype(str).tolist()
        if len(rename_header) == 1:
            new_name = clean_filename(rename_header[0]) + ".xlsx"
            df = df[1:].reset_index(drop=True)
        else:
            new_name = filename

        # Step 4: Remove row if it contains (M.D)
        if "(M.D)" in df.iloc[0].astype(str).values:
            df = df[1:].reset_index(drop=True)

        # Step 5: Build header from rows 0, 1, and optionally 2
        header_rows = []
        max_header_rows = 3
        for offset in range(max_header_rows):
            if offset >= len(df):
                break
            row = df.iloc[offset]
            row_str = row.astype(str)
            numeric_count = sum(row_str.str.contains(r"\d").fillna(False))
            if offset == 2 and numeric_count >= len(row) // 2:
                break
            header_rows.append(row.tolist())

        # Build final header
        final_header = []
        for col in range(df.shape[1]):
            parts = [
                str(header_rows[row][col]).strip()
                for row in range(len(header_rows))
                if col < len(header_rows[row]) and str(header_rows[row][col]).strip() != ""
            ]
            final_header.append(" ".join(parts).strip())

        # Apply header and remove header rows
        df.columns = final_header
        df = df[len(header_rows):].reset_index(drop=True)

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate output path
        output_filename = clean_filename(filename)  # Or your naming logic
        output_path = os.path.join(output_dir, output_filename)
        
        # Save with explicit error handling
        try:
            df.to_excel(output_path, index=False, engine='openpyxl')
            return True, output_filename
        except Exception as e:
            raise IOError(f"Error saving file: {str(e)}")
            
    except Exception as e:
        print(f"ERROR processing {filename}: {str(e)}")
        return False, None

def main():
    print("Setting up CGA directories...")
    setup_directories()
    
    print("Loading processed files log...")
    processed_files = get_processed_files()
    
    total_files = 0
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    
    print("\nStarting CGA file processing...")
    for root, dirs, files in os.walk(INPUT_DIR):
        for filename in files:
            if filename.lower().endswith('.xlsx'):
                total_files += 1
                input_path = os.path.join(root, filename)
                
                if not needs_processing(input_path, processed_files):
                    print(f"[SKIP] Already processed: {filename}")
                    skipped_count += 1
                    continue
                
                print(f"\nProcessing: {filename}")
                success, output_filename = process_cga_file(input_path, OUTPUT_DIR)
                
                if success:
                    log_processing(filename, "SUCCESS", output_filename)
                    processed_count += 1
                    print(f"-> Saved as: {output_filename}")
                else:
                    log_processing(filename, "FAILED")
                    failed_count += 1
                    print("-> Processing failed")
    
    print("\nCGA Processing summary:")
    print(f"Total files found: {total_files}")
    print(f"Successfully processed: {processed_count}")
    print(f"Skipped (already processed): {skipped_count}")
    print(f"Failed: {failed_count}")
    
    # Verify output
    output_files = [f for f in os.listdir(OUTPUT_DIR) if f.lower().endswith('.xlsx')]
    print(f"\nFound {len(output_files)} cleaned files in output directory")

if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")



# In[ ]:





# In[ ]:




