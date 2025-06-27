#!/usr/bin/env python
import os
import argparse
import pandas as pd
import multiprocessing
from datetime import datetime

def process_file_wrapper(args):
    input_path, output_dir, processor = args
    try:
        return processor(input_path, output_dir)
    except Exception as e:
        print(f"Error processing {os.path.basename(input_path)}: {str(e)}")
        return False, None

def optimized_process_files(source_dir, dest_dir, processor, max_workers=4):
    file_args = [
        (os.path.join(root, file), dest_dir, processor)
        for root, _, files in os.walk(source_dir)
        for file in files if file.lower().endswith('.xlsx')
    ]

    if not file_args:
        print(f"No files found in {source_dir}")
        return 0, 0

    with multiprocessing.Pool(max_workers) as pool:
        results = pool.map(process_file_wrapper, file_args)

    processed = sum(1 for r in results if r and r[0])
    failed = sum(1 for r in results if not r or not r[0])
    return processed, failed

def main():
    parser = argparse.ArgumentParser(description="Optimized Table Processing")
    parser.add_argument('--cga', action='store_true', help='Process CGA tables only')
    parser.add_argument('--ftusa', action='store_true', help='Process FTUSA tables only')
    args = parser.parse_args()

    from table_layout_mod_CGA_FINAL import process_cga_file
    from table_layout_mod_FTUSA import process_ftusa_file

    os.makedirs("fully_cleaned_tables_CGA", exist_ok=True)
    os.makedirs("fully_cleaned_tables_FTUSA", exist_ok=True)

    start_time = datetime.now()

    if args.cga:
        print("Processing CGA tables...")
        cga_processed, cga_failed = optimized_process_files(
            "extracted_tables_CGA", "fully_cleaned_tables_CGA", process_cga_file
        )
        print(f"CGA done: {cga_processed} processed, {cga_failed} failed")

    if args.ftusa:
        print("\nProcessing FTUSA tables...")
        ftusa_processed, ftusa_failed = optimized_process_files(
            "extracted_tables_FTUSA", "fully_cleaned_tables_FTUSA", process_ftusa_file
        )
        print(f"FTUSA done: {ftusa_processed} processed, {ftusa_failed} failed")

    if not args.cga and not args.ftusa:
        print("No argument provided. Use --cga or --ftusa")

    print(f"\nTotal time: {(datetime.now() - start_time).total_seconds():.2f} seconds")

if __name__ == "__main__":
    main()