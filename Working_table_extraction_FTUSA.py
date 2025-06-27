#!/usr/bin/env python
# coding: utf-8

# In[5]:


import os
import re
import pandas as pd
import numpy as np
import pdfplumber
import PyPDF2
import camelot
import warnings
from datetime import datetime
from typing import Tuple, Optional

# Configuration de Ghostscript
#camelot.__gs_path__ = r"C:\Program Files\gs\gs10.05.0\bin\gswin64c.exe"
# ===== GHOSTSCRIPT CONFIGURATION =====
GS_PATH = r"C:\Program Files\gs\gs10.05.1\bin\gswin64c.exe"
#camelot.__gs_path__ = r"C:\Program Files\gs\gs10.05.0\bin\gswin64c.exe"

# Verify Ghostscript exists
if not os.path.exists(GS_PATH):
    print(f"ERROR: Ghostscript not found at {GS_PATH}")
    print("Please install Ghostscript 10.05.1 from https://www.ghostscript.com/")
    sys.exit(1)

# Configure Camelot
try:
    import camelot
    camelot.__gs_path__ = GS_PATH
    print(f"\nSuccessfully configured Camelot with Ghostscript at: {GS_PATH}")
except Exception as e:
    print(f"\nERROR configuring Camelot: {str(e)}")
    sys.exit(1)


def extract_year_from_filename(filename: str) -> Optional[str]:
    year_match = re.search(r'(?:20)?(\d{2})', filename)
    if year_match:
        return f"20{year_match.group(1)}" if len(year_match.group(1)) == 2 else year_match.group(0)
    return None


def find_annexes_range(pdf_path: str) -> Optional[Tuple[int, int]]:
    filename = os.path.basename(pdf_path)
    year = extract_year_from_filename(filename)
    if not year:
        raise ValueError(f"Could not extract year from filename: {filename}")

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


def extract_tables_from_pdf(pdf_path, start_page, end_page, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for page_num in range(start_page, end_page + 1):
        print(f"\n{'='*50}")
        print(f"PROCESSING PAGE {page_num}")
        print(f"{'='*50}")

        try:
            tables_lattice = camelot.read_pdf(pdf_path, pages=str(page_num), flavor='lattice')
            if tables_lattice and tables_lattice[0].df.size > 0:
                tables = tables_lattice
                print(f"Lattice mode found {len(tables)} table(s) on page {page_num}")
            else:
                tables_stream = camelot.read_pdf(
                    pdf_path,
                    pages=str(page_num),
                    flavor='stream',
                    edge_tol=100,
                    row_tol=10
                )
                if tables_stream and tables_stream[0].df.size > 0:
                    tables = tables_stream
                    print(f"Stream mode found {len(tables)} table(s) on page {page_num}")
                else:
                    print("No tables found on this page.")
                    continue

            for i, table in enumerate(tables):
                df = table.df.replace('', np.nan).dropna(how='all').dropna(axis=1, how='all')
                if not df.empty:
                    filename = os.path.splitext(os.path.basename(pdf_path))[0]
                    output_file = os.path.join(output_dir, f"{filename}_page_{page_num}_table_{i+1}.xlsx")
                    df.to_excel(output_file, index=False, header=False)
                    print(f"Saved table {i+1} to {output_file}")
                else:
                    print(f"Table {i+1} on page {page_num} is empty after cleaning.")

        except Exception as e:
            print(f"Error extracting tables from page {page_num}: {e}")


def extract_tables_with_pypdf2_pdfplumber(pdf_path, start_page, end_page, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_idx in range(start_page - 1, end_page):
                page_num = page_idx + 1
                print(f"\n{'='*50}")
                print(f"PROCESSING PAGE {page_num}")
                print(f"{'='*50}")

                page = pdf.pages[page_idx]
                tables = page.extract_tables()

                if tables:
                    for i, table in enumerate(tables):
                        df = pd.DataFrame(table)
                        if len(df) > 1:
                            df.columns = df.iloc[0]
                            df = df.iloc[1:]

                        df = df.replace('', np.nan).dropna(how='all').dropna(axis=1, how='all')

                        if not df.empty:
                            filename = os.path.splitext(os.path.basename(pdf_path))[0]
                            output_file = os.path.join(output_dir, f"{filename}_page_{page_num}_table_{i+1}.xlsx")
                            df.to_excel(output_file, index=False)
                            print(f"Saved table {i+1} to {output_file}")
                        else:
                            print(f"Table {i+1} on page {page_num} is empty.")
                else:
                    print("No tables found.")

    except Exception as e:
        print(f"Error with pdfplumber extraction: {e}")


# === PARAMÈTRES GLOBAUX ===
# Old absolute paths (Windows)
# source_folder = r"C:/Users/DELL/SCRAPING_PFE/scraped_pdfs_final_3/Rapports_FTUSA"
# export_root = r"C:/Users/DELL/SCRAPING_PFE/extracted_tables_FTUSA"

# New relative paths (cross-platform)
source_folder = os.path.join("scraped_pdfs_final_3", "Rapports_FTUSA")
export_root = "extracted_tables_FTUSA"

log_file_path = os.path.join(source_folder, "log_traitements.txt")
method = "camelot"

# === Fichiers à traiter ===
all_pdfs = [f for f in os.listdir(source_folder) if f.lower().endswith(".pdf")]
if not os.path.exists(log_file_path):
    with open(log_file_path, 'w', encoding='utf-8') as f:
        f.write("")

with open(log_file_path, 'r', encoding='utf-8') as f:
    processed_files = set(line.strip().split(" | ")[0] for line in f.readlines())

# === TRAITEMENT AUTOMATIQUE DES NOUVEAUX FICHIERS ===
for pdf_filename in all_pdfs:
    if pdf_filename in processed_files:
        print(f"[SKIP] {pdf_filename} déjà traité.")
        continue

    pdf_path = os.path.join(source_folder, pdf_filename)
    print(f"\n[TRAITEMENT] {pdf_filename}...")

    try:
        # Extraire année et créer sous-dossier de sortie
        year = extract_year_from_filename(pdf_filename)
        if not year:
            print(f"[ERREUR] Impossible de détecter l'année pour : {pdf_filename}")
            continue

        output_dir_year = os.path.join(export_root, year, "raw_extracted_tables")
        os.makedirs(output_dir_year, exist_ok=True)

        # Détection des pages annexes
        start_page, end_page = find_annexes_range(pdf_path)
        print(f"Annexes détectées: pages {start_page} à {end_page}")

        # Extraction
        if method == "camelot":
            try:
                extract_tables_from_pdf(pdf_path, start_page, end_page, output_dir_year)
            except Exception as e:
                print(f"Camelot a échoué : {e}")
                print("Passage à pdfplumber...")
                extract_tables_with_pypdf2_pdfplumber(pdf_path, start_page, end_page, output_dir_year)
        else:
            extract_tables_with_pypdf2_pdfplumber(pdf_path, start_page, end_page, output_dir_year)

        # Mise à jour du log avec date et heure
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_file_path, 'a', encoding='utf-8') as f:
            f.write(f"{pdf_filename} | {timestamp}\n")
        print(f"[LOG] Ajouté au log : {pdf_filename} à {timestamp}")

    except Exception as e:
        print(f"[ERREUR] Échec du traitement de {pdf_filename} : {e}")



# In[ ]:





# In[ ]:




