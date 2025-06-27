#!/usr/bin/env python
# coding: utf-8

# In[11]:


import os
import pandas as pd
import numpy as np
import re
import time
from datetime import datetime
from pathlib import Path

# Configuration
INPUT_DIR = os.path.join("extracted_tables_FTUSA")
OUTPUT_DIR = os.path.join("fully_cleaned_tables_FTUSA")
LOG_FILE = os.path.join(OUTPUT_DIR, "log_cleaning_FTUSA.txt")

def setup_directories():
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w') as f:
            f.write("filename|status|timestamp|output_file\n")

def get_processed_files():
    processed = {}
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            for line in f.readlines()[1:]:
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
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a') as f:
        f.write(f"{filename}|{status}|{timestamp}|{output_file or ''}\n")

def needs_processing(input_file, processed_files):
    if not os.path.exists(input_file):
        return False
    filename = os.path.basename(input_file)
    if filename not in processed_files:
        return True
    input_mtime = os.path.getmtime(input_file)
    log_time = datetime.strptime(processed_files[filename]['timestamp'], "%Y-%m-%d %H:%M:%S").timestamp()
    return input_mtime > log_time

def detecter_ligne_nulle_et_titre(df_brut):
    def est_vide(cell):
        return str(cell).strip().lower() in ["", "nan", "-", "–", "—"]
    for idx, row in df_brut.iterrows():
        if all(est_vide(cell) for cell in row):
            for prev_idx in range(idx - 1, -1, -1):
                prev_row = df_brut.iloc[prev_idx]
                non_vides = prev_row[~prev_row.apply(est_vide)]
                if len(non_vides) == 1:
                    col_idx = non_vides.index[0]
                    return (idx, str(non_vides.iloc[0]), str(df_brut.columns[col_idx]))
            return (idx, None, None)
    return (None, None, None)

def generer_nom_fichier_conforme(df_brut, ligne_nulle_idx):
    main_title = ""
    sub_title = ""
    if ligne_nulle_idx is not None:
        for i in range(ligne_nulle_idx - 1, max(-1, ligne_nulle_idx - 4), -1):
            row = df_brut.iloc[i]
            non_vides = row[row.notna() & (row != '')]
            if len(non_vides) > 0:
                text = ' '.join(str(x) for x in non_vides if str(x).strip())
                if not main_title:
                    main_title = text
                elif not sub_title and text != main_title:
                    sub_title = text
    def clean_text(t):
        t = re.sub(r'[\\/*?:"<>|]', " ", t)
        t = re.sub(r'\s+', ' ', t).strip()
        return t[:100]
    main_title = clean_text(main_title) if main_title else "Donnees"
    sub_title = clean_text(sub_title) if sub_title else ""
    if main_title and sub_title:
        return f"{sub_title} - {main_title}.xlsx"
    elif main_title:
        return f"{main_title}.xlsx"
    else:
        return "Donnees.xlsx"

def nettoyer_dataframe(df_brut, ligne_nulle_idx):
    if ligne_nulle_idx is None or ligne_nulle_idx + 1 >= len(df_brut):
        return pd.DataFrame()
    df = df_brut.iloc[ligne_nulle_idx + 1:].copy()
    if len(df) > 0:
        df.columns = df.iloc[0]
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = [str(col).strip() for col in df.columns]
    return df

def renommer_colonne_speciale(df):
    if df.empty or len(df.columns) == 0:
        return df
    premiere_col = df.columns[0]
    valeurs_compagnie = {"STAR", "MAGHREBIA", "GAT"}
    sample = df.iloc[:50, 0].astype(str).str.strip()
    if any(val in sample.values for val in valeurs_compagnie):
        nouveau_nom = "Compagnie d'assurance"
    else:
        nouveau_nom = "indicateur"
    return df.rename(columns={premiere_col: nouveau_nom})

def normaliser_chiffres_strict(df):
    def convertir(v):
        if pd.isna(v) or v == '':
            return v
        try:
            return int(str(v).replace(' ', ''))
        except:
            return v
    return df.applymap(convertir)

def garantir_nom_unique(nom_base, output_dir):
    base, ext = os.path.splitext(nom_base)
    compteur = 1
    nom_final = nom_base
    while os.path.exists(os.path.join(output_dir, nom_final)):
        nom_final = f"{base}_{compteur}{ext}"
        compteur += 1
    return nom_final

def process_ftusa_file(input_path, output_dir):
    filename = os.path.basename(input_path)
    try:
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        try:
            df = pd.read_excel(input_path, header=None, engine='openpyxl')
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {str(e)}")
        ligne_nulle_idx, _, _ = detecter_ligne_nulle_et_titre(df)
        if ligne_nulle_idx is None:
            print(f"[WARN] No empty line found in {filename}. Fallback: skip first row.")
            ligne_nulle_idx = 0
        nom_base = generer_nom_fichier_conforme(df, ligne_nulle_idx)
        nom_final = garantir_nom_unique(nom_base, output_dir)
        df_clean = nettoyer_dataframe(df, ligne_nulle_idx)
        df_clean = renommer_colonne_speciale(df_clean)
        df_final = normaliser_chiffres_strict(df_clean)
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, nom_final)
        try:
            df_final.to_excel(output_path, index=False, engine='openpyxl')
            return True, nom_final
        except Exception as e:
            raise IOError(f"Error saving file: {str(e)}")
    except Exception as e:
        print(f"ERROR processing {filename}: {str(e)}")
        return False, None

def main():
    print("Setting up FTUSA directories...")
    setup_directories()
    print("Loading processed files log...")
    processed_files = get_processed_files()
    total_files = 0
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    print("\nStarting FTUSA file processing...")
    for root, dirs, files in os.walk(INPUT_DIR):
        for filename in files:
            if filename.lower().endswith('.xlsx'):
                total_files += 1
                input_path = os.path.join(root, filename)
                if not needs_processing(input_path, processed_files):
                    print(f"[SKIP] Already processed: {filename}")
                    skipped_count += 1
                    continue
                print(f"Processing: {filename}")
                success, output_filename = process_ftusa_file(input_path, OUTPUT_DIR)
                if success:
                    log_processing(filename, "SUCCESS", output_filename)
                    processed_count += 1
                    print(f"--> Saved as: {output_filename}")
                else:
                    log_processing(filename, "FAILED")
                    failed_count += 1
                    print("--> Processing failed")
    print("\nFTUSA Processing summary:")
    print(f"Total files found: {total_files}")
    print(f"Successfully processed: {processed_count}")
    print(f"Skipped (already processed): {skipped_count}")
    print(f"Failed: {failed_count}")
    output_files = [f for f in os.listdir(OUTPUT_DIR) if f.lower().endswith('.xlsx')]
    print(f"\nFound {len(output_files)} cleaned files in output directory")

if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")

