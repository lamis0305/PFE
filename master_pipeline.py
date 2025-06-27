import os
import subprocess
import time
from datetime import datetime

# Configuration
DOWNLOAD_SCRIPT = "download_pdfs_mailing_FINAL.ipynb"
CGA_EXTRACTION_SCRIPT = "Working_table_extraction_CGA.ipynb"
FTUSA_EXTRACTION_SCRIPT = "Working_table_extraction_FTUSA.ipynb"
CGA_LAYOUT_MOD_SCRIPT = "table_layout_mod_CGA_FINAL.ipynb"
FTUSA_LAYOUT_MOD_SCRIPT = "table_layout_mod_FTUSA.ipynb"

# Paths
PDF_DOWNLOAD_DIR = "scraped_pdfs_final_3"
CGA_PDF_DIR = os.path.join(PDF_DOWNLOAD_DIR, "Rapports_CGA")
FTUSA_PDF_DIR = os.path.join(PDF_DOWNLOAD_DIR, "Rapports_FTUSA")

# Email configuration
EMAIL_CONFIG = {
    'sender': 'reports.new.alerts@gmail.com',
    'password': 'cerp jauo kveh epip',
    'recipient': 'lamis.mokrani@esprit.tn',
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

def convert_notebook_to_script(notebook_path):
    try:
        subprocess.run(["jupyter", "nbconvert", "--to", "script", notebook_path], check=True)
        return notebook_path.replace(".ipynb", ".py")
    except subprocess.CalledProcessError as e:
        print(f"Error converting notebook {notebook_path} to script: {e}")
        return None

def run_script(script_path, args=None):
    if not os.path.exists(script_path):
        print(f"ERROR: Script not found: {script_path}")
        return False
    try:
        cmd = ["python", script_path]
        if args:
            cmd.extend(args)
        print(f"\n{'='*50}\nRUNNING: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=900)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("ERROR OUTPUT:")
            print(result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"\nSCRIPT FAILED: {script_path}")
        print(f"Exit Code: {e.returncode}")
        print("Output:")
        print(e.stdout)
        print("Errors:")
        print(e.stderr)
        return False
    except Exception as e:
        print(f"Unexpected error running {script_path}: {str(e)}")
        return False

def send_email_notification(subject, body):
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender']
        msg['To'] = EMAIL_CONFIG['recipient']
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender'], EMAIL_CONFIG['password'])
            server.send_message(msg)

        print("\nEmail notification sent successfully")
    except Exception as e:
        print(f"\nError sending email: {str(e)}")

def check_new_files(directory, log_file):
    if not os.path.exists(log_file):
        return [f for f in os.listdir(directory) if f.lower().endswith('.pdf')]
    with open(log_file, 'r') as f:
        processed_files = set(line.strip().split('|')[0].strip() for line in f if line.strip())
    current_files = set(f for f in os.listdir(directory) if f.lower().endswith('.pdf'))
    return list(current_files - processed_files)

def log_processed_file(log_file, filename):
    with open(log_file, 'a') as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"{filename} | {timestamp}\n")

def main():
    # Convert notebooks to scripts
    download_script = convert_notebook_to_script(DOWNLOAD_SCRIPT)
    cga_extraction_script = convert_notebook_to_script(CGA_EXTRACTION_SCRIPT)
    ftusa_extraction_script = convert_notebook_to_script(FTUSA_EXTRACTION_SCRIPT)
    cga_layout_script = convert_notebook_to_script(CGA_LAYOUT_MOD_SCRIPT)
    ftusa_layout_script = convert_notebook_to_script(FTUSA_LAYOUT_MOD_SCRIPT)

    if not all([download_script, cga_extraction_script, ftusa_extraction_script, cga_layout_script, ftusa_layout_script]):
        print("Error converting one or more notebooks to scripts")
        return

    # Step 1: Download
    print("="*50)
    print("STEP 1: Downloading new PDF reports")
    print("="*50)
    run_script(download_script)
    time.sleep(5)

    # Step 2: CGA Extraction
    print("\n" + "="*50)
    print("STEP 2: Processing CGA reports")
    print("="*50)
    cga_log_file = os.path.join(CGA_PDF_DIR, "log_traitements_rapports_CGA.txt")
    new_cga_files = check_new_files(CGA_PDF_DIR, cga_log_file)

    if new_cga_files:
        print(f"Found {len(new_cga_files)} new CGA files to process")
        run_script(cga_extraction_script)
    else:
        print("No new CGA files to process")

    # Step 3: CGA Layout Mod
    print("\n" + "="*50)
    print("STEP 3: Modifying CGA table layouts")
    print("="*50)
    run_script(cga_layout_script)

    # Step 4: FTUSA Extraction
    print("\n" + "="*50)
    print("STEP 4: Processing FTUSA reports")
    print("="*50)
    ftusa_log_file = os.path.join(FTUSA_PDF_DIR, "log_traitements.txt")
    new_ftusa_files = check_new_files(FTUSA_PDF_DIR, ftusa_log_file)

    if new_ftusa_files:
        print(f"Found {len(new_ftusa_files)} new FTUSA files to process")
        run_script(ftusa_extraction_script)
    else:
        print("No new FTUSA files to process")

    # Step 5: FTUSA Layout Mod
    print("\n" + "="*50)
    print("STEP 5: Modifying FTUSA table layouts")
    print("="*50)
    run_script(ftusa_layout_script)
    
    # Final status
    completion_message = (
        " Pipeline terminé avec succès\n\n"
        f"Nouveaux rapports CGA détectés : {len(new_cga_files)}\n"
        f"Nouveaux rapports FTUSA détectés : {len(new_ftusa_files)}\n"
        f"Fin à : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    send_email_notification("PDF Processing Complete", completion_message)

    # Step 6: Remplissage du fichier centralisé (données 2023)
        print("\n" + "="*50)
    print("STEP 6: Remplissage du fichier centralisé pour 2023")
    print("="*50)

    import shutil

    dossier_temp = "2023"
    dossier_ftusa = "fully_cleaned_tables_FTUSA"
    dossier_cga = "fully_cleaned_tables_CGA"

    mapping_fichiers = {
        "CHIFFRES D’AFFAIRES PAR BRANCHE & PAR ENTREPRISE - AFFAIRES DIRECTES & ACCEPTATIONS EXERCICE 2023.xlsx": [
            os.path.join(dossier_ftusa, "CHIFFRES D’AFFAIRES PAR BRANCHE & PAR ENTREPRISE - AFFAIRES DIRECTES & ACCEPTATIONS EXERCICE 2023.xlsx"),
            os.path.join(dossier_ftusa, "Donnees_21.xlsx")
        ],
        "COMPTE_D'EXPLOITATION_AFFAIRES DIRECTES & ACCEPTATIONS EXERCICE 2023.xlsx": [
            os.path.join(dossier_cga, "COMPTE_D'EXPLOITATION_AFFAIRES DIRECTES & ACCEPTATIONS EXERCICE 2023.xlsx"),
            os.path.join(dossier_ftusa, "Donnees_24.xlsx")
        ],
         "PRINCIPAUX_INDICATEURS_DU_SECTEUR_D’ASSURANCE_PAR_COMPAGNIE.xlsx": [
        os.path.join(dossier_cga, "PRINCIPAUX_INDICATEURS_DU_SECTEUR_D’ASSURANCE_PAR_COMPAGNIE.xlsx")
        ],
        "RESULTAT TECHNIQUE PAR BRANCHE & PAR ENTREPRISE - AFFAIRES DIRECTES & ACCEPTATIONS EXERCICE 2023.xlsx": [
            os.path.join(dossier_ftusa, "RESULTAT TECHNIQUE PAR BRANCHE & PAR ENTREPRISE - AFFAIRES DIRECTES & ACCEPTATIONS EXERCICE 2023.xlsx"),
            os.path.join(dossier_ftusa, "Donnees_23.xlsx")
        ],
        "SINISTRES REGLES PAR BRANCHE & PAR ENTREPRISE - AFFAIRES DIRECTES & ACCEPTATIONS EXERCICE 2023.xlsx": [
            os.path.join(dossier_ftusa, "SINISTRES REGLES PAR BRANCHE & PAR ENTREPRISE - AFFAIRES DIRECTES & ACCEPTATIONS EXERCICE 2023.xlsx"),
            os.path.join(dossier_ftusa, "Donnees_22.xlsx")
        ]
    }

    if not os.path.exists(dossier_temp):
        os.makedirs(dossier_temp)

    fichiers_manquants = []

    for nom_final, chemins_possibles in mapping_fichiers.items():
        trouve = False
        for chemin in chemins_possibles:
            if os.path.exists(chemin):
                shutil.copy(chemin, os.path.join(dossier_temp, nom_final))
                trouve = True
                break
        if not trouve:
            fichiers_manquants.append(nom_final)

    if not fichiers_manquants:
        print("✅ Tous les fichiers nécessaires ont été trouvés. Lancement du script de remplissage...")
        run_script("script_remplissage_complet_final.py")
    else:
        print("⚠️ Fichiers manquants pour le remplissage :")
        for f in fichiers_manquants:
            print(f" - {f}")
        print("Étape de remplissage ignorée.")

    # Nettoyage du dossier temporaire
    try:
        shutil.rmtree(dossier_temp)
    except Exception as e:
        print(f"Erreur lors de la suppression du dossier temporaire : {e}")

if __name__ == "__main__":
    main()
