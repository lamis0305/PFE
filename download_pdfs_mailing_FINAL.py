#!/usr/bin/env python
# coding: utf-8

# In[1]:


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import re
import time
import requests
import hashlib
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

# Configuration
DOWNLOAD_DIR = "scraped_pdfs_final_3"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
DOWNLOAD_LOG = os.path.join(DOWNLOAD_DIR, "download_log.txt")
MAX_RETRIES = 3
INITIAL_TIMEOUT = 30  # seconds
BACKOFF_FACTOR = 2  # Multiply timeout by this factor on each retry
CHUNK_SIZE = 8192  # bytes

EMAIL_CONFIG = {
    'sender': 'reports.new.alerts@gmail.com',
    'password': 'cerp jauo kveh epip',
    'recipient': 'lamis.mokrani@esprit.tn',
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

def send_email_notification(new_reports):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender']
        msg['To'] = EMAIL_CONFIG['recipient']
        msg['Subject'] = f"Nouveaux rapports disponibles ({len(new_reports)} nouveaux)"

        body = "Les rapports suivants ont été téléchargés :\n\n"
        for report in new_reports:
            body += f"- {report['type']} - {report.get('company', '')}\n"
            body += f"  Titre: {report['title']}\n"
            body += f"  Fichier: {report['filename']}\n"
            body += f"  URL: {report['url']}\n\n"

        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender'], EMAIL_CONFIG['password'])
            server.send_message(msg)

        print("\nEmail de notification envoyé avec succès")
    except Exception as e:
        print(f"\nErreur lors de l'envoi de l'email: {str(e)}")

def setup_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    if not os.path.exists(DOWNLOAD_LOG):
        with open(DOWNLOAD_LOG, 'w') as f:
            f.write("url,filename,download_date,file_size,file_hash,type,company\n")

def create_subfolder(report_type, company):
    clean_type = re.sub(r'[^a-zA-Z0-9]', '_', report_type)
    subfolder_path = os.path.join(DOWNLOAD_DIR, clean_type)
    if company:
        clean_company = re.sub(r'[^a-zA-Z0-9]', '_', company)
        subfolder_path = os.path.join(subfolder_path, clean_company)
    os.makedirs(subfolder_path, exist_ok=True)
    return subfolder_path

def log_downloaded_file(pdf_info, file_path):
    file_size = os.path.getsize(file_path)
    file_hash = calculate_file_hash(file_path)
    with open(DOWNLOAD_LOG, 'a') as f:
        f.write(f"{pdf_info['url']},{os.path.basename(file_path)},"
                f"{datetime.now().isoformat()},{file_size},{file_hash},"
                f"{pdf_info.get('type', 'Unknown')},{pdf_info.get('company', 'Unknown')}\n")

def calculate_file_hash(file_path, chunk_size=8192):
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()

def is_file_already_downloaded(pdf_info):
    report_type = pdf_info.get('type', 'Unknown')
    company = pdf_info.get('company', 'Unknown')
    subfolder_path = os.path.join(DOWNLOAD_DIR,
                                  re.sub(r'[^a-zA-Z0-9]', '_', report_type),
                                  re.sub(r'[^a-zA-Z0-9]', '_', company))
    filename = pdf_info.get('filename', os.path.basename(pdf_info['url']))
    file_path = os.path.join(subfolder_path, filename)
    if os.path.exists(file_path):
        return True
    if os.path.exists(DOWNLOAD_LOG):
        with open(DOWNLOAD_LOG, 'r') as f:
            for line in f.readlines()[1:]:
                if line.startswith(pdf_info['url'] + ','):
                    return True
    return False

def extract_pdf_date(pdf_url, session=None):
    try:
        filename = unquote(os.path.basename(pdf_url))
        
        # Skip procurement notices
        if "appel_d_offre" in filename.lower():
            return None
            
        # Specific patterns for CGA reports
        cga_patterns = [
            r'RAP_CGA_FR_ANG_(\d{4})',
            r'RAPPORT_CGA_FR_ANG_(\d{4})',
            r'Rapport_FR-ANG_-_CGA_(\d{4})',
            r'rapport_annuel__FR_(\d{4})',
            r'Rapport_FR_(\d{4})'
        ]
        
        for pattern in cga_patterns:
            match = re.search(pattern, filename, re.IGNORECASE)
            if match:
                return datetime.strptime(match.group(1), "%Y")
                
        # Extract year from title if in standard format
        year_match = re.search(r'(20\d{2})', filename)
        if year_match:
            return datetime.strptime(year_match.group(1), "%Y")
            
        return None
        
    except Exception as e:
        print(f"  Erreur d'extraction de date: {str(e)}")
        return None

def download_pdf(pdf_info, force_redownload=False):
    if not pdf_info or 'url' not in pdf_info:
        print("  [ERROR] No PDF URL provided")
        return None
        
    # Skip if this is a procurement notice
    if "appel_d_offre" in pdf_info.get('filename', '').lower():
        print(f"  [SKIP] Procurement notice: {pdf_info['filename']}")
        return None
        
    report_type = pdf_info.get('type', 'Autres_Rapports')
    company = pdf_info.get('company', 'Divers')
    subfolder_path = create_subfolder(report_type, company)
    filename = pdf_info.get('filename', os.path.basename(pdf_info['url']))
    save_path = os.path.join(subfolder_path, filename)
    
    if not force_redownload and is_file_already_downloaded(pdf_info):
        print(f"  [INFO] File already exists: {filename}")
        return None
        
    headers = {
        'User-Agent': USER_AGENT,
        'Accept': 'application/pdf',
        'Accept-Encoding': 'identity'  # Disable compression to track progress
    }
    
    current_timeout = INITIAL_TIMEOUT
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"\n  [ATTEMPT {attempt + 1}/{MAX_RETRIES}] Downloading {filename}")
            print(f"  URL: {pdf_info['url']}")
            print(f"  Timeout: {current_timeout}s")
            
            with requests.Session() as session:
                session.headers.update(headers)
                
                # First, make a HEAD request to check content length
                try:
                    head_response = session.head(
                        pdf_info['url'],
                        allow_redirects=True,
                        timeout=current_timeout
                    )
                    head_response.raise_for_status()
                    
                    content_length = head_response.headers.get('content-length')
                    if content_length:
                        file_size = int(content_length)
                        if file_size > MAX_FILE_SIZE:
                            raise ValueError(f"File too large ({file_size/1024/1024:.2f}MB > {MAX_FILE_SIZE/1024/1024:.2f}MB)")
                        print(f"  File size: {file_size/1024/1024:.2f}MB")
                except Exception as e:
                    print(f"  [WARNING] HEAD request failed: {str(e)}")
                
                # Then make the GET request with streaming
                with session.get(
                    pdf_info['url'],
                    stream=True,
                    allow_redirects=True,
                    timeout=current_timeout
                ) as response:
                    response.raise_for_status()
                    
                    # Check content type
                    content_type = response.headers.get('content-type', '')
                    if 'pdf' not in content_type.lower():
                        raise ValueError(f"Unexpected content type: {content_type}")
                    
                    # Start download
                    downloaded_bytes = 0
                    start_time = time.time()
                    
                    with open(save_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                            if chunk:  # filter out keep-alive chunks
                                f.write(chunk)
                                downloaded_bytes += len(chunk)
                                
                                # Calculate progress
                                elapsed = time.time() - start_time
                                speed = downloaded_bytes / (1024 * 1024) / (elapsed + 0.0001)  # MB/s
                                
                                if content_length:
                                    progress = (downloaded_bytes / file_size) * 100
                                    print(f"  Downloading: {progress:.1f}% | {speed:.2f} MB/s", end='\r')
                    
                    # Verify download completed
                    if content_length and downloaded_bytes != file_size:
                        raise IOError(f"Incomplete download ({downloaded_bytes} of {file_size} bytes)")
                    
                    print(f"\n  [SUCCESS] Downloaded {downloaded_bytes/1024/1024:.2f}MB in {elapsed:.1f}s ({speed:.2f} MB/s)")
                    
                    # Verify file is actually a PDF
                    with open(save_path, 'rb') as f:
                        header = f.read(4)
                        if header != b'%PDF':
                            raise ValueError("Downloaded file is not a valid PDF")
                    
                    log_downloaded_file(pdf_info, save_path)
                    return save_path
                    
        except requests.exceptions.RequestException as e:
            last_exception = e
            print(f"  [ERROR] Download attempt {attempt + 1} failed: {str(e)}")
            
            # Clean up partial download
            if os.path.exists(save_path):
                try:
                    os.remove(save_path)
                except Exception as e:
                    print(f"  [WARNING] Could not delete partial file: {str(e)}")
            
            # Exponential backoff
            if attempt < MAX_RETRIES - 1:
                sleep_time = min(current_timeout, 60)  # Cap at 60 seconds
                print(f"  [INFO] Retrying in {sleep_time}s...")
                time.sleep(sleep_time)
                current_timeout *= BACKOFF_FACTOR
                
        except Exception as e:
            last_exception = e
            print(f"  [ERROR] Unexpected error: {str(e)}")
            break
    
    print(f"  [FAILED] Could not download after {MAX_RETRIES} attempts")
    if last_exception:
        print(f"  Last error: {str(last_exception)}")
    return None
    
def get_pdfs_from_page(url, keywords=None):
    pdf_files = []
    try:
        print(f"\nAnalyse du site : {url}")
        headers = {'User-Agent': USER_AGENT}
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(url, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text().strip()
            
            # Skip if not PDF or if procurement notice
            if not href.lower().endswith('.pdf') or "appel_d_offre" in link_text.lower():
                continue
                
            # Handle Google Drive links
            if "drive.google.com" in href:
                # Extract file ID from Google Drive URL
                file_id_match = re.search(r'/file/d/([^/]+)', href)
                if file_id_match:
                    file_id = file_id_match.group(1)
                    pdf_url = f"https://drive.google.com/uc?export=download&id={file_id}"
                else:
                    continue
            else:
                pdf_url = urljoin(url, href)
                
            # Extract year from title
            year_match = re.search(r'(20\d{2})', link_text)
            year = year_match.group(1) if year_match else None
            
            # Create filename
            if year:
                filename = f"Rapport_CGA_{year}.pdf"
            else:
                filename = os.path.basename(pdf_url)
                
            pdf_files.append({
                'url': pdf_url,
                'date': datetime.strptime(year, "%Y") if year else None,
                'filename': filename,
                'title': link_text,
                'context': ' '.join(link.find_parent().get_text().strip().split()[:20]),
                'source_url': url,
                'type': "Rapports_CGA",
                'company': ""
            })
            
        return pdf_files
        
    except Exception as e:
        print(f"Erreur lors de l'accès au site {url}: {str(e)}")
        return []

def get_ftusa_reports(url, keywords=None):
    try:
        print(f"\nAnalyse spécifique du site FTUSA : {url}")
        session = requests.Session()
        session.headers.update({'User-Agent': USER_AGENT})
        response = session.get(url, timeout=40)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        report_links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text().strip().lower()
            
            if href.lower().endswith('.pdf'):
                absolute_url = urljoin(url, href)
                if not re.search(r'Rapport-FTUSA-20\d{2}', absolute_url, re.IGNORECASE):
                    continue
                    
                year_match = re.search(r'Rapport-FTUSA-(20\d{2})', absolute_url, re.IGNORECASE)
                year = year_match.group(1) if year_match else None
                
                if year:
                    filename = f"Rapport-FTUSA-{year}.pdf"
                    report_links.append({
                        'url': absolute_url,
                        'title': link_text,
                        'date': datetime.strptime(year, "%Y"),
                        'filename': filename,
                        'context': ' '.join(link.find_parent().get_text().strip().split()[:20]),
                        'source_url': url,
                        'type': "Rapports_FTUSA",
                        'company': ""
                    })
                    
        return report_links
        
    except Exception as e:
        print(f"Erreur lors de l'analyse spécifique de FTUSA: {str(e)}")
        return []

SITES_CONFIG = [
    {
        'url': "https://www.ftusanet.org/indicateurs-du-marche-2/",
        'keywords': ['rapport', 'ftusa'],
        'description': "Rapports FTUSA sur les indicateurs de marché",
        'handler': get_ftusa_reports
    },
    {
        'url': "https://www.cga.gov.tn/index.php?id=96&L=0",
        'keywords': ['rapport', 'annuel', 'cga', 'assurance'],
        'description': "Rapports annuels du CGA",
        'retry_attempts': 3,
        'handler': get_pdfs_from_page
    }
]

def main():
    setup_download_dir()
    total_downloaded = 0
    new_reports = []
    
    for config in SITES_CONFIG:
        print(f"\n{'-'*50}")
        print(f"Recherche des PDFs: {config['description']}")
        print(f"URL: {config['url']}")
        
        pdfs = config['handler'](config['url'], config.get('keywords'))
        if not pdfs:
            print("  Aucun PDF correspondant trouvé.")
            continue
            
        for pdf in pdfs:
            # Skip if no date (except for CGA reports where we extract from title)
            if pdf['date'] is None and "CGA" not in pdf['type']:
                print(f"\nFichier sans date détecté - ignoré:")
                print(f"  URL: {pdf['url']}")
                print(f"  Titre: {pdf.get('title', 'N/A')}")
                continue
                
            for attempt in range(config.get('retry_attempts', 1)):
                print(f"\nTentative {attempt + 1}/{config.get('retry_attempts', 1)}")
                success = download_pdf(pdf)
                if success:
                    total_downloaded += 1
                    new_reports.append(pdf)
                    break
                elif attempt < config.get('retry_attempts', 1) - 1:
                    print("  Nouvelle tentative dans 5 secondes...")
                    time.sleep(5)
                    
    print(f"\nOpération terminée. {total_downloaded} nouveaux fichiers téléchargés avec succès.")
    if total_downloaded > 0:
        send_email_notification(new_reports)
    else:
        print("\nAucun nouveau rapport - aucun email envoyé")

if __name__ == "__main__":
    main()


# In[ ]:




