import os
import wget
import zipfile
import logging
import subprocess
from datetime import datetime
import pandas as pd

class MarketData:
    def __init__(self):
        self.FILES_DIR = "files/"  # Directory to store downloaded files
        self.SYMBOL_FILES = {
            'NSE': 'NSE_symbols.txt',
            'BSE': 'BSE_symbols.txt',
            'NFO': 'NFO_symbols.txt',
            'BFO': 'BFO_symbols.txt',
            'CDS': 'CDS_symbols.txt',
            'MCX': 'MCX_symbols.txt'
        }
        self.LAST_UPDATE_FILE = os.path.join(self.FILES_DIR, "last_update.txt")
        self.GIT_REPO_DIR = os.getcwd()  # Change this if needed

    def delete_old_files(self):
        """Deletes old symbol files before downloading new ones"""
        logging.info("Deleting old symbol files...")
        for filename in self.SYMBOL_FILES.values():
            file_path = os.path.join(self.FILES_DIR, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info(f"Deleted: {file_path}")

    def download_symbol_files(self):
        """Downloads and extracts updated symbol files"""
        logging.info("Downloading updated symbol files...")
        self.delete_old_files()  # Ensure fresh data daily

        for exchange, filename in self.SYMBOL_FILES.items():
            zip_filename = f"{filename}.zip"
            file_path = os.path.join(self.FILES_DIR, filename)
            url = f"https://api.shoonya.com/{zip_filename}"
            try:
                wget.download(url, out=os.path.join(self.FILES_DIR, zip_filename))
                with zipfile.ZipFile(os.path.join(self.FILES_DIR, zip_filename), 'r') as zip_ref:
                    zip_ref.extractall(self.FILES_DIR)
                os.remove(os.path.join(self.FILES_DIR, zip_filename))
                logging.info(f"Downloaded and extracted {exchange} symbols.")
            except Exception as e:
                logging.error(f"Error downloading/extracting {exchange} symbols: {e}")

        # Record the update time after successful download
        with open(self.LAST_UPDATE_FILE, "w") as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))



    def update_github_repo(self):
        """Commits and pushes updated files to GitHub"""
        try:
            os.chdir(self.GIT_REPO_DIR)
            subprocess.run(["git", "add", "."], check=True)
            subprocess.run(["git", "commit", "-m", f"Update symbol files - {datetime.now().strftime('%Y-%m-%d')}"], check=True)
            subprocess.run(["git", "push"], check=True)
            logging.info("Successfully pushed updates to GitHub.")
        except subprocess.CalledProcessError as e:
            logging.error(f"Error updating GitHub: {e}")

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    market_data = MarketData()
    market_data.download_symbol_files()
    market_data.update_github_repo()
