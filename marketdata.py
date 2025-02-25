import os
import wget
import zipfile
import logging
from datetime import datetime, timedelta
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
        # Check if update is needed
        if not self.should_update_files():
            logging.info("Files are up to date. Skipping download.")
            return

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
        self.record_update_time()

    def should_update_files(self):
        """Check if files should be updated based on time and file existence"""
        # First check if all symbol files exist and are non-empty
        for filename in self.SYMBOL_FILES.values():
            file_path = os.path.join(self.FILES_DIR, filename)
            if not os.path.exists(file_path):
                logging.info(f"File {filename} missing. Update needed.")
                return True
            if os.path.getsize(file_path) == 0:
                logging.info(f"File {filename} is empty. Update needed.")
                return True
        
        # Then check the timestamp
        if not os.path.exists(self.LAST_UPDATE_FILE):
            logging.info("No last update timestamp found. Update needed.")
            return True
        
        try:
            with open(self.LAST_UPDATE_FILE, 'r') as f:
                last_update = datetime.fromisoformat(f.read().strip())
            
            # Update if last update was more than 24 hours ago
            time_since_update = datetime.now() - last_update
            if time_since_update > timedelta(hours=24):
                logging.info(f"Files are {time_since_update.total_seconds()/3600:.1f} hours old. Update needed.")
                return True
            
            logging.info(f"Files are up to date. Last update was {time_since_update.total_seconds()/3600:.1f} hours ago.")
            return False
        
        except Exception as e:
            logging.error(f"Error checking update time: {e}")
            return True  # If there's any error reading the timestamp, trigger an update

    def load_exchange_data(self):
        """Load exchange data from downloaded symbol files"""
        try:
            self.exchange_dfs = {}
            for exchange, filename in self.SYMBOL_FILES.items():
                file_path = os.path.join(self.FILES_DIR, filename)
                if os.path.exists(file_path):
                    self.exchange_dfs[exchange] = pd.read_csv(file_path)
                    print(f"{exchange} data loaded successfully.")
                else:
                    print(f"File {filename} not found, skipping {exchange} data.")

        except Exception as e:
            print(f"Error loading exchange data: {e}")
            raise

    def record_update_time(self):
        """Record the time of the latest update"""
        # Ensure the directory exists
        os.makedirs(self.FILES_DIR, exist_ok=True)
        
        # Write the current timestamp
        with open(self.LAST_UPDATE_FILE, 'w') as f:
            f.write(datetime.now().isoformat())
        logging.info("Update time recorded successfully.")

# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    market_data = MarketData()
    market_data.download_symbol_files()