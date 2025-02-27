import pandas as pd
from IPython.display import display, clear_output
import websocket
import json
import threading
import time
import requests
from datetime import datetime, timedelta
import logging
import pyotp
from api_helper import ShoonyaApiPy
from queue import Queue
import sys
import signal
import os
import zipfile
import wget
import subprocess
import pytz
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# Move page config to top level, before any other Streamlit commands
st.set_page_config(
    page_title="Market Monitor",
    page_icon="📈",
    layout="wide"
)

def signal_handler(signum, frame):
    """Handle cleanup when the program is terminated"""
    print("Shutting down gracefully...")
    sys.exit(0)

@st.cache_resource
def login(name):
    users = pd.read_csv(f'https://docs.google.com/spreadsheets/d/1jdmLEIr2AWoUD0MEPu8YsRt8Ty6J2pk-qIN0uSXveZY/gviz/tq?tqx=out:csv&sheet=Sheet1',index_col='name')
    user = users.loc[name]
    totp = pyotp.TOTP(user.fa2)
    api = ShoonyaApiPy()
    ret = api.login(userid=user.uid, password=user.pwd, twoFA=totp.now(), 
                   vendor_code=f'{user.uid}_U', api_secret=user.api_key, imei='abc1234')
    if ret['susertoken']:
        st.success('Login successful')
    else:
        st.error('Login failed')
    return api

api = login('pavan')
# Define the desired timezone (e.g., IST)
ist = pytz.timezone('Asia/Kolkata')

class MarketData:
    def __init__(self):
        self.exchange_dfs = {}
        self.FILES_DIR = "files/"  # Adjust this path if needed
        self.MARKET_CLOSE_TIME = "15:30"  # 24-hour format
        self.SYMBOL_FILES = {
            'NSE': 'NSE_symbols.txt',
            'BSE': 'BSE_symbols.txt',
            'NFO': 'NFO_symbols.txt',
            'BFO': 'BFO_symbols.txt',
            'CDS': 'CDS_symbols.txt',
            'MCX': 'MCX_symbols.txt'
        }


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


@st.cache_resource
def initialize_market_data():
    market_data = MarketData()
    market_data.load_exchange_data()
    return market_data, market_data.exchange_dfs

# Replace the current initialization with:
market_data, exchange_dfs = initialize_market_data()

class StreamlitUI:
    def __init__(self):
        self.stopped_rows = {}
        self.market_data = market_data  # Use the cached instance
        self.setup_streamlit()
        
    def setup_streamlit(self):
        st.title("Market Data Monitor")
        
        # Auto refresh every 1 second (1000 milliseconds)
        st_autorefresh(interval=1000, key="datarefresh")
        
        # Initialize session state for storing data with explicit dtypes
        if 'data' not in st.session_state:
            st.session_state.data = pd.DataFrame({
                "SEGMENT": pd.Series(dtype='str'),
                "SCRIPT / STOCK": pd.Series(dtype='str'),
                "EXCH": pd.Series(dtype='str'),
                "EXPIRY": pd.Series(dtype='str'),
                "CE / PE": pd.Series(dtype='str'),
                "STRIKE": pd.Series(dtype='float64'),
                "BUY / SELL": pd.Series(dtype='str'),
                "TGT": pd.Series(dtype='float64'),
                "SL": pd.Series(dtype='float64'),
                "LTP": pd.Series(dtype='float64'),
                "HIGH": pd.Series(dtype='float64'),
                "LOW": pd.Series(dtype='float64'),
                "PCECLOSE": pd.Series(dtype='float64'),
                "Date/Time": pd.Series(dtype='str')
            })

    def add_new_row(self):
        with st.form("new_stock_form"):
            cols = st.columns([2, 2, 2, 2, 2, 2, 2])
            
            # First row of inputs
            segment = cols[0].selectbox("Segment", ["CASH", "FUTURES", "OPTIONS"], key="segment")
            script_stock = cols[1].text_input("Script/Stock")
            exch = cols[2].selectbox("Exchange", ["NSE", "BSE", "NFO"])
            expiry = cols[3].date_input("Expiry", value=None)
            otype = cols[4].selectbox("Option Type", ["None", "CE", "PE"])
            strike = cols[5].number_input("Strike Price", min_value=0.0)
            buy_sell = cols[6].selectbox("Buy/Sell", ["BUY", "SELL"])
            
            # Second row of inputs
            cols2 = st.columns([3, 3])
            tgt = cols2[0].number_input("Target", min_value=0.0)
            sl = cols2[1].number_input("Stop Loss", min_value=0.0)
            
            # Submit button
            if st.form_submit_button("Add Stock"):
                # Format expiry date if it exists
                formatted_expiry = expiry.strftime("%d/%m/%Y") if expiry else None
                
                new_data = {
                    "SEGMENT": segment,
                    "SCRIPT / STOCK": script_stock,
                    "EXCH": exch,
                    "EXPIRY": formatted_expiry,
                    "CE / PE": None if otype == "None" else otype,
                    "STRIKE": strike,
                    "BUY / SELL": buy_sell,
                    "TGT": tgt,
                    "SL": sl,
                    "LTP": 0,
                    "HIGH": 0,
                    "LOW": 0,
                    "PCECLOSE": 0,
                    "Date/Time": datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
                }
                
                print("\nNew Stock Added:")
                print("---------------")
                for key, value in new_data.items():
                    print(f"{key}: {value}")
                print("---------------\n")
                
                # Create a new DataFrame with explicit dtypes
                new_row = pd.DataFrame([new_data], columns=st.session_state.data.columns).astype({
                    "SEGMENT": str,
                    "SCRIPT / STOCK": str,
                    "EXCH": str,
                    "EXPIRY": str,
                    "CE / PE": str,
                    "STRIKE": float,
                    "BUY / SELL": str,
                    "TGT": float,
                    "SL": float,
                    "LTP": float,
                    "HIGH": float,
                    "LOW": float,
                    "PCECLOSE": float,
                    "Date/Time": str
                })
                
                st.session_state.data = pd.concat([
                    st.session_state.data,
                    new_row
                ], ignore_index=True)

    def display_data(self):
        st.dataframe(
            st.session_state.data,
            use_container_width=True,
            hide_index=True
        )

    def update_market_data(self):
        for idx, row in st.session_state.data.iterrows():
            updated_data = self.process_row(row, idx)
            if updated_data:
                for key, value in updated_data.items():
                    if key != 'index':
                        st.session_state.data.at[idx, key] = value

    def process_row(self, row, index):
        """Process a single row to fetch live data and update values"""
        try:
            now = datetime.now(ist)
            
            # Handle None values with safe string conversion
            segment = str(row.get("SEGMENT", "")).strip().upper()
            script_stock = str(row.get("SCRIPT / STOCK", "")).strip().upper()
            exch = str(row.get("EXCH", "")).strip().upper()
            expiry = str(row.get("EXPIRY", "")) if row.get("EXPIRY") else ""
            otype = str(row.get("CE / PE", "")).strip().upper()
            strike = row.get("STRIKE", 0.0)
            buy_sell = str(row.get("BUY / SELL", "")).strip().upper()

            if not script_stock or not exch:
                return {
                    "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "LTP": 0, "HIGH": 0, "LOW": 0, "PCECLOSE": 0
                }

            # Check if row is stopped
            if index in self.stopped_rows:
                last_TGT, last_SL, last_checked_time = self.stopped_rows[index]

                if (row.get("TGT") and float(row["TGT"]) != last_TGT) or \
                   (row.get("SL") and float(row["SL"]) != last_SL):
                    self.stopped_rows.pop(index)
                else:
                    if now - last_checked_time < timedelta(minutes=1):
                        return None
                    self.stopped_rows[index] = (last_TGT, last_SL, now)

            # Get scrip and fetch live data
            scrip = get_scrip(segment, exch, script_stock, expiry, otype, strike)
            if not scrip:
                return {
                    "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "LTP": 0, "HIGH": 0, "LOW": 0, "PCECLOSE": 0
                }

            token = scrip.split("|")[-1]
            live_data = fetch_live_data(token, exch)
            if not live_data:
                return {
                    "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "LTP": 0, "HIGH": 0, "LOW": 0, "PCECLOSE": 0
                }

            LTP = float(live_data["LTP"])

            # Process TGT and SL
            try:
                TGT = float(row.get("TGT", 0)) if row.get("TGT") is not None else None
                SL = float(row.get("SL", 0)) if row.get("SL") is not None else None
            except ValueError:
                TGT, SL = None, None

            # Check TGT/SL conditions
            if TGT is not None or SL is not None:
                if buy_sell == "BUY":
                    if TGT is not None and LTP >= TGT:
                        self.stopped_rows[index] = (TGT, SL, now)
                        return {
                            "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
                            "LTP": LTP,
                            "HIGH": live_data["HIGH"],
                            "LOW": live_data["LOW"],
                            "PCECLOSE": live_data["PCECLOSE"]
                        }
                    if SL is not None and LTP <= SL:
                        self.stopped_rows[index] = (TGT, SL, now)
                        return {
                            "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
                            "LTP": LTP,
                            "HIGH": live_data["HIGH"],
                            "LOW": live_data["LOW"],
                            "PCECLOSE": live_data["PCECLOSE"]
                        }
                elif buy_sell == "SELL":
                    if TGT is not None and LTP <= TGT:
                        self.stopped_rows[index] = (TGT, SL, now)
                        return {
                            "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
                            "LTP": LTP,
                            "HIGH": live_data["HIGH"],
                            "LOW": live_data["LOW"],
                            "PCECLOSE": live_data["PCECLOSE"]
                        }
                    if SL is not None and LTP >= SL:
                        self.stopped_rows[index] = (TGT, SL, now)
                        return {
                            "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
                            "LTP": LTP,
                            "HIGH": live_data["HIGH"],
                            "LOW": live_data["LOW"],
                            "PCECLOSE": live_data["PCECLOSE"]
                        }

            return {
                "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
                "LTP": LTP,
                "HIGH": live_data["HIGH"],
                "LOW": live_data["LOW"],
                "PCECLOSE": live_data["PCECLOSE"]
            }

        except Exception as e:
            print(f"Error processing row {index}: {e}")
            return None

def get_scrip(segment, exchange, symbol, expiry=None, otype=None, strike=None):
    """
    Construct the full scrip string based on the segment, exchange, symbol, and optional parameters.
    """
    try:
        # Handle CASH segment
        if segment == 'CASH':
            return exchange + '|' + str(get_token(exchange, symbol + '-EQ'))
            
        # Skip if no expiry date for FUTURES or OPTIONS
        if not expiry:
            return None
            
        # Convert expiry to string format if it's a date
        expiry_str = expiry
        if hasattr(expiry, 'strftime'):  # Check if object has date formatting method
            expiry_str = expiry.strftime("%d/%m/%Y")
            
        # Get normalized expiry format (DDMMMYY)
        m = normalize_expiry_date(expiry_str)
        if not m:
            return None
            
        # Handle FUTURES segment
        if segment == 'FUTURES':
            return exchange + '|' + str(get_token(exchange, symbol.upper() + m + 'F'))
            
        # Handle OPTIONS segment
        if segment == 'OPTIONS' and otype and strike:
            return exchange + '|' + str(get_token(exchange, symbol.upper() + m + otype[0] + str(int(strike))))
            
        return None

    except Exception as e:
        print(f"Error getting scrip: {e}")
        return None

def fetch_live_data(token, exchange):
    try:
        token = int(token)
        quote = api.get_quotes(exchange=exchange, token=str(token))
        if quote:
            live_data = {
                "LTP": float(quote.get('lp', 0)),
                "HIGH": float(quote.get('h', 0)),
                "LOW": float(quote.get('l', 0)),
                "PCECLOSE": float(quote.get('c', 0))
            }
            print(f"Fetched live data for token {token}: {live_data}")
            return live_data
        else:
            print(f"No live data returned for token {token}")
            return None
    except requests.exceptions.Timeout:
        print(f"Timeout while fetching live data for token {token}")
        return None
    except Exception as e:
        print(f"Error fetching live data for token {token}: {e}")
        return None

def normalize_expiry_date(expiry):
    try:
        d =  datetime.strptime(expiry, "%d/%m/%Y").strftime("%d%b%y").upper()
    except ValueError:
        return None
    return d

def get_token(exchange, scrip):
    try:
        df = exchange_dfs.get(exchange.upper())
        if df is not None:
            tokens = df[df['TradingSymbol'] == scrip]['Token'].values
            return int(tokens[0]) if len(tokens) > 0 else None
    except Exception as e:
        print(f"Error fetching token for {scrip}: {e}")
    return None

def main():
    ui = StreamlitUI()
    ui.add_new_row()
    ui.display_data()
    ui.update_market_data()

if __name__ == "__main__":
    main()
