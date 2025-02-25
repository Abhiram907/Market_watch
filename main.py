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
from marketdata import MarketData

# Add these imports at the top
from datetime import datetime, timedelta

@st.cache_data(ttl=24*3600)  # Cache for exactly 24 hours
def download_and_get_market_data():
    """Downloads market data files and returns the loaded data"""
    try:
        market_data = MarketData()
        market_data.download_symbol_files()
        market_data.load_exchange_data()
        return market_data.exchange_dfs
    except Exception as e:
        st.error(f"Failed to download/load market data: {e}")
        return None

# Replace the ensure_market_data_files function with this
def initialize_app():
    """Initialize the application with market data"""
    market_dfs = download_and_get_market_data()
    if market_dfs is None:
        st.error("Unable to start application. Please check your internet connection and try again.")
        st.stop()
    return market_dfs

# At the start of your app
st.set_page_config(
    page_title="Market Monitor",
    page_icon="📈",
    layout="wide"
)

# Initialize the app and get market data
market_data = initialize_app()

def signal_handler(signum, frame):
    """Handle cleanup when the program is terminated"""
    print("Shutting down gracefully...")
    sys.exit(0)

@st.cache_resource
def login(name):
    try:
        users = pd.read_csv(f'https://docs.google.com/spreadsheets/d/1jdmLEIr2AWoUD0MEPu8YsRt8Ty6J2pk-qIN0uSXveZY/gviz/tq?tqx=out:csv&sheet=Sheet1',index_col='name')
        user = users.loc[name]
        totp = pyotp.TOTP(user.fa2)
        api = ShoonyaApiPy()
        ret = api.login(userid=user.uid, password=user.pwd, twoFA=totp.now(), 
                       vendor_code=f'{user.uid}_U', api_secret=user.api_key, imei='abc1234')
        if ret['susertoken']:
            return api, True  # Return both api and success status
        return None, False
    except KeyError:
        return None, "not_found"  # Special case for user not found
    except Exception as e:
        return None, False

# Initialize session state for login status if it doesn't exist
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False

# Only show login input if not logged in
if not st.session_state.logged_in:
    user_name = st.text_input("Enter your account name:", key="user_name")
    if user_name:  # Only attempt login if user has entered a name
        api, login_status = login(user_name)
        if login_status == "not_found":
            st.error("User does not exist. Please check your account name.")
            st.stop()
        elif login_status:
            st.session_state.logged_in = True
            st.session_state.api = api
            st.success("Login successful!")
            st.rerun()  # Refresh the page to hide login section
        else:
            st.error("Login failed. Please try again.")
            st.stop()
    else:
        st.warning("Please enter your account name to login")
        st.stop()

# Use the stored API instance after successful login
api = st.session_state.api if st.session_state.logged_in else None

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
    """Initialize market data once and cache it"""
    market_data = MarketData()
    # No need to download here since we already did it in ensure_market_data_files()
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
                "QUANTITY": pd.Series(dtype='int64'),
                "TGT": pd.Series(dtype='float64'),
                "SL": pd.Series(dtype='float64'),
                "LTP": pd.Series(dtype='float64'),
                "HIGH": pd.Series(dtype='float64'),
                "LOW": pd.Series(dtype='float64'),
                "PCECLOSE": pd.Series(dtype='float64'),
                "ENTRY": pd.Series(dtype='float64'),
                "P&L": pd.Series(dtype='float64'),
                "Date/Time": pd.Series(dtype='str')
            })
        
        # Add tabs for Add/Update operations
        tab1, tab2 = st.tabs(["Add New Entry", "Update Existing Entry"])
        
        with tab1:
            self.add_new_row()
            
        with tab2:
            if not st.session_state.data.empty:
                # Create a list of row descriptions for the dropdown
                row_options = [f"Row {i+1}: {row['SCRIPT / STOCK']} - {row['SEGMENT']} - {row['EXCH']}" 
                             for i, row in st.session_state.data.iterrows()]
                selected_row = st.selectbox("Select row to update:", row_options)
                if selected_row:
                    # Extract row index from selection
                    row_index = int(selected_row.split(':')[0].replace('Row ', '')) - 1
                    self.update_row_form(row_index)
            else:
                st.info("No entries to update. Add some entries first.")

    def add_new_row(self):
        with st.form("new_stock_form"):
            cols = st.columns([2, 2, 2, 2, 2, 2, 2])
            
            # First row of inputs
            segment = cols[0].selectbox("Segment", ["CASH", "FUTURES", "OPTIONS"], key="segment")
            script_stock = cols[1].text_input("Script/Stock")
            exch = cols[2].selectbox("Exchange", ["NSE", "BSE", "NFO", "BFO", "CDS", "MCX"])
            expiry = cols[3].date_input("Expiry", value=None)
            otype = cols[4].selectbox("Option Type", ["None", "CE", "PE"])
            strike = cols[5].number_input("Strike Price", min_value=0.0)
            buy_sell = cols[6].selectbox("Buy/Sell", ["BUY", "SELL"])
            
            # Second row of inputs
            cols2 = st.columns([4, 4, 4, 4])
            tgt = cols2[0].number_input("Target", min_value=0.0)
            sl = cols2[1].number_input("Stop Loss", min_value=0.0)
            entry = cols2[2].number_input("Entry Price", min_value=0.0)
            quantity = cols2[3].number_input("Quantity", min_value=1, value=1, step=1)
            
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
                    "QUANTITY": int(quantity),
                    "TGT": tgt,
                    "SL": sl,
                    "LTP": 0,
                    "HIGH": 0,
                    "LOW": 0,
                    "PCECLOSE": 0,
                    "ENTRY": float(entry),
                    "P&L": 0,
                    "Date/Time": datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
                }
                
                # Create a new DataFrame with explicit dtypes
                new_row = pd.DataFrame([new_data], columns=st.session_state.data.columns).astype({
                    "SEGMENT": str,
                    "SCRIPT / STOCK": str,
                    "EXCH": str,
                    "EXPIRY": str,
                    "CE / PE": str,
                    "STRIKE": float,
                    "BUY / SELL": str,
                    "QUANTITY": int,
                    "TGT": float,
                    "SL": float,
                    "LTP": float,
                    "HIGH": float,
                    "LOW": float,
                    "PCECLOSE": float,
                    "ENTRY": float,
                    "P&L": float,
                    "Date/Time": str
                })
                
                st.session_state.data = pd.concat([
                    st.session_state.data,
                    new_row
                ], ignore_index=True)

    def display_data(self):
        # Move the display above the tabs
        st.subheader("Current Entries")
        
        # Create a styled dataframe
        def color_pnl(val):
            try:
                val = float(val)
                if val < 0:
                    return 'color: red'
                elif val > 0:
                    return 'color: green'
                return ''  # No color for zero values
            except:
                return ''  # No color for non-numeric values
        
        # Format numeric columns to 2 decimal places and apply P&L coloring
        styled_df = st.session_state.data.style\
            .format({
                'LTP': '{:.2f}',
                'HIGH': '{:.2f}',
                'LOW': '{:.2f}',
                'PCECLOSE': '{:.2f}',
                'ENTRY': '{:.2f}',
                'P&L': '{:.2f}',
                'TGT': '{:.2f}',
                'SL': '{:.2f}',
                'STRIKE': '{:.2f}'
            })\
            .map(color_pnl, subset=['P&L'])
        
        st.dataframe(
            styled_df,
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
        try:
            # Check if this row is already stopped
            if index in self.stopped_rows:
                print(f"Row {index} is already stopped, returning stored values")
                return self.stopped_rows[index]

            now = datetime.now(ist)
            
            # Get the essential values with proper type conversion
            entry_price = float(row["ENTRY"])
            quantity = int(row["QUANTITY"])
            buy_sell = str(row["BUY / SELL"]).strip().upper()
            # Only set target/stop-loss if they are greater than 0
            target = float(row["TGT"]) if row["TGT"] and float(row["TGT"]) > 0 else None
            stop_loss = float(row["SL"]) if row["SL"] and float(row["SL"]) > 0 else None
            
            # Get scrip and fetch live data
            scrip = get_scrip(
                str(row["SEGMENT"]).strip().upper(),
                str(row["EXCH"]).strip().upper(),
                str(row["SCRIPT / STOCK"]).strip().upper(),
                row["EXPIRY"] if pd.notna(row["EXPIRY"]) else None,
                str(row["CE / PE"]).strip().upper() if pd.notna(row["CE / PE"]) else None,
                float(row["STRIKE"]) if pd.notna(row["STRIKE"]) else None
            )
            
            if not scrip:
                return self.get_default_result(now, entry_price, quantity)

            token = scrip.split("|")[-1]
            live_data = fetch_live_data(token, row["EXCH"])
            
            if not live_data:
                return self.get_default_result(now, entry_price, quantity)

            LTP = float(live_data["LTP"])
            
            # Calculate P&L if we have valid entry price
            if LTP > 0 and entry_price > 0:
                if buy_sell == "BUY":
                    pnl = round((LTP - entry_price) * quantity, 2)
                else:  # SELL
                    pnl = round((entry_price - LTP) * quantity, 2)
            else:
                pnl = 0

            # Check target/stop-loss conditions regardless of entry price
            if LTP > 0:  # Only need valid LTP to check targets
                if buy_sell == "BUY":
                    if (target is not None and LTP >= target) or (stop_loss is not None and LTP <= stop_loss):
                        print(f"Row {index}: BUY {'Target' if target and LTP >= target else 'Stop-loss'} hit - LTP: {LTP}")
                        result = self.create_result(now, live_data, entry_price, quantity, pnl, LTP)
                        self.stopped_rows[index] = result
                        return result
                else:  # SELL
                    if (target is not None and LTP <= target) or (stop_loss is not None and LTP >= stop_loss):
                        print(f"Row {index}: SELL {'Target' if target and LTP <= target else 'Stop-loss'} hit - LTP: {LTP}")
                        result = self.create_result(now, live_data, entry_price, quantity, pnl, LTP)
                        self.stopped_rows[index] = result
                        return result

            return self.create_result(now, live_data, entry_price, quantity, pnl, LTP)

        except Exception as e:
            print(f"Error processing row {index}: {e}")
            return None

    def get_default_result(self, now, entry_price, quantity):
        return {
            "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "LTP": 0,
            "HIGH": 0,
            "LOW": 0,
            "PCECLOSE": 0,
            "ENTRY": entry_price,
            "QUANTITY": quantity,
            "P&L": 0
        }

    def create_result(self, now, live_data, entry_price, quantity, pnl, LTP):
        return {
            "Date/Time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "LTP": LTP,
            "HIGH": live_data["HIGH"],
            "LOW": live_data["LOW"],
            "PCECLOSE": live_data["PCECLOSE"],
            "ENTRY": entry_price,
            "QUANTITY": quantity,
            "P&L": pnl
        }

    def update_row_form(self, row_index):
        with st.form("update_stock_form"):
            row_data = st.session_state.data.iloc[row_index]
            
            # Add delete button at the top
            cols_top = st.columns([8, 2])  # 80-20 split for layout
            with cols_top[1]:
                delete_row = st.form_submit_button("🗑️ Delete Row", type="primary", use_container_width=True)
            
            cols = st.columns([2, 2, 2, 2, 2, 2, 2])
            
            # First row of inputs
            segment = cols[0].selectbox("Segment", ["CASH", "FUTURES", "OPTIONS"], 
                                      key="update_segment", index=["CASH", "FUTURES", "OPTIONS"].index(row_data["SEGMENT"]))
            script_stock = cols[1].text_input("Script/Stock", value=row_data["SCRIPT / STOCK"])
            exch = cols[2].selectbox("Exchange", ["NSE", "BSE", "NFO", "BFO", "CDS", "MCX"], 
                                   key="update_exch", index=["NSE", "BSE", "NFO", "BFO", "CDS", "MCX"].index(row_data["EXCH"]))
            
            # Convert the date string to datetime object for date_input
            expiry_date = None
            if pd.notna(row_data["EXPIRY"]):
                try:
                    expiry_date = datetime.strptime(row_data["EXPIRY"], "%d/%m/%Y").date()
                except ValueError:
                    expiry_date = None
                
            expiry = cols[3].date_input("Expiry", value=expiry_date)
            otype = cols[4].selectbox("Option Type", ["None", "CE", "PE"], 
                                    index=["None", "CE", "PE"].index(row_data["CE / PE"] if pd.notna(row_data["CE / PE"]) else "None"))
            strike = cols[5].number_input("Strike Price", min_value=0.0, value=float(row_data["STRIKE"]))
            buy_sell = cols[6].selectbox("Buy/Sell", ["BUY", "SELL"], 
                                       index=["BUY", "SELL"].index(row_data["BUY / SELL"]))
            
            # Second row of inputs
            cols2 = st.columns([4, 4, 4, 4])
            tgt = cols2[0].number_input("Target", min_value=0.0, value=float(row_data["TGT"]))
            sl = cols2[1].number_input("Stop Loss", min_value=0.0, value=float(row_data["SL"]))
            entry = cols2[2].number_input("Entry Price", min_value=0.0, value=float(row_data["ENTRY"]))
            quantity = cols2[3].number_input("Quantity", min_value=1, value=int(row_data["QUANTITY"]), step=1)
            
            update_stock = st.form_submit_button("Update Stock")
            
            if delete_row:
                # Remove the row from the DataFrame
                st.session_state.data = st.session_state.data.drop(index=row_index).reset_index(drop=True)
                st.success("Row deleted successfully!")
                st.rerun()  # Rerun the app to refresh the UI
                
            if update_stock:
                updated_data = {
                    "SEGMENT": segment,
                    "SCRIPT / STOCK": script_stock,
                    "EXCH": exch,
                    "EXPIRY": expiry.strftime("%d/%m/%Y") if expiry else None,
                    "CE / PE": None if otype == "None" else otype,
                    "STRIKE": strike,
                    "BUY / SELL": buy_sell,
                    "QUANTITY": int(quantity),
                    "TGT": tgt,
                    "SL": sl,
                    "LTP": row_data["LTP"],
                    "HIGH": row_data["HIGH"],
                    "LOW": row_data["LOW"],
                    "PCECLOSE": row_data["PCECLOSE"],
                    "ENTRY": entry,
                    "P&L": row_data["P&L"],
                    "Date/Time": row_data["Date/Time"]
                }
                
                # Update the DataFrame
                st.session_state.data.iloc[row_index] = updated_data
                st.success("Stock updated successfully!")

def normalize_expiry_date(expiry, exchange):
    """
    Normalize expiry date based on exchange format
    NFO format: DDMMMYY (e.g., 27MAR25)
    BFO format: YYMMM (e.g., 25MAR)
    """
    try:
        date_obj = datetime.strptime(expiry, "%d/%m/%Y")
        if exchange == 'NFO':
            return date_obj.strftime("%d%b%y").upper()  # Format: DDMMMYY
        elif exchange == 'BFO':
            return date_obj.strftime("%y%b").upper()    # Format: YYMMM
        return date_obj.strftime("%d%b%y").upper()      # Default format
    except ValueError:
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
        if hasattr(expiry, 'strftime'):
            expiry_str = expiry.strftime("%d/%m/%Y")
            
        # Get normalized expiry format based on exchange
        m = normalize_expiry_date(expiry_str, exchange)
        if not m:
            return None
            
        # Handle FUTURES segment
        if segment == 'FUTURES':
            return exchange + '|' + str(get_token(exchange, symbol.upper() + m + 'F'))
            
        # Handle OPTIONS segment
        if segment == 'OPTIONS' and otype and strike:
            if exchange == 'BFO':
                # BFO format: SYMBOL + YYMMM + STRIKE + CE/PE (e.g., TCS25MAR4400CE)
                return exchange + '|' + str(get_token(exchange, symbol.upper() + m + str(int(strike)) + otype))
            else:
                # NFO format: SYMBOL + DDMMMYY + CE/PE + STRIKE (e.g., TCS27MAR25P2500)
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
    ui.display_data()  # Move display_data call before the tabs
    ui.update_market_data()

if __name__ == "__main__":
    main()
