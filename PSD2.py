"""
psd3.py

1) Fills historical 1-min data into the <symbol>_1min tables (in A:\lasha\Database\market_data.db)
   based on a roll sheet at A:\lasha\Database\contracts.csv with columns:
       Symbol, Front Contract, Back Contract, Roll Date

   - For each symbol, we parse the Roll Date (MM/DD/YYYY).
   - The "effective roll boundary" is (Roll Date - 1 day) at 18:00 EST.
   - Data before that boundary is pulled from the "Back Contract"; on/after, from the "Front Contract".
   - Historical data is pulled from the last available timestamp (or now-10 days if none) to the current time.
   - Trading day is 18:00 EST (previous day) to 17:59:59 EST (current day).
   - We store a "contract" column to record which contract was used.

2) Starts streaming for your continuous futures symbols. For each streaming candle:
   - We only update which contract to use once per day (17:00–17:59 EST) by comparing openInterest
     of the front vs. future contract. The chosen contract is cached in a dictionary.
   - We insert the 1-min candle into <symbol>_1min with a "contract" column.

Timestamps in the DB are stored in EST with the format "YYYY-MM-DD hh:mm:ss".
"""

import os
import re
import time
import json
import logging
import sqlite3
import pandas as pd
from datetime import datetime, timedelta, time as dt_time
import pytz
from dotenv import load_dotenv
from typing import Tuple

import schwabdev  # Your Schwab API library

# ------------------------------------------------------------------------------
# 0) CONFIGURATION & SETUP
# ------------------------------------------------------------------------------

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database and contracts sheet paths
DB_PATH = r"A:\lasha\Database\market_data.db"
CONTRACTS_CSV = r"A:\lasha\Database\contracts.csv"

# Initialize Schwab API client
client = schwabdev.Client(
    os.getenv("app_key"),
    os.getenv("app_secret"),
    os.getenv("callback_url")
)
streamer = client.stream

# Connect to SQLite database
conn = sqlite3.connect(DB_PATH)

# Time zones
et_tz = pytz.timezone("US/Eastern")
utc_tz = pytz.UTC

# Continuous futures symbols, comma-separated (leading slash).
symbols = "/MNQ,/ZF,/MGC,/RTY,/TN,/ZT,/ZN,/MBT,/UB,/ZB,/ZW,/ZO,/MES"

# Product-specific valid sets for stepping from front to future contract
DEFAULT_MONTH_CODES = "FGHJKMNQUVXZ"
PRODUCT_MONTHS = {
    "/MNQ": "HMUZ",
    "/ZF": "HMUZ",
    "/MGC": "GJMQVZ",
    "/RTY": "HMUZ",
    "/TN": "HMUZ",
    "/ZT": "HMUZ",
    "/ZN": "HMUZ",
    "/MBT": "FGHJKMNQUVXZ",  # example: trades quarterly
    "/UB": "HMUZ",
    "/ZB": "HMUZ",
    "/ZW": "HKNUZ",
    "/ZO": "HKNUZ",
    "/MES": "HMUZ"
}

# Shared list for streaming messages
shared_list = []

# Dictionaries for streaming contract selection
active_contracts = {}  # e.g. {"/ZF": "/ZFM25"}
last_update = {}       # e.g. {"/ZF": datetime in EST}

# ------------------------------------------------------------------------------
# 1) HELPER FUNCTIONS (HISTORICAL & SHARED)
# ------------------------------------------------------------------------------

def clean_symbol(symbol: str) -> str:
    """Remove leading slash for table naming."""
    return symbol.replace("/", "")

def create_table_if_not_exists(symbol: str):
    """
    Creates <symbol>_1min if not exists, with a 'contract' column.
    """
    table_name = f"{symbol}_1min"
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT,
            timeframe TEXT,
            timestamp TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            contract TEXT,
            UNIQUE(symbol, timeframe, timestamp)
        )
    """)
    conn.commit()
    # Ensure 'contract' column
    cur = conn.execute(f"PRAGMA table_info({table_name})")
    cols = [row[1] for row in cur.fetchall()]
    if "contract" not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN contract TEXT")
        conn.commit()

def get_last_timestamp(symbol: str) -> datetime:
    """
    Returns the last timestamp in <symbol>_1min as a datetime in EST, or None if table empty.
    """
    table_name = f"{symbol}_1min"
    df = pd.read_sql(f"SELECT MAX(timestamp) as last_ts FROM {table_name}", conn)
    last_ts_str = df["last_ts"].iloc[0]
    if last_ts_str is None:
        return None
    ts = pd.to_datetime(last_ts_str)
    if ts.tzinfo is None:
        ts = et_tz.localize(ts)
    else:
        ts = ts.astimezone(et_tz)
    return ts

def is_market_closed(dt_obj: datetime) -> bool:
    """
    Returns True if dt_obj (in EST) is between 17:00 and 18:00.
    """
    return dt_obj.time() >= dt_time(17, 0) and dt_obj.time() < dt_time(18, 0)

# ------------------------------------------------------------------------------
# 2) HISTORICAL DATA FILLING
# ------------------------------------------------------------------------------

def get_trading_day_bounds(dt_obj: datetime) -> Tuple[datetime, datetime]:
    """
    Trading day is from 18:00 EST on the previous calendar day
    to 17:59:59 EST on the current day.
    """
    if dt_obj.tzinfo is None:
        dt_obj = et_tz.localize(dt_obj)
    else:
        dt_obj = dt_obj.astimezone(et_tz)
    if dt_obj.hour < 18:
        day = dt_obj.date() - timedelta(days=1)
    else:
        day = dt_obj.date()
    trading_start = et_tz.localize(datetime.combine(day, dt_time(18, 0, 0)))
    trading_end = trading_start + timedelta(hours=23, minutes=59, seconds=59)
    return trading_start, trading_end

def fill_historical_1min(symbol: str, contract: str, start_et: datetime, end_et: datetime):
    """
    Pulls 1-min data for <contract> from start_et to end_et (EST) and inserts with a 'contract' column.
    """
    table_name = f"{symbol}_1min"
    start_utc = start_et.astimezone(utc_tz)
    end_utc = end_et.astimezone(utc_tz)
    start_ms = int(start_utc.timestamp() * 1000)
    end_ms = int(end_utc.timestamp() * 1000)
    logger.info(f"Pulling 1-min data for {contract} from {start_et} to {end_et}")
    try:
        resp = client.price_history(
            symbol=contract,
            periodType="day",
            frequencyType="minute",
            frequency=1,
            startDate=start_ms,
            endDate=end_ms,
            needExtendedHoursData=True
        )
        if resp.ok:
            data = resp.json()
            candles = data.get("candles", [])
            if candles:
                df = pd.DataFrame(candles)
                df["timestamp"] = (
                    pd.to_datetime(df["datetime"], unit="ms", utc=True)
                    .dt.tz_convert(et_tz)
                    .dt.strftime("%Y-%m-%d %H:%M:%S")
                )
                df["symbol"] = symbol
                df["timeframe"] = "1min"
                df["contract"] = contract
                df.rename(columns={"open": "open", "high": "high", "low": "low",
                                   "close": "close", "volume": "volume"}, inplace=True)
                df = df[["symbol", "timeframe", "timestamp", "open", "high", "low", "close", "volume", "contract"]]
                rows = [tuple(row) for row in df.itertuples(index=False)]
                sql = f"""
                    INSERT OR IGNORE INTO {table_name}
                    (symbol, timeframe, timestamp, open, high, low, close, volume, contract)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                conn.executemany(sql, rows)
                conn.commit()
                logger.info(f"Inserted {len(rows)} rows into {table_name} for {contract}")
            else:
                logger.info(f"No 1-min data returned for {contract} from {start_et} to {end_et}")
        else:
            logger.warning(f"API call failed for {contract}: {resp.status_code} - {resp.text}")
    except Exception as e:
        logger.error(f"Error fetching 1-min data for {contract}: {e}")

def fill_gaps_for_symbol(symbol: str, front_contract: str, back_contract: str, roll_date_str: str):
    """
    Fills historical data for 'symbol' using the roll sheet logic:
      - Roll Date is the day the front contract becomes active.
      - The day before at 18:00 EST is the cutoff for the back contract.
    """
    sym = clean_symbol(symbol)
    create_table_if_not_exists(sym)
    now_et = datetime.now(et_tz).replace(second=0, microsecond=0)
    last_ts = get_last_timestamp(sym)
    if last_ts is None:
        gap_start = now_et - timedelta(days=10)
        logger.info(f"No data for {symbol}; starting gap from {gap_start}")
    else:
        gap_start = last_ts + timedelta(minutes=1)
    if gap_start.tzinfo is None:
        gap_start = et_tz.localize(gap_start)
    if gap_start >= now_et:
        logger.info(f"No gap to fill for {symbol}. Already up-to-date.")
        return

    # Parse roll date and compute effective boundary: (Roll Date - 1 day) at 18:00 EST
    roll_date_dt = pd.to_datetime(roll_date_str)
    effective_roll = et_tz.localize(datetime.combine((roll_date_dt - timedelta(days=1)).date(), dt_time(18, 0, 0)))
    logger.info(f"For {symbol}: effective roll boundary is {effective_roll.strftime('%Y-%m-%d %H:%M:%S')}")

    segments = []
    if gap_start < effective_roll:
        segments.append((gap_start, effective_roll - timedelta(minutes=1), back_contract))
    if effective_roll < now_et:
        segments.append((max(gap_start, effective_roll), now_et, front_contract))

    for seg_start, seg_end, contract in segments:
        current = seg_start
        while current < seg_end:
            td_start, td_end = get_trading_day_bounds(current)
            fill_start = max(current, td_start)
            fill_end = min(td_end, seg_end)
            if fill_start < fill_end:
                fill_historical_1min(sym, contract, fill_start, fill_end)
            current = td_end + timedelta(seconds=1)

# ------------------------------------------------------------------------------
# 3) STREAMING FUNCTIONS
# ------------------------------------------------------------------------------

def get_front_contract(symbol: str) -> str:
    """Retrieve front-month contract using client.instruments, pad single-digit year if needed."""
    try:
        resp = client.instruments(symbol, "search").json()
        instruments = resp.get("instruments", [])
        if instruments:
            front_symbol = instruments[0].get("symbol", symbol)
            front_contract = front_symbol.split(":")[0]
            pattern = re.compile(r'^(/[^0-9]+)([A-Z])(\d)$')
            m = pattern.match(front_contract)
            if m:
                front_contract = f"{m.group(1)}{m.group(2)}2{m.group(3)}"
            return front_contract
    except Exception as e:
        logger.error(f"Error retrieving instrument for {symbol}: {e}")
    return symbol

def compute_future_contract(front_contract: str, symbol: str) -> str:
    """
    Compute next contract by stepping one letter forward in the valid set, wrapping if needed.
    """
    fc = front_contract.split(":")[0]
    symbol_root = None
    for root in symbols.split(","):
        if fc.startswith(root):
            symbol_root = root
            break
    if not symbol_root:
        symbol_root = fc[:3]
    remainder = fc[len(symbol_root):]
    if len(remainder) < 3:
        logger.error(f"Unexpected front contract format: {front_contract}")
        return front_contract
    front_letter = remainder[0]
    year_digits = remainder[1:]
    valid_set = PRODUCT_MONTHS.get(symbol, DEFAULT_MONTH_CODES)
    if front_letter not in valid_set:
        valid_set = DEFAULT_MONTH_CODES
    idx = valid_set.find(front_letter)
    if idx == -1:
        return front_contract
    if idx == len(valid_set) - 1:
        next_letter = valid_set[0]
        new_year = int(year_digits) + 1
    else:
        next_letter = valid_set[idx + 1]
        new_year = int(year_digits)
    return f"{symbol_root}{next_letter}{new_year:02d}"

def choose_contract_for_streaming(symbol: str) -> str:
    """
    Chooses the active contract for the given continuous symbol, updating once a day
    (17:00-17:59 EST) by comparing openInterest for the front vs. future contract.
    """
    now_et = datetime.now(et_tz)
    update_start = now_et.replace(hour=17, minute=0, second=0, microsecond=0)
    update_end = now_et.replace(hour=17, minute=59, second=59, microsecond=999999)
    symbol_key = symbol
    # If we're in the update window, and haven't updated today, do so:
    if update_start <= now_et <= update_end:
        last = last_update.get(symbol_key)
        if not last or last.date() < now_et.date():
            front = get_front_contract(symbol)
            future = compute_future_contract(front, symbol)
            try:
                resp = client.quotes([front, future])
                if resp.ok:
                    data = resp.json()
                    front_oi = data.get(front, {}).get("quote", {}).get("openInterest", 0)
                    future_oi = data.get(future, {}).get("quote", {}).get("openInterest", 0)
                    logger.info(f"Updating {symbol}: front OI={front_oi}, future OI={future_oi}")
                    chosen = front if front_oi >= future_oi else future
                    active_contracts[symbol_key] = chosen
                    last_update[symbol_key] = now_et
                else:
                    logger.warning(f"Quotes API failed for {symbol} in update window.")
                    # fallback to existing or front
                    chosen = active_contracts.get(symbol_key, front)
                    active_contracts[symbol_key] = chosen
            except Exception as e:
                logger.error(f"Error updating contract for {symbol}: {e}")
                chosen = active_contracts.get(symbol_key, front)
                active_contracts[symbol_key] = chosen
            return active_contracts[symbol_key]

    # Otherwise, if no entry for this symbol yet, initialize:
    if symbol_key not in active_contracts:
        front = get_front_contract(symbol)
        future = compute_future_contract(front, symbol)
        try:
            resp = client.quotes([front, future])
            if resp.ok:
                data = resp.json()
                front_oi = data.get(front, {}).get("quote", {}).get("openInterest", 0)
                future_oi = data.get(future, {}).get("quote", {}).get("openInterest", 0)
                chosen = front if front_oi >= future_oi else future
                active_contracts[symbol_key] = chosen
                last_update[symbol_key] = now_et
            else:
                logger.warning(f"Quotes API failed for {symbol} on initialization.")
                active_contracts[symbol_key] = front
                last_update[symbol_key] = now_et
        except Exception as e:
            logger.error(f"Error initializing contract for {symbol}: {e}")
            active_contracts[symbol_key] = front
            last_update[symbol_key] = now_et

    return active_contracts[symbol_key]

def insert_streaming_data(symbol: str, contract: str, ts: datetime, fields: dict):
    """
    Inserts a 1-min streaming candle into <symbol>_1min with a "contract" column.
    """
    table_name = f"{symbol}_1min"
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        INSERT OR IGNORE INTO {table_name}
        (symbol, timeframe, timestamp, open, high, low, close, volume, contract)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    values = (symbol, "1min", ts_str,
              fields.get("open"), fields.get("high"), fields.get("low"),
              fields.get("close"), fields.get("volume"), contract)
    try:
        conn.execute(sql, values)
        conn.commit()
        logger.info(f"Inserted streaming data for {symbol} from {contract} at {ts_str}")
    except Exception as e:
        logger.error(f"DB insertion error for {symbol}: {e}")

def process_stream():
    """
    Continuously processes streaming data:
      - For each CHART_FUTURES message, choose the active contract once/day (17:00-17:59).
      - Adjust the timestamp by subtracting 1 minute, zero out seconds/microseconds,
        convert to EST, skip if market-closed.
      - Insert the candle with a "contract" column.
    """
    logger.info("Starting streaming process...")
    while True:
        while shared_list:
            message = json.loads(shared_list.pop(0))
            for rtype, services in message.items():
                if rtype == "data":
                    for service in services:
                        if service.get("service") == "CHART_FUTURES":
                            contents = service.get("content", [])
                            for content in contents:
                                continuous_symbol = content.pop("key", "NO KEY")
                                chosen_contract = choose_contract_for_streaming(continuous_symbol)
                                symbol_clean = clean_symbol(continuous_symbol)
                                create_table_if_not_exists(symbol_clean)
                                # Adjust timestamp from ms -> UTC -> EST -> minus 1 minute -> zero out seconds
                                ts = datetime.fromtimestamp(service["timestamp"] / 1000, tz=utc_tz)
                                adjusted_time = (ts - timedelta(minutes=1)).replace(second=0, microsecond=0).astimezone(et_tz)
                                if not is_market_closed(adjusted_time):
                                    fields = {
                                        "open": content.get("2"),
                                        "high": content.get("3"),
                                        "low": content.get("4"),
                                        "close": content.get("5"),
                                        "volume": content.get("6")
                                    }
                                    insert_streaming_data(symbol_clean, chosen_contract, adjusted_time, fields)
        time.sleep(0.5)

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    # 1) Fill historical data from the roll sheet
    try:
        df_contracts = pd.read_csv(CONTRACTS_CSV)
        df_contracts.columns = df_contracts.columns.str.strip()
        # Columns: Symbol, Front Contract, Back Contract, Roll Date
        for _, row in df_contracts.iterrows():
            symbol = row["Symbol"].strip()     # e.g. /ZB
            front = row["Front Contract"].strip()
            back = row["Back Contract"].strip()
            roll_date = str(row["Roll Date"]).strip()
            logger.info(f"Filling historical data for {symbol} => front={front}, back={back}, roll={roll_date}")
            fill_gaps_for_symbol(symbol, front, back, roll_date)
    except Exception as e:
        logger.error(f"Error filling historical data: {e}")

    # 2) Start streaming
    streamer.start(lambda msg: shared_list.append(msg))
    streamer.send(streamer.chart_futures(symbols, "0,1,2,3,4,5,6,7,8"))
    logger.info(f"Started streaming for symbols: {symbols}")
    try:
        process_stream()
    except KeyboardInterrupt:
        logger.info("Stopping stream gracefully...")
        conn.commit()
        conn.close()
        streamer.stop()
        logger.info("Stream stopped and database closed.")
