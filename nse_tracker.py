"""
NSE & MCX Tracker — Om's Dashboard v20.9.1
======================================================
v20.9.3 bugfix (Dashboard — remove 4H pattern signal boxes):
  ✅ FIXED: 4H pullback pattern signals (CEMPRO, HEMIPROP, IRCON, SCILAL etc.)
     were still generating "🚨 RE-ENTRY SIGNAL DETECTED!" boxes with
     CONFIRM & ENTER / IGNORE buttons — user does not want these.
  ✅ Only 15M EMA cross signals now generate actionable signal boxes.
  ✅ 4H pattern signals still appear in the table (Pattern / Status columns)
     as informational rows — no action boxes below the table.

  ✅ FIXED: 15M RE-ENTRY signals (BPCL SHORT, CDSL SHORT, HDFCBANK SHORT etc.)
     were shown as passive info strips — no CONFIRM & ENTER / IGNORE buttons.
  ✅ Now 15M signals render a full "🚨 15M RE-ENTRY SIGNAL DETECTED!" box
     with CONFIRM & ENTER TRADE and IGNORE SIGNAL buttons, identical UX to
     the 4H pullback signal_items boxes.
  ✅ Dismiss logic uses is_signal_dismissed() so ignored 15M signals don't
     reappear until a new EMA cross fires.
  ✅ ADX weak-trend warning and position-size calculator included in buttons.

  ✅ FIXED: "RE-ENTRY SIGNAL DETECTED!" banner was appearing for stocks
     with ADX in RANGING zone (ADX < threshold), even though the table
     showed no BULLISH/BEARISH RE-ENTRY column value.
  ✅ Root cause: signal_items was populated before ADX check; the ADX
     AVOID guard was only inside show_reentry_buttons(), which ran AFTER
     the banner was already rendered.
  ✅ Fix: added _adx_ranging guard when appending to signal_items for
     both LONG_REENTRY_SIGNAL / SHORT_REENTRY_SIGNAL and
     BULLISH_RETRACEMENT_EXIT / BEARISH_RETRACEMENT_EXIT signals.
     Ranging-market stocks are now silently skipped — no banner shown.

v20.9 changes (Dashboard + Futures Scanner — target timeframe):
  ✅ 15M RE-ENTRY target now uses nearest 4H swing high/low (was 15M/Daily):
     - BULLISH RE-ENTRY (LONG): Target = nearest 4H swing HIGH above entry
     - BEARISH RE-ENTRY (SHORT): Target = nearest 4H swing LOW below entry
  ✅ Applies to: _check_rsi_15m_filter (Dashboard + MCX) AND Futures Scanner
  ✅ Dashboard already used compute_sr_targets (4H) — now fully consistent
  ✅ Fallback: +1.5% / -1.5% from entry if no 4H swing found
  ✅ All other pages and scanners unchanged

v20.8 changes (Option Scanner + Futures Scanner):
  ✅ Option Scanner RSI thresholds changed:
     - CALL BUY: 5M RSI > 70 (was 60), 15M RSI > 70 (was 55)
     - PUT BUY: 5M RSI < 30 (was 40), 15M RSI < 30 (was 45)
  ✅ Option Scanner signal timestamp filter: 10 minutes → 30 minutes
  ✅ Futures Scanner RSI thresholds relaxed:
     - LONG: 15M RSI ≥ 50 (was 60)
     - SHORT: 15M RSI ≤ 50 (was 40)
  ✅ Futures Scanner internal thresholds relaxed to 50/50
  ✅ Added debug output to help identify signal filtering issues
  ✅ All other pages unchanged
"""

import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
import pytz
import json
import os
import time
import logging
import warnings
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

# ── TradingView data feed (primary source) ──────────────────
try:
    from tvdatafeed import TvDatafeed, Interval as TvInterval
    _TV = TvDatafeed()
    TV_AVAILABLE = True
    logging.info("tvdatafeed loaded ✅")
except Exception as _tv_err:
    TV_AVAILABLE = False
    logging.warning(f"tvdatafeed not available ({_tv_err}). Install: pip install tvdatafeed")
# ────────────────────────────────────────────────────────────

warnings.filterwarnings("ignore")

# ============================================================
# CONFIGURATION
# ============================================================

logging.basicConfig(level=logging.WARNING)
# Suppress noisy third-party loggers
for _noisy in ("yfinance", "peewee", "urllib3", "requests"):
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)

# Session-level cache: tickers that failed ALL data sources this run.
_failed_tickers: set[str] = set()

IST = pytz.timezone("Asia/Kolkata")
DB_FILE = "nse_tracker.db"

# ADX Configuration
DEFAULT_ADX_THRESHOLD = 25
DEFAULT_ADX_WEAK = 20

DEFAULT_NSE_WATCHLIST = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "HINDUNILVR.NS", "LT.NS",
    "KOTAKBANK.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS",
    "TATAMOTORS.NS", "SUNPHARMA.NS", "WIPRO.NS", "JIOFIN.NS", "MCX.NS",
]

MCX_COMMODITIES = {
    "GC=F":        {"name": "Gold",              "yf_ticker": "GC=F",  "fallback": "GLD",  "unit": "₹/10g",     "conversion_type": "gold",   "is_usd": True},
    "SI=F":        {"name": "Silver",            "yf_ticker": "SI=F",  "fallback": "SLV",  "unit": "₹/kg",      "conversion_type": "silver", "is_usd": True},
    "CL=F":        {"name": "Crude Oil",         "yf_ticker": "CL=F",  "fallback": "USO",  "unit": "₹/barrel",  "conversion_type": "simple", "is_usd": True},
    "NG=F":        {"name": "Natural Gas",       "yf_ticker": "NG=F",  "fallback": "UNG",  "unit": "₹/MMBtu",   "conversion_type": "simple", "is_usd": True},
    "HG=F":        {"name": "Copper",            "yf_ticker": "HG=F",  "fallback": "CPER", "unit": "₹/kg",      "conversion_type": "copper", "is_usd": True},
    "ZNC=F":       {"name": "Zinc",              "yf_ticker": "ZNC=F", "fallback": None,   "unit": "₹/kg",      "conversion_type": "zinc",   "is_usd": True},
    "NG=F_MINI":   {"name": "Natural Gas Mini",  "yf_ticker": "NG=F",  "fallback": "UNG",  "unit": "₹/MMBtu",   "conversion_type": "simple", "is_usd": True,  "lot_size": 250,    "is_mini": True},
    "ZNC=F_MINI":  {"name": "Zinc Mini",         "yf_ticker": "ZNC=F", "fallback": None,   "unit": "₹/kg",      "conversion_type": "zinc",   "is_usd": True,  "lot_size": 1000,   "is_mini": True},
    "ALI=F_MINI":  {"name": "Aluminium Mini",    "yf_ticker": "ALI=F", "fallback": None,   "unit": "₹/kg",      "conversion_type": "aluminium", "is_usd": True, "lot_size": 1000, "is_mini": True},
}

REALISTIC_MCX_PRICES = {
    "GC=F": 78500, "SI=F": 92000, "CL=F": 6800,
    "NG=F": 259,   "HG=F": 850,   "ZNC=F": 230,
    "NG=F_MINI": 259,   "ZNC=F_MINI": 230,   "ALI=F_MINI": 210,
}

MOCK_NSE_PRICES = {
    "RELIANCE.NS": 2450.50, "TCS.NS": 3850.75, "INFY.NS": 1450.25,
    "HDFCBANK.NS": 1680.30, "ICICIBANK.NS": 1120.45, "SBIN.NS": 780.60,
    "BHARTIARTL.NS": 1100.80, "ITC.NS": 445.20, "HINDUNILVR.NS": 2520.15,
    "LT.NS": 3250.00, "KOTAKBANK.NS": 1850.90, "AXISBANK.NS": 1050.35,
    "ASIANPAINT.NS": 3150.55, "MARUTI.NS": 10400.00, "TATAMOTORS.NS": 950.40,
    "SUNPHARMA.NS": 1480.70, "WIPRO.NS": 520.85, "JIOFIN.NS": 285.50, "MCX.NS": 2956.20,
}

# ── NSE F&O Universe (~200 stocks) ──────────────────────────
NSE_FNO_UNIVERSE = [
    "RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS","SBIN.NS",
    "BHARTIARTL.NS","ITC.NS","HINDUNILVR.NS","LT.NS","KOTAKBANK.NS","AXISBANK.NS",
    "ASIANPAINT.NS","MARUTI.NS","TATAMOTORS.NS","SUNPHARMA.NS","WIPRO.NS","JIOFIN.NS",
    "MCX.NS","TATASTEEL.NS","ADANIENT.NS","ADANIPORTS.NS","ULTRACEMCO.NS","TECHM.NS",
    "BAJFINANCE.NS","BAJAJFINSV.NS","HCLTECH.NS","NESTLEIND.NS","POWERGRID.NS",
    "NTPC.NS","ONGC.NS","COALINDIA.NS","GRASIM.NS","DRREDDY.NS","CIPLA.NS",
    "DIVISLAB.NS","BPCL.NS","BRITANNIA.NS","EICHERMOT.NS","HEROMOTOCO.NS",
    "APOLLOHOSP.NS","TITAN.NS","TATACONSUM.NS","HINDALCO.NS","JSWSTEEL.NS",
    "M&M.NS","INDUSINDBK.NS","BAJAJ-AUTO.NS","CHOLAFIN.NS","DLF.NS",
    "GODREJCP.NS","HAVELLS.NS","PIDILITIND.NS","BALKRISIND.NS","MUTHOOTFIN.NS",
    "LUPIN.NS","TORNTPHARM.NS","ALKEM.NS","BIOCON.NS","BANDHANBNK.NS",
    "FEDERALBNK.NS","IDFCFIRSTB.NS","PNB.NS","BANKBARODA.NS","CANBK.NS",
    "UNIONBANK.NS","IOC.NS","HINDPETRO.NS","MGL.NS","IGL.NS","GAIL.NS",
    "PETRONET.NS","SAIL.NS","NATIONALUM.NS","VEDL.NS","HINDZINC.NS",
    "AMBUJACEM.NS","ACC.NS","DALBHARAT.NS","SIEMENS.NS","ABB.NS",
    "BEL.NS","HAL.NS","IRCTC.NS","CONCOR.NS","RECLTD.NS","PFC.NS",
    "IRFC.NS","HUDCO.NS","NHPC.NS","SJVN.NS","TATAPOWER.NS","TORNTPOWER.NS",
    "ADANIGREEN.NS","ADANIPOWER.NS","ZOMATO.NS","NYKAA.NS","PAYTM.NS",
    "DELHIVERY.NS","POLICYBZR.NS","MPHASIS.NS","LTIM.NS","PERSISTENT.NS",
    "COFORGE.NS","KPITTECH.NS","TATAELXSI.NS","OFSS.NS","CYIENT.NS",
    "ESCORTS.NS","ASHOKLEY.NS","TIINDIA.NS","MOTHERSON.NS","ENDURANCE.NS",
    "SUNDARMFIN.NS","SHRIRAMFIN.NS","MANAPPURAM.NS","M&MFIN.NS","PIRAMAL.NS",
    "SBILIFE.NS","HDFCLIFE.NS","ICICIPRULI.NS","LICI.NS","STARHEALTH.NS",
    "PIIND.NS","UPL.NS","RALLIS.NS","JUBLFOOD.NS","VBL.NS","RADICO.NS",
    "COLPAL.NS","DABUR.NS","EMAMILTD.NS","MARICO.NS","PAGEIND.NS",
    "APLAPOLLO.NS","RVNL.NS","IRCON.NS","KALYANKJIL.NS","TRENT.NS",
    "BHEL.NS","THERMAX.NS","CUMMINSIND.NS","SCHAEFFLER.NS","SYNGENE.NS",
    "GLAND.NS","STRIDES.NS","GRANULES.NS","DEEPAKNTR.NS","AARTI.NS",
    "VINATI.NS","NAVINFLUOR.NS","SRF.NS","ZYDUSLIFE.NS","ABBOTINDIA.NS",
    "PFIZER.NS","TATACHEM.NS","GNFC.NS","CESC.NS","JSWENERGY.NS",
    "CHOLAHLDNG.NS","ICICIGI.NS","BAJAJHLDNG.NS","HONASA.NS","ABCAPITAL.NS",
    "INDIAMART.NS","RPOWER.NS","GSPL.NS",
    "360ONE.NS","AUBANK.NS","ANGELONE.NS","BANKINDIA.NS","CDSL.NS","INDIANB.NS",
    "IEX.NS","KFINTECH.NS","LICHSGFIN.NS","LTFH.NS","MFSL.NS","MOTILALOFS.NS",
    "NAM-INDIA.NS","NUVAMA.NS","PNBHOUSING.NS","RBLBANK.NS","SBICARD.NS","IIFL.NS",
    "BSE.NS","CAMS.NS","HDFCAMC.NS","AUROPHARMA.NS","FORTIS.NS","GLENMARK.NS",
    "LAURUSLABS.NS","MANKIND.NS","MAXHEALTH.NS","LTTS.NS","KAYNES.NS","DIXON.NS",
    "PGEL.NS","DMART.NS","GODFRYPHLP.NS","MCDOWELL-N.NS","PATANJALI.NS","VISHALMEGA.NS",
    "AMBER.NS","ASTRAL.NS","BLUESTARCO.NS","BOSCHLTD.NS","CGPOWER.NS","CROMPTON.NS",
    "FORCEMOT.NS","KEI.NS","SONACOMS.NS","SUPREMEIND.NS","TVSMOTOR.NS","UNOMINDA.NS",
    "VOLTAS.NS","GODREJPROP.NS","LODHA.NS","OBEROIRLTY.NS","PHOENIXLTD.NS","PRESTIGE.NS",
    "ADANIENSOL.NS","IREDA.NS","INDUSTOWER.NS","INOXWIND.NS","PREMIERENE.NS","SUZLON.NS",
    "WAAREEENER.NS","BDL.NS","COCHINSHIP.NS","MAZDOCK.NS","BHARATFORG.NS","JINDALSTEL.NS",
    "NMDC.NS","OIL.NS","INDHOTEL.NS","INDIGO.NS","ETERNAL.NS","GMRAIRPORT.NS",
    "HYUNDAI.NS","NBCC.NS","NAUKRI.NS","POWERINDIA.NS","SWIGGY.NS","IDEA.NS","YESBANK.NS",
]

# ============================================================
# ADX (Average Directional Index) for Trend Strength Filter
# ============================================================

def calculate_adx(df: pd.DataFrame, period: int = 14) -> dict:
    if df is None or df.empty or len(df) < period + 5:
        return {
            "adx": 0.0,
            "plus_di": 0.0,
            "minus_di": 0.0,
            "regime": "NO_DATA",
            "recommendation": "AVOID",
            "color": "#FF1744",
            "message": "Insufficient data"
        }
    
    df = df.copy()
    
    df['H-L'] = df['High'] - df['Low']
    df['H-PC'] = abs(df['High'] - df['Close'].shift(1))
    df['L-PC'] = abs(df['Low'] - df['Close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    
    df['UpMove'] = df['High'] - df['High'].shift(1)
    df['DownMove'] = df['Low'].shift(1) - df['Low']
    
    df['PlusDM'] = 0.0
    df['MinusDM'] = 0.0
    
    df.loc[(df['UpMove'] > df['DownMove']) & (df['UpMove'] > 0), 'PlusDM'] = df['UpMove']
    df.loc[(df['DownMove'] > df['UpMove']) & (df['DownMove'] > 0), 'MinusDM'] = df['DownMove']
    
    atr = df['TR'].ewm(alpha=1/period, adjust=False).mean()
    plus_di = 100 * (df['PlusDM'].ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, np.nan))
    minus_di = 100 * (df['MinusDM'].ewm(alpha=1/period, adjust=False).mean() / atr.replace(0, np.nan))
    
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(alpha=1/period, adjust=False).mean()
    
    current_adx = float(adx.iloc[-1]) if not pd.isna(adx.iloc[-1]) else 0.0
    current_plus_di = float(plus_di.iloc[-1]) if not pd.isna(plus_di.iloc[-1]) else 0.0
    current_minus_di = float(minus_di.iloc[-1]) if not pd.isna(minus_di.iloc[-1]) else 0.0
    
    adx_threshold = _get_adx_threshold()
    adx_weak = _get_adx_weak_threshold()
    
    if current_adx > 40:
        regime = "STRONG_TREND"
        recommendation = "TRADE"
        color = "#00E676"
        message = f"🔥 Very Strong Trend (ADX: {current_adx:.1f})"
    elif current_adx > adx_threshold:
        regime = "TRENDING"
        recommendation = "TRADE"
        color = "#00C853"
        message = f"✅ Trending Market (ADX: {current_adx:.1f})"
    elif current_adx > adx_weak:
        regime = "WEAK_TREND"
        recommendation = "REDUCE_SIZE"
        color = "#FFD700"
        message = f"⚠️ Weak Trend (ADX: {current_adx:.1f}) - Reduce position size"
    else:
        regime = "RANGING"
        recommendation = "AVOID"
        color = "#FF1744"
        message = f"❌ Ranging Market (ADX: {current_adx:.1f}) - No trades"
    
    return {
        "adx": round(current_adx, 1),
        "plus_di": round(current_plus_di, 1),
        "minus_di": round(current_minus_di, 1),
        "regime": regime,
        "recommendation": recommendation,
        "color": color,
        "message": message
    }


def _get_adx_threshold() -> int:
    try:
        conn = get_db()
        row = conn.execute("SELECT value FROM settings WHERE key='adx_threshold'").fetchone()
        conn.close()
        if row:
            return int(row["value"])
    except Exception:
        pass
    return DEFAULT_ADX_THRESHOLD


def _get_adx_weak_threshold() -> int:
    try:
        conn = get_db()
        row = conn.execute("SELECT value FROM settings WHERE key='adx_weak'").fetchone()
        conn.close()
        if row:
            return int(row["value"])
    except Exception:
        pass
    return DEFAULT_ADX_WEAK


def _save_adx_threshold(value: int):
    try:
        conn = get_db()
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('adx_threshold', ?)", (str(value),))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"_save_adx_threshold: {e}")


def _save_adx_weak(value: int):
    try:
        conn = get_db()
        conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('adx_weak', ?)", (str(value),))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"_save_adx_weak: {e}")


# ============================================================
# DATABASE — SQLite
# ============================================================

_db_lock = threading.Lock()


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _db_lock:
        conn = get_db()
        c = conn.cursor()

        c.executescript("""
            CREATE TABLE IF NOT EXISTS watchlist_nse (
                ticker TEXT PRIMARY KEY,
                added_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS watchlist_mcx (
                ticker TEXT PRIMARY KEY,
                added_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS portfolio (
                pid TEXT PRIMARY KEY,
                ticker TEXT,
                quantity REAL,
                entry_price REAL,
                added_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS reentry_state (
                symbol TEXT PRIMARY KEY,
                entry_price REAL,
                entry_time TEXT,
                trend TEXT,
                pattern_high REAL,
                pattern_low REAL,
                pattern_name TEXT,
                status TEXT DEFAULT 'IN_POSITION',
                timestamp TEXT
            );
            CREATE TABLE IF NOT EXISTS signals_shown (
                key TEXT PRIMARY KEY,
                notified_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                symbol TEXT,
                type TEXT,
                old_val TEXT,
                new_val TEXT,
                message TEXT,
                read INTEGER DEFAULT 0,
                is_signal INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS trade_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                symbol_name TEXT,
                direction TEXT,
                entry_price REAL,
                exit_price REAL,
                quantity REAL,
                pnl REAL,
                entry_time TEXT,
                exit_time TEXT,
                pattern_name TEXT,
                trend TEXT,
                stop_loss REAL,
                target REAL,
                notes TEXT,
                adx_at_entry REAL,
                market_regime TEXT
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        conn.commit()
        conn.close()

    _migrate_from_json()


def _migrate_from_json():
    migrations = [
        ("watchlist_nse.json", "watchlist_nse", lambda d: [(t,) for t in d], "INSERT OR IGNORE INTO watchlist_nse (ticker) VALUES (?)"),
        ("watchlist_mcx.json", "watchlist_mcx", lambda d: [(t,) for t in d], "INSERT OR IGNORE INTO watchlist_mcx (ticker) VALUES (?)"),
    ]
    for fname, _, make_rows, sql in migrations:
        if os.path.exists(fname):
            try:
                with open(fname) as f:
                    data = json.load(f)
                conn = get_db()
                conn.executemany(sql, make_rows(data))
                conn.commit()
                conn.close()
                os.rename(fname, fname + ".migrated")
            except Exception as e:
                logging.warning(f"JSON migration {fname}: {e}")

    if os.path.exists("portfolio.json"):
        try:
            with open("portfolio.json") as f:
                pf = json.load(f)
            conn = get_db()
            for pid, pos in pf.items():
                conn.execute(
                    "INSERT OR IGNORE INTO portfolio (pid, ticker, quantity, entry_price) VALUES (?,?,?,?)",
                    (pid, pos["ticker"], pos["quantity"], pos["entry_price"]),
                )
            conn.commit()
            conn.close()
            os.rename("portfolio.json", "portfolio.json.migrated")
        except Exception as e:
            logging.warning(f"JSON migration portfolio.json: {e}")

    if os.path.exists("reentry_state.json"):
        try:
            with open("reentry_state.json") as f:
                state = json.load(f)
            conn = get_db()
            for sym, info in state.items():
                conn.execute(
                    "INSERT OR IGNORE INTO reentry_state "
                    "(symbol,entry_price,entry_time,trend,pattern_high,pattern_low,pattern_name,status,timestamp) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (sym, info.get("entry_price"), info.get("entry_time"), info.get("trend"),
                     info.get("pattern_high"), info.get("pattern_low"), info.get("pattern_name"),
                     info.get("status", "IN_POSITION"), info.get("timestamp")),
                )
            conn.commit()
            conn.close()
            os.rename("reentry_state.json", "reentry_state.json.migrated")
        except Exception as e:
            logging.warning(f"JSON migration reentry_state.json: {e}")


# ============================================================
# SIGNAL NOTIFICATION TRACKING
# ============================================================

def was_signal_notified(symbol: str, breakout_time: str) -> bool:
    try:
        key = f"{symbol}_{breakout_time}"
        conn = get_db()
        conn.execute("DELETE FROM signals_shown WHERE notified_at < datetime('now', '-1 day')")
        row = conn.execute("SELECT 1 FROM signals_shown WHERE key=?", (key,)).fetchone()
        conn.commit()
        conn.close()
        return row is not None
    except Exception as e:
        logging.warning(f"was_signal_notified: {e}")
        return False


def mark_signal_notified(symbol: str, breakout_time: str):
    try:
        key = f"{symbol}_{breakout_time}"
        conn = get_db()
        conn.execute("INSERT OR IGNORE INTO signals_shown (key) VALUES (?)", (key,))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"mark_signal_notified: {e}")


# ============================================================
# RE-ENTRY STATE MANAGEMENT
# ============================================================

def mark_reentry_done(symbol, entry_price, entry_time, trend, pattern_high, pattern_low, pattern_name, adx_value=None, market_regime=None):
    try:
        conn = get_db()
        conn.execute(
            "INSERT OR REPLACE INTO reentry_state "
            "(symbol,entry_price,entry_time,trend,pattern_high,pattern_low,pattern_name,status,timestamp) "
            "VALUES (?,?,?,?,?,?,?,'IN_POSITION',?)",
            (symbol, entry_price, entry_time, trend, pattern_high, pattern_low, pattern_name, datetime.now().isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"mark_reentry_done: {e}")


def clear_reentry_state(symbol):
    try:
        conn = get_db()
        conn.execute("DELETE FROM reentry_state WHERE symbol=?", (symbol,))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"clear_reentry_state: {e}")


def is_reentry_done(symbol) -> bool:
    try:
        conn = get_db()
        row = conn.execute("SELECT 1 FROM reentry_state WHERE symbol=?", (symbol,)).fetchone()
        conn.close()
        return row is not None
    except Exception as e:
        logging.warning(f"is_reentry_done: {e}")
        return False


def get_reentry_info(symbol):
    try:
        conn = get_db()
        row = conn.execute("SELECT * FROM reentry_state WHERE symbol=?", (symbol,)).fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        logging.warning(f"get_reentry_info: {e}")
        return None


# ============================================================
# USD/INR RATE
# ============================================================

def get_usd_inr_rate() -> float:
    cache = st.session_state.get("usd_inr_cache")
    last  = st.session_state.get("usd_inr_last_fetch")
    if cache and last and (datetime.now() - last).seconds < 300:
        return cache

    try:
        df = yf.Ticker("USDINR=X").history(period="1d")
        if not df.empty:
            rate = float(df["Close"].iloc[-1])
            if 70 < rate < 100:
                st.session_state["usd_inr_cache"] = rate
                st.session_state["usd_inr_last_fetch"] = datetime.now()
                return rate
    except Exception as e:
        logging.warning(f"USD/INR primary: {e}")

    try:
        resp = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
        if resp.status_code == 200:
            rate = resp.json().get("rates", {}).get("INR", 0)
            if 70 < rate < 100:
                st.session_state["usd_inr_cache"] = rate
                st.session_state["usd_inr_last_fetch"] = datetime.now()
                return rate
    except Exception as e:
        logging.warning(f"USD/INR secondary: {e}")

    return cache if cache else 85.5


# ============================================================
# COMMODITY HELPERS
# ============================================================

def get_commodity_info(ticker: str) -> dict:
    return MCX_COMMODITIES.get(
        ticker,
        {"name": ticker.replace("=F", " Futures"), "fallback": None,
         "unit": "USD", "conversion_type": None, "is_usd": False},
    )


def convert_usd_to_inr_for_mcx(usd_price: float, ticker: str) -> float:
    info = get_commodity_info(ticker)
    ct = info.get("conversion_type")
    if ct is None:
        return usd_price
    r = get_usd_inr_rate()
    if ct == "simple":
        return round(usd_price * r, 2)
    elif ct == "gold":
        return round((usd_price / 31.1034768) * 10 * r, 2)
    elif ct == "silver":
        return round((usd_price / 31.1034768) * 1000 * r, 2)
    elif ct in ("copper", "zinc"):
        return round((usd_price / 0.45359237) * r, 2)
    elif ct == "aluminium":
        return round((usd_price / 1000) * r, 2)
    return round(usd_price * r, 2)


def get_mock_price(ticker: str) -> float:
    if ticker.endswith(".NS"):
        return MOCK_NSE_PRICES.get(ticker, 500.0)
    return REALISTIC_MCX_PRICES.get(ticker, 1000.0)


def get_current_price(ticker: str, df=None) -> float:
    if df is not None and not df.empty:
        usd_price = float(df["Close"].iloc[-1])
        info = get_commodity_info(ticker)
        if info.get("is_usd") or ticker in MCX_COMMODITIES:
            return convert_usd_to_inr_for_mcx(usd_price, ticker)
        return usd_price
    return get_mock_price(ticker)


_INDEX_DISPLAY_NAMES = {
    "^NSEI":    "NIFTY 50",
    "^NSEBANK": "BANK NIFTY",
}

def get_symbol_display_name(ticker: str) -> str:
    if ticker in _INDEX_DISPLAY_NAMES:
        return _INDEX_DISPLAY_NAMES[ticker]
    if ticker.endswith(".NS"):
        return ticker.replace(".NS", "")
    if ticker in MCX_COMMODITIES:
        return MCX_COMMODITIES[ticker]["name"]
    if "=F" in ticker:
        return ticker.replace("=F", " Futures")
    return ticker


def format_price(price, is_nse=True) -> str:
    if price is None:
        return "N/A"
    try:
        price = float(price)
        if pd.isna(price) or price <= 0:
            return "N/A"
        if price >= 1000:
            return f"₹{price:,.0f}"
        elif price >= 1:
            return f"₹{price:,.2f}"
        else:
            return f"₹{price:.4f}"
    except Exception:
        return "N/A"


def format_pnl(pnl) -> str:
    if pnl is None:
        return "N/A"
    try:
        pnl = float(pnl)
        if pd.isna(pnl):
            return "N/A"
        sign = "+" if pnl >= 0 else ""
        if abs(pnl) >= 1000:
            return f"{sign}₹{pnl:,.0f}"
        elif abs(pnl) >= 0.01:
            return f"{sign}₹{pnl:,.2f}"
        else:
            return f"{sign}₹{pnl:.4f}"
    except Exception:
        return "N/A"


# ============================================================
# MARKET SESSION INDICATOR
# ============================================================

def get_market_status() -> dict:
    now = datetime.now(IST)
    wd  = now.weekday()
    result = {}

    def mk_time(h, m):
        return now.replace(hour=h, minute=m, second=0, microsecond=0)

    nse_open, nse_close = mk_time(9, 15), mk_time(15, 30)
    if wd < 5 and nse_open <= now <= nse_close:
        result["NSE"] = ("🟢 OPEN", "#00C853")
    elif wd < 5 and now < nse_open:
        mins = int((nse_open - now).total_seconds() / 60)
        result["NSE"] = (f"⏰ Opens {mins}m", "#FFD700")
    else:
        result["NSE"] = ("🔴 CLOSED", "#FF1744")

    mcx_open, mcx_close = mk_time(9, 0), mk_time(23, 30)
    if wd < 5 and mcx_open <= now <= mcx_close:
        result["MCX"] = ("🟢 OPEN", "#00C853")
    else:
        result["MCX"] = ("🔴 CLOSED", "#FF1744")

    return result


# ============================================================
# DATA FETCHING
# ============================================================

_TV_NSE_EXCHANGE = "NSE"

_TV_MCX_MAP = {
    "GC=F": ("GOLD", "MCX"),
    "SI=F": ("SILVER", "MCX"),
    "CL=F": ("CRUDEOIL", "MCX"),
    "NG=F": ("NATURALGAS", "MCX"),
    "HG=F": ("COPPER", "MCX"),
    "ZNC=F": ("ZINC", "MCX"),
    "NG=F_MINI": ("NATURALGAS", "MCX"),
    "ZNC=F_MINI": ("ZINC", "MCX"),
    "ALI=F_MINI": ("ALUMINIUM", "MCX"),
}

_TV_INTERVAL_MAP = {
    "1m": TvInterval.in_1_minute if TV_AVAILABLE else None,
    "2m": TvInterval.in_2_minute if TV_AVAILABLE else None,
    "5m": TvInterval.in_5_minute if TV_AVAILABLE else None,
    "15m": TvInterval.in_15_minute if TV_AVAILABLE else None,
    "1h": TvInterval.in_1_hour if TV_AVAILABLE else None,
    "4h": TvInterval.in_4_hour if TV_AVAILABLE else None,
    "1d": TvInterval.in_daily if TV_AVAILABLE else None,
} if TV_AVAILABLE else {}

_PERIOD_BARS = {
    "2m": {"5d": 975, "30d": 2000, "60d": 3500},
    "5m": {"5d": 500, "30d": 1500, "60d": 2000},
    "15m": {"5d": 200, "30d": 600, "60d": 1000},
    "1h": {"5d": 100, "30d": 300, "60d": 500},
    "4h": {"5d": 50, "30d": 120, "60d": 250},
    "1d": {"5d": 10, "30d": 35, "60d": 70},
}


def _n_bars(interval: str, period: str) -> int:
    return _PERIOD_BARS.get(interval, {}).get(period, 300)


def _tv_fetch(tv_symbol: str, exchange: str, interval: str, period: str) -> pd.DataFrame:
    if not TV_AVAILABLE:
        return pd.DataFrame()
    tv_interval = _TV_INTERVAL_MAP.get(interval)
    if tv_interval is None:
        return pd.DataFrame()
    n = _n_bars(interval, period)
    try:
        df = _TV.get_hist(
            symbol=tv_symbol,
            exchange=exchange,
            interval=tv_interval,
            n_bars=n,
            extended_session=False,
        )
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.rename(columns={
            "open": "Open", "high": "High",
            "low": "Low", "close": "Close", "volume": "Volume",
        })
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("UTC")
        if float(df["Close"].iloc[-1]) > 0:
            return df
    except Exception as e:
        logging.warning(f"tvdatafeed {tv_symbol}@{exchange} {interval}: {str(e)[:100]}")
    return pd.DataFrame()


def fetch_data_robust(ticker: str, period="60d", interval="1d", max_retries=2) -> pd.DataFrame:
    if ticker in _failed_tickers:
        return pd.DataFrame()

    info = get_commodity_info(ticker)
    yf_ticker = info.get("yf_ticker") or ticker

    _TV_INDEX_MAP = {
        "^NSEI":    ("NIFTY",     _TV_NSE_EXCHANGE),
        "^NSEBANK": ("BANKNIFTY", _TV_NSE_EXCHANGE),
    }

    if TV_AVAILABLE:
        if ticker in _TV_INDEX_MAP:
            tv_sym, tv_exch = _TV_INDEX_MAP[ticker]
            df = _tv_fetch(tv_sym, tv_exch, interval, period)
            if not df.empty:
                return df
        elif ticker.endswith(".NS"):
            tv_sym = ticker.replace(".NS", "")
            df = _tv_fetch(tv_sym, _TV_NSE_EXCHANGE, interval, period)
            if not df.empty:
                return df
        elif ticker in _TV_MCX_MAP:
            tv_sym, tv_exch = _TV_MCX_MAP[ticker]
            df = _tv_fetch(tv_sym, tv_exch, interval, period)
            if not df.empty:
                return df

    yf_interval = interval

    for attempt in range(max_retries):
        try:
            df = yf.Ticker(yf_ticker).history(period=period, interval=yf_interval, auto_adjust=False)
            if not df.empty and pd.notna(df["Close"].iloc[-1]) and float(df["Close"].iloc[-1]) > 0:
                return df
        except Exception as e:
            logging.warning(f"yfinance attempt {attempt+1} {yf_ticker}: {str(e)[:80]}")
        if attempt < max_retries - 1:
            time.sleep(0.5)

    if ticker.endswith(".NS"):
        try:
            df = yf.Ticker(ticker.replace(".NS", ".BO")).history(period=period, interval=yf_interval)
            if not df.empty and float(df["Close"].iloc[-1]) > 0:
                return df
        except Exception as e:
            logging.warning(f"BSE fallback {ticker}: {e}")

    fallback = info.get("fallback")
    if fallback:
        try:
            df = yf.Ticker(fallback).history(period=period, interval=yf_interval)
            if not df.empty and float(df["Close"].iloc[-1]) > 0:
                return df
        except Exception as e:
            logging.warning(f"ETF fallback {ticker}: {e}")

    _failed_tickers.add(ticker)
    logging.warning(f"No data for {ticker} ({interval}) — skipping for this session")
    return pd.DataFrame()


# ============================================================
# TECHNICAL INDICATORS
# ============================================================

def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0).ewm(com=period - 1, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0.0)).ewm(com=period - 1, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ============================================================
# TREND — EMA alignment + RSI + ADX
# ============================================================

def get_daily_trend_ema(ticker: str):
    daily = fetch_data_robust(ticker, period="60d", interval="1d")

    if daily.empty:
        cp = get_mock_price(ticker)
        return "NEUTRAL", "⚪", cp, cp * 0.98, cp * 0.96, cp * 0.94, 50.0, None

    if len(daily) < 30:
        return "NEUTRAL", "⚪", get_current_price(ticker, daily), None, None, None, None, None

    daily["EMA_10"] = daily["Close"].ewm(span=10, adjust=False).mean()
    daily["EMA_20"] = daily["Close"].ewm(span=20, adjust=False).mean()
    daily["EMA_50"] = daily["Close"].ewm(span=50, adjust=False).mean()
    daily["RSI"] = calc_rsi(daily["Close"])

    adx_info = calculate_adx(daily)

    is_commodity = ticker in MCX_COMMODITIES or get_commodity_info(ticker).get("is_usd", False)

    if is_commodity:
        def c(v): return convert_usd_to_inr_for_mcx(float(v), ticker)
        ema_10, ema_20, ema_50 = c(daily["EMA_10"].iloc[-1]), c(daily["EMA_20"].iloc[-1]), c(daily["EMA_50"].iloc[-1])
        current_price = get_current_price(ticker, daily)
    else:
        ema_10, ema_20, ema_50 = float(daily["EMA_10"].iloc[-1]), float(daily["EMA_20"].iloc[-1]), float(daily["EMA_50"].iloc[-1])
        current_price = float(daily["Close"].iloc[-1])

    rsi = float(daily["RSI"].iloc[-1]) if not daily["RSI"].isna().all() else 50.0

    if ema_10 > ema_20 > ema_50:
        return "BULLISH", "🟢", current_price, ema_10, ema_20, ema_50, rsi, adx_info
    elif ema_10 < ema_20 < ema_50:
        return "BEARISH", "🔴", current_price, ema_10, ema_20, ema_50, rsi, adx_info
    else:
        return "NEUTRAL", "⚪", current_price, ema_10, ema_20, ema_50, rsi, adx_info


# ============================================================
# PULLBACK PATTERN DETECTION — 4H
# ============================================================

def _volume_declining(pattern_candles: pd.DataFrame, all_candles: pd.DataFrame) -> bool:
    if "Volume" not in all_candles.columns or all_candles["Volume"].sum() == 0:
        return True
    avg_vol = all_candles["Volume"].mean()
    pat_vol = pattern_candles["Volume"].mean()
    return pat_vol < avg_vol


def _candle_body(c) -> float:
    return abs(float(c["Close"]) - float(c["Open"]))

def _candle_range(c) -> float:
    return float(c["High"]) - float(c["Low"])

def _upper_wick(c) -> float:
    return float(c["High"]) - max(float(c["Close"]), float(c["Open"]))

def _lower_wick(c) -> float:
    return min(float(c["Close"]), float(c["Open"])) - float(c["Low"])

def _is_bearish(c) -> bool:
    return float(c["Close"]) < float(c["Open"])

def _is_bullish(c) -> bool:
    return float(c["Close"]) >= float(c["Open"])


def _detect_bearish_candle_pattern(closed: pd.DataFrame) -> dict | None:
    if len(closed) < 3:
        return None

    scan = closed.tail(12).reset_index(drop=True)
    n = len(scan)

    def make_result(name, idx_start, idx_end):
        pat = scan.iloc[idx_start: idx_end + 1]
        return {
            "type": "BEARISH_RETRACEMENT",
            "name": name,
            "high": float(pat["High"].max()),
            "low": float(pat["Low"].min()),
            "candles": len(pat),
            "vol_declining": _volume_declining(pat, closed),
        }

    best = None

    if n >= 5:
        for i in range(n - 1, 3, -1):
            c0 = scan.iloc[i - 4]
            c4 = scan.iloc[i]
            mid = [scan.iloc[i - 4 + j] for j in range(1, 4)]
            body0 = _candle_body(c0)
            body4 = _candle_body(c4)
            mid_bullish = all(_is_bullish(m) for m in mid)
            mid_small = all(_candle_body(m) < body0 * 0.5 for m in mid)
            mid_inside = all(float(m["Close"]) < float(c0["Open"]) and float(m["Low"]) > float(c0["Low"]) for m in mid)
            if (_is_bearish(c0) and body0 > 0 and _is_bearish(c4) and body4 > 0
                    and mid_bullish and mid_small and mid_inside
                    and float(c4["Close"]) < float(c0["Close"])):
                if best is None or i > best[1]:
                    best = (make_result("FALLING THREE METHODS", i - 4, i), i)

    for i in range(n - 1, 1, -1):
        c0 = scan.iloc[i - 2]
        c1 = scan.iloc[i - 1]
        c2 = scan.iloc[i]
        body0 = _candle_body(c0)
        body1 = _candle_body(c1)
        body2 = _candle_body(c2)

        result_3 = None

        if (_is_bullish(c0) and body0 > 0
                and body1 / max(_candle_range(c1), 0.0001) < 0.15
                and float(c1["Low"]) > float(c0["High"])
                and _is_bearish(c2)
                and float(c2["High"]) < float(c1["Low"])):
            result_3 = make_result("BEARISH ABANDONED BABY", i - 2, i)

        elif (_is_bearish(c0) and _is_bearish(c1) and _is_bearish(c2)
                and body0 > 0 and body1 > 0 and body2 > 0
                and float(c1["Open"]) < float(c0["Open"]) and float(c1["Open"]) > float(c0["Close"])
                and float(c2["Open"]) < float(c1["Open"]) and float(c2["Open"]) > float(c1["Close"])
                and float(c1["Close"]) < float(c0["Close"])
                and float(c2["Close"]) < float(c1["Close"])):
            result_3 = make_result("THREE BLACK CROWS", i - 2, i)

        elif (_is_bullish(c0) and body0 > 0
                and body1 < body0 * 0.4
                and _is_bearish(c2)
                and body2 > body0 * 0.5
                and float(c2["Close"]) < (float(c0["Open"]) + body0 * 0.5)):
            pat = scan.iloc[i - 2: i + 1]
            result_3 = {
                "type": "BEARISH_RETRACEMENT",
                "name": "EVENING STAR",
                "high": float(pat["High"].max()),
                "low": float(pat["Low"].min()),
                "candles": 3,
                "vol_declining": _volume_declining(pat, closed),
            }

        if result_3 and (best is None or i > best[1]):
            best = (result_3, i)

    for i in range(n - 1, 0, -1):
        c0 = scan.iloc[i - 1]
        c1 = scan.iloc[i]
        body0 = _candle_body(c0)
        body1 = _candle_body(c1)

        result_2 = None

        if (_is_bullish(c0) and _is_bearish(c1) and body1 > 0
                and float(c1["Open"]) >= float(c0["Close"])
                and float(c1["Close"]) <= float(c0["Open"])
                and body1 > body0 * 0.98):
            result_2 = make_result("BEARISH ENGULFING", i - 1, i)

        elif (_is_bullish(c0) and _is_bearish(c1) and body0 > 0
                and float(c1["Open"]) > float(c0["High"])
                and float(c1["Close"]) < (float(c0["Open"]) + body0 * 0.5)):
            result_2 = make_result("DARK CLOUD COVER", i - 1, i)

        elif (abs(float(c0["High"]) - float(c1["High"])) / max(float(c0["High"]), 0.0001) < 0.0005
                and _is_bullish(c0) and _is_bearish(c1)):
            result_2 = make_result("TWEEZER TOP", i - 1, i)

        elif (_is_bullish(c0) and _is_bearish(c1) and body0 > 0
                and float(c1["Open"]) <= float(c0["Close"])
                and float(c1["Close"]) >= float(c0["Open"])
                and body1 < body0 * 0.6):
            result_2 = make_result("BEARISH HARAMI", i - 1, i)

        if result_2 and (best is None or i > best[1]):
            best = (result_2, i)

    for i in range(n - 1, max(n - 5, 0), -1):
        c1 = scan.iloc[i]
        rng1 = _candle_range(c1)
        body1 = _candle_body(c1)

        result_1 = None

        if (rng1 > 0 and body1 > 0
                and _upper_wick(c1) >= body1 * 2.0
                and _lower_wick(c1) <= body1 * 0.5
                and _is_bearish(c1)):
            result_1 = make_result("SHOOTING STAR", i, i)

        elif (rng1 > 0 and body1 / rng1 < 0.1
                and _upper_wick(c1) >= rng1 * 0.7):
            result_1 = make_result("GRAVESTONE DOJI", i, i)

        elif (_is_bearish(c1) and rng1 > 0 and body1 > 0
                and _lower_wick(c1) >= body1 * 2.0
                and _upper_wick(c1) <= body1 * 0.3):
            result_1 = make_result("HANGING MAN", i, i)

        elif (_is_bearish(c1) and rng1 > 0 and body1 / rng1 > 0.90
                and body1 > 0):
            result_1 = make_result("BEARISH MARUBOZU", i, i)

        if result_1 and (best is None or i > best[1]):
            best = (result_1, i)

    return best[0] if best else None


def _detect_bullish_candle_pattern(closed: pd.DataFrame) -> dict | None:
    if len(closed) < 3:
        return None

    scan = closed.tail(12).reset_index(drop=True)
    n = len(scan)

    def make_result(name, idx_start, idx_end):
        pat = scan.iloc[idx_start: idx_end + 1]
        return {
            "type": "BULLISH_RETRACEMENT",
            "name": name,
            "high": float(pat["High"].max()),
            "low": float(pat["Low"].min()),
            "candles": len(pat),
            "vol_declining": _volume_declining(pat, closed),
        }

    best = None

    for i in range(n - 1, 1, -1):
        c0 = scan.iloc[i - 2]
        c1 = scan.iloc[i - 1]
        c2 = scan.iloc[i]
        body0 = _candle_body(c0)
        body1 = _candle_body(c1)
        body2 = _candle_body(c2)

        result_3 = None

        if (_is_bullish(c0) and _is_bullish(c1) and _is_bullish(c2)
                and body0 > 0 and body1 > 0 and body2 > 0
                and float(c1["Open"]) > float(c0["Open"]) and float(c1["Open"]) < float(c0["Close"])
                and float(c2["Open"]) > float(c1["Open"]) and float(c2["Open"]) < float(c1["Close"])
                and float(c1["Close"]) > float(c0["Close"])
                and float(c2["Close"]) > float(c1["Close"])):
            result_3 = make_result("THREE WHITE SOLDIERS", i - 2, i)

        elif (_is_bearish(c0) and body0 > 0
                and _is_bullish(c1) and body1 < body0 * 0.6
                and float(c1["Open"]) >= float(c0["Close"])
                and float(c1["Close"]) <= float(c0["Open"])
                and float(c1["Open"]) <= float(c0["Open"])
                and float(c1["Close"]) >= float(c0["Close"])
                and _is_bullish(c2)
                and float(c2["Close"]) > float(c0["Open"])):
            result_3 = make_result("THREE INSIDE UP", i - 2, i)

        elif (_is_bearish(c0) and body0 > 0
                and body1 < body0 * 0.4
                and _is_bullish(c2)
                and body2 > body0 * 0.5
                and float(c2["Close"]) > (float(c0["Open"]) - body0 * 0.5)):
            pat = scan.iloc[i - 2: i + 1]
            result_3 = {
                "type": "BULLISH_RETRACEMENT",
                "name": "MORNING STAR",
                "high": float(pat["High"].max()),
                "low": float(pat["Low"].min()),
                "candles": 3,
                "vol_declining": _volume_declining(pat, closed),
            }

        if result_3 and (best is None or i > best[1]):
            best = (result_3, i)

    for i in range(n - 1, 0, -1):
        c0 = scan.iloc[i - 1]
        c1 = scan.iloc[i]
        body0 = _candle_body(c0)
        body1 = _candle_body(c1)

        result_2 = None

        if (_is_bearish(c0) and _is_bullish(c1) and body1 > 0
                and float(c1["Open"]) <= float(c0["Close"])
                and float(c1["Close"]) >= float(c0["Open"])
                and body1 > body0 * 0.98):
            result_2 = make_result("BULLISH ENGULFING", i - 1, i)

        elif (_is_bearish(c0) and _is_bullish(c1) and body0 > 0
                and float(c1["Open"]) < float(c0["Low"])
                and float(c1["Close"]) > (float(c0["Open"]) - body0 * 0.5)):
            result_2 = make_result("PIERCING LINE", i - 1, i)

        elif (abs(float(c0["Low"]) - float(c1["Low"])) / max(float(c0["Low"]), 0.0001) < 0.0005
                and _is_bearish(c0) and _is_bullish(c1)):
            result_2 = make_result("TWEEZER BOTTOMS", i - 1, i)

        elif (_is_bearish(c0) and _is_bullish(c1) and body0 > 0
                and float(c1["Open"]) >= float(c0["Close"])
                and float(c1["Close"]) <= float(c0["Open"])
                and body1 < body0 * 0.6):
            result_2 = make_result("BULLISH HARAMI", i - 1, i)

        if result_2 and (best is None or i > best[1]):
            best = (result_2, i)

    for i in range(n - 1, max(n - 5, 0), -1):
        c1 = scan.iloc[i]
        rng1 = _candle_range(c1)
        body1 = _candle_body(c1)

        result_1 = None

        if (_is_bullish(c1) and rng1 > 0 and body1 > 0
                and _lower_wick(c1) >= body1 * 2.0
                and _upper_wick(c1) <= body1 * 0.5):
            result_1 = make_result("HAMMER", i, i)

        elif (rng1 > 0 and body1 / rng1 < 0.1
                and _lower_wick(c1) >= rng1 * 0.7):
            result_1 = make_result("DRAGONFLY DOJI", i, i)

        elif (_is_bullish(c1) and rng1 > 0 and body1 > 0
                and _upper_wick(c1) >= body1 * 2.0
                and _lower_wick(c1) <= body1 * 0.3):
            result_1 = make_result("INVERTED HAMMER", i, i)

        elif (_is_bullish(c1) and rng1 > 0 and body1 / rng1 > 0.90
                and body1 > 0):
            result_1 = make_result("BULLISH MARUBOZU", i, i)

        if result_1 and (best is None or i > best[1]):
            best = (result_1, i)

    return best[0] if best else None


def detect_bearish_pullback_pattern(df_4h: pd.DataFrame):
    if df_4h is None or df_4h.empty or len(df_4h) < 5:
        return None
    closed = df_4h.iloc[:-1].tail(30).copy().reset_index(drop=True)
    return _detect_bearish_candle_pattern(closed)


def detect_bullish_pullback_pattern(df_4h: pd.DataFrame):
    if df_4h is None or df_4h.empty or len(df_4h) < 5:
        return None
    closed = df_4h.iloc[:-1].tail(30).copy().reset_index(drop=True)
    return _detect_bullish_candle_pattern(closed)


@st.cache_data(ttl=60)
def detect_retracement_15m(ticker: str, trend: str) -> dict:
    """
    Detects TWO types of 15M signals for each trend direction:

    BULLISH TREND:
      - RETRACEMENT (exit signal): EMA10 crosses DOWN through EMA20 + RSI ≤ 40
        → "Bullish Retracement" — consider exiting long trade
      - RE-ENTRY (new long signal): EMA10 crosses UP through EMA20 + RSI ≥ 60
        → "Bullish Re-Entry" — re-enter long; SL = low of cross candle

    BEARISH TREND:
      - RETRACEMENT (exit signal): EMA10 crosses UP through EMA20 + RSI ≥ 60
        → "Bearish Retracement" — consider exiting short trade
      - RE-ENTRY (new short signal): EMA10 crosses DOWN through EMA20 + RSI ≤ 40
        → "Bearish Re-Entry" — re-enter short; SL = high of cross candle
    """
    result = {
        "is_retracement": False,
        "is_reentry": False,
        "direction": "NONE",
        "ema10": None, "ema20": None, "rsi_15m": None,
        "cross_time": None,
        "cross_candle_low": None,   # SL for bullish re-entry
        "cross_candle_high": None,  # SL for bearish re-entry
        "description": "No 15M data",
    }

    df_15m = fetch_data_robust(ticker, period="30d", interval="15m")
    if df_15m is None or df_15m.empty or len(df_15m) < 30:
        result["description"] = "Insufficient 15M data"
        return result

    df = df_15m.copy()

    df["EMA10"] = df["Close"].ewm(span=10, adjust=False).mean()
    df["EMA20"] = df["Close"].ewm(span=20, adjust=False).mean()

    delta = df["Close"].diff()
    gain = delta.clip(lower=0).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1/14, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    df["RSI"] = 100 - (100 / (1 + rs))

    closed_15m = df.iloc[:-1]
    if len(closed_15m) < 3:
        return result

    cur = closed_15m.iloc[-1]
    prev = closed_15m.iloc[-2]

    e10_now = float(cur["EMA10"])
    e20_now = float(cur["EMA20"])
    e10_prev = float(prev["EMA10"])
    e20_prev = float(prev["EMA20"])
    rsi_now = float(cur["RSI"]) if not pd.isna(cur["RSI"]) else 50.0

    result["ema10"] = e10_now
    result["ema20"] = e20_now
    result["rsi_15m"] = rsi_now

    def _find_cross_candle(direction_up: bool):
        """Return (cross_time_str, candle_low, candle_high) for the most recent crossover."""
        for i in range(len(closed_15m) - 1, max(len(closed_15m) - 8, 0), -1):
            r  = closed_15m.iloc[i]
            rp = closed_15m.iloc[i - 1]
            if direction_up:
                if float(rp["EMA10"]) <= float(rp["EMA20"]) and float(r["EMA10"]) > float(r["EMA20"]):
                    return _to_ist(closed_15m.index[i]), float(r["Low"]), float(r["High"])
            else:
                if float(rp["EMA10"]) >= float(rp["EMA20"]) and float(r["EMA10"]) < float(r["EMA20"]):
                    return _to_ist(closed_15m.index[i]), float(r["Low"]), float(r["High"])
        return None, None, None

    def _recent_cross(direction_up: bool, window: int = 6) -> bool:
        recent = closed_15m.tail(window)
        for i in range(len(recent) - 1):
            e10_i  = float(recent["EMA10"].iloc[i])
            e20_i  = float(recent["EMA20"].iloc[i])
            e10_i1 = float(recent["EMA10"].iloc[i + 1])
            e20_i1 = float(recent["EMA20"].iloc[i + 1])
            if direction_up:
                if e10_i <= e20_i and e10_i1 > e20_i1:
                    return True
            else:
                if e10_i >= e20_i and e10_i1 < e20_i1:
                    return True
        return False

    if trend == "BULLISH":
        # ── BULLISH RETRACEMENT (exit long): EMA10 crosses DOWN + RSI ≤ 40 ──
        crossed_down = (e10_prev >= e20_prev) and (e10_now < e20_now)
        if (crossed_down or _recent_cross(direction_up=False)) and rsi_now <= 40:
            ct, c_low, c_high = _find_cross_candle(direction_up=False)
            result.update({
                "is_retracement": True,
                "is_reentry": False,
                "direction": "BEARISH_CROSS",
                "cross_time": ct,
                "cross_candle_low": c_low,
                "cross_candle_high": c_high,
                "description": (
                    f"⚠️ BULLISH RETRACEMENT — EXIT LONG: 15M EMA10({e10_now:.2f}) crossed ↓ EMA20({e20_now:.2f}) | "
                    f"RSI {rsi_now:.1f} ≤ 40 — Consider exiting long position"
                ),
            })

        # ── BULLISH RE-ENTRY (new long): EMA10 crosses UP + RSI ≥ 60 ──
        elif (((e10_prev <= e20_prev) and (e10_now > e20_now)) or _recent_cross(direction_up=True)) and rsi_now >= 60:
            ct, c_low, c_high = _find_cross_candle(direction_up=True)
            result.update({
                "is_retracement": False,
                "is_reentry": True,
                "direction": "BULLISH_REENTRY",
                "cross_time": ct,
                "cross_candle_low": c_low,   # SL = low of this candle
                "cross_candle_high": c_high,
                "description": (
                    f"🟢 BULLISH RE-ENTRY: 15M EMA10({e10_now:.2f}) crossed ↑ EMA20({e20_now:.2f}) | "
                    f"RSI {rsi_now:.1f} ≥ 60 — Re-enter LONG | SL = Low of cross candle"
                ),
            })

    elif trend == "BEARISH":
        # ── BEARISH RETRACEMENT (exit short): EMA10 crosses UP + RSI ≥ 60 ──
        crossed_up = (e10_prev <= e20_prev) and (e10_now > e20_now)
        if (crossed_up or _recent_cross(direction_up=True)) and rsi_now >= 60:
            ct, c_low, c_high = _find_cross_candle(direction_up=True)
            result.update({
                "is_retracement": True,
                "is_reentry": False,
                "direction": "BULLISH_CROSS",
                "cross_time": ct,
                "cross_candle_low": c_low,
                "cross_candle_high": c_high,
                "description": (
                    f"⚠️ BEARISH RETRACEMENT — EXIT SHORT: 15M EMA10({e10_now:.2f}) crossed ↑ EMA20({e20_now:.2f}) | "
                    f"RSI {rsi_now:.1f} ≥ 60 — Consider exiting short position"
                ),
            })

        # ── BEARISH RE-ENTRY (new short): EMA10 crosses DOWN + RSI ≤ 40 ──
        elif (((e10_prev >= e20_prev) and (e10_now < e20_now)) or _recent_cross(direction_up=False)) and rsi_now <= 40:
            ct, c_low, c_high = _find_cross_candle(direction_up=False)
            result.update({
                "is_retracement": False,
                "is_reentry": True,
                "direction": "BEARISH_REENTRY",
                "cross_time": ct,
                "cross_candle_low": c_low,
                "cross_candle_high": c_high,  # SL = high of this candle
                "description": (
                    f"🔴 BEARISH RE-ENTRY: 15M EMA10({e10_now:.2f}) crossed ↓ EMA20({e20_now:.2f}) | "
                    f"RSI {rsi_now:.1f} ≤ 40 — Re-enter SHORT | SL = High of cross candle"
                ),
            })

    return result


# ============================================================
# OPTION SCANNER — NSE F&O (UPDATED with v20.8 RSI thresholds)
# ============================================================

@st.cache_data(ttl=60)
def _get_option_scan_data(ticker: str) -> dict | None:
    """
    Fetch daily trend + 5M EMA10/20 crossover + RSI for a single ticker.
    v20.8 UPDATED RSI THRESHOLDS:
      CALL BUY: Daily BULLISH + 5M EMA10 crosses UP EMA20 + RSI > 70 rising toward 80
      PUT  BUY: Daily BEARISH + 5M EMA10 crosses DOWN EMA20 + RSI < 30 falling toward 20
    SL = close of crossover bar | Target = PREVIOUS SWING HIGH (call) or LOW (put) on 5M
    ENTRY = Current stock price
    """
    try:
        df_1d = fetch_data_robust(ticker, period="180d", interval="1d")
        if df_1d is None or df_1d.empty or len(df_1d) < 55:
            return None
        df_1d["EMA10"] = df_1d["Close"].ewm(span=10, adjust=False).mean()
        df_1d["EMA20"] = df_1d["Close"].ewm(span=20, adjust=False).mean()
        df_1d["EMA50"] = df_1d["Close"].ewm(span=50, adjust=False).mean()
        e10 = float(df_1d["EMA10"].iloc[-1])
        e20 = float(df_1d["EMA20"].iloc[-1])
        e50 = float(df_1d["EMA50"].iloc[-1])
        if e10 > e20 > e50:
            daily_trend = "BULLISH"
        elif e10 < e20 < e50:
            daily_trend = "BEARISH"
        else:
            daily_trend = "NEUTRAL"

        cur_price = float(df_1d["Close"].iloc[-1])

        signal = "NONE"
        rsi_2m = None
        rsi_15m_conf = None
        sl_price = None
        tgt_price = None
        entry_price = cur_price
        signal_time = datetime.now(IST).strftime("%H:%M:%S")

        if daily_trend in ("BULLISH", "BEARISH"):
            direction = "LONG" if daily_trend == "BULLISH" else "SHORT"

            passes_2m, rsi_2m_val, sl_c, tgt_c, entry_c, cross_time = _check_rsi_5m_filter_v208(ticker, direction)
            rsi_2m = round(rsi_2m_val, 1) if rsi_2m_val is not None else None

            sl_price = round(sl_c, 2) if sl_c else None
            if entry_c:
                entry_price = round(entry_c, 2)
            tgt_price = round(tgt_c, 2) if tgt_c else round(entry_price * 1.01, 2)
            if cross_time and cross_time != "—":
                signal_time = cross_time

            try:
                _, rsi_15m_raw, _, _, _, _ = _check_rsi_15m_filter_v208(ticker, direction)
                rsi_15m_conf = round(rsi_15m_raw, 1) if rsi_15m_raw is not None else None
            except Exception:
                rsi_15m_conf = None

            passes_15m_rsi = False
            if rsi_15m_conf is not None:
                if direction == "LONG":
                    passes_15m_rsi = rsi_15m_conf > 70
                else:
                    passes_15m_rsi = rsi_15m_conf < 30
            else:
                passes_15m_rsi = True

            if passes_2m and passes_15m_rsi:
                signal = "CALL_BUY" if daily_trend == "BULLISH" else "PUT_BUY"

        name = get_symbol_display_name(ticker)
        return {
            "ticker": ticker,
            "name": name,
            "price": cur_price,
            "entry_price": entry_price,
            "daily_trend": daily_trend,
            "rsi_15m": rsi_2m,
            "rsi_15m_conf": rsi_15m_conf,
            "sl": sl_price,
            "target": tgt_price,
            "signal": signal,
            "signal_time": signal_time,
        }
    except Exception as e:
        logging.warning(f"Option scan {ticker}: {e}")
        return None


# ============================================================
# SHARED SIGNAL LOGIC — Watchlist / Portfolio / MCX Scanner
# ============================================================

@st.cache_data(ttl=60)
def _get_watchlist_signal_data(ticker: str) -> dict | None:
    try:
        df_1d = fetch_data_robust(ticker, period="180d", interval="1d")
        if df_1d is None or df_1d.empty or len(df_1d) < 55:
            return None

        df_1d["EMA10"] = df_1d["Close"].ewm(span=10, adjust=False).mean()
        df_1d["EMA20"] = df_1d["Close"].ewm(span=20, adjust=False).mean()
        df_1d["EMA50"] = df_1d["Close"].ewm(span=50, adjust=False).mean()
        e10 = float(df_1d["EMA10"].iloc[-1])
        e20 = float(df_1d["EMA20"].iloc[-1])
        e50 = float(df_1d["EMA50"].iloc[-1])

        if e10 > e20 > e50:
            daily_trend = "BULLISH"
        elif e10 < e20 < e50:
            daily_trend = "BEARISH"
        else:
            daily_trend = "NEUTRAL"

        is_commodity = ticker in MCX_COMMODITIES or get_commodity_info(ticker).get("is_usd", False)
        cur_price_raw = float(df_1d["Close"].iloc[-1])
        cur_price = convert_usd_to_inr_for_mcx(cur_price_raw, ticker) if is_commodity else cur_price_raw

        signal = "NONE"
        rsi_15m = None
        sl_price = None
        tgt_price = None
        entry_price = cur_price
        signal_time = datetime.now(IST).strftime("%H:%M:%S")

        if daily_trend in ("BULLISH", "BEARISH"):
            direction = "LONG" if daily_trend == "BULLISH" else "SHORT"
            passes, rsi_val, sl_c, tgt_c, entry_c, cross_time = _check_rsi_15m_filter(ticker, direction)
            rsi_15m = round(rsi_val, 1) if rsi_val is not None else None

            if is_commodity:
                entry_price = round(convert_usd_to_inr_for_mcx(entry_c, ticker), 2) if entry_c else cur_price
                sl_price    = round(convert_usd_to_inr_for_mcx(sl_c,    ticker), 2) if sl_c    else None
                tgt_price   = round(convert_usd_to_inr_for_mcx(tgt_c,   ticker), 2) if tgt_c   else None
            else:
                entry_price = round(entry_c, 2) if entry_c else cur_price
                sl_price    = round(sl_c,    2) if sl_c    else None
                tgt_price   = round(tgt_c,   2) if tgt_c   else None

            # ── Target: nearest significant 4H swing high/low ──
            if passes and entry_price:
                tgt_4h = _find_4h_swing_target(ticker, direction, entry_price)
                if tgt_4h:
                    tgt_price = tgt_4h

            if cross_time and cross_time != "—":
                signal_time = cross_time

            if passes:
                signal = "LONG" if daily_trend == "BULLISH" else "SHORT"

        name = get_symbol_display_name(ticker)
        return {
            "ticker": ticker,
            "name": name,
            "price": cur_price,
            "entry_price": entry_price,
            "daily_trend": daily_trend,
            "rsi_15m": rsi_15m,
            "sl": sl_price,
            "target": tgt_price,
            "signal": signal,
            "signal_time": signal_time,
            "is_commodity": is_commodity,
        }
    except Exception as e:
        logging.warning(f"Watchlist signal scan {ticker}: {e}")
        return None


def _tv_chart_url(ticker: str) -> str:
    _index_tv = {
        "^NSEI":    "NSE:NIFTY",
        "^NSEBANK": "NSE:BANKNIFTY",
    }
    if ticker in _index_tv:
        return f"https://in.tradingview.com/chart/?symbol={_index_tv[ticker]}&interval=D"
    return f"https://in.tradingview.com/chart/?symbol=NSE:{ticker.replace('.NS', '')}&interval=D"


def render_option_scanner_page():
    st.markdown('<p class="main-header">🎯 OPTION SCANNER — NSE F&O</p>', unsafe_allow_html=True)
    st.markdown("---")

    col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns(5)
    with col_f1:
        show_filter = st.selectbox("Show", ["Signals Only", "All Stocks"], key="opt_filter")
    with col_f2:
        trend_filter = st.selectbox("Trend Filter", ["All", "BULLISH", "BEARISH", "NEUTRAL"], key="opt_trend")
    with col_f3:
        call_rsi_min = st.number_input("🟢 Call 5M RSI >", min_value=0, max_value=100, value=70, step=1, key="opt_call_rsi_min",
                                        help="Only show CALL BUY signals where 5M RSI is above this value")
    with col_f4:
        put_rsi_max = st.number_input("🔴 Put 5M RSI <", min_value=0, max_value=100, value=30, step=1, key="opt_put_rsi_max",
                                       help="Only show PUT BUY signals where 5M RSI is below this value")
    with col_f5:
        if st.button("🔄 Refresh Scan", use_container_width=True, key="opt_refresh"):
            st.cache_data.clear()

    col_r1, col_r2, col_r3 = st.columns([1, 1, 3])
    with col_r1:
        call_15m_rsi_min = st.number_input(
            "🟢 Call 15M RSI >",
            min_value=0, max_value=100, value=70, step=1,
            key="opt_call_15m_rsi_min",
            help="Only show CALL BUY where 15M RSI confirmation is above this value"
        )
    with col_r2:
        put_15m_rsi_max = st.number_input(
            "🔴 Put 15M RSI <",
            min_value=0, max_value=100, value=30, step=1,
            key="opt_put_15m_rsi_max",
            help="Only show PUT BUY where 15M RSI confirmation is below this value"
        )
    with col_r3:
        st.markdown(
            "<div style='padding-top:28px;color:#888;font-size:.82rem;'>"
            "ℹ️ <b>15M RSI</b> is a dual-timeframe confirmation layer. v20.8: CALL needs 5M RSI > 70 & 15M RSI > 70; "
            "PUT needs 5M RSI < 30 & 15M RSI < 30.</div>",
            unsafe_allow_html=True
        )

    st.markdown("---")

    INDEX_SCAN_TICKERS = ["^NSEI", "^NSEBANK"]
    universe = list(dict.fromkeys(INDEX_SCAN_TICKERS + NSE_FNO_UNIVERSE))

    prog = st.progress(0, text="Scanning NSE F&O universe…")
    status = st.empty()

    results = []
    call_signals = []
    put_signals = []

    try:
        from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx
        _ctx = get_script_run_ctx()
    except Exception:
        _ctx = None

    def _scan(tk):
        if _ctx is not None:
            try:
                import threading as _t
                add_script_run_ctx(_t.current_thread(), _ctx)
            except Exception:
                pass
        return _get_option_scan_data(tk)

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_scan, tk): tk for tk in universe}
        done = 0
        for fut in as_completed(futures):
            done += 1
            tk = futures[fut]
            prog.progress(done / len(universe), text=f"Scanning {get_symbol_display_name(tk)}… ({done}/{len(universe)})")
            try:
                res = fut.result()
                if res:
                    results.append(res)
                    rsi_2m_val = res.get("rsi_15m") or 0
                    rsi_15m_val = res.get("rsi_15m_conf")
                    if res["signal"] == "CALL_BUY":
                        passes_2m_ui = rsi_2m_val > call_rsi_min
                        passes_15m_ui = (rsi_15m_val is None) or (rsi_15m_val > call_15m_rsi_min)
                        if passes_2m_ui and passes_15m_ui:
                            call_signals.append(res)
                    elif res["signal"] == "PUT_BUY":
                        passes_2m_ui = rsi_2m_val < put_rsi_max
                        passes_15m_ui = (rsi_15m_val is None) or (rsi_15m_val < put_15m_rsi_max)
                        if passes_2m_ui and passes_15m_ui:
                            put_signals.append(res)
            except Exception as e:
                logging.warning(f"Option scan future {tk}: {e}")

    prog.empty()
    status.empty()

    def _signal_within_30min(sig: dict) -> bool:
        raw = sig.get("signal_time", "")
        if not raw or raw == "—":
            return True
        now_ist = datetime.now(IST)
        today = now_ist.date()
        for fmt in ("%H:%M %d-%b", "%H:%M:%S"):
            try:
                parsed = datetime.strptime(raw, fmt)
                signal_dt = IST.localize(parsed.replace(year=today.year, month=today.month, day=today.day))
                if fmt == "%H:%M %d-%b":
                    parsed2 = datetime.strptime(f"{raw} {today.year}", "%H:%M %d-%b %Y")
                    signal_dt = IST.localize(parsed2)
                diff_minutes = (now_ist - signal_dt).total_seconds() / 60
                return diff_minutes <= 30
            except Exception:
                continue
        return True

    call_signals = [s for s in call_signals if _signal_within_30min(s)]
    put_signals = [s for s in put_signals if _signal_within_30min(s)]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("📊 Scanned", len(results))
    m2.metric("🟢 CALL BUY", len(call_signals))
    m3.metric("🔴 PUT BUY", len(put_signals))
    m4.metric("⚪ No Signal", len(results) - len(call_signals) - len(put_signals))

    if call_signals or put_signals:
        scan_time = datetime.now(IST).strftime("%d %b %Y %H:%M IST")
        all_lines = [f"🎯 OPTION SCANNER — {scan_time}",
                     f"📊 Scanned: {len(results)} | 🟢 CALL: {len(call_signals)} | 🔴 PUT: {len(put_signals)}",
                     ""]
        if call_signals:
            all_lines.append("🟢 CALL BUY Signals:")
            for r in sorted(call_signals, key=lambda x: x.get("rsi_15m") or 0, reverse=True):
                r15 = f"{r['rsi_15m']:.1f}" if r.get("rsi_15m") else "—"
                r15c = f"{r['rsi_15m_conf']:.1f}" if r.get("rsi_15m_conf") else "—"
                entry_s = f"₹{r['entry_price']:,.2f}" if r.get("entry_price") else "—"
                sl_s = f"₹{r['sl']:,.2f}" if r.get("sl") else "—"
                tg_s = f"₹{r['target']:,.2f}" if r.get("target") else "—"
                all_lines.append(f"  • {r['name']} | Entry:{entry_s} | 5M RSI:{r15} | 15M RSI:{r15c} | SL:{sl_s} | Tgt:{tg_s} | Time:{r.get('signal_time','—')}")
        if put_signals:
            all_lines.append("")
            all_lines.append("🔴 PUT BUY Signals:")
            for r in sorted(put_signals, key=lambda x: x.get("rsi_15m") or 100):
                r15 = f"{r['rsi_15m']:.1f}" if r.get("rsi_15m") else "—"
                r15c = f"{r['rsi_15m_conf']:.1f}" if r.get("rsi_15m_conf") else "—"
                entry_s = f"₹{r['entry_price']:,.2f}" if r.get("entry_price") else "—"
                sl_s = f"₹{r['sl']:,.2f}" if r.get("sl") else "—"
                tg_s = f"₹{r['target']:,.2f}" if r.get("target") else "—"
                all_lines.append(f"  • {r['name']} | Entry:{entry_s} | 5M RSI:{r15} | 15M RSI:{r15c} | SL:{sl_s} | Tgt:{tg_s} | Time:{r.get('signal_time','—')}")
        all_lines.append("\n— Om NSE Tracker v20.8")
        combined_msg = "\n".join(all_lines)

        tg_col1, tg_col2 = st.columns([3, 1])
        with tg_col2:
            if st.button("📱 Send All to Telegram", use_container_width=True, key="opt_tg_all"):
                import threading as _thr
                _result = {"ok": False, "err": ""}
                def _send():
                    ok, err = send_telegram_alert(combined_msg)
                    _result["ok"] = ok
                    _result["err"] = err
                t = _thr.Thread(target=_send, daemon=True)
                t.start()
                t.join(timeout=8)
                if _result["ok"]:
                    st.success("✅ Sent to Telegram!")
                else:
                    st.error(f"❌ {_result['err'] or 'Timeout — check your bot token'}")

    st.markdown("---")

    if call_signals or put_signals:
        if call_signals:
            st.markdown("### 🟢 CALL BUY Signals")
            for idx, r in enumerate(sorted(call_signals, key=lambda x: x.get("rsi_15m") or 0, reverse=True)):
                r15 = f"{r['rsi_15m']:.1f}" if r.get("rsi_15m") else "—"
                r15c = f"{r['rsi_15m_conf']:.1f}" if r.get("rsi_15m_conf") else "—"
                entry_s = f"₹{r['entry_price']:,.2f}" if r.get("entry_price") else "—"
                sl_s = f"₹{r['sl']:,.2f}" if r.get("sl") else "—"
                tg_s = f"₹{r['target']:,.2f}" if r.get("target") else "—"
                time_s = r.get('signal_time', '—')
                tv = _tv_chart_url(r['ticker'])
                card_col, btn_col = st.columns([5, 1])
                with card_col:
                    st.markdown(
                        f"<div style='background:rgba(0,200,83,.12);border-left:4px solid #00C853;"
                        f"padding:10px 16px;margin:6px 0;border-radius:6px;'>"
                        f"<b style='color:#00E676;font-size:1.05rem;'>🟢 {r['name']}</b> "
                        f"<span style='color:#aaa;font-size:.85rem;'>({r['ticker']})</span>"
                        f"<span style='float:right;color:#00E676;font-weight:bold;'>CALL BUY</span><br>"
                        f"<span style='color:#ccc;font-size:.9rem;'>"
                        f"Entry: <b style='color:#00F5FF;'>{entry_s}</b> | "
                        f"Trend: 🟢 BULLISH | "
                        f"5M RSI: <b style='color:#FFD700;'>{r15}</b> | "
                        f"15M RSI: <b style='color:#FFA726;'>{r15c}</b> | "
                        f"SL: <b style='color:#FF5252;'>{sl_s}</b> | "
                        f"Target: <b style='color:#00E676;'>{tg_s}</b> (Prev Swing High) | "
                        f"Time: <b style='color:#888;'>{time_s}</b> | "
                        f"<a href='{tv}' target='_blank' style='color:#00F5FF;'>📊 Chart ↗</a></span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with btn_col:
                    tg_msg = (
                        f"🟢 CALL BUY — {r['name']} ({r['ticker']})\n"
                        f"Entry: {entry_s}\n"
                        f"Daily Trend: BULLISH\n"
                        f"5M RSI: {r15} | 15M RSI: {r15c}\n"
                        f"SL: {sl_s}\n"
                        f"Target: {tg_s}\n"
                        f"Signal Time: {time_s}\n"
                        f"Chart: {tv}\n"
                        f"— Om NSE Tracker v20.8"
                    )
                    if st.button("📱", key=f"opt_tg_call_{idx}_{r['ticker']}",
                                 help=f"Send {r['name']} CALL signal to Telegram"):
                        import threading as _thr2
                        _res2 = {"ok": False, "err": ""}
                        def _s2(_m=tg_msg):
                            ok2, err2 = send_telegram_alert(_m)
                            _res2["ok"] = ok2
                            _res2["err"] = err2
                        _t2 = _thr2.Thread(target=_s2, daemon=True)
                        _t2.start()
                        _t2.join(timeout=8)
                        if _res2["ok"]:
                            st.toast(f"✅ {r['name']} sent to Telegram!", icon="📱")
                        else:
                            st.error(f"❌ {_res2['err']}")

        if put_signals:
            st.markdown("### 🔴 PUT BUY Signals")
            for idx, r in enumerate(sorted(put_signals, key=lambda x: x.get("rsi_15m") or 100)):
                r15 = f"{r['rsi_15m']:.1f}" if r.get("rsi_15m") else "—"
                r15c = f"{r['rsi_15m_conf']:.1f}" if r.get("rsi_15m_conf") else "—"
                entry_s = f"₹{r['entry_price']:,.2f}" if r.get("entry_price") else "—"
                sl_s = f"₹{r['sl']:,.2f}" if r.get("sl") else "—"
                tg_s = f"₹{r['target']:,.2f}" if r.get("target") else "—"
                time_s = r.get('signal_time', '—')
                tv = _tv_chart_url(r['ticker'])
                card_col, btn_col = st.columns([5, 1])
                with card_col:
                    st.markdown(
                        f"<div style='background:rgba(255,23,68,.12);border-left:4px solid #FF1744;"
                        f"padding:10px 16px;margin:6px 0;border-radius:6px;'>"
                        f"<b style='color:#FF5252;font-size:1.05rem;'>🔴 {r['name']}</b> "
                        f"<span style='color:#aaa;font-size:.85rem;'>({r['ticker']})</span>"
                        f"<span style='float:right;color:#FF5252;font-weight:bold;'>PUT BUY</span><br>"
                        f"<span style='color:#ccc;font-size:.9rem;'>"
                        f"Entry: <b style='color:#00F5FF;'>{entry_s}</b> | "
                        f"Trend: 🔴 BEARISH | "
                        f"5M RSI: <b style='color:#FFD700;'>{r15}</b> | "
                        f"15M RSI: <b style='color:#FFA726;'>{r15c}</b> | "
                        f"SL: <b style='color:#FF5252;'>{sl_s}</b> | "
                        f"Target: <b style='color:#00E676;'>{tg_s}</b> (Prev Swing Low) | "
                        f"Time: <b style='color:#888;'>{time_s}</b> | "
                        f"<a href='{tv}' target='_blank' style='color:#00F5FF;'>📊 Chart ↗</a></span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with btn_col:
                    tg_msg = (
                        f"🔴 PUT BUY — {r['name']} ({r['ticker']})\n"
                        f"Entry: {entry_s}\n"
                        f"Daily Trend: BEARISH\n"
                        f"5M RSI: {r15} | 15M RSI: {r15c}\n"
                        f"SL: {sl_s}\n"
                        f"Target: {tg_s}\n"
                        f"Signal Time: {time_s}\n"
                        f"Chart: {tv}\n"
                        f"— Om NSE Tracker v20.8"
                    )
                    if st.button("📱", key=f"opt_tg_put_{idx}_{r['ticker']}",
                                 help=f"Send {r['name']} PUT signal to Telegram"):
                        import threading as _thr3
                        _res3 = {"ok": False, "err": ""}
                        def _s3(_m=tg_msg):
                            ok3, err3 = send_telegram_alert(_m)
                            _res3["ok"] = ok3
                            _res3["err"] = err3
                        _t3 = _thr3.Thread(target=_s3, daemon=True)
                        _t3.start()
                        _t3.join(timeout=8)
                        if _res3["ok"]:
                            st.toast(f"✅ {r['name']} sent to Telegram!", icon="📱")
                        else:
                            st.error(f"❌ {_res3['err']}")
    else:
        st.info("No CALL or PUT signals found right now. Try refreshing during market hours. Note: v20.8 requires 5M RSI >70 for CALL and <30 for PUT.")

    st.markdown("---")

    with st.expander(f"📋 Full Scan Table ({len(results)} stocks)", expanded=show_filter == "All Stocks"):
        rows = []
        for r in results:
            t = r["daily_trend"]
            if trend_filter != "All" and t != trend_filter:
                continue
            if show_filter == "Signals Only" and r["signal"] == "NONE":
                continue
            sig_icon = "🟢 CALL BUY" if r["signal"] == "CALL_BUY" else ("🔴 PUT BUY" if r["signal"] == "PUT_BUY" else "—")
            trend_icon = "🟢" if t == "BULLISH" else ("🔴" if t == "BEARISH" else "⚪")
            rows.append({
                "Symbol": r["name"],
                "Price ₹": f"₹{r['price']:.2f}",
                "Entry ₹": f"₹{r['entry_price']:.2f}" if r.get("entry_price") else "—",
                "Daily Trend": f"{trend_icon} {t}",
                "5M RSI": f"{r['rsi_15m']:.1f}" if r.get("rsi_15m") else "—",
                "15M RSI": f"{r['rsi_15m_conf']:.1f}" if r.get("rsi_15m_conf") else "—",
                "SL ₹": f"₹{r['sl']:,.2f}" if r.get("sl") else "—",
                "Target ₹": f"₹{r['target']:,.2f}" if r.get("target") else "—",
                "Signal Time": r.get("signal_time", "—"),
                "Signal": sig_icon,
                "Chart": _tv_chart_url(r['ticker']),
            })
        if rows:
            st.dataframe(
                pd.DataFrame(rows),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Chart": st.column_config.LinkColumn("📊 Chart", display_text="Open ↗"),
                },
            )
        else:
            st.info("No stocks match the current filter.")


def _to_ist(ts) -> str:
    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
        ts = pytz.UTC.localize(ts)
    return ts.astimezone(IST).strftime("%d/%b %H:%M")


def _filter_to_session(df: pd.DataFrame) -> pd.DataFrame:
    try:
        idx = df.index
        if idx.tzinfo is None:
            idx = idx.tz_localize("UTC")
        idx_ist = idx.tz_convert(IST)
        today_date = datetime.now(IST).date()
        mask = idx_ist.date == today_date
        today_bars = df[mask]
        return today_bars if len(today_bars) >= 3 else df.tail(150)
    except Exception as e:
        logging.warning(f"_filter_to_session: {e}")
        return df.tail(150)


def _scan_closed_bars(closed: pd.DataFrame, pattern: dict, trend: str, tf_label: str):
    ph, pl = pattern["high"], pattern["low"]

    has_volume = ("Volume" in closed.columns and closed["Volume"].sum() > 0)
    avg_vol = float(closed["Volume"].tail(20).mean()) if has_volume else 0.0

    def _vol_ok(row) -> bool:
        if not has_volume or avg_vol == 0:
            return True
        return float(row["Volume"]) >= avg_vol * 0.9

    if trend == "BULLISH":
        rows = closed[closed["Close"] > ph]
        if not rows.empty:
            r = rows.iloc[-1]
            if _vol_ok(r):
                vol_tag = f" Vol✅{float(r['Volume'])/avg_vol:.1f}x" if has_volume else ""
                return "RE-ENTRY", float(r["Close"]), f"⚡{tf_label} CLOSE ABOVE @ {_to_ist(r.name)}{vol_tag}"
            else:
                vol_ratio = float(r["Volume"]) / avg_vol if avg_vol else 0
                return None, None, f"⚠️ Breakout candle LOW VOLUME ({vol_ratio:.1f}x avg) — waiting for confirmation"
        return None, None, f"Need {tf_label} close > {ph:.4f}"
    elif trend == "BEARISH":
        rows = closed[closed["Close"] < pl]
        if not rows.empty:
            r = rows.iloc[-1]
            if _vol_ok(r):
                vol_tag = f" Vol✅{float(r['Volume'])/avg_vol:.1f}x" if has_volume else ""
                return "RE-ENTRY", float(r["Close"]), f"⚡{tf_label} CLOSE BELOW @ {_to_ist(r.name)}{vol_tag}"
            else:
                vol_ratio = float(r["Volume"]) / avg_vol if avg_vol else 0
                return None, None, f"⚠️ Breakout candle LOW VOLUME ({vol_ratio:.1f}x avg) — waiting for confirmation"
        return None, None, f"Need {tf_label} close < {pl:.4f}"
    return None, None, "Trend undefined"


@st.cache_data(ttl=15)
def check_retracement_exit_5m(ticker: str, pattern_high, pattern_low, trend: str) -> dict:
    out = {"triggered": False, "signal_text": "", "price": None}
    if pattern_high is None or pattern_low is None:
        return out

    df_5m = fetch_data_robust(ticker, period="5d", interval="5m")
    if df_5m.empty or len(df_5m) < 2:
        return out

    closed_5m = _filter_to_session(df_5m.iloc[:-1])
    if closed_5m.empty:
        return out

    if trend == "BULLISH":
        rows = closed_5m[closed_5m["Close"] < pattern_low]
        if not rows.empty:
            r = rows.iloc[-1]
            out["triggered"] = True
            out["price"] = float(r["Close"])
            out["signal_text"] = f"⚠️ BULLISH RETRACEMENT — Consider EXIT FROM LONG TRADE (5M closed below {pattern_low:.4f} @ {_to_ist(r.name)})"
    else:
        rows = closed_5m[closed_5m["Close"] > pattern_high]
        if not rows.empty:
            r = rows.iloc[-1]
            out["triggered"] = True
            out["price"] = float(r["Close"])
            out["signal_text"] = f"⚠️ BEARISH RETRACEMENT — Consider EXIT FROM SHORT TRADE (5M closed above {pattern_high:.4f} @ {_to_ist(r.name)})"

    return out


@st.cache_data(ttl=15)
def check_reentry_live_fast(ticker: str, pattern_high, pattern_low, trend: str) -> dict:
    out = {
        "signal": "PULLBACK_ACTIVE", "entry_price": None,
        "breakout_time": None, "entry_tf": None,
        "approaching": False, "approach_price": None, "wait_msg": "",
    }

    if pattern_high is None or pattern_low is None:
        out["wait_msg"] = "No pattern levels"
        return out

    ph, pl = pattern_high, pattern_low

    df_5m = fetch_data_robust(ticker, period="5d", interval="5m")
    if not df_5m.empty:
        live = df_5m.iloc[-1]
        live_close = float(live["Close"])
        if trend == "BULLISH" and float(live["High"]) > ph:
            out["approaching"] = True
            out["approach_price"] = live_close
        elif trend == "BEARISH" and float(live["Low"]) < pl:
            out["approaching"] = True
            out["approach_price"] = live_close

        if len(df_5m) >= 2:
            closed_5m = _filter_to_session(df_5m.iloc[:-1])
            sig, ep, msg = _scan_closed_bars(closed_5m, {"high": ph, "low": pl}, trend, "5M")
            if sig == "RE-ENTRY":
                out.update({
                    "signal": "RE-ENTRY_SIGNAL", "entry_price": ep,
                    "breakout_time": msg.split("@ ")[-1] if "@ " in msg else None,
                    "entry_tf": "5m", "wait_msg": msg,
                })
                return out
            out["wait_msg"] = msg

    if out["approaching"]:
        out["signal"] = "APPROACHING"

    return out


def should_skip_due_to_adx(adx_info: dict) -> tuple[bool, str]:
    if adx_info is None:
        return False, ""
    
    adx_threshold = _get_adx_threshold()
    adx_weak = _get_adx_weak_threshold()
    
    if adx_info["adx"] < adx_weak:
        return True, f"❌ ADX FILTER: {adx_info['message']}"
    elif adx_info["adx"] < adx_threshold:
        return True, f"⚠️ ADX FILTER: {adx_info['message']} - Consider reducing size"
    
    return False, ""


# ============================================================
# OPTION SCANNER V20.8 FILTER FUNCTIONS
# ============================================================

def _check_rsi_5m_filter_v208(ticker: str, direction: str) -> tuple[bool, float, float | None, float | None, float | None, str]:
    """5-Min filter for Option Scanner v20.8 with RSI >70 for LONG, <30 for SHORT"""
    try:
        df3 = fetch_data_robust(ticker, period="5d", interval="5m")
        if df3 is None or df3.empty or len(df3) < 30:
            return False, 50.0, None, None, None, "—"

        df3 = df3.copy()
        df3["EMA10"] = df3["Close"].ewm(span=10, adjust=False).mean()
        df3["EMA20"] = df3["Close"].ewm(span=20, adjust=False).mean()
        rsi_s = _intra_rsi(df3["Close"], 14)

        closed = df3.iloc[:-1]
        if len(closed) < 5:
            return False, 50.0, None, None, None, "—"

        curr_rsi = float(rsi_s.iloc[-2]) if not pd.isna(rsi_s.iloc[-2]) else 50.0
        prev_rsi = float(rsi_s.iloc[-3]) if len(rsi_s) >= 3 and not pd.isna(rsi_s.iloc[-3]) else curr_rsi

        e10_now = float(closed["EMA10"].iloc[-1])
        e20_now = float(closed["EMA20"].iloc[-1])
        e10_prev = float(closed["EMA10"].iloc[-2])
        e20_prev = float(closed["EMA20"].iloc[-2])

        cross_bar_close = None
        cross_bar_time = None
        scan_window = min(195, len(closed) - 1)

        if direction == "LONG":
            for i in range(len(closed) - 1, len(closed) - 1 - scan_window, -1):
                if i < 1:
                    break
                if float(closed["EMA10"].iloc[i - 1]) <= float(closed["EMA20"].iloc[i - 1]) and \
                   float(closed["EMA10"].iloc[i]) > float(closed["EMA20"].iloc[i]):
                    cross_bar_close = float(closed["Close"].iloc[i])
                    cross_bar_time = closed.index[i]
                    break
        else:
            for i in range(len(closed) - 1, len(closed) - 1 - scan_window, -1):
                if i < 1:
                    break
                if float(closed["EMA10"].iloc[i - 1]) >= float(closed["EMA20"].iloc[i - 1]) and \
                   float(closed["EMA10"].iloc[i]) < float(closed["EMA20"].iloc[i]):
                    cross_bar_close = float(closed["Close"].iloc[i])
                    cross_bar_time = closed.index[i]
                    break

        if cross_bar_time is not None:
            try:
                if hasattr(cross_bar_time, "tzinfo") and cross_bar_time.tzinfo is not None:
                    ct_ist = cross_bar_time.astimezone(IST)
                else:
                    ct_ist = cross_bar_time.tz_localize("UTC").astimezone(IST)
                cross_time_str = ct_ist.strftime("%H:%M %d-%b")
            except Exception:
                cross_time_str = "—"
        else:
            cross_time_str = "—"

        if direction == "LONG":
            just_crossed = (e10_prev <= e20_prev) and (e10_now > e20_now)
            already_above = e10_now > e20_now
            ema_ok = just_crossed or (already_above and cross_bar_close is not None)
            rsi_ok = curr_rsi > 70 and curr_rsi > prev_rsi

            if ema_ok and rsi_ok:
                cur_price = float(closed["Close"].iloc[-1])
                entry = round(cur_price * 1.001, 2)

                swing_highs = []
                lookback = min(50, len(closed) - 5)
                for i in range(len(closed) - 2, max(len(closed) - lookback, 5), -1):
                    curr_high = float(closed["High"].iloc[i])
                    prev_high = float(closed["High"].iloc[i - 1])
                    next_high = float(closed["High"].iloc[i + 1]) if i + 1 < len(closed) else curr_high
                    if curr_high > prev_high and curr_high > next_high:
                        swing_highs.append(curr_high)
                swing_highs_above = [h for h in swing_highs if h > cur_price]
                target = round(min(swing_highs_above), 2) if swing_highs_above else round(cur_price * 1.01, 2)

                sl = round(cross_bar_close, 2) if cross_bar_close else round(e20_now, 2)
                return True, curr_rsi, sl, target, entry, cross_time_str
            return False, curr_rsi, None, None, None, cross_time_str

        else:
            just_crossed = (e10_prev >= e20_prev) and (e10_now < e20_now)
            already_below = e10_now < e20_now
            ema_ok = just_crossed or (already_below and cross_bar_close is not None)
            rsi_ok = curr_rsi < 30 and curr_rsi < prev_rsi

            if ema_ok and rsi_ok:
                cur_price = float(closed["Close"].iloc[-1])
                entry = round(cur_price * 0.999, 2)

                swing_lows = []
                lookback = min(50, len(closed) - 5)
                for i in range(len(closed) - 2, max(len(closed) - lookback, 5), -1):
                    curr_low = float(closed["Low"].iloc[i])
                    prev_low = float(closed["Low"].iloc[i - 1])
                    next_low = float(closed["Low"].iloc[i + 1]) if i + 1 < len(closed) else curr_low
                    if curr_low < prev_low and curr_low < next_low:
                        swing_lows.append(curr_low)
                swing_lows_below = [l for l in swing_lows if l < cur_price]
                target = round(max(swing_lows_below), 2) if swing_lows_below else round(cur_price * 0.99, 2)

                sl = round(cross_bar_close, 2) if cross_bar_close else round(e20_now, 2)
                return True, curr_rsi, sl, target, entry, cross_time_str
            return False, curr_rsi, None, None, None, cross_time_str

    except Exception as e:
        logging.warning(f"_check_rsi_5m_filter_v208 {ticker}: {e}")
        return True, 50.0, None, None, None, "—"


def _check_rsi_15m_filter_v208(ticker: str, direction: str) -> tuple[bool, float, float | None, float | None, float | None, str]:
    """15-Min filter for Option Scanner v20.8 with RSI >70 for LONG, <30 for SHORT"""
    try:
        df15 = fetch_data_robust(ticker, period="10d", interval="15m")
        if df15 is None or df15.empty or len(df15) < 30:
            return False, 50.0, None, None, None, "—"

        df15 = df15.copy()
        df15["EMA10"] = df15["Close"].ewm(span=10, adjust=False).mean()
        df15["EMA20"] = df15["Close"].ewm(span=20, adjust=False).mean()
        rsi_s = _intra_rsi(df15["Close"], 14)

        closed = df15.iloc[:-1]
        if len(closed) < 5:
            return False, 50.0, None, None, None, "—"

        curr_rsi = float(rsi_s.iloc[-2]) if not pd.isna(rsi_s.iloc[-2]) else 50.0
        prev_rsi = float(rsi_s.iloc[-3]) if len(rsi_s) >= 3 and not pd.isna(rsi_s.iloc[-3]) else curr_rsi

        e10_now = float(closed["EMA10"].iloc[-1])
        e20_now = float(closed["EMA20"].iloc[-1])
        e10_prev = float(closed["EMA10"].iloc[-2])
        e20_prev = float(closed["EMA20"].iloc[-2])

        cross_bar_close = None
        cross_bar_time = None
        scan_window = min(26, len(closed) - 1)

        if direction == "LONG":
            for i in range(len(closed) - 1, len(closed) - 1 - scan_window, -1):
                if i < 1:
                    break
                if float(closed["EMA10"].iloc[i - 1]) <= float(closed["EMA20"].iloc[i - 1]) and \
                   float(closed["EMA10"].iloc[i]) > float(closed["EMA20"].iloc[i]):
                    cross_bar_close = float(closed["Close"].iloc[i])
                    cross_bar_time = closed.index[i]
                    break
        else:
            for i in range(len(closed) - 1, len(closed) - 1 - scan_window, -1):
                if i < 1:
                    break
                if float(closed["EMA10"].iloc[i - 1]) >= float(closed["EMA20"].iloc[i - 1]) and \
                   float(closed["EMA10"].iloc[i]) < float(closed["EMA20"].iloc[i]):
                    cross_bar_close = float(closed["Close"].iloc[i])
                    cross_bar_time = closed.index[i]
                    break

        if cross_bar_time is not None:
            try:
                if hasattr(cross_bar_time, "tzinfo") and cross_bar_time.tzinfo is not None:
                    ct_ist = cross_bar_time.astimezone(IST)
                else:
                    ct_ist = cross_bar_time.tz_localize("UTC").astimezone(IST)
                cross_time_str = ct_ist.strftime("%H:%M %d-%b")
            except Exception:
                cross_time_str = "—"
        else:
            cross_time_str = "—"

        if direction == "LONG":
            just_crossed = (e10_prev <= e20_prev) and (e10_now > e20_now)
            already_above = e10_now > e20_now
            ema_ok = just_crossed or (already_above and cross_bar_close is not None)
            rsi_ok = curr_rsi > 70 and curr_rsi > prev_rsi

            if ema_ok and rsi_ok:
                cur_price = float(closed["Close"].iloc[-1])
                entry = round(cur_price * 1.001, 2)

                swing_highs = []
                lookback = min(50, len(closed) - 5)
                for i in range(len(closed) - 2, max(len(closed) - lookback, 5), -1):
                    curr_high = float(closed["High"].iloc[i])
                    prev_high = float(closed["High"].iloc[i - 1])
                    next_high = float(closed["High"].iloc[i + 1]) if i + 1 < len(closed) else curr_high
                    if curr_high > prev_high and curr_high > next_high:
                        swing_highs.append(curr_high)
                swing_highs_above = [h for h in swing_highs if h > cur_price]
                target = round(min(swing_highs_above), 2) if swing_highs_above else round(cur_price * 1.015, 2)

                sl = round(cross_bar_close, 2) if cross_bar_close else round(e20_now, 2)
                return True, curr_rsi, sl, target, entry, cross_time_str
            return False, curr_rsi, None, None, None, cross_time_str

        else:
            just_crossed = (e10_prev >= e20_prev) and (e10_now < e20_now)
            already_below = e10_now < e20_now
            ema_ok = just_crossed or (already_below and cross_bar_close is not None)
            rsi_ok = curr_rsi < 30 and curr_rsi < prev_rsi

            if ema_ok and rsi_ok:
                cur_price = float(closed["Close"].iloc[-1])
                entry = round(cur_price * 0.999, 2)

                swing_lows = []
                lookback = min(50, len(closed) - 5)
                for i in range(len(closed) - 2, max(len(closed) - lookback, 5), -1):
                    curr_low = float(closed["Low"].iloc[i])
                    prev_low = float(closed["Low"].iloc[i - 1])
                    next_low = float(closed["Low"].iloc[i + 1]) if i + 1 < len(closed) else curr_low
                    if curr_low < prev_low and curr_low < next_low:
                        swing_lows.append(curr_low)
                swing_lows_below = [l for l in swing_lows if l < cur_price]
                target = round(max(swing_lows_below), 2) if swing_lows_below else round(cur_price * 0.985, 2)

                sl = round(cross_bar_close, 2) if cross_bar_close else round(e20_now, 2)
                return True, curr_rsi, sl, target, entry, cross_time_str
            return False, curr_rsi, None, None, None, cross_time_str

    except Exception as e:
        logging.warning(f"_check_rsi_15m_filter_v208 {ticker}: {e}")
        return True, 50.0, None, None, None, "—"


# ============================================================
# MAIN FETCH FUNCTION (for Dashboard)
# ============================================================

@st.cache_data(ttl=300)
def fetch_stock_data(ticker: str) -> dict:
    _empty = {
        "current_price": get_mock_price(ticker), "trend": "NEUTRAL", "trend_icon": "⚪",
        "ema_10": None, "ema_20": None, "ema_50": None, "rsi": None,
        "signal": None, "signal_display": "Data unavailable", "entry_price": None,
        "stop_loss": None, "target_1r": None, "target_2r": None,
        "pattern_high": None, "pattern_low": None,
        "pattern_high_display": None, "pattern_low_display": None,
        "pattern_name": "NONE", "breakout_time": None,
        "support": None, "resistance": None,
        "pullback_pattern": None, "reentry_done": False, "reentry_info": None,
        "new_signal_detected": False, "vol_declining": None,
        "adx_info": None, "adx_filter_skip": False, "adx_filter_message": "",
    }
    try:
        trend, trend_icon, current_price, ema_10, ema_20, ema_50, rsi, adx_info = get_daily_trend_ema(ticker)

        if current_price is None:
            current_price = get_mock_price(ticker)
            trend, trend_icon = "NEUTRAL", "⚪"

        reentry_done = is_reentry_done(ticker)
        reentry_info = get_reentry_info(ticker) if reentry_done else None

        is_nse = ticker.endswith(".NS")
        is_commodity = ticker in MCX_COMMODITIES

        def fmt(p): return format_price(p, is_nse or is_commodity)

        signal = signal_display = None
        entry_price = stop_loss = target_1r = target_2r = None
        pattern_high = pattern_low = pattern_high_display = pattern_low_display = pattern_name = breakout_time = None
        new_signal_detected = False
        vol_declining = None

        if reentry_done:
            signal = "IN_POSITION"
            if reentry_info:
                _trend = reentry_info.get("trend", "BULLISH")
                sl_raw = reentry_info.get("pattern_low") if _trend == "BULLISH" else reentry_info.get("pattern_high")
                pattern_name = reentry_info.get("pattern_name", "UNKNOWN")
                ep = reentry_info["entry_price"]
                sl = float(sl_raw) if sl_raw else None
                if sl and is_commodity:
                    sl = convert_usd_to_inr_for_mcx(sl, ticker)
                target_1r, target_2r = compute_sr_targets(
                    ticker, _trend, ep,
                    reentry_info.get("pattern_high", ep),
                    reentry_info.get("pattern_low", ep),
                )
                lbl = "✅ LONG RE-ENTRY DONE" if _trend == "BULLISH" else "✅ SHORT RE-ENTRY DONE"
                signal_display = (f"{lbl} | Entry:{fmt(ep)} | SL:{fmt(sl)} | T1:{fmt(target_1r)}")

        elif trend == "BULLISH":
            retrace = detect_retracement_15m(ticker, "BULLISH")

            if retrace["is_retracement"]:
                # ── BULLISH RETRACEMENT: EMA10 crossed ↓ EMA20, RSI ≤ 40 → EXIT LONG ──
                df_15m_rt = fetch_data_robust(ticker, period="5d", interval="15m")
                if df_15m_rt is not None and not df_15m_rt.empty and len(df_15m_rt) > 5:
                    recent_15m = df_15m_rt.iloc[:-1].tail(8)
                    pattern_high = float(recent_15m["High"].max())
                    pattern_low = float(recent_15m["Low"].min())
                else:
                    pattern_high = current_price * 1.005
                    pattern_low = current_price * 0.995

                pattern_name = f"15M EMA CROSS ↓ (RSI {retrace['rsi_15m']:.1f})"
                cross_tag = f" | Cross@{retrace['cross_time']}" if retrace["cross_time"] else ""

                if is_commodity:
                    pattern_high_display = convert_usd_to_inr_for_mcx(pattern_high, ticker)
                    pattern_low_display = convert_usd_to_inr_for_mcx(pattern_low, ticker)
                else:
                    pattern_high_display = pattern_high
                    pattern_low_display = pattern_low

                rsi_15m_val = retrace["rsi_15m"]
                rsi_tag = f" | 15M RSI {rsi_15m_val:.1f}↓ (≤ 40)" if rsi_15m_val else ""

                exit_check = check_retracement_exit_5m(ticker, pattern_high, pattern_low, "BULLISH")
                if exit_check["triggered"]:
                    signal = "BULLISH_RETRACEMENT_EXIT"
                    signal_display = exit_check["signal_text"]
                else:
                    signal = "BULLISH_RETRACEMENT"
                    signal_display = (
                        f"⚠️ BULLISH RETRACEMENT — EXIT LONG: 15M EMA10 crossed ↓ EMA20{rsi_tag}{cross_tag} | "
                        f"Zone High:{fmt(pattern_high_display)} Low:{fmt(pattern_low_display)} | "
                        f"Waiting for 5M close BELOW {fmt(pattern_low_display)} to confirm exit"
                    )

            elif retrace["is_reentry"]:
                # ── BULLISH RE-ENTRY: EMA10 crossed ↑ EMA20, RSI ≥ 60 → RE-ENTER LONG ──
                cross_tag = f" | Cross@{retrace['cross_time']}" if retrace["cross_time"] else ""
                rsi_15m_val = retrace["rsi_15m"]
                rsi_tag = f" | 15M RSI {rsi_15m_val:.1f}↑ (≥ 60)" if rsi_15m_val else ""

                # SL = low of the EMA cross candle (raw price)
                c_low_raw = retrace.get("cross_candle_low")
                if c_low_raw:
                    sl_raw = c_low_raw
                    stop_loss = convert_usd_to_inr_for_mcx(sl_raw, ticker) if is_commodity else sl_raw
                else:
                    stop_loss = current_price * 0.99

                entry_price = current_price
                # Target = next resistance on 4H
                target_1r, target_2r = compute_sr_targets(
                    ticker, "BULLISH", entry_price,
                    current_price * 1.01, stop_loss
                )
                pattern_high = current_price * 1.01
                pattern_low = stop_loss if not is_commodity else convert_usd_to_inr_for_mcx(c_low_raw, ticker) if c_low_raw else current_price * 0.99
                pattern_name = f"15M EMA CROSS ↑ — BULLISH RE-ENTRY (RSI {rsi_15m_val:.1f})"
                signal = "BULLISH_REENTRY"
                signal_display = (
                    f"🟢 BULLISH RE-ENTRY: 15M EMA10 crossed ↑ EMA20{rsi_tag}{cross_tag} | "
                    f"Entry:{fmt(entry_price)} | SL:{fmt(stop_loss)} (Low of cross candle) | "
                    f"Target:{fmt(target_1r)} (Next 4H Resistance)"
                )

            else:
                df_4h_re = fetch_data_robust(ticker, period="30d", interval="4h")
                reentry_bull_pattern = detect_bullish_pullback_pattern(df_4h_re) if not df_4h_re.empty else None

                if reentry_bull_pattern:
                    pattern_high = reentry_bull_pattern["high"]
                    pattern_low = reentry_bull_pattern["low"]
                    pattern_name = reentry_bull_pattern.get("name", "BULLISH PATTERN")
                    vol_declining = reentry_bull_pattern.get("vol_declining")

                    if is_commodity:
                        pattern_high_display = convert_usd_to_inr_for_mcx(pattern_high, ticker)
                        pattern_low_display = convert_usd_to_inr_for_mcx(pattern_low, ticker)
                    else:
                        pattern_high_display = pattern_high
                        pattern_low_display = pattern_low

                    rsi_tag = ""
                    if rsi is not None:
                        if rsi >= 50:
                            rsi_tag = " 📊RSI↑(≥ 60, rising toward 80✅)"
                        else:
                            rsi_tag = " ⚠️RSI below 50 (wait for rise)"

                    fast = check_reentry_live_fast(ticker, pattern_high, pattern_low, "BULLISH")
                    if fast and fast["signal"] == "RE-ENTRY_SIGNAL":
                        raw_ep = fast["entry_price"]
                        ep_val = convert_usd_to_inr_for_mcx(float(raw_ep), ticker) if (is_commodity and raw_ep) else raw_ep
                        sl_val = pattern_low_display
                        target_1r, target_2r = compute_sr_targets(
                            ticker, "BULLISH", ep_val, pattern_high, pattern_low
                        )
                        entry_price = ep_val
                        stop_loss = sl_val
                        breakout_time = fast.get("breakout_time")
                        signal = "LONG_REENTRY_SIGNAL"
                        signal_display = (
                            f"🟢 LONG RE-ENTRY SIGNAL | Pattern:{pattern_name}{rsi_tag} | "
                            f"Entry:{fmt(ep_val)} | SL:{fmt(sl_val)} (Pat Low) | "
                            f"T1:{fmt(target_1r)} (4H swing resistance)"
                        )
                    elif fast and fast.get("approaching"):
                        signal = "APPROACHING_REENTRY"
                        signal_display = (
                            f"⚡ APPROACHING LONG RE-ENTRY | Pattern:{pattern_name}{rsi_tag} | "
                            f"Need 5M close ABOVE {fmt(pattern_high_display)} | Vol≥20-bar avg"
                        )
                    else:
                        signal = "WAIT_LONG_REENTRY"
                        signal_display = (
                            f"⏳ WAIT LONG RE-ENTRY | 4H Bullish Pattern:{pattern_name}{rsi_tag} | "
                            f"Need 5M close ABOVE {fmt(pattern_high_display)} with Vol≥avg"
                        )
                else:
                    signal = "FRESH_LONG_WATCH"
                    signal_display = (
                        f"🟢 BULLISH TREND — FRESH LONG | No retracement yet | "
                        f"RSI:{f'{rsi:.1f}' if rsi else 'N/A'} | "
                        f"15M: {retrace.get('description','—')}"
                    )

        elif trend == "BEARISH":
            retrace = detect_retracement_15m(ticker, "BEARISH")

            if retrace["is_retracement"]:
                # ── BEARISH RETRACEMENT: EMA10 crossed ↑ EMA20, RSI ≥ 60 → EXIT SHORT ──
                df_15m_rt = fetch_data_robust(ticker, period="5d", interval="15m")
                if df_15m_rt is not None and not df_15m_rt.empty and len(df_15m_rt) > 5:
                    recent_15m = df_15m_rt.iloc[:-1].tail(8)
                    pattern_high = float(recent_15m["High"].max())
                    pattern_low = float(recent_15m["Low"].min())
                else:
                    pattern_high = current_price * 1.005
                    pattern_low = current_price * 0.995

                pattern_name = f"15M EMA CROSS ↑ (RSI {retrace['rsi_15m']:.1f})"
                cross_tag = f" | Cross@{retrace['cross_time']}" if retrace["cross_time"] else ""

                if is_commodity:
                    pattern_high_display = convert_usd_to_inr_for_mcx(pattern_high, ticker)
                    pattern_low_display = convert_usd_to_inr_for_mcx(pattern_low, ticker)
                else:
                    pattern_high_display = pattern_high
                    pattern_low_display = pattern_low

                rsi_15m_val = retrace["rsi_15m"]
                rsi_tag = f" | 15M RSI {rsi_15m_val:.1f}↑ (≥ 60)" if rsi_15m_val else ""

                exit_check = check_retracement_exit_5m(ticker, pattern_high, pattern_low, "BEARISH")
                if exit_check["triggered"]:
                    signal = "BEARISH_RETRACEMENT_EXIT"
                    signal_display = exit_check["signal_text"]
                else:
                    signal = "BEARISH_RETRACEMENT"
                    signal_display = (
                        f"⚠️ BEARISH RETRACEMENT — EXIT SHORT: 15M EMA10 crossed ↑ EMA20{rsi_tag}{cross_tag} | "
                        f"Zone High:{fmt(pattern_high_display)} Low:{fmt(pattern_low_display)} | "
                        f"Waiting for 5M close ABOVE {fmt(pattern_high_display)} to confirm exit"
                    )

            elif retrace["is_reentry"]:
                # ── BEARISH RE-ENTRY: EMA10 crossed ↓ EMA20, RSI ≤ 40 → RE-ENTER SHORT ──
                cross_tag = f" | Cross@{retrace['cross_time']}" if retrace["cross_time"] else ""
                rsi_15m_val = retrace["rsi_15m"]
                rsi_tag = f" | 15M RSI {rsi_15m_val:.1f}↓ (≤ 40)" if rsi_15m_val else ""

                # SL = high of the EMA cross candle
                c_high_raw = retrace.get("cross_candle_high")
                if c_high_raw:
                    sl_raw = c_high_raw
                    stop_loss = convert_usd_to_inr_for_mcx(sl_raw, ticker) if is_commodity else sl_raw
                else:
                    stop_loss = current_price * 1.01

                entry_price = current_price
                # Target = next support on 4H
                target_1r, target_2r = compute_sr_targets(
                    ticker, "BEARISH", entry_price,
                    stop_loss, current_price * 0.99
                )
                pattern_high = stop_loss if not is_commodity else (convert_usd_to_inr_for_mcx(c_high_raw, ticker) if c_high_raw else current_price * 1.01)
                pattern_low = current_price * 0.99
                pattern_name = f"15M EMA CROSS ↓ — BEARISH RE-ENTRY (RSI {rsi_15m_val:.1f})"
                signal = "BEARISH_REENTRY"
                signal_display = (
                    f"🔴 BEARISH RE-ENTRY: 15M EMA10 crossed ↓ EMA20{rsi_tag}{cross_tag} | "
                    f"Entry:{fmt(entry_price)} | SL:{fmt(stop_loss)} (High of cross candle) | "
                    f"Target:{fmt(target_1r)} (Next 4H Support)"
                )

            else:
                df_4h_re = fetch_data_robust(ticker, period="30d", interval="4h")
                reentry_bear_pattern = detect_bearish_pullback_pattern(df_4h_re) if not df_4h_re.empty else None

                if reentry_bear_pattern:
                    pattern_high = reentry_bear_pattern["high"]
                    pattern_low = reentry_bear_pattern["low"]
                    pattern_name = reentry_bear_pattern.get("name", "BEARISH PATTERN")
                    vol_declining = reentry_bear_pattern.get("vol_declining")

                    if is_commodity:
                        pattern_high_display = convert_usd_to_inr_for_mcx(pattern_high, ticker)
                        pattern_low_display = convert_usd_to_inr_for_mcx(pattern_low, ticker)
                    else:
                        pattern_high_display = pattern_high
                        pattern_low_display = pattern_low

                    rsi_tag = ""
                    if rsi is not None:
                        if rsi <= 50:
                            rsi_tag = " 📊RSI↓(≤ 40, falling toward 20✅)"
                        else:
                            rsi_tag = " ⚠️RSI above 50 (wait for drop)"

                    fast = check_reentry_live_fast(ticker, pattern_high, pattern_low, "BEARISH")
                    if fast and fast["signal"] == "RE-ENTRY_SIGNAL":
                        raw_ep = fast["entry_price"]
                        ep_val = convert_usd_to_inr_for_mcx(float(raw_ep), ticker) if (is_commodity and raw_ep) else raw_ep
                        sl_val = pattern_high_display
                        target_1r, target_2r = compute_sr_targets(
                            ticker, "BEARISH", ep_val, pattern_high, pattern_low
                        )
                        entry_price = ep_val
                        stop_loss = sl_val
                        breakout_time = fast.get("breakout_time")
                        signal = "SHORT_REENTRY_SIGNAL"
                        signal_display = (
                            f"🔴 SHORT RE-ENTRY SIGNAL | Pattern:{pattern_name}{rsi_tag} | "
                            f"Entry:{fmt(ep_val)} | SL:{fmt(sl_val)} (Pat High) | "
                            f"T1:{fmt(target_1r)} (4H swing support)"
                        )
                    elif fast and fast.get("approaching"):
                        signal = "APPROACHING_REENTRY"
                        signal_display = (
                            f"⚡ APPROACHING SHORT RE-ENTRY | Pattern:{pattern_name}{rsi_tag} | "
                            f"Need 5M close BELOW {fmt(pattern_low_display)} | Vol≥20-bar avg"
                        )
                    else:
                        signal = "WAIT_SHORT_REENTRY"
                        signal_display = (
                            f"⏳ WAIT SHORT RE-ENTRY | 4H Bearish Pattern:{pattern_name}{rsi_tag} | "
                            f"Need 5M close BELOW {fmt(pattern_low_display)} with Vol≥avg"
                        )
                else:
                    signal = "FRESH_SHORT_WATCH"
                    signal_display = (
                        f"🔴 BEARISH TREND — FRESH SHORT | No retracement yet | "
                        f"RSI:{f'{rsi:.1f}' if rsi else 'N/A'} | "
                        f"15M: {retrace.get('description','—')}"
                    )

        else:
            signal, signal_display, pattern_name = "NEUTRAL", "NEUTRAL — No clear trend (1D EMA alignment required)", "NONE"

        support = resistance = None
        df_4h = fetch_data_robust(ticker, period="5d", interval="4h")
        if not df_4h.empty:
            lo = float(df_4h["Low"].tail(20).min())
            hi = float(df_4h["High"].tail(20).max())
            if is_commodity:
                support, resistance = convert_usd_to_inr_for_mcx(lo, ticker), convert_usd_to_inr_for_mcx(hi, ticker)
            else:
                support, resistance = lo, hi

        return {
            "current_price": current_price, "trend": trend, "trend_icon": trend_icon,
            "ema_10": ema_10, "ema_20": ema_20, "ema_50": ema_50,
            "rsi": rsi,
            "signal": signal, "signal_display": signal_display,
            "entry_price": entry_price, "stop_loss": stop_loss,
            "target_1r": target_1r, "target_2r": target_2r,
            "pattern_high": pattern_high, "pattern_low": pattern_low,
            "pattern_high_display": pattern_high_display,
            "pattern_low_display": pattern_low_display,
            "pattern_name": pattern_name, "breakout_time": breakout_time,
            "support": support, "resistance": resistance,
            "pullback_pattern": None,
            "reentry_done": reentry_done, "reentry_info": reentry_info,
            "new_signal_detected": new_signal_detected,
            "vol_declining": vol_declining,
            "adx_info": adx_info,
            "adx_filter_skip": False,
            "adx_filter_message": "",
        }

    except Exception as e:
        logging.error(f"fetch_stock_data {ticker}: {e}")
        return _empty


# ============================================================
# TRADINGVIEW LIVE CHART
# ============================================================

def compute_sr_targets(ticker: str, trend: str, entry_price: float,
                        pattern_high: float, pattern_low: float) -> tuple:
    try:
        df4 = fetch_data_robust(ticker, period="30d", interval="4h")
        if df4.empty or len(df4) < 10:
            raise ValueError("no data")

        is_commodity = ticker in MCX_COMMODITIES

        def _inr(v):
            return convert_usd_to_inr_for_mcx(float(v), ticker) if is_commodity else float(v)

        df4 = df4.copy()
        swing_window = df4.iloc[:-3].tail(50)
        highs = swing_window["High"].apply(_inr)
        lows = swing_window["Low"].apply(_inr)

        ep = entry_price
        price_step = ep * 0.01

        if trend == "BULLISH":
            above = highs[highs > ep]
            if not above.empty:
                t1 = float(above.min())
            else:
                t1 = _inr(pattern_high) + price_step

            higher = highs[highs > t1]
            t2 = float(higher.min()) if not higher.empty else t1 + price_step

        else:
            below = lows[lows < ep]
            if not below.empty:
                t1 = float(below.max())
            else:
                t1 = _inr(pattern_low) - price_step

            lower = lows[lows < t1]
            t2 = float(lower.max()) if not lower.empty else t1 - price_step

        return round(t1, 2), round(t2, 2)

    except Exception:
        _is_commodity = ticker in MCX_COMMODITIES

        def _fb_inr(v):
            if v is None:
                return None
            try:
                return convert_usd_to_inr_for_mcx(float(v), ticker) if _is_commodity else float(v)
            except Exception:
                return float(v) if v else None

        step = entry_price * 0.01
        if trend == "BULLISH":
            pl_inr = _fb_inr(pattern_low)
            risk = abs(entry_price - pl_inr) if pl_inr else step
            return round(entry_price + risk, 2), round(entry_price + 2 * risk, 2)
        else:
            ph_inr = _fb_inr(pattern_high)
            risk = abs(ph_inr - entry_price) if ph_inr else step
            return round(entry_price - risk, 2), round(entry_price - 2 * risk, 2)


def get_tradingview_url(ticker: str, timeframe: str = "4H") -> str:
    if ticker.endswith(".NS"):
        tv_sym = f"NSE:{ticker.replace('.NS', '')}"
    else:
        tv_sym = {
            "GC=F": "COMEX:GC1!",
            "SI=F": "COMEX:SI1!",
            "CL=F": "NYMEX:CL1!",
            "NG=F": "NYMEX:NG1!",
            "HG=F": "COMEX:HG1!",
            "ZNC=F": "LME:ZN1!",
            "NG=F_MINI": "NYMEX:NG1!",
            "ZNC=F_MINI": "LME:ZN1!",
            "ALI=F_MINI": "LME:AH1!",
        }.get(ticker, ticker)
    tf_map = {"1M": "1", "5M": "5", "15M": "15", "1H": "60", "4H": "240", "1D": "D"}
    tv_tf = tf_map.get(timeframe, "240")
    return f"https://www.tradingview.com/chart/?symbol={tv_sym}&interval={tv_tf}"


def render_tradingview_chart(ticker: str, timeframe: str = "4H",
                              entry_price=None, stop_loss=None,
                              pattern_high=None, pattern_low=None):
    import streamlit.components.v1 as components
    import json

    if timeframe == "5M":
        df = fetch_data_robust(ticker, period="5d", interval="5m")
        tf_label = "5-Min"
    else:
        df = fetch_data_robust(ticker, period="30d", interval="4h")
        tf_label = "4-Hour"

    if df.empty:
        st.info("No chart data available.")
        return

    df = df.copy()

    is_commodity = ticker in MCX_COMMODITIES
    if is_commodity:
        for col in ["Open", "High", "Low", "Close"]:
            df[col] = df[col].apply(
                lambda x: convert_usd_to_inr_for_mcx(float(x), ticker)
            )

    df.index = pd.to_datetime(df.index)
    if df.index.tzinfo is not None:
        df.index = df.index.tz_convert("UTC")
    else:
        df.index = df.index.tz_localize("UTC")

    df["EMA10"] = df["Close"].ewm(span=10, adjust=False).mean()
    df["EMA20"] = df["Close"].ewm(span=20, adjust=False).mean()
    df["EMA50"] = df["Close"].ewm(span=50, adjust=False).mean()
    df["RSI"] = calc_rsi(df["Close"])

    candles, ema10_data, ema20_data, ema50_data, vol_data = [], [], [], [], []
    for ts, row in df.iterrows():
        t = int(ts.timestamp())
        if (pd.isna(row["Open"]) or pd.isna(row["High"])
                or pd.isna(row["Low"]) or pd.isna(row["Close"])):
            continue
        candles.append({
            "time": t,
            "open": round(float(row["Open"]), 2),
            "high": round(float(row["High"]), 2),
            "low": round(float(row["Low"]), 2),
            "close": round(float(row["Close"]), 2),
        })
        if not pd.isna(row["EMA10"]):
            ema10_data.append({"time": t, "value": round(float(row["EMA10"]), 2)})
        if not pd.isna(row["EMA20"]):
            ema20_data.append({"time": t, "value": round(float(row["EMA20"]), 2)})
        if not pd.isna(row["EMA50"]):
            ema50_data.append({"time": t, "value": round(float(row["EMA50"]), 2)})
        if "Volume" in df.columns and not pd.isna(row["Volume"]):
            vol_data.append({"time": t, "value": float(row["Volume"]),
                             "color": "#00C85340" if row["Close"] >= row["Open"] else "#FF174440"})

    price_lines = []
    if entry_price:
        price_lines.append({
            "price": round(float(entry_price), 2),
            "color": "#00FF88", "lineWidth": 2, "lineStyle": 0,
            "title": f"Entry {format_price(entry_price, True)}"
        })
    if stop_loss:
        price_lines.append({
            "price": round(float(stop_loss), 2),
            "color": "#FF1744", "lineWidth": 2, "lineStyle": 1,
            "title": f"SL {format_price(stop_loss, True)}"
        })
    if pattern_high:
        price_lines.append({
            "price": round(float(pattern_high), 2),
            "color": "#FFD700", "lineWidth": 1, "lineStyle": 2,
            "title": f"Pat High {format_price(pattern_high, True)}"
        })
    if pattern_low:
        price_lines.append({
            "price": round(float(pattern_low), 2),
            "color": "#FF6B00", "lineWidth": 1, "lineStyle": 2,
            "title": f"Pat Low {format_price(pattern_low, True)}"
        })

    sym_name = get_symbol_display_name(ticker)
    pl_js = json.dumps(price_lines)
    candle_js = json.dumps(candles)
    ema10_js = json.dumps(ema10_data)
    ema20_js = json.dumps(ema20_data)
    ema50_js = json.dumps(ema50_data)
    vol_js = json.dumps(vol_data)

    html = f"""
<!DOCTYPE html>
<html>
<head>
  <style>
    * {{ margin:0; padding:0; box-sizing:border-box; }}
    body {{ background:#0A1628; font-family:'Share Tech Mono',monospace; }}
    #chartTitle {{
      color:#00F5FF; font-size:.78rem; letter-spacing:.09em;
      padding:8px 12px 4px; text-shadow:0 0 10px rgba(0,245,255,.6);
    }}
    #chart {{ width:100%; height:420px; }}
    #legend {{
      display:flex; gap:16px; flex-wrap:wrap;
      padding:6px 12px; font-size:.72rem; color:#88B4CC;
    }}
    .leg {{ display:flex; align-items:center; gap:5px; }}
    .leg-dot {{ width:12px; height:3px; border-radius:2px; }}
  </style>
</head>
<body>
  <div id="chartTitle">📈 {sym_name} — {tf_label} | EMA 10/20 | Live via yfinance</div>
  <div id="chart"></div>
  <div id="legend">
    <div class="leg"><div class="leg-dot" style="background:#00C853"></div>Bullish</div>
    <div class="leg"><div class="leg-dot" style="background:#FF1744"></div>Bearish</div>
    <div class="leg"><div class="leg-dot" style="background:#FFD700"></div>EMA 10</div>
    <div class="leg"><div class="leg-dot" style="background:#2196F3"></div>EMA 20</div>
    <div class="leg"><div class="leg-dot" style="background:#FF6B00"></div>EMA 50</div>
    {"".join([
      f'<div class="leg"><div class="leg-dot" style="background:{pl["color"]}"></div>'
      f'{pl["title"]}</div>'
      for pl in price_lines
    ])}
  </div>

  <script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
  <script>
    const chart = LightweightCharts.createChart(document.getElementById('chart'), {{
      width:  document.getElementById('chart').clientWidth,
      height: 420,
      layout: {{
        background: {{ type: 'solid', color: '#0A1628' }},
        textColor:  '#88B4CC',
        fontSize:   11,
      }},
      grid: {{
        vertLines: {{ color: 'rgba(0,245,255,0.04)' }},
        horzLines: {{ color: 'rgba(0,245,255,0.04)' }},
      }},
      crosshair: {{ mode: LightweightCharts.CrosshairMode.Normal }},
      rightPriceScale: {{
        borderColor: 'rgba(0,245,255,0.2)',
        scaleMargins: {{ top: 0.08, bottom: 0.18 }},
      }},
      timeScale: {{
        borderColor: 'rgba(0,245,255,0.2)',
        timeVisible: true,
        secondsVisible: false,
      }},
    }});

    const candleSeries = chart.addCandlestickSeries({{
      upColor:        '#00C853',
      downColor:      '#FF1744',
      borderUpColor:  '#00C853',
      borderDownColor:'#FF1744',
      wickUpColor:    '#00C853',
      wickDownColor:  '#FF1744',
    }});
    candleSeries.setData({candle_js});

    const plines = {pl_js};
    plines.forEach(pl => {{
      candleSeries.createPriceLine({{
        price:            pl.price,
        color:            pl.color,
        lineWidth:        pl.lineWidth,
        lineStyle:        pl.lineStyle,
        axisLabelVisible: true,
        title:            pl.title,
      }});
    }});

    const ema10Series = chart.addLineSeries({{
      color: '#FFD700', lineWidth: 1.5,
      priceLineVisible: false, lastValueVisible: true, title: 'EMA10',
    }});
    ema10Series.setData({ema10_js});

    const ema20Series = chart.addLineSeries({{
      color: '#2196F3', lineWidth: 1.5,
      priceLineVisible: false, lastValueVisible: true, title: 'EMA20',
    }});
    ema20Series.setData({ema20_js});

    const ema50Series = chart.addLineSeries({{
      color: '#FF6B00', lineWidth: 2,
      priceLineVisible: false, lastValueVisible: true, title: 'EMA50',
    }});
    ema50Series.setData({ema50_js});

    const volSeries = chart.addHistogramSeries({{
      priceFormat:   {{ type: 'volume' }},
      priceScaleId:  'vol',
    }});
    chart.priceScale('vol').applyOptions({{
      scaleMargins: {{ top: 0.85, bottom: 0.0 }},
    }});
    volSeries.setData({vol_js});

    chart.timeScale().fitContent();

    new ResizeObserver(() => {{
      chart.applyOptions({{ width: document.getElementById('chart').clientWidth }});
    }}).observe(document.getElementById('chart'));
  </script>
</body>
</html>"""

    components.html(html, height=490, scrolling=False)


# ============================================================
# ALERT MANAGER
# ============================================================

class AlertManager:
    def __init__(self):
        if "alert_prev_states" not in st.session_state:
            st.session_state["alert_prev_states"] = {}
        self.new_alerts = []
        self.pending_toasts = []

    @property
    def previous_states(self):
        return st.session_state.get("alert_prev_states", {})

    @previous_states.setter
    def previous_states(self, value):
        st.session_state["alert_prev_states"] = value

    def _db_add(self, name, atype, old, new, msg, is_signal):
        try:
            conn = get_db()
            conn.execute(
                "INSERT INTO alert_history (timestamp,symbol,type,old_val,new_val,message,read,is_signal) "
                "VALUES (?,?,?,?,?,?,0,?)",
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), name, atype, str(old), str(new), msg, 1 if is_signal else 0),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logging.warning(f"AlertManager._db_add: {e}")

    def add_alert(self, symbol, name, atype, old, new, msg, is_signal=False):
        alert = {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "symbol": name, "type": atype, "message": msg,
                 "read": False, "is_signal": is_signal}
        self._db_add(name, atype, old, new, msg, is_signal)
        self.new_alerts.append(alert)
        self.pending_toasts.append(alert)
        return alert

    def get_pending_toasts(self):
        t = self.pending_toasts.copy()
        self.pending_toasts = []
        return t

    def check_and_alert(self, symbol, name, trend, signal, entry_price=None,
                        stop_loss=None, breakout_time=None, new_signal=False, pattern_name=None):
        ps = self.previous_states

        key = f"{symbol}_trend"
        prev = ps.get(key)
        if prev and prev != trend and trend != "NEUTRAL":
            msg = f"📊 {name}: Trend {prev} → {trend}"
            self.add_alert(symbol, name, "Trend Change", prev, trend, msg)
        ps[key] = trend

        if signal in {"BEARISH_PATTERN_4H", "BULLISH_PATTERN_4H"}:
            sk = f"{symbol}_retracement"
            if ps.get(sk) != signal:
                direction = "BEARISH" if signal == "BEARISH_PATTERN_4H" else "BULLISH"
                msg = f"📉 {name}: {direction} RETRACEMENT — {pattern_name} on 4H | Watching for 5M confirmation"
                self.add_alert(symbol, name, "Retracement Pattern", ps.get(sk), signal, msg)
            ps[sk] = signal

        if signal in {"BULLISH_RETRACEMENT_EXIT", "BEARISH_RETRACEMENT_EXIT"} and new_signal:
            sk = f"{symbol}_exit_signal"
            bt = breakout_time or str(datetime.now().timestamp())
            if not was_signal_notified(symbol, bt):
                exit_type = "EXIT LONG" if signal == "BULLISH_RETRACEMENT_EXIT" else "EXIT SHORT"
                msg = f"⚠️🚨 {name}: {exit_type} — Retracement confirmed on 5M"
                self.add_alert(symbol, name, exit_type, ps.get(sk), signal, msg, is_signal=True)
                mark_signal_notified(symbol, bt)
                ps[sk] = signal

        if signal in {"LONG_REENTRY_SIGNAL", "SHORT_REENTRY_SIGNAL"} and new_signal:
            sk = f"{symbol}_signal"
            bt = breakout_time or str(datetime.now().timestamp())
            if not was_signal_notified(symbol, bt):
                trade_type = "LONG RE-ENTRY" if signal == "LONG_REENTRY_SIGNAL" else "SHORT RE-ENTRY"
                msg = (f"🎯🚨 {name}: {trade_type} SIGNAL! ({pattern_name}) "
                       f"Entry:{format_price(entry_price,True)} SL:{format_price(stop_loss,True)}")
                self.add_alert(symbol, name, f"{trade_type} SIGNAL", ps.get(sk), signal, msg, is_signal=True)
                mark_signal_notified(symbol, bt)
                ps[sk] = signal
                if "pending_telegram_msgs" not in st.session_state:
                    st.session_state["pending_telegram_msgs"] = []
                st.session_state["pending_telegram_msgs"].append(msg)

        if signal not in {"LONG_REENTRY_SIGNAL", "SHORT_REENTRY_SIGNAL",
                          "BEARISH_PATTERN_4H", "BULLISH_PATTERN_4H", "IN_POSITION",
                          "BULLISH_RETRACEMENT_EXIT", "BEARISH_RETRACEMENT_EXIT"}:
            ps.pop(f"{symbol}_signal", None)
            ps.pop(f"{symbol}_retracement", None)

        self.previous_states = ps

    def get_unread_count(self) -> int:
        try:
            conn = get_db()
            row = conn.execute("SELECT COUNT(*) as n FROM alert_history WHERE read=0").fetchone()
            conn.close()
            return row["n"] if row else 0
        except Exception as e:
            logging.warning(f"get_unread_count: {e}")
            return 0

    def mark_all_read(self):
        try:
            conn = get_db()
            conn.execute("UPDATE alert_history SET read=1")
            conn.commit()
            conn.close()
        except Exception as e:
            logging.warning(f"mark_all_read: {e}")
        self.new_alerts = []

    def get_recent_alerts(self, limit=50):
        try:
            conn = get_db()
            rows = conn.execute("SELECT * FROM alert_history ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            logging.warning(f"get_recent_alerts: {e}")
            return []


# ============================================================
# TELEGRAM INTEGRATION
# ============================================================

def _get_setting(key: str) -> str:
    try:
        conn = get_db()
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
        return row["value"] if row else ""
    except Exception:
        return ""


def _save_setting(key: str, value: str):
    try:
        conn = get_db()
        conn.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"_save_setting {key}: {e}")


def send_telegram_alert(message: str) -> tuple[bool, str]:
    token = _get_setting("telegram_token")
    chat_id = _get_setting("telegram_chat_id")
    if not token or not token.strip():
        return False, "Bot token not set"
    if not chat_id or not chat_id.strip():
        return False, "Chat ID not set"
    import re as _re
    clean_msg = _re.sub(r"<[^>]+>", "", message)
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token.strip()}/sendMessage",
            json={"chat_id": chat_id.strip(), "text": clean_msg},
            timeout=6,
        )
        if resp.status_code == 200:
            return True, "OK"
        err = resp.json().get("description", resp.text[:200])
        logging.warning(f"Telegram {resp.status_code}: {err}")
        return False, f"Telegram error {resp.status_code}: {err}"
    except Exception as e:
        logging.warning(f"send_telegram_alert: {e}")
        return False, str(e)


def _send_futures_scan_to_telegram(signals: list, universe_label: str) -> tuple[bool, str]:
    token = _get_setting("telegram_token")
    chat_id = _get_setting("telegram_chat_id")
    if not token or not token.strip():
        return False, "Bot token not configured. Go to Settings → Telegram Alerts."
    if not chat_id or not chat_id.strip():
        return False, "Chat ID not configured. Go to Settings → Telegram Alerts."

    now_str = datetime.now(IST).strftime("%d %b %Y  %H:%M IST")

    lines = [
        "⚡ FUTURES SCANNER — NSE F&O",
        f"🕐 {now_str}",
        f"📊 Universe: {universe_label}",
        f"✅ {len(signals)} Setup(s) Found",
        "━" * 32,
    ]

    for i, s in enumerate(signals, 1):
        rr_star = "⭐" if s["rr"] >= 2 else ""
        chg_icon = "▲" if s["change_pct"] >= 0 else "▼"
        tv_sym = s["ticker"].replace(".NS", "")
        tv_link = f"https://www.tradingview.com/chart/?symbol=NSE%3A{tv_sym}"
        lines += [
            "",
            f"{i}. {s['name']}  ({s['ticker']})",
            f"   Signal : {s['label']}",
            f"   Trend  : {s.get('daily_trend_label', s.get('daily_trend','—'))}",
            f"   Price  : ₹{s['price']:,.2f}  {chg_icon}{abs(s['change_pct']):.2f}%",
            f"   Entry  : ₹{s['entry']:,.2f}",
            f"   SL     : ₹{s['sl']:,.2f}",
            f"   Target : ₹{s['target']:,.2f}",
            f"   R:R    : {s['rr']:.1f}x {rr_star}",
            f"   RSI(5M): {s['rsi']}  RSI(15M): {s.get('rsi_15m','—')}  |  Vol: ₹{s['volume_cr']:.1f}Cr",
            f"   🕐 Signal Time: {s.get('cross_time', '—')}",
            f"   📌 15M EMA cross + RSI filter (Daily trend gate)",
            f"   📊 Chart: {tv_link}",
        ]

    lines += [
        "",
        "━" * 32,
        "⚠️ Educational only. Not SEBI-registered advice.",
    ]

    full_msg = "\n".join(lines)

    MAX = 4000
    chunks = [full_msg[i:i + MAX] for i in range(0, len(full_msg), MAX)]

    try:
        for chunk in chunks:
            resp = requests.post(
                f"https://api.telegram.org/bot{token.strip()}/sendMessage",
                json={"chat_id": chat_id.strip(), "text": chunk},
                timeout=10,
            )
            if resp.status_code != 200:
                err = resp.json().get("description", resp.text[:200])
                return False, f"Telegram error {resp.status_code}: {err}"
        return True, f"Sent {len(chunks)} message(s) to Telegram ✅"
    except Exception as e:
        return False, str(e)


def _send_single_futures_signal_to_telegram(sig: dict) -> tuple[bool, str]:
    token = _get_setting("telegram_token")
    chat_id = _get_setting("telegram_chat_id")
    if not token or not token.strip():
        return False, "Bot token not configured. Go to Settings → Telegram Alerts."
    if not chat_id or not chat_id.strip():
        return False, "Chat ID not configured. Go to Settings → Telegram Alerts."

    now_str = datetime.now(IST).strftime("%d %b %Y  %H:%M IST")
    rr_star = "⭐" if sig["rr"] >= 2 else ""
    chg_icon = "▲" if sig["change_pct"] >= 0 else "▼"
    tv_sym = sig["ticker"].replace(".NS", "")
    tv_link = f"https://www.tradingview.com/chart/?symbol=NSE%3A{tv_sym}"

    lines = [
        f"⚡ FUTURES SIGNAL — {now_str}",
        "━" * 32,
        f"📈 {sig['name']}  ({sig['ticker']})",
        f"   Signal : {sig['label']}",
        f"   Trend  : {sig.get('daily_trend_label', sig.get('daily_trend', '—'))}",
        f"   Price  : ₹{sig['price']:,.2f}  {chg_icon}{abs(sig['change_pct']):.2f}%",
        f"   Entry  : ₹{sig['entry']:,.2f}",
        f"   SL     : ₹{sig['sl']:,.2f}",
        f"   Target : ₹{sig['target']:,.2f}",
        f"   R:R    : {sig['rr']:.1f}x {rr_star}",
        f"   RSI(5M): {sig['rsi']}  RSI(15M): {sig.get('rsi_15m', '—')}  |  Vol: ₹{sig['volume_cr']:.1f}Cr",
        f"   🕐 Signal Time: {sig.get('cross_time', '—')}",
        f"   📌 15M EMA cross + RSI filter (Daily trend gate)",
        f"   📊 Chart: {tv_link}",
        "",
        "━" * 32,
        "⚠️ Educational only. Not SEBI-registered advice.",
    ]

    full_msg = "\n".join(lines)
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token.strip()}/sendMessage",
            json={"chat_id": chat_id.strip(), "text": full_msg},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, f"{sig['name']} sent to Telegram ✅"
        err = resp.json().get("description", resp.text[:200])
        return False, f"Telegram error {resp.status_code}: {err}"
    except Exception as e:
        return False, str(e)


# ============================================================
# WATCHLIST & PORTFOLIO
# ============================================================

def load_watchlist_nse():
    try:
        conn = get_db()
        rows = conn.execute("SELECT ticker FROM watchlist_nse").fetchall()
        conn.close()
        tickers = [r["ticker"] for r in rows]
        if not tickers:
            save_watchlist_nse(DEFAULT_NSE_WATCHLIST.copy())
            return DEFAULT_NSE_WATCHLIST.copy()
        return tickers
    except Exception as e:
        logging.warning(f"load_watchlist_nse: {e}")
        return DEFAULT_NSE_WATCHLIST.copy()


def save_watchlist_nse(tickers):
    try:
        conn = get_db()
        conn.execute("DELETE FROM watchlist_nse")
        conn.executemany("INSERT INTO watchlist_nse (ticker) VALUES (?)", [(t,) for t in tickers])
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"save_watchlist_nse: {e}")


def load_watchlist_mcx():
    defaults = ["GC=F", "SI=F", "CL=F", "NG=F", "HG=F", "ZNC=F", "NG=F_MINI", "ZNC=F_MINI", "ALI=F_MINI"]
    try:
        conn = get_db()
        rows = conn.execute("SELECT ticker FROM watchlist_mcx").fetchall()
        conn.close()
        tickers = [r["ticker"] for r in rows]
        if not tickers:
            save_watchlist_mcx(defaults)
            return defaults
        return tickers
    except Exception as e:
        logging.warning(f"load_watchlist_mcx: {e}")
        return defaults


def save_watchlist_mcx(tickers):
    try:
        conn = get_db()
        conn.execute("DELETE FROM watchlist_mcx")
        conn.executemany("INSERT INTO watchlist_mcx (ticker) VALUES (?)", [(t,) for t in tickers])
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"save_watchlist_mcx: {e}")


def load_portfolio() -> dict:
    try:
        conn = get_db()
        rows = conn.execute("SELECT * FROM portfolio").fetchall()
        conn.close()
        return {r["pid"]: {"ticker": r["ticker"], "quantity": r["quantity"], "entry_price": r["entry_price"]} for r in rows}
    except Exception as e:
        logging.warning(f"load_portfolio: {e}")
        return {}


def save_portfolio_position(pid, ticker, quantity, entry_price):
    try:
        conn = get_db()
        conn.execute("INSERT OR REPLACE INTO portfolio (pid,ticker,quantity,entry_price) VALUES (?,?,?,?)",
                     (pid, ticker, quantity, entry_price))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"save_portfolio_position: {e}")


def delete_portfolio_position(pid):
    try:
        conn = get_db()
        conn.execute("DELETE FROM portfolio WHERE pid=?", (pid,))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"delete_portfolio_position: {e}")


def clear_portfolio():
    try:
        conn = get_db()
        conn.execute("DELETE FROM portfolio")
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"clear_portfolio: {e}")


# ============================================================
# TRADE JOURNAL
# ============================================================

def log_trade(symbol, symbol_name, direction, entry_price, exit_price, quantity,
              entry_time, exit_time, pattern_name, trend, stop_loss, target, notes="", adx_at_entry=None, market_regime=None):
    pnl = (exit_price - entry_price) * quantity if direction == "LONG" \
          else (entry_price - exit_price) * quantity
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO trade_history "
            "(symbol,symbol_name,direction,entry_price,exit_price,quantity,pnl,"
            " entry_time,exit_time,pattern_name,trend,stop_loss,target,notes,adx_at_entry,market_regime) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (symbol, symbol_name, direction, entry_price, exit_price, quantity, pnl,
             entry_time, exit_time, pattern_name, trend, stop_loss, target, notes, adx_at_entry, market_regime),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.warning(f"log_trade: {e}")


def get_trade_history_df() -> pd.DataFrame:
    try:
        conn = get_db()
        rows = conn.execute("SELECT * FROM trade_history ORDER BY id").fetchall()
        conn.close()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception as e:
        logging.warning(f"get_trade_history_df: {e}")
        return pd.DataFrame()


# ============================================================
# POSITION SIZE CALCULATOR
# ============================================================

def position_size_calc(capital: float, risk_pct: float, entry: float, sl: float):
    if not all([capital, risk_pct, entry, sl]) or entry == sl:
        return None, None, None
    risk_amt = capital * risk_pct / 100
    risk_per_unit = abs(entry - sl)
    qty = max(1, int(risk_amt / risk_per_unit))
    return qty, qty * entry, qty * risk_per_unit


# ============================================================
# SOUND ALERT
# ============================================================

def play_signal_sound():
    st.markdown("""
    <script>
    (function(){
        var ctx=new(window.AudioContext||window.webkitAudioContext)();
        function beep(f,d,t){
            var o=ctx.createOscillator(),g=ctx.createGain();
            o.connect(g);g.connect(ctx.destination);
            o.frequency.value=f;o.type='sine';
            g.gain.setValueAtTime(0.25,ctx.currentTime+t);
            g.gain.exponentialRampToValueAtTime(0.001,ctx.currentTime+t+d);
            o.start(ctx.currentTime+t);o.stop(ctx.currentTime+t+d);
        }
        beep(880,0.18,0);beep(1100,0.25,0.22);beep(1320,0.35,0.5);
    })();
    </script>
    """, unsafe_allow_html=True)


# ============================================================
# NOTIFICATION BADGE
# ============================================================

def get_notification_badge(count: int) -> str:
    if count == 0:
        return ""
    return f"""
    <div style="position:fixed;top:70px;right:20px;z-index:10000;">
      <div style="background:linear-gradient(135deg,#FF1744,#FF6B00);border-radius:50%;
                  width:55px;height:55px;display:flex;align-items:center;justify-content:center;
                  box-shadow:0 0 18px rgba(255,23,68,0.75);animation:pulse 1.2s infinite;">
        <span style="color:#fff;font-size:20px;font-weight:bold;">{count}</span>
      </div>
      <div style="position:absolute;top:-3px;right:-3px;width:12px;height:12px;
                  background:#00FF00;border-radius:50%;animation:blink 1s infinite;"></div>
    </div>
    <style>
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.65}}}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.2}}}}
    </style>"""


def _dismissed_key(ticker: str, breakout_time: str) -> str:
    return f"dismissed_{ticker}_{breakout_time}"


def is_signal_dismissed(ticker: str, breakout_time: str) -> bool:
    if not breakout_time:
        return False
    return st.session_state.get(_dismissed_key(ticker, breakout_time), False)


def dismiss_signal(ticker: str, breakout_time: str):
    if breakout_time:
        st.session_state[_dismissed_key(ticker, breakout_time)] = True


# ============================================================
# RE-ENTRY BUTTONS
# ============================================================

def show_reentry_buttons(ticker, symbol_name, entry_price, pattern_high, pattern_low,
                         trend, pattern_name, target_1r=None, target_2r=None,
                         breakout_time=None, adx_info=None):
    
    if adx_info and adx_info.get("recommendation") == "REDUCE_SIZE":
        st.warning(f"⚠️ {adx_info['message']} — Consider reducing position size by 50%")
    elif adx_info and adx_info.get("recommendation") == "AVOID":
        st.error(f"❌ {adx_info['message']} — Trading not recommended in ranging market")
        if st.button("❌ Skip Trade (ADX Filter)", key=f"adx_skip_{ticker}"):
            dismiss_signal(ticker, breakout_time)
            st.rerun()
        return
    
    with st.expander("📐 Position Size Calculator (1% Risk Rule)", expanded=False):
        ca, cb = st.columns(2)
        with ca:
            capital = st.number_input("Capital (₹)", value=100_000, step=10_000, key=f"cap_{ticker}")
            risk_pct = st.number_input("Risk %", value=1.0, step=0.5, min_value=0.1, max_value=10.0, key=f"risk_{ticker}")

            if adx_info and adx_info.get("recommendation") == "REDUCE_SIZE":
                risk_pct = risk_pct * 0.5
                st.info(f"⚠️ ADX {adx_info['adx']:.1f} (weak trend) → Reducing risk to {risk_pct:.1f}% (50% size)")

        with cb:
            sl = pattern_low if trend == "BULLISH" else pattern_high
            st.markdown(f"**Entry:** {format_price(entry_price, True)}")
            st.markdown(f"**SL (Pattern {'Low' if trend == 'BULLISH' else 'High'}):** {format_price(sl, True)}")
            if target_1r:
                st.markdown(f"**T1 (Nearest S/R):** {format_price(target_1r, True)}")
            if adx_info and adx_info.get("adx"):
                st.markdown(f"**ADX:** {adx_info['adx']:.1f} — {adx_info['regime']}")

        qty, total, actual_risk = position_size_calc(capital, risk_pct, entry_price, sl) if sl else (None, None, None)
        if qty:
            st.success(f"Qty: **{qty}** | Invested: {format_price(total,True)} | Risk: {format_price(actual_risk,True)}")
        st.caption("📌 Re-entry after SL hit: Use 50% position size only")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("✅ Confirm & Enter Trade", key=f"reentry_done_{ticker}", use_container_width=True):
            mark_reentry_done(ticker, entry_price, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                              trend, pattern_high, pattern_low, pattern_name)
            st.success(f"✅ Trade ENTERED — {symbol_name} @ {format_price(entry_price, True)}")
            st.balloons()
            play_signal_sound()
            st.cache_data.clear()
            st.rerun()
    with c2:
        if st.button("❌ Ignore Signal", key=f"ignore_signal_{ticker}", use_container_width=True):
            dismiss_signal(ticker, breakout_time)
            st.info(f"Signal dismissed for {symbol_name}. It won't reappear until a new breakout fires.")
            st.rerun()


def show_close_position_button(ticker, symbol_name, trend=None):
    ri = get_reentry_info(ticker)
    _trend = trend or (ri.get("trend") if ri else "BULLISH")
    direction = "LONG" if _trend == "BULLISH" else "SHORT"
    label = "✅ REENTRY DONE" if _trend == "BULLISH" else "✅ TEMP EXITED"
    color = "#00C853" if _trend == "BULLISH" else "#FF6F00"

    live_pnl_html = ""
    if ri:
        ep = ri.get("entry_price", 0)
        info = fetch_stock_data(ticker)
        curr = info.get("current_price", ep) if info else ep
        pnl = (curr - ep) if _trend == "BULLISH" else (ep - curr)
        pc = "#00C853" if pnl >= 0 else "#FF1744"
        live_pnl_html = f"&nbsp;|&nbsp;<span style='color:{pc}'>P&L/unit: {format_pnl(pnl)}</span>"

    st.markdown(f"""
    <div style="background:{color}22;border:2px solid {color};border-radius:10px;
                padding:11px 16px;margin:8px 0;font-weight:bold;color:{color};">
        {label}&nbsp;|&nbsp;{symbol_name}{live_pnl_html}
    </div>""", unsafe_allow_html=True)

    if st.button("🔒 Close Position", key=f"close_{ticker}", use_container_width=True):
        if ri:
            info = fetch_stock_data(ticker)
            exit_price = info.get("current_price", ri.get("entry_price", 0)) if info else ri.get("entry_price", 0)
            sl_raw_val = ri.get("pattern_low") if _trend == "BULLISH" else ri.get("pattern_high")
            sl_val = float(sl_raw_val) if sl_raw_val else None
            log_trade(
                symbol=ticker, symbol_name=symbol_name, direction=direction,
                entry_price=ri.get("entry_price", 0), exit_price=exit_price,
                quantity=1,
                entry_time=ri.get("entry_time", ""), exit_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                pattern_name=ri.get("pattern_name", ""), trend=_trend,
                stop_loss=sl_val, target=None, notes="",
            )
        clear_reentry_state(ticker)
        st.success(f"Position CLOSED — {symbol_name}. Trade auto-logged to Journal.")
        st.cache_data.clear()
        st.rerun()


# ============================================================
# AUTO-REFRESH
# ============================================================

def auto_refresh_ui():
    with st.sidebar:
        st.markdown("---")
        st.markdown("### ⏰ Auto-Refresh")
        auto = st.checkbox("Enable Auto-Refresh", value=False)
        interval_hr = st.selectbox("Interval", [1, 2, 3], index=0, format_func=lambda x: f"{x} hr")
        interval = interval_hr * 3600
        if auto:
            st.markdown(f"🔄 Every {interval_hr} hr")
            time.sleep(interval)
            st.rerun()
        return auto, interval


# ============================================================
# FUTURES SCANNER (UPDATED v20.8 with relaxed RSI thresholds)
# ============================================================

FUTURES_UNIVERSE = [
    "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "SBIN.NS", "BHARTIARTL.NS", "ITC.NS", "HINDUNILVR.NS", "LT.NS",
    "KOTAKBANK.NS", "AXISBANK.NS", "MARUTI.NS", "TATAMOTORS.NS",
    "SUNPHARMA.NS", "WIPRO.NS", "HCLTECH.NS", "BAJFINANCE.NS",
    "ASIANPAINT.NS", "TITAN.NS", "NESTLEIND.NS", "POWERGRID.NS",
    "NTPC.NS", "ONGC.NS", "ADANIENT.NS", "ADANIPORTS.NS",
    "TATAPOWER.NS", "IRCTC.NS", "ZOMATO.NS", "PAYTM.NS",
    "TATASTEEL.NS", "JSWSTEEL.NS", "SAIL.NS", "HINDALCO.NS",
    "UPL.NS", "GRASIM.NS", "DIVISLAB.NS", "DRREDDY.NS",
    "CIPLA.NS", "APOLLOHOSP.NS", "MUTHOOTFIN.NS",
    "INDHOTEL.NS", "DMART.NS", "NAUKRI.NS", "PERSISTENT.NS",
    "LTIM.NS", "MPHASIS.NS", "COFORGE.NS", "OFSS.NS",
    "FEDERALBNK.NS", "IDFCFIRSTB.NS", "BANDHANBNK.NS",
    "PNB.NS", "CANBK.NS", "BANKINDIA.NS",
    "BAJAJ-AUTO.NS", "HEROMOTOCO.NS", "M&M.NS", "EICHERMOT.NS",
    "IDEA.NS", "TATACOMM.NS",
]

MIDCAP_UNIVERSE = [
    "CHOLAFIN.NS", "LICHSGFIN.NS", "M&MFIN.NS", "MANAPPURAM.NS",
    "ABCAPITAL.NS", "SUNDARMFIN.NS", "UJJIVANSFB.NS", "EQUITASBNK.NS",
    "LTTS.NS", "KPITTECH.NS", "TATAELXSI.NS", "MASTEK.NS", "ZENSAR.NS",
    "MOTHERSON.NS", "BALKRISIND.NS", "EXIDEIND.NS",
    "BHARATFORG.NS", "TIINDIA.NS", "MRF.NS",
    "AUROPHARMA.NS", "GLENMARK.NS", "TORNTPHARM.NS", "ALKEM.NS",
    "IPCALAB.NS", "LALPATHLAB.NS", "METROPOLIS.NS", "MAXHEALTH.NS",
    "PIIND.NS", "DEEPAKNTR.NS", "ATUL.NS", "SUDARSCHEM.NS", "FINEORG.NS",
    "TATACONSUM.NS", "GODREJCP.NS", "MARICO.NS", "DABUR.NS",
    "EMAMILTD.NS", "JYOTHYLAB.NS", "VBL.NS",
    "OBEROIRLTY.NS", "PHOENIXLTD.NS", "BRIGADE.NS", "PRESTIGE.NS",
    "KNRCON.NS", "KAJARIACER.NS",
    "CUMMINSIND.NS", "THERMAX.NS", "SCHAEFFLER.NS", "GRINDWELL.NS",
    "AIAENG.NS", "ELGIEQUIP.NS",
    "ZEEL.NS", "PVRINOX.NS", "NYKAA.NS", "VEDL.NS", "NMDC.NS", "MOIL.NS",
]


def _check_rsi_15m_filter(ticker: str, direction: str) -> tuple[bool, float, float | None, float | None, float | None, str]:
    """
    15-Min filter for Futures Scanner — updated signal logic:

    BULLISH TREND / LONG:
      - RE-ENTRY signal: 15M EMA10 crosses UP through EMA20 + RSI ≥ 60
      - SL = Low of the EMA cross candle
      - Target = next 15M swing resistance above entry

    BEARISH TREND / SHORT:
      - RE-ENTRY signal: 15M EMA10 crosses DOWN through EMA20 + RSI ≤ 40
      - SL = High of the EMA cross candle
      - Target = next 15M swing support below entry
    """
    try:
        df15 = fetch_data_robust(ticker, period="10d", interval="15m")
        if df15 is None or df15.empty or len(df15) < 30:
            return False, 50.0, None, None, None, "—"

        df15 = df15.copy()
        df15["EMA10"] = df15["Close"].ewm(span=10, adjust=False).mean()
        df15["EMA20"] = df15["Close"].ewm(span=20, adjust=False).mean()
        rsi_s = _intra_rsi(df15["Close"], 14)

        closed = df15.iloc[:-1]
        if len(closed) < 5:
            return False, 50.0, None, None, None, "—"

        curr_rsi = float(rsi_s.iloc[-2]) if not pd.isna(rsi_s.iloc[-2]) else 50.0
        prev_rsi = float(rsi_s.iloc[-3]) if len(rsi_s) >= 3 and not pd.isna(rsi_s.iloc[-3]) else curr_rsi

        e10_now = float(closed["EMA10"].iloc[-1])
        e20_now = float(closed["EMA20"].iloc[-1])
        e10_prev = float(closed["EMA10"].iloc[-2])
        e20_prev = float(closed["EMA20"].iloc[-2])

        # ── Find the most recent EMA cross candle ──
        cross_bar_low  = None   # low of cross candle  → SL for LONG re-entry
        cross_bar_high = None   # high of cross candle → SL for SHORT re-entry
        cross_bar_time = None
        scan_window = min(26, len(closed) - 1)

        if direction == "LONG":
            # Looking for EMA10 crossing UP through EMA20
            for i in range(len(closed) - 1, len(closed) - 1 - scan_window, -1):
                if i < 1:
                    break
                if (float(closed["EMA10"].iloc[i - 1]) <= float(closed["EMA20"].iloc[i - 1]) and
                        float(closed["EMA10"].iloc[i]) > float(closed["EMA20"].iloc[i])):
                    cross_bar_low  = float(closed["Low"].iloc[i])
                    cross_bar_high = float(closed["High"].iloc[i])
                    cross_bar_time = closed.index[i]
                    break
        else:
            # Looking for EMA10 crossing DOWN through EMA20
            for i in range(len(closed) - 1, len(closed) - 1 - scan_window, -1):
                if i < 1:
                    break
                if (float(closed["EMA10"].iloc[i - 1]) >= float(closed["EMA20"].iloc[i - 1]) and
                        float(closed["EMA10"].iloc[i]) < float(closed["EMA20"].iloc[i])):
                    cross_bar_low  = float(closed["Low"].iloc[i])
                    cross_bar_high = float(closed["High"].iloc[i])
                    cross_bar_time = closed.index[i]
                    break

        if cross_bar_time is not None:
            try:
                if hasattr(cross_bar_time, "tzinfo") and cross_bar_time.tzinfo is not None:
                    ct_ist = cross_bar_time.astimezone(IST)
                else:
                    ct_ist = cross_bar_time.tz_localize("UTC").astimezone(IST)
                cross_time_str = ct_ist.strftime("%H:%M %d-%b")
            except Exception:
                cross_time_str = "—"
        else:
            cross_time_str = "—"

        if direction == "LONG":
            # ── BULLISH RE-ENTRY: EMA10 crosses UP + RSI ≥ 60 ──
            just_crossed = (e10_prev <= e20_prev) and (e10_now > e20_now)
            already_above = e10_now > e20_now
            ema_ok = just_crossed or (already_above and cross_bar_time is not None)
            rsi_ok = curr_rsi >= 60 and curr_rsi > prev_rsi

            if ema_ok and rsi_ok:
                cur_price = float(closed["Close"].iloc[-1])
                entry = round(cur_price * 1.001, 2)

                # SL = low of the EMA cross candle
                sl = round(cross_bar_low, 2) if cross_bar_low is not None else round(e20_now, 2)

                # Target = nearest significant 4H swing HIGH above entry
                tgt_4h = _find_4h_swing_target(ticker, "LONG", cur_price)
                target = tgt_4h if tgt_4h else round(cur_price * 1.015, 2)

                return True, curr_rsi, sl, target, entry, cross_time_str
            return False, curr_rsi, None, None, None, cross_time_str

        else:  # SHORT
            # ── BEARISH RE-ENTRY: EMA10 crosses DOWN + RSI ≤ 40 ──
            just_crossed = (e10_prev >= e20_prev) and (e10_now < e20_now)
            already_below = e10_now < e20_now
            ema_ok = just_crossed or (already_below and cross_bar_time is not None)
            rsi_ok = curr_rsi <= 40 and curr_rsi < prev_rsi

            if ema_ok and rsi_ok:
                cur_price = float(closed["Close"].iloc[-1])
                entry = round(cur_price * 0.999, 2)

                # SL = high of the EMA cross candle
                sl = round(cross_bar_high, 2) if cross_bar_high is not None else round(e20_now, 2)

                # Target = nearest significant 4H swing LOW below entry
                tgt_4h = _find_4h_swing_target(ticker, "SHORT", cur_price)
                target = tgt_4h if tgt_4h else round(cur_price * 0.985, 2)

                return True, curr_rsi, sl, target, entry, cross_time_str
            return False, curr_rsi, None, None, None, cross_time_str

    except Exception as e:
        logging.warning(f"_check_rsi_15m_filter {ticker}: {e}")
        return True, 50.0, None, None, None, "—"


def _find_4h_swing_target(ticker: str, direction: str, entry: float) -> float | None:
    """
    Find the nearest SIGNIFICANT 4H swing high (LONG) or swing low (SHORT).

    A pivot qualifies only if:
      - It is the highest/lowest bar in a rolling ±5 bar (±20 hr) window
      - It is at least MIN_SWING_PCT% away from its neighbouring bars
        (eliminates minor dips/pops that are just noise)
      - It is at least 1% away from entry price

    Returns the target price, or None if no valid pivot found.
    """
    MIN_SWING_PCT = 0.015   # pivot must be ≥1.5% beyond neighbours — kills minor dips
    MIN_ENTRY_PCT = 0.01    # pivot must be ≥1% away from entry
    WIN = 5                 # ±5 x 4H bars = ±20 hours

    try:
        df4h = fetch_data_robust(ticker, period="60d", interval="4h")
        if df4h is None or df4h.empty or len(df4h) < (WIN * 2 + 5):
            return None

        df4h = df4h.iloc[:-1].copy()   # drop live unfinished bar

        highs = df4h["High"].values.astype(float)
        lows  = df4h["Low"].values.astype(float)
        n = len(highs)

        if direction == "LONG":
            pivots = []
            for i in range(WIN, n - WIN):
                bar_h = highs[i]
                window_h = highs[i - WIN: i + WIN + 1]
                # must be the highest bar in the window
                if bar_h < window_h.max():
                    continue
                # must be at least MIN_SWING_PCT above left and right neighbours
                left_max  = highs[max(0, i - WIN): i].max()
                right_max = highs[i + 1: i + WIN + 1].max()
                if bar_h < left_max * (1 + MIN_SWING_PCT) or bar_h < right_max * (1 + MIN_SWING_PCT):
                    continue
                if bar_h > entry * (1 + MIN_ENTRY_PCT):
                    pivots.append(bar_h)
            return round(min(pivots), 2) if pivots else None

        else:  # SHORT
            pivots = []
            for i in range(WIN, n - WIN):
                bar_l = lows[i]
                window_l = lows[i - WIN: i + WIN + 1]
                # must be the lowest bar in the window
                if bar_l > window_l.min():
                    continue
                # must be at least MIN_SWING_PCT below left and right neighbours
                left_min  = lows[max(0, i - WIN): i].min()
                right_min = lows[i + 1: i + WIN + 1].min()
                if bar_l > left_min * (1 - MIN_SWING_PCT) or bar_l > right_min * (1 - MIN_SWING_PCT):
                    continue
                if bar_l < entry * (1 - MIN_ENTRY_PCT):
                    pivots.append(bar_l)
            return round(max(pivots), 2) if pivots else None

    except Exception as e:
        logging.warning(f"_find_4h_swing_target {ticker}: {e}")
        return None


def _intra_rsi(s: pd.Series, n: int = 14) -> pd.Series:
    d = s.diff()
    g = d.where(d > 0, 0.0).ewm(com=n - 1, adjust=False).mean()
    l = (-d.where(d < 0, 0.0)).ewm(com=n - 1, adjust=False).mean()
    return 100 - 100 / (1 + g / l.replace(0, np.nan))


def _get_daily_trend_futures(ticker: str, current_price: float) -> tuple[str, str]:
    try:
        dfd = yf.Ticker(ticker).history(period="60d", interval="1d", auto_adjust=False)
        if len(dfd) < 50:
            return "NEUTRAL", "〰️ Daily: Insufficient data"

        ema10 = float(dfd["Close"].ewm(span=10, adjust=False).mean().iloc[-1])
        ema20 = float(dfd["Close"].ewm(span=20, adjust=False).mean().iloc[-1])
        ema50 = float(dfd["Close"].ewm(span=50, adjust=False).mean().iloc[-1])

        if ema10 > ema20 > ema50:
            return "BULLISH", f"📈 Daily: BULLISH (EMA10 > EMA20 > EMA50)"
        elif ema10 < ema20 < ema50:
            return "BEARISH", f"📉 Daily: BEARISH (EMA10 < EMA20 < EMA50)"
        else:
            return "NEUTRAL", f"〰️ Daily: NEUTRAL"
    except Exception:
        return "NEUTRAL", "〰️ Daily: Unknown"


def scan_futures_nse(
    tickers: list,
    signal_filter: str = "ALL",
    min_volume_cr: float = 0.0,
    use_daily_filter: bool = True,
    progress_cb=None,
) -> list:
    results = []
    total = len(tickers)

    for i, ticker in enumerate(tickers):
        if progress_cb:
            progress_cb(i, total)
        try:
            df5 = yf.Ticker(ticker).history(period="2d", interval="5m", auto_adjust=False)
            if df5.empty or len(df5) < 10:
                continue

            last_c = float(df5["Close"].iloc[-1])
            prev_c = float(df5["Close"].iloc[-2])
            change_pct = (last_c - prev_c) / prev_c * 100 if prev_c else 0

            today_vol = df5[df5.index.date == datetime.now(IST).date()]["Volume"].sum() if not df5.empty else 0
            volume_cr = (today_vol * last_c) / 1e7

            if volume_cr < min_volume_cr:
                continue

            daily_trend, daily_trend_label = _get_daily_trend_futures(ticker, last_c)
            rsi_val = float(_intra_rsi(df5["Close"], 9).iloc[-1])

            direction = None
            if daily_trend == "BULLISH":
                direction = "LONG"
            elif daily_trend == "BEARISH":
                direction = "SHORT"
            else:
                continue

            if signal_filter != "ALL" and signal_filter != direction:
                continue

            rsi_15m_pass, rsi_15m_val, sl, target, entry, cross_time = _check_rsi_15m_filter(ticker, direction)
            if not rsi_15m_pass:
                continue

            if entry is None:
                entry = last_c
            if sl is None:
                sl = last_c * 0.99 if direction == "LONG" else last_c * 1.01

            # Target = nearest significant 4H swing high (LONG) / low (SHORT)
            tgt_4h = _find_4h_swing_target(ticker, direction, float(entry))
            target = tgt_4h if tgt_4h else (last_c * 1.015 if direction == "LONG" else last_c * 0.985)

            risk = abs(entry - sl)
            reward = abs(target - entry)
            rr = round(reward / risk, 2) if risk > 0 else 0

            priority = 0
            if rr >= 2:
                priority += 30
            elif rr >= 1.5:
                priority += 15
            if volume_cr >= 5:
                priority += 20
            elif volume_cr >= 1:
                priority += 10
            if 45 < rsi_val < 65:
                priority += 5
            if (direction == "LONG" and daily_trend == "BULLISH") or \
               (direction == "SHORT" and daily_trend == "BEARISH"):
                priority += 20

            label = "🟢 BULLISH RE-ENTRY" if direction == "LONG" else "🔴 BEARISH RE-ENTRY"
            signal = f"FUTURES_{direction}"

            results.append({
                "ticker": ticker,
                "name": ticker.replace(".NS", ""),
                "price": last_c,
                "change_pct": round(change_pct, 2),
                "volume_cr": round(volume_cr, 2),
                "rsi": round(rsi_val, 1),
                "rsi_15m": round(rsi_15m_val, 1),
                "signal": signal,
                "label": label,
                "entry": round(entry, 2),
                "sl": round(sl, 2),
                "target": round(target, 2),
                "rr": rr,
                "direction": direction,
                "priority": priority,
                "daily_trend": daily_trend,
                "daily_trend_label": daily_trend_label,
                "pat_name_1h": "15M EMA Cross",
                "cross_time": cross_time,
            })
        except Exception as e:
            logging.warning(f"scan_futures_nse {ticker}: {e}")
            continue

    if progress_cb:
        progress_cb(total, total)

    results.sort(key=lambda x: x["priority"], reverse=True)
    return results


def render_futures_scanner_page(nse_watchlist: list):
    st.markdown('<p class="main-header">⚡ FUTURES SCANNER — NSE F&O</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="info-text">Daily Trend (EMA 10/20/50) → 15M RSI/EMA Crossover → Signal (v20.8)</p>',
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        scan_mode = st.selectbox(
            "Scan Universe",
            [
                "My NSE Watchlist",
                "Nifty 50 + Large-caps (~60 stocks)",
                "Nifty Midcap 100 (~50 stocks)",
                "NSE F&O All Stocks (~200 stocks)",
            ],
            key="futures_scan_mode_select",
        )
    with col2:
        sig_filter = st.selectbox("Signal Direction", ["ALL", "LONG", "SHORT"], key="futures_dir")
    with col3:
        min_vol = st.number_input("Min Volume (₹ Cr)", min_value=0.0, value=0.5, step=0.5, key="futures_vol")
    with col4:
        st.markdown("<br>", unsafe_allow_html=True)
        run_scan = st.button("🔍 SCAN NOW", use_container_width=True, key="run_futures_scan")

    mtf_col1, mtf_col2 = st.columns([3, 1])
    with mtf_col1:
        use_daily_filter = st.checkbox(
            "📈 Daily Trend Filter — only show LONG signals when Daily BULLISH, SHORT when Daily BEARISH",
            value=True,
            key="futures_daily_filter",
        )

    now_ist = datetime.now(IST)
    nse_open_t = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
    nse_close_t = now_ist.replace(hour=15, minute=30, second=0, microsecond=0)
    market_open = now_ist.weekday() < 5 and nse_open_t <= now_ist <= nse_close_t
    if not market_open:
        st.warning(
            "⚠️ NSE is currently CLOSED. Scanner uses latest available data. "
            "Live signals are most reliable during market hours (09:15–15:30 IST Mon–Fri)."
        )

    with st.expander("📖 Signal Logic & Filters"):
        st.markdown("""
**🔵 Signal Flow:**

| Timeframe | Role |
|-----------|------|
| **Daily Chart** | Trend direction via EMA 10/20/50 alignment — gates all trades |
| **15-Min Chart** | EMA 10/20 crossover + RSI filter → Re-Entry or Retracement signal |

**🔴 Daily Trend Gate (EMA 10/20/50):**
- EMA10 > EMA20 > EMA50 → **BULLISH** → Only LONG trades shown
- EMA10 < EMA20 < EMA50 → **BEARISH** → Only SHORT trades shown

**⚡ 15-Min Signal Logic (Re-Entry):**
| Signal | Condition |
|--------|-----------|
| 🟢 **BULLISH RE-ENTRY (LONG)** | Daily BULLISH + **15M EMA10 crosses UP EMA20** + **RSI ≥ 60 rising** |
| 🔴 **BEARISH RE-ENTRY (SHORT)** | Daily BEARISH + **15M EMA10 crosses DOWN EMA20** + **RSI ≤ 40 falling** |
| ⚠️ **BULLISH RETRACEMENT** | Daily BULLISH + EMA10 crosses DOWN EMA20 + RSI ≤ 40 → EXIT LONG signal |
| ⚠️ **BEARISH RETRACEMENT** | Daily BEARISH + EMA10 crosses UP EMA20 + RSI ≥ 60 → EXIT SHORT signal |

**📐 Entry, SL & Target:**
- **ENTRY** = Current price (with 0.1% buffer)
- **SL (LONG)** = Low of the EMA crossover candle on 15M
- **SL (SHORT)** = High of the EMA crossover candle on 15M
- **TARGET** = Next RESISTANCE (LONG) or next SUPPORT (SHORT) on **4H chart**

⚠️ *For educational purposes only. Not SEBI-registered advice.*
        """)

    if "futures_signals" not in st.session_state:
        st.session_state["futures_signals"] = None
    if "futures_n_tickers" not in st.session_state:
        st.session_state["futures_n_tickers"] = 0
    if "futures_scan_time" not in st.session_state:
        st.session_state["futures_scan_time"] = ""
    if "futures_scan_mode_label" not in st.session_state:
        st.session_state["futures_scan_mode_label"] = ""

    if run_scan:
        if scan_mode == "My NSE Watchlist":
            tickers = nse_watchlist
        elif scan_mode == "Nifty 50 + Large-caps (~60 stocks)":
            tickers = FUTURES_UNIVERSE
        elif scan_mode == "Nifty Midcap 100 (~50 stocks)":
            tickers = MIDCAP_UNIVERSE
        elif scan_mode == "NSE F&O All Stocks (~200 stocks)":
            tickers = list(dict.fromkeys(NSE_FNO_UNIVERSE))
        else:
            tickers = FUTURES_UNIVERSE
            
        if not tickers:
            st.warning("NSE watchlist is empty. Switch to Large-caps or Midcap universe, or add stocks to your watchlist first.")
            return

        prog_bar = st.progress(0)
        status = st.empty()

        def _prog(done, total):
            pct = int(done / total * 100) if total else 100
            prog_bar.progress(pct)
            status.markdown(
                f"<span style='color:#00F5FF;font-size:.82rem;'>"
                f"Scanning {done}/{total} symbols…</span>",
                unsafe_allow_html=True,
            )

        with st.spinner(""):
            signals = scan_futures_nse(tickers, signal_filter=sig_filter,
                                       min_volume_cr=min_vol,
                                       use_daily_filter=use_daily_filter,
                                       progress_cb=_prog)

        st.session_state["futures_signals"] = signals
        st.session_state["futures_n_tickers"] = len(tickers)
        st.session_state["futures_scan_time"] = datetime.now(IST).strftime("%H:%M:%S IST")
        st.session_state["futures_scan_mode_label"] = scan_mode

        prog_bar.empty()
        status.empty()

    if st.session_state["futures_signals"] is not None:
        signals = st.session_state["futures_signals"]
        scan_mode_label = st.session_state.get("futures_scan_mode_label", scan_mode)
        n_tickers = st.session_state.get("futures_n_tickers", "?")
        scan_time = st.session_state.get("futures_scan_time", "")

        st.markdown(f"### ✅ {len(signals)} Setup(s) Found &nbsp;|&nbsp; Scanned {n_tickers} symbols")
        st.markdown(
            f"<span style='color:#88B4CC;font-size:.78rem;'>Scan time: {scan_time}</span>",
            unsafe_allow_html=True,
        )
        st.markdown("---")

        if not signals:
            st.info(
                "No qualifying setups right now. Try: lower Min Volume, select a larger Universe, "
                "or scan during market hours (09:15–15:30 IST). "
                "Signals require 15M EMA crossover + RSI ≥ 60 (LONG) or RSI ≤ 40 (SHORT)."
            )
        else:
            for sig_idx, sig in enumerate(signals):
                direction = sig["direction"]
                card_col = "#00C853" if direction == "LONG" else "#FF1744"
                bg_col = "rgba(0,200,83,.07)" if direction == "LONG" else "rgba(255,23,68,.07)"
                border = "rgba(0,200,83,.40)" if direction == "LONG" else "rgba(255,23,68,.40)"
                chg_color = "#00C853" if sig["change_pct"] >= 0 else "#FF1744"
                chg_icon = "▲" if sig["change_pct"] >= 0 else "▼"
                rr_color = "#00C853" if sig["rr"] >= 2 else ("#FFD700" if sig["rr"] >= 1.5 else "#FF8C00")
                tv_sym = sig["ticker"].replace(".NS", "")
                tv_url = f"https://www.tradingview.com/chart/?symbol=NSE%3A{tv_sym}"
                trend_col = "#00C853" if sig["daily_trend"] == "BULLISH" else "#FF4444" if sig["daily_trend"] == "BEARISH" else "#FFD700"

                st.markdown(f"""
<div style="background:{bg_col};border:1px solid {border};border-left:4px solid {card_col};
            border-radius:8px;padding:14px 18px;margin:8px 0;">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;">
    <div>
      <span style="font-family:'Orbitron',monospace;font-size:1.05rem;color:#FFFFFF;
                   font-weight:700;letter-spacing:.08em;">{sig['name']}</span>
      <span style="font-size:.78rem;color:#88B4CC;margin-left:10px;">{sig['ticker']}</span>
      <a href="{tv_url}" target="_blank"
         style="margin-left:10px;font-size:.72rem;color:#00F5FF;text-decoration:none;
                border:1px solid #00F5FF44;border-radius:4px;padding:1px 7px;
                background:rgba(0,245,255,.08);">📊 TradingView ↗</a>
    </div>
    <div style="text-align:right;">
      <span style="font-family:'Share Tech Mono',monospace;font-size:1.1rem;
                   color:#E8F4FF;font-weight:700;">₹{sig['price']:,.2f}</span>
      <span style="color:{chg_color};font-size:.85rem;margin-left:8px;">
        {chg_icon} {abs(sig['change_pct']):.2f}%</span>
    </div>
  </div>
  <div style="margin:8px 0 4px;">
    <span style="background:{card_col};color:#000;font-weight:700;font-size:.78rem;
                 padding:3px 10px;border-radius:12px;">{sig['label']}</span>
    <span style="color:{rr_color};font-size:.80rem;margin-left:10px;font-weight:600;">
      R:R = {sig['rr']:.1f}</span>
    <span style="color:#88B4CC;font-size:.75rem;margin-left:12px;">
      Vol ₹{sig['volume_cr']:.1f}Cr · RSI(5M) {sig['rsi']} · RSI(15M) {sig.get('rsi_15m','—')}</span>
  </div>
  <div style="font-size:.78rem;color:#A0C4E0;margin:4px 0 4px;font-style:italic;">
    📌 Re-Entry: 15M EMA cross + RSI {sig.get('rsi_15m','—')} | SL = cross candle {'low' if sig['direction']=='LONG' else 'high'} | Target = 4H {'resistance' if sig['direction']=='LONG' else 'support'}</div>
  <div style="font-size:.75rem;margin:2px 0 6px;display:flex;gap:14px;flex-wrap:wrap;">
    <span style="color:{trend_col};font-weight:600;">
      📊 {sig.get("daily_trend_label","")}</span>
    <span style="color:#88B4CC;font-size:.72rem;">📐 Signal Source: {sig.get("pat_name_1h","15M EMA Cross")}</span>
    <span style="color:#88B4CC;font-size:.72rem;">🕐 Cross Time: {sig.get('cross_time','—')}</span>
  </div>
  <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:6px;">
    <div style="background:rgba(0,245,255,.08);border-radius:6px;padding:6px;text-align:center;">
      <div style="font-size:.65rem;color:#88B4CC;margin-bottom:2px;">ENTRY</div>
      <div style="font-size:.92rem;font-weight:700;color:#00F5FF;">₹{sig['entry']:,.2f}</div>
    </div>
    <div style="background:rgba(255,23,68,.1);border-radius:6px;padding:6px;text-align:center;">
      <div style="font-size:.65rem;color:#88B4CC;margin-bottom:2px;">STOP LOSS</div>
      <div style="font-size:.92rem;font-weight:700;color:#FF4444;">₹{sig['sl']:,.2f}</div>
    </div>
    <div style="background:rgba(0,200,83,.1);border-radius:6px;padding:6px;text-align:center;">
      <div style="font-size:.65rem;color:#88B4CC;margin-bottom:2px;">TARGET</div>
      <div style="font-size:.92rem;font-weight:700;color:#00C853;">₹{sig['target']:,.2f}</div>
    </div>
  </div>
</div>""", unsafe_allow_html=True)

                tg_btn_key = f"tg_single_futures_{sig['ticker']}_{sig_idx}"
                if st.button(f"📱 Send {sig['name']} to Telegram", key=tg_btn_key, use_container_width=False):
                    ok, msg = _send_single_futures_signal_to_telegram(sig)
                    if ok:
                        st.success(msg)
                    else:
                        st.error(f"❌ {msg}")

            st.markdown("---")
            st.markdown("#### 📋 Quick Summary Table")
            df_out = pd.DataFrame([{
                "Stock": s["name"],
                "Daily Trend": (
                    "🟢 BULLISH" if s["daily_trend"] == "BULLISH"
                    else ("🔴 BEARISH" if s["daily_trend"] == "BEARISH" else "⚪ NEUTRAL")
                ),
                "Signal": s["label"],
                "Signal Source": "15M EMA Cross + RSI",
                "Price ₹": f"{s['price']:,.2f}",
                "Chg %": f"{s['change_pct']:+.2f}",
                "Entry ₹": f"{s['entry']:,.2f}",
                "SL ₹": f"{s['sl']:,.2f}",
                "Target ₹": f"{s['target']:,.2f}",
                "R:R": f"{s['rr']:.1f}",
                "RSI 5M": f"{s['rsi']}",
                "RSI 15M": f"{s.get('rsi_15m', '—')}",
                "Cross Time": s.get("cross_time", "—"),
                "Vol Cr": f"{s['volume_cr']:.1f}",
            } for s in signals])
            st.dataframe(df_out, use_container_width=True, hide_index=True)

            csv = df_out.to_csv(index=False)
            tg_col, csv_col = st.columns(2)
            with csv_col:
                st.download_button(
                    "⬇️ Export to CSV",
                    data=csv,
                    file_name=f"futures_scan_{datetime.now(IST).strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with tg_col:
                if st.button("📱 Send to Telegram", use_container_width=True, key="tg_send_futures"):
                    ok, msg = _send_futures_scan_to_telegram(signals, scan_mode_label)
                    if ok:
                        st.success(f"✅ {msg}")
                    else:
                        st.error(f"❌ {msg}")

        st.markdown(
            '<div style="background:rgba(255,184,0,.08);border-left:3px solid #FFB800;'
            'padding:8px 12px;margin-top:14px;border-radius:3px;font-size:.72rem;color:#C8A000;">'
            '<b>DISCLAIMER:</b> Signals are algorithmically generated for study purposes only. '
            'Not SEBI-registered advice. Past patterns do not guarantee future results. '
            'Apply your own analysis and risk management before placing any trade.</div>',
            unsafe_allow_html=True,
        )

    elif not run_scan:
        st.markdown("""
<div style="text-align:center;padding:60px 20px;">
  <div style="font-size:4rem;">⚡</div>
  <div style="font-family:'Orbitron',monospace;font-size:1.1rem;color:#00F5FF;
              letter-spacing:.12em;margin-top:14px;">FUTURES SCANNER</div>
  <div style="font-size:.85rem;margin-top:12px;color:#88B4CC;max-width:500px;
              margin-inline:auto;line-height:1.7;">
    Click <b style="color:#00F5FF;">SCAN NOW</b> to detect live futures setups.<br>
    <b style="color:#FFD700;">SIMPLIFIED LOGIC v20.8:</b> Daily Trend → 15M EMA Cross + RSI (50/50) → ENTRY/SL/TARGET<br><br>
    Best scan window: <b style="color:#FFD700;">09:30 – 14:00 IST</b>
  </div>
</div>""", unsafe_allow_html=True)


# ============================================================
# MAIN APPLICATION
# ============================================================

def main():
    init_db()

    st.set_page_config(
        page_title="NEURAL NEXUS — Market Terminal v20.8",
        page_icon="⬡",
        layout="wide",
    )

    if "alert_manager" not in st.session_state:
        st.session_state["alert_manager"] = AlertManager()
    alert_manager: AlertManager = st.session_state["alert_manager"]

    if "show_alerts" not in st.session_state:
        st.session_state.show_alerts = False

    for toast in alert_manager.get_pending_toasts():
        if toast.get("is_signal"):
            st.toast(f"🔔🚨 {toast['message']}", icon="🎯")
            play_signal_sound()
        elif "PULLBACK" in toast.get("message", ""):
            st.toast(f"📊 {toast['message']}", icon="📉")
        else:
            st.toast(f"ℹ️ {toast['message']}", icon="📊")

    unread = alert_manager.get_unread_count()
    if unread > 0:
        st.markdown(get_notification_badge(unread), unsafe_allow_html=True)

    # ── SCI-FI THEME INJECTION ─────────────────────────────────
    st.markdown("""
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;600;700;900&family=Share+Tech+Mono&family=Exo+2:wght@300;400;600;700&family=Rajdhani:wght@400;500;600;700&display=swap" rel="stylesheet">

    <style>
    html, body, .stApp {
        background: #050D1A !important;
        font-family: 'Exo 2', sans-serif !important;
        color: #E8F4FF !important;
    }
    .stApp::before {
        content: '';
        position: fixed; inset: 0;
        background-image:
            linear-gradient(rgba(0,245,255,.04) 1px, transparent 1px),
            linear-gradient(90deg, rgba(0,245,255,.04) 1px, transparent 1px);
        background-size: 48px 48px;
        pointer-events: none; z-index: 0;
        animation: gridPulse 8s ease-in-out infinite;
    }
    @keyframes gridPulse { 0%,100%{opacity:.6} 50%{opacity:1} }
    .stApp::after {
        content: '';
        position: fixed; inset: 0;
        background: repeating-linear-gradient(
            0deg, transparent, transparent 3px,
            rgba(0,0,0,.04) 3px, rgba(0,0,0,.04) 4px
        );
        pointer-events: none; z-index: 1;
    }
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #060F1E 0%, #0A1830 60%, #07111F 100%) !important;
        border-right: 1px solid rgba(0,245,255,.35) !important;
        box-shadow: 4px 0 32px rgba(0,245,255,.12), inset -1px 0 0 rgba(0,245,255,.08) !important;
    }
    [data-testid="stSidebar"] * {
        color: #D0EEFF !important;
    }
    .main-header {
        font-family: 'Orbitron', monospace !important;
        font-size: 1.75rem !important;
        font-weight: 900 !important;
        color: #00F5FF !important;
        text-align: center !important;
        text-shadow: 0 0 20px rgba(0,245,255,.95), 0 0 45px rgba(0,245,255,.45), 0 0 80px rgba(0,245,255,.2) !important;
        letter-spacing: .16em !important;
        margin-bottom: .4rem !important;
        padding: .5rem 0 !important;
    }
    .info-text {
        font-family: 'Share Tech Mono', monospace !important;
        font-size: .78rem !important;
        color: #7ECFEE !important;
        text-align: center !important;
        letter-spacing: .1em !important;
        margin-bottom: 1rem !important;
        text-shadow: 0 0 8px rgba(0,200,255,.4) !important;
    }
    .stButton > button {
        background: linear-gradient(135deg, rgba(0,30,60,.8), rgba(0,15,40,.9)) !important;
        border: 1px solid rgba(0,245,255,.65) !important;
        color: #D0F4FF !important;
        font-family: 'Orbitron', monospace !important;
        font-size: .69rem !important;
        font-weight: 700 !important;
        letter-spacing: .09em !important;
        border-radius: 3px !important;
        padding: .48rem 1.1rem !important;
        transition: all .2s ease !important;
        text-transform: uppercase !important;
        text-shadow: 0 0 8px rgba(0,245,255,.4) !important;
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, rgba(0,245,255,.18), rgba(0,200,255,.1)) !important;
        border-color: #00F5FF !important;
        box-shadow: 0 0 20px rgba(0,245,255,.55), 0 0 40px rgba(0,245,255,.2) !important;
        color: #FFFFFF !important;
        text-shadow: 0 0 12px rgba(0,245,255,.9) !important;
        transform: translateY(-2px) !important;
    }
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(0,40,75,.65), rgba(0,25,50,.8)) !important;
        border: 1px solid rgba(0,245,255,.3) !important;
        border-top: 2px solid rgba(0,245,255,.6) !important;
        border-radius: 6px !important;
        padding: 14px 18px !important;
        box-shadow: 0 4px 20px rgba(0,245,255,.08), inset 0 1px 0 rgba(0,245,255,.15) !important;
    }
    [data-testid="stMetricValue"] {
        font-family: 'Orbitron', monospace !important;
        color: #00F5FF !important;
        text-shadow: 0 0 14px rgba(0,245,255,.75), 0 0 28px rgba(0,245,255,.3) !important;
        font-size: 1.45rem !important;
        font-weight: 700 !important;
    }
    [data-testid="stMetricLabel"] {
        font-family: 'Share Tech Mono', monospace !important;
        color: #80CCEE !important;
        font-size: .72rem !important;
        letter-spacing: .09em !important;
        text-transform: uppercase !important;
    }
    [data-testid="stDataFrame"] th {
        background: rgba(0,60,100,.7) !important;
        color: #00F5FF !important;
        font-family: 'Share Tech Mono', monospace !important;
        font-size: .76rem !important;
        letter-spacing: .07em !important;
        text-transform: uppercase !important;
    }
    [data-testid="stDataFrame"] td {
        color: #D8EEFF !important;
        font-family: 'Rajdhani', sans-serif !important;
        font-size: .88rem !important;
    }
    .signal-box {
        background: linear-gradient(135deg, rgba(255,30,80,.22), rgba(255,184,0,.15)) !important;
        border: 2px solid rgba(255,50,90,.9) !important;
        border-radius: 6px !important;
        padding: 16px !important;
        margin: 12px 0 !important;
        box-shadow: 0 0 28px rgba(255,0,110,.45), inset 0 0 20px rgba(255,0,110,.08) !important;
        animation: signalPulse 1.3s ease-in-out infinite !important;
    }
    @keyframes signalPulse {
        0%,100%{box-shadow:0 0 28px rgba(255,0,110,.45),inset 0 0 20px rgba(255,0,110,.08)}
        50%{box-shadow:0 0 50px rgba(255,0,110,.7), inset 0 0 32px rgba(255,0,110,.14)}
    }
    p, span, div, li, td, th, h1, h2, h3, h4, h5, h6,
    .stMarkdown, .stMarkdown p, .stMarkdown span,
    [data-testid="stMarkdownContainer"] p,
    [data-testid="stMarkdownContainer"] span,
    [data-testid="stMarkdownContainer"] li {
        color: #E8F4FF !important;
    }
    label, label p, label span,
    label[data-testid="stWidgetLabel"],
    label[data-testid="stWidgetLabel"] p,
    label[data-testid="stWidgetLabel"] span,
    div[data-testid="stSelectbox"] label,
    div[data-testid="stSelectbox"] label p,
    div[data-testid="stNumberInput"] label,
    div[data-testid="stNumberInput"] label p,
    div[data-testid="stTextInput"] label,
    div[data-testid="stTextInput"] label p,
    div[data-testid="stCheckbox"] label,
    div[data-testid="stCheckbox"] label p,
    div[data-testid="stRadio"] label,
    div[data-testid="stRadio"] label p,
    div[data-testid="stSlider"] label,
    div[data-testid="stSlider"] label p,
    div[data-testid="stMultiSelect"] label,
    div[data-testid="stMultiSelect"] label p,
    div[data-testid="stDateInput"] label,
    div[data-testid="stTextArea"] label {
        color: #00F5FF !important;
        font-family: 'Share Tech Mono', monospace !important;
        font-size: .82rem !important;
        letter-spacing: .05em !important;
        text-shadow: 0 0 8px rgba(0,245,255,.45) !important;
    }
    div[data-testid="stSelectbox"] div[data-baseweb="select"] div,
    div[data-testid="stSelectbox"] div[data-baseweb="select"] span,
    div[data-testid="stSelectbox"] [data-baseweb="select"] * {
        color: #E8F4FF !important;
        background-color: transparent !important;
    }
    div[data-testid="stNumberInput"] input,
    div[data-testid="stTextInput"] input,
    div[data-testid="stTextArea"] textarea {
        color: #E8F4FF !important;
        background: rgba(0,20,50,.7) !important;
        border-color: rgba(0,245,255,.35) !important;
    }
    div[data-testid="stCheckbox"] span,
    div[data-testid="stCheckbox"] p,
    div[data-testid="stRadio"] span,
    div[data-testid="stRadio"] p,
    .stCheckbox span, .stCheckbox p,
    .stRadio span, .stRadio p {
        color: #C8EEFF !important;
    }
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] summary p,
    [data-testid="stExpander"] summary span,
    [data-testid="stExpander"] summary div {
        color: #00F5FF !important;
    }
    [data-testid="stTabs"] button[role="tab"],
    [data-testid="stTabs"] button[role="tab"] p,
    [data-testid="stTabs"] button[role="tab"] div {
        color: #7ECFEE !important;
    }
    [data-testid="stTabs"] button[role="tab"][aria-selected="true"],
    [data-testid="stTabs"] button[role="tab"][aria-selected="true"] p {
        color: #00F5FF !important;
    }
    [data-baseweb="popover"] li,
    [data-baseweb="menu"] li,
    [data-baseweb="list-item"],
    ul[data-baseweb="menu"] li,
    [role="option"] {
        color: #E8F4FF !important;
        background: #071428 !important;
    }
    [role="option"]:hover,
    [data-baseweb="menu"] li:hover {
        background: rgba(0,245,255,.12) !important;
        color: #00F5FF !important;
    }
    [data-testid="stAlert"] p,
    [data-testid="stAlert"] div,
    [data-testid="stInfo"] p,
    [data-testid="stWarning"] p,
    [data-testid="stSuccess"] p,
    [data-testid="stError"] p {
        color: #E8F4FF !important;
    }
    div[data-testid="stSlider"] div,
    div[data-testid="stSlider"] span,
    div[data-testid="stSlider"] p {
        color: #C8EEFF !important;
    }
    small, .caption, [data-testid="stCaptionContainer"] p {
        color: #88B4CC !important;
    }
    [data-testid="stProgressBar"] p {
        color: #C8EEFF !important;
    }
    </style>
    """, unsafe_allow_html=True)

    # ── SIDEBAR ────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## 📊 Om's Tracker v20.9.3")

        if TV_AVAILABLE:
            st.markdown(
                "<div style='text-align:center;background:rgba(0,200,100,.12);"
                "border:1px solid rgba(0,220,110,.4);border-radius:4px;"
                "padding:5px 8px;margin-bottom:6px;font-size:.72rem;"
                "color:#00E676;font-family:Share Tech Mono,monospace;letter-spacing:.06em;'>"
                "📡 DATA: TRADINGVIEW LIVE</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div style='text-align:center;background:rgba(255,180,0,.1);"
                "border:1px solid rgba(255,200,0,.35);border-radius:4px;"
                "padding:5px 8px;margin-bottom:6px;font-size:.72rem;"
                "color:#FFE033;font-family:Share Tech Mono,monospace;letter-spacing:.06em;'>"
                "⚠️ DATA: YFINANCE (install tvdatafeed)</div>",
                unsafe_allow_html=True,
            )

        ms = get_market_status()
        cols = st.columns(2)
        for i, (mkt, (txt, color)) in enumerate(ms.items()):
            cols[i].markdown(
                f"<div style='text-align:center;color:{color};font-size:.75rem;font-weight:bold;'>"
                f"{mkt}<br>{txt}</div>", unsafe_allow_html=True)

        usd_inr = get_usd_inr_rate()
        st.markdown(f"<div style='text-align:center;color:#888;font-size:.75rem;'>💱 USD/INR ₹{usd_inr:.2f}</div>",
                    unsafe_allow_html=True)
        st.markdown("---")

        with st.expander(f"🔔 NOTIFICATIONS ({unread} new)", expanded=st.session_state.show_alerts):
            c1, c2 = st.columns([3, 1])
            with c2:
                if st.button("✓ Read", key="mark_read_btn", use_container_width=True):
                    alert_manager.mark_all_read()
                    st.rerun()
            for alert in alert_manager.get_recent_alerts(30):
                is_sig = alert.get("is_signal")
                is_pu = "PULLBACK" in alert.get("message", "")
                icon = "🎯🚨" if is_sig else ("📉" if is_pu else "📊")
                color = "#FF4444" if is_sig else ("#FFD700" if is_pu else "#88CCEE")
                bg = "rgba(255,50,50,.12)" if not alert.get("read") else "rgba(255,255,255,.04)"
                border = "2px" if not alert.get("read") else "1px"
                msg_color = "#FFFFFF" if not alert.get("read") else "#C0DCF0"
                st.markdown(f"""
                <div style="background:{bg};border-left:{border} solid {color};
                            padding:7px 10px;margin:4px 0;border-radius:4px;">
                    <div style="color:#88B4CC;font-size:.72rem;margin-bottom:2px;">
                        {alert['timestamp']}
                    </div>
                    <div style="color:{msg_color};font-size:.84rem;">
                        {icon} {alert['message']}
                    </div>
                </div>""", unsafe_allow_html=True)
            if not alert_manager.get_recent_alerts(1):
                st.info("No notifications yet")

        st.markdown("---")
        page = st.radio("Navigation", [
            "📊 Dashboard", "📋 NSE Watchlist", "🛢️ MCX Watchlist",
            "📈 4H Charts", "💼 Portfolio", "📒 Trade Journal",
            "🎯 Option Scanner", "⚡ Futures Scanner", "⚙️ Settings",
        ])

        auto_refresh_ui()

        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    nse_watchlist = load_watchlist_nse()
    mcx_watchlist = load_watchlist_mcx()
    portfolio = load_portfolio()

    # ── DASHBOARD ─────────────────────────────────────────────
    if page == "📊 Dashboard":
        st.markdown('<p class="main-header">📈 NSE & MCX Dashboard</p>', unsafe_allow_html=True)
        st.markdown('<p class="info-text">4H Pullback → 5M Entry | RSI 60/40 Zones | EMA 10/20/50 | ADX Filter | v20.8</p>', unsafe_allow_html=True)

        try:
            conn = get_db()
            active_pos = conn.execute("SELECT COUNT(*) as n FROM reentry_state").fetchone()["n"]
            conn.close()
        except Exception:
            active_pos = 0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("📋 NSE", len(nse_watchlist))
        c2.metric("🛢️ MCX", len(mcx_watchlist))
        c3.metric("💼 Portfolio", len(portfolio))
        c4.metric("🔔 Alerts", unread)
        c5.metric("🎯 Active Trades", active_pos)
        st.markdown("---")

        def render_table(watchlist, is_nse=True):
            if not watchlist:
                st.info("No symbols in watchlist.")
                return

            data = []
            signal_items = []
            in_position_tks = []
            prog = st.progress(0)
            stat = st.empty()

            for idx, ticker in enumerate(watchlist):
                name = get_symbol_display_name(ticker)
                stat.text(f"Analyzing {name}… ({idx+1}/{len(watchlist)})")
                info = fetch_stock_data(ticker)
                sig15 = _get_watchlist_signal_data(ticker)

                if info:
                    pn = info.get("pattern_name", "NONE")
                    trend_dir = info.get("trend", "")
                    if pn and pn != "NONE":
                        if trend_dir == "BULLISH":
                            pdp = f"📉 {pn}"
                        elif trend_dir == "BEARISH":
                            pdp = f"📈 {pn}"
                        else:
                            pdp = pn
                    else:
                        pdp = "—"

                    is_commodity = not is_nse

                    def _to_inr_status(raw_price, _ticker=ticker):
                        if raw_price is None:
                            return None
                        if is_commodity:
                            return convert_usd_to_inr_for_mcx(float(raw_price), _ticker)
                        return raw_price

                    sig = info["signal"]

                    wait_for_reentry = sig in {"WAIT_LONG_REENTRY", "WAIT_SHORT_REENTRY",
                                               "BEARISH_PATTERN_4H", "BULLISH_PATTERN_4H",
                                               "APPROACHING_REENTRY"}
                    if wait_for_reentry and not info.get("reentry_done"):
                        fast = check_reentry_live_fast(
                            ticker,
                            info.get("pattern_high"),
                            info.get("pattern_low"),
                            info["trend"],
                        )
                        if fast["signal"] == "RE-ENTRY_SIGNAL":
                            raw_ep = fast["entry_price"]
                            if is_commodity and raw_ep:
                                fast_ep_inr = convert_usd_to_inr_for_mcx(float(raw_ep), ticker)
                            else:
                                fast_ep_inr = raw_ep
                            ph_inr = info.get("pattern_high_display") or info.get("pattern_high")
                            pl_inr = info.get("pattern_low_display") or info.get("pattern_low")
                            sl_inr = pl_inr if info["trend"] == "BULLISH" else ph_inr
                            t1, t2 = compute_sr_targets(
                                ticker, info["trend"], fast_ep_inr,
                                info.get("pattern_high", fast_ep_inr),
                                info.get("pattern_low", fast_ep_inr),
                            )
                            sig_lbl = "LONG_REENTRY_SIGNAL" if info["trend"] == "BULLISH" else "SHORT_REENTRY_SIGNAL"
                            info = {**info,
                                    "signal": sig_lbl,
                                    "entry_price": fast_ep_inr,
                                    "stop_loss": sl_inr,
                                    "target_1r": t1,
                                    "target_2r": t2,
                                    "breakout_time": fast.get("breakout_time"),
                                    "new_signal_detected": True,
                                    }
                            sig = sig_lbl
                        elif fast["signal"] == "APPROACHING":
                            info = {**info, "signal": "APPROACHING_REENTRY"}
                            sig = "APPROACHING_REENTRY"

                    alert_manager.check_and_alert(
                        ticker, name, info["trend"], sig,
                        info.get("entry_price"), info.get("stop_loss"),
                        info.get("breakout_time"), info.get("new_signal_detected", False),
                        info.get("pattern_name"),
                    )

                    # ── STATUS: 3 clean states only ──────────────────────────────
                    if info.get("reentry_done") or sig == "IN_POSITION":
                        ri = info.get("reentry_info") or {}
                        ep   = ri.get("entry_price")
                        curr = info["current_price"]
                        _trend_ri = ri.get("trend", trend_dir)
                        # check SL hit
                        sl_raw = ri.get("pattern_low") if _trend_ri == "BULLISH" else ri.get("pattern_high")
                        sl_v = float(sl_raw) if sl_raw else None
                        if sl_v and is_commodity:
                            sl_v = convert_usd_to_inr_for_mcx(sl_v, ticker)
                        # check target hit
                        tgt_ri = _find_4h_swing_target(ticker, "LONG" if _trend_ri == "BULLISH" else "SHORT", float(ep) if ep else curr)
                        sl_hit  = sl_v and (curr <= sl_v if _trend_ri == "BULLISH" else curr >= sl_v)
                        tgt_hit = tgt_ri and (curr >= tgt_ri if _trend_ri == "BULLISH" else curr <= tgt_ri)
                        if sl_hit:
                            sdp = "🔴 Stoploss Hit"
                        elif tgt_hit:
                            sdp = "🎯 Target Hit"
                        else:
                            sdp = "✅ Re-entry Done — Waiting for Target / SL"
                        in_position_tks.append(ticker)

                    elif sig in {"BULLISH_RETRACEMENT", "BEARISH_RETRACEMENT",
                                 "BULLISH_RETRACEMENT_EXIT", "BEARISH_RETRACEMENT_EXIT",
                                 "WAIT_LONG_REENTRY", "WAIT_SHORT_REENTRY",
                                 "BEARISH_PATTERN_4H", "BULLISH_PATTERN_4H",
                                 "APPROACHING_REENTRY",
                                 "LONG_REENTRY_SIGNAL", "SHORT_REENTRY_SIGNAL"}:
                        sdp = "⚠️ Retracement — Waiting for Re-entry"

                    else:
                        sdp = "—"
                    # ─────────────────────────────────────────────────────────────

                    rsi_v = info.get("rsi")
                    rsi_s = f"{rsi_v:.0f}" if rsi_v else "—"
                    
                    adx_val = info.get("adx_info", {}).get("adx") if info.get("adx_info") else None
                    adx_display = f"{adx_val:.1f}" if adx_val else "—"

                    entry_disp = "—"
                    t1_disp = "—"
                    if info.get("reentry_done") and info.get("reentry_info"):
                        ri = info["reentry_info"]
                        ep_ri = ri.get("entry_price")
                        entry_disp = format_price(ep_ri, is_nse) if ep_ri else "—"
                        t1_v, _ = compute_sr_targets(
                            ticker, ri.get("trend", trend_dir), float(ep_ri) if ep_ri else 0,
                            ri.get("pattern_high", ep_ri), ri.get("pattern_low", ep_ri)
                        ) if ep_ri else (None, None)
                        t1_disp = format_price(t1_v, is_nse) if t1_v else "—"
                    elif info.get("entry_price"):
                        entry_disp = format_price(info["entry_price"], is_nse)
                        t1_disp = format_price(info.get("target_1r"), is_nse) if info.get("target_1r") else "—"

                    sig_time_key = f"sig_time_{ticker}_{sig}"
                    signal_active = sig in {
                        "LONG_REENTRY_SIGNAL", "SHORT_REENTRY_SIGNAL",
                        "BULLISH_RETRACEMENT_EXIT", "BEARISH_RETRACEMENT_EXIT",
                        "BEARISH_PATTERN_4H", "BULLISH_PATTERN_4H",
                        "APPROACHING_REENTRY",
                    }
                    if signal_active:
                        if sig_time_key not in st.session_state:
                            st.session_state[sig_time_key] = datetime.now(IST).strftime("%H:%M:%S")
                        sig_time_display = st.session_state[sig_time_key]
                    else:
                        for _k in list(st.session_state.keys()):
                            if _k.startswith(f"sig_time_{ticker}_") and _k != sig_time_key:
                                del st.session_state[_k]
                        sig_time_display = "—"

                    # ── 15M RSI gate: LONG≥60 (bullish re-entry), SHORT≤40 (bearish re-entry) ──
                    _sig15_rsi = sig15["rsi_15m"] if sig15 else None
                    _sig15_dir = sig15["signal"] if sig15 else "NONE"
                    _sig15_passes_rsi = (
                        (_sig15_dir == "LONG"  and _sig15_rsi is not None and _sig15_rsi >= 60) or
                        (_sig15_dir == "SHORT" and _sig15_rsi is not None and _sig15_rsi <= 40)
                    )
                    _sig15_display = sig15["signal"] if _sig15_passes_rsi else "NONE"

                    row = {
                        "Symbol": name,
                        "Chart": get_tradingview_url(ticker, "4H"),
                        "Price": format_price(info["current_price"], is_nse),
                        "Trend": f"{info['trend_icon']} {info['trend']}",
                        "ADX": adx_display,
                        "RSI": rsi_s,
                        "Pattern": pdp,
                        "Status": sdp,
                        "15M Signal": (
                            "🟢 RE-ENTRY LONG"  if _sig15_display == "LONG"  else
                            "🔴 RE-ENTRY SHORT" if _sig15_display == "SHORT" else
                            ("〰️ " + (sig15["daily_trend"] if sig15 else "—"))
                        ),
                        "15M RSI":    f"{sig15['rsi_15m']}" if (sig15 and sig15["rsi_15m"] is not None) else "—",
                        "Cross Time": sig15["signal_time"] if (sig15 and sig15.get("signal_time","—") != "—") else "—",
                        "15M Entry":  format_price(sig15["entry_price"], is_nse) if (sig15 and _sig15_passes_rsi and sig15["entry_price"]) else "—",
                        "15M SL":     format_price(sig15["sl"],          is_nse) if (sig15 and _sig15_passes_rsi and sig15["sl"])          else "—",
                        "15M Target": format_price(sig15["target"],      is_nse) if (sig15 and _sig15_passes_rsi and sig15["target"])      else "—",
                    }
                    if not is_nse:
                        row["Unit"] = MCX_COMMODITIES.get(ticker, {}).get("unit", "")
                    data.append(row)

                    if sig15 and _sig15_passes_rsi and sig15["signal"] in ("LONG", "SHORT"):
                        _sig15_key = f"sig15_banner_{ticker}_{sig15['signal']}"
                        if _sig15_key not in st.session_state:
                            st.session_state[_sig15_key] = sig15["signal_time"]
                        _clr = "#00C853" if sig15["signal"] == "LONG" else "#FF1744"
                        _ico = "🟢" if sig15["signal"] == "LONG" else "🔴"
                        _sl_str   = format_price(sig15["sl"],          is_nse) if sig15["sl"]          else "—"
                        _tgt_str  = format_price(sig15["target"],      is_nse) if sig15["target"]      else "—"
                        _entry_str = format_price(sig15["entry_price"], is_nse) if sig15["entry_price"] else "—"
                        _sl_label  = "SL (cross candle low)" if sig15["signal"] == "LONG" else "SL (cross candle high)"
                        _rr_str = "—"
                        _rr_val = 0.0
                        if sig15["sl"] and sig15["target"] and sig15["entry_price"]:
                            _risk   = abs(sig15["entry_price"] - sig15["sl"])
                            _reward = abs(sig15["target"]      - sig15["entry_price"])
                            if _risk > 0:
                                _rr_val = _reward / _risk
                                _rr_str = f"{_rr_val:.1f}"
                        # Skip banner if R:R is zero (target == entry, no meaningful trade)
                        if _rr_val <= 0.0:
                            pass
                        else:
                            _sig15_dismissed = is_signal_dismissed(ticker, sig15.get("signal_time"))
                            if not _sig15_dismissed:
                                _sig15_direction = "BULLISH" if sig15["signal"] == "LONG" else "BEARISH"
                                _sig15_d_label   = "BUY"     if sig15["signal"] == "LONG" else "SELL"
                                _sig15_pullback  = "🟢 BULLISH PULLBACK" if sig15["signal"] == "LONG" else "🔻 BEARISH PULLBACK"
                                tv_link15 = get_tradingview_url(ticker, "15M")
                                st.markdown(f"""
                                <div class="signal-box">
                                    <h3>🚨 15M RE-ENTRY SIGNAL DETECTED! 🚨</h3>
                                    <p><strong><a href="{tv_link15}" target="_blank" style="color:#FF6090;text-decoration:none;">{name} ↗</a></strong>
                                    &nbsp;|&nbsp; {_sig15_pullback}
                                    &nbsp;|&nbsp; {_sig15_d_label} @ {_entry_str}
                                    &nbsp;|&nbsp; {_sl_label}: {_sl_str}
                                    &nbsp;|&nbsp; Target (4H): {_tgt_str}
                                    &nbsp;|&nbsp; R:R {_rr_str}
                                    &nbsp;|&nbsp; 15M RSI: {sig15["rsi_15m"] if sig15["rsi_15m"] else "—"}</p>
                                </div>""", unsafe_allow_html=True)
                                # Build adx_info stub for the buttons (15M signals carry adx from info)
                                _adx_for_15m = info.get("adx_info") if info else None
                                show_reentry_buttons(
                                    ticker + "_15m", name,
                                    sig15["entry_price"],
                                    sig15["entry_price"],   # pattern_high (cross candle level)
                                    sig15["sl"],            # pattern_low / SL level
                                    _sig15_direction,
                                    f"15M EMA CROSS — {'BULLISH' if sig15['signal']=='LONG' else 'BEARISH'} RE-ENTRY",
                                    target_1r=sig15["target"],
                                    target_2r=None,
                                    breakout_time=sig15.get("signal_time"),
                                    adx_info=_adx_for_15m,
                                )

                prog.progress((idx + 1) / len(watchlist))

            prog.empty()
            stat.empty()
            if data:
                st.dataframe(
                    pd.DataFrame(data),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Chart": st.column_config.LinkColumn(
                            "📊 4H Chart",
                            display_text="Open ↗",
                        ),
                    },
                )

            bad = [t for t in watchlist if t in _failed_tickers]
            if bad:
                bad_names = ", ".join(t.replace(".NS", "").replace(".BO", "") for t in bad)
                st.warning(
                    f"⚠️ **No data found for:** {bad_names}  \n"
                    f"These symbols may be delisted, renamed, or not available on Yahoo Finance. "
                    f"Check the correct NSE ticker and update your watchlist.",
                    icon="⚠️",
                )

            for ticker in in_position_tks:
                info = fetch_stock_data(ticker)
                if info and (info.get("reentry_done") or info["signal"] == "IN_POSITION"):
                    show_close_position_button(ticker, get_symbol_display_name(ticker), info.get("trend"))

            pending_msgs = st.session_state.get("pending_telegram_msgs", [])
            if pending_msgs:
                st.markdown("---")
                st.info(f"📱 **{len(pending_msgs)} alert(s) ready to send to Telegram**")
                tg_btn_col, tg_clr_col = st.columns(2)
                with tg_btn_col:
                    if st.button("📱 Send All Alerts to Telegram", key=f"send_pending_tg_{is_nse}", use_container_width=True):
                        combined = "\n\n".join(pending_msgs)
                        ok, err = send_telegram_alert(combined)
                        if ok:
                            st.success("✅ Alerts sent to Telegram!")
                            st.session_state["pending_telegram_msgs"] = []
                        else:
                            st.error(f"❌ {err}")
                with tg_clr_col:
                    if st.button("🗑️ Dismiss Alerts", key=f"dismiss_pending_tg_{is_nse}", use_container_width=True):
                        st.session_state["pending_telegram_msgs"] = []
                        st.rerun()

        tab1, tab2 = st.tabs(["🏦 NSE STOCKS", "🛢️ MCX COMMODITIES"])
        with tab1:
            render_table(nse_watchlist, is_nse=True)
        with tab2:
            render_table(mcx_watchlist, is_nse=False)

        with st.expander("📖 Strategy Guide (Trading Notes)"):
            st.markdown("""
            | | BULLISH Trade | BEARISH Trade |
            |---|---|---|
            | **Trend (Daily)** | EMA 10 > 20 > 50 | EMA 10 < 20 < 50 |
            | **Pullback (4H)** | Bearish candle cluster | Bullish candle cluster |
            | **RSI during pullback** | Falls to 40–50 ✅ | Rises to 50–60 ✅ |
            | **Entry trigger** | 5M close **ABOVE** Pattern High + vol ≥ avg | 5M close **BELOW** Pattern Low + vol ≥ avg |
            | **Stop Loss** | Pattern LOW | Pattern HIGH |
            | **T1** | Nearest 4H swing resistance | Nearest 4H swing support |

            **RSI Zones:**
            - RSI **> 60** → Bullish zone → LONG allowed ✅
            - RSI **40–60** → Sideways zone → **No trade** ⚠️
            - RSI **< 40** → Bearish zone → SHORT allowed ✅

            **ADX:** >25 full size | 20-25 reduce 50% | <20 skip.
            """)

    # ── NSE WATCHLIST ──────────────────────────────────────────
    elif page == "📋 NSE Watchlist":
        st.markdown('<p class="main-header">📋 NSE Stocks Watchlist</p>', unsafe_allow_html=True)
        col1, col2 = st.columns([2, 1])
        with col1:
            for i, t in enumerate(nse_watchlist):
                c1, c2 = st.columns([4, 1])
                c1.write(f"**{t.replace('.NS','')}**")
                c1.caption(t)
                if c2.button("❌", key=f"del_nse_{i}"):
                    nse_watchlist.pop(i)
                    save_watchlist_nse(nse_watchlist)
                    st.rerun()
                st.divider()
        with col2:
            with st.form("add_nse"):
                sym = st.text_input("Symbol", placeholder="RELIANCE, TCS, MCX").upper()
                if st.form_submit_button("Add"):
                    if sym:
                        ticker = sym + ".NS" if not sym.endswith(".NS") else sym
                        if ticker not in nse_watchlist:
                            nse_watchlist.append(ticker)
                            save_watchlist_nse(nse_watchlist)
                            st.rerun()
                        else:
                            st.warning("Already in watchlist")

    # ── MCX WATCHLIST ──────────────────────────────────────────
    elif page == "🛢️ MCX Watchlist":
        st.markdown('<p class="main-header">🛢️ MCX Commodities Watchlist</p>', unsafe_allow_html=True)
        col1, col2 = st.columns([2, 1])
        with col1:
            for i, t in enumerate(mcx_watchlist):
                c1, c2 = st.columns([4, 1])
                info = get_commodity_info(t)
                c1.write(f"**{info['name']}**")
                c1.caption(f"{t} | {info.get('unit','')}")
                if c2.button("❌", key=f"del_mcx_{i}"):
                    mcx_watchlist.pop(i)
                    save_watchlist_mcx(mcx_watchlist)
                    st.rerun()
                st.divider()
        with col2:
            with st.form("add_mcx"):
                sel = st.selectbox("Commodity", list(MCX_COMMODITIES.keys()),
                                   format_func=lambda x: MCX_COMMODITIES[x]["name"])
                if st.form_submit_button("Add"):
                    if sel not in mcx_watchlist:
                        mcx_watchlist.append(sel)
                        save_watchlist_mcx(mcx_watchlist)
                        st.rerun()
                    else:
                        st.warning("Already in watchlist")

    # ── CHARTS ─────────────────────────────────────────────────
    elif page == "📈 4H Charts":
        st.markdown('<p class="main-header">📈 Interactive Charts</p>', unsafe_allow_html=True)
        all_syms = [(t, get_symbol_display_name(t)) for t in nse_watchlist + mcx_watchlist]
        if all_syms:
            cc1, cc2, cc3 = st.columns([3, 1, 1])
            with cc1:
                sel = st.selectbox("Select Symbol", all_syms, format_func=lambda x: x[1])
            with cc2:
                tf = st.selectbox("Timeframe", ["4H", "5M"], index=0)
            with cc3:
                st.markdown("<br>", unsafe_allow_html=True)
                tv_url = get_tradingview_url(sel[0], tf)
                st.markdown(
                    f'<a href="{tv_url}" target="_blank">' +
                    '<button style="' +
                    'width:100%;padding:.42rem .5rem;' +
                    'background:rgba(0,20,40,.6);' +
                    'border:1px solid rgba(0,245,255,.55);' +
                    'color:#B8ECFF;' +
                    'font-family:Orbitron,monospace;' +
                    'font-size:.60rem;font-weight:600;' +
                    'letter-spacing:.06em;border-radius:2px;' +
                    'cursor:pointer;text-transform:uppercase;">' +
                    '🔗 Open in TradingView</button></a>',
                    unsafe_allow_html=True,
                )
            ticker, name = sel
            info = fetch_stock_data(ticker)

            if info:
                is_nse = ticker.endswith(".NS")
                adx_info = info.get("adx_info", {})
                
                c1, c2, c3, c4, c5, c6 = st.columns(6)
                c1.metric("Price", format_price(info["current_price"], is_nse))
                c2.metric("Trend", f"{info['trend_icon']} {info['trend']}")
                rsi = info.get("rsi")
                rsi_tag = ("Bullish >60" if rsi and rsi > 60 else ("Bearish <40" if rsi and rsi < 40 else "Sideways 40-60"))
                c3.metric("RSI (14)", f"{rsi:.1f}" if rsi else "N/A", rsi_tag)
                
                adx_val = adx_info.get("adx") if adx_info else None
                adx_display = f"{adx_val:.1f}" if adx_val else "N/A"
                c4.metric("ADX", adx_display, adx_info.get("regime", "") if adx_info else "")
                
                c5.metric("Support", format_price(info.get("support"), is_nse))
                c6.metric("Resistance", format_price(info.get("resistance"), is_nse))
                
                if adx_info and adx_info.get("message"):
                    if adx_info["recommendation"] == "AVOID":
                        st.error(f"🚫 {adx_info['message']} — No trades in ranging market")
                    elif adx_info["recommendation"] == "REDUCE_SIZE":
                        st.warning(f"⚠️ {adx_info['message']} — Consider reducing position size")
                    else:
                        st.info(f"📈 {adx_info['message']} — Good trending conditions")

                pn = info.get("pattern_name", "NONE")
                if pn and pn != "NONE":
                    trend_dir = info.get("trend", "")
                    if trend_dir == "BULLISH":
                        pdp = f"📉 BEARISH RETRACEMENT PATTERN: {pn}"
                    elif trend_dir == "BEARISH":
                        pdp = f"📈 BULLISH RETRACEMENT PATTERN: {pn}"
                    else:
                        pdp = f"Pattern: {pn}"
                    st.info(f"**{pdp}** | {info.get('signal_display','')}")

                _is_comm_chart = ticker in MCX_COMMODITIES
                active_wait_sigs = {"WAIT_LONG_REENTRY", "WAIT_SHORT_REENTRY",
                                     "BEARISH_PATTERN_4H", "BULLISH_PATTERN_4H",
                                     "APPROACHING_REENTRY"}
                if info["signal"] in active_wait_sigs and not info.get("reentry_done"):
                    fast_c = check_reentry_live_fast(
                        ticker, info.get("pattern_high"), info.get("pattern_low"), info["trend"]
                    )
                    if fast_c["signal"] == "RE-ENTRY_SIGNAL":
                        raw_ep_c = fast_c["entry_price"]
                        ep_inr_c = convert_usd_to_inr_for_mcx(float(raw_ep_c), ticker) if (_is_comm_chart and raw_ep_c) else raw_ep_c
                        ph_inr_c = info.get("pattern_high_display") or info.get("pattern_high")
                        pl_inr_c = info.get("pattern_low_display") or info.get("pattern_low")
                        sl_inr_c = pl_inr_c if info["trend"] == "BULLISH" else ph_inr_c
                        t1_c, _ = compute_sr_targets(
                            ticker, info["trend"], ep_inr_c,
                            info.get("pattern_high", ep_inr_c),
                            info.get("pattern_low", ep_inr_c),
                        )
                        sig_lbl = "LONG_REENTRY_SIGNAL" if info["trend"] == "BULLISH" else "SHORT_REENTRY_SIGNAL"
                        info = {**info,
                                "signal": sig_lbl,
                                "entry_price": ep_inr_c,
                                "stop_loss": sl_inr_c,
                                "target_1r": t1_c,
                                "breakout_time": fast_c.get("breakout_time"),
                                }

                if info["signal"] in {"BULLISH_RETRACEMENT_EXIT", "BEARISH_RETRACEMENT_EXIT"}:
                    st.warning(f"🚨 {info.get('signal_display','RETRACEMENT EXIT SIGNAL')}")

                chart_entry = None
                chart_sl = None
                if info.get("reentry_done") and info.get("reentry_info"):
                    ri_c = info["reentry_info"]
                    chart_entry = ri_c.get("entry_price")
                    sl_raw_c = ri_c.get("pattern_low") if ri_c.get("trend") == "BULLISH" else ri_c.get("pattern_high")
                    chart_sl = float(sl_raw_c) if sl_raw_c else None
                elif info.get("signal") in {"LONG_REENTRY_SIGNAL", "SHORT_REENTRY_SIGNAL"}:
                    chart_entry = info.get("entry_price")
                    chart_sl = info.get("stop_loss")

                _ph_disp = info.get("pattern_high_display") or info.get("pattern_high")
                _pl_disp = info.get("pattern_low_display") or info.get("pattern_low")

                render_tradingview_chart(ticker, timeframe=tf,
                                         entry_price=chart_entry, stop_loss=chart_sl,
                                         pattern_high=_ph_disp, pattern_low=_pl_disp)

                if info["signal"] in {"LONG_REENTRY_SIGNAL", "SHORT_REENTRY_SIGNAL"} \
                        and not info.get("reentry_done") \
                        and not is_signal_dismissed(ticker, info.get("breakout_time")):
                    show_reentry_buttons(ticker, name, info["entry_price"],
                                         _ph_disp, _pl_disp,
                                         info["trend"], info.get("pattern_name"),
                                         info.get("target_1r"), info.get("target_2r"),
                                         breakout_time=info.get("breakout_time"),
                                         adx_info=info.get("adx_info"))
                elif info.get("reentry_done"):
                    show_close_position_button(ticker, name, info.get("trend"))
            else:
                st.info("No data available.")
        else:
            st.info("Add symbols to watchlist first.")

    # ── PORTFOLIO ──────────────────────────────────────────────
    elif page == "💼 Portfolio":
        st.markdown('<p class="main-header">💼 Portfolio Tracker</p>', unsafe_allow_html=True)
        col1, col2 = st.columns([2, 1])
        with col2:
            st.markdown("#### ➕ Add Position")

            nse_map = {t.replace(".NS", "").upper(): t for t in nse_watchlist}
            mcx_map = {}
            for key, meta in MCX_COMMODITIES.items():
                mcx_map[meta["name"].upper()] = key
                mcx_map[key.upper()] = key

            def resolve_ticker(raw: str):
                s = raw.strip().upper()
                if not s:
                    return None, "Enter a symbol"
                if s in nse_map:
                    return nse_map[s], s
                if s in mcx_map:
                    key = mcx_map[s]
                    return key, MCX_COMMODITIES[key]["name"]
                if s.endswith(".NS"):
                    return s, s.replace(".NS", "")
                if "=F" in s or s.startswith("GC") or s.startswith("CL") or s.startswith("SI"):
                    return s, s
                return s + ".NS", s

            sym_input = st.text_input(
                "Symbol",
                placeholder="RELIANCE · TCS · GOLD · CRUDE OIL · SBIN…",
                key="port_sym_search",
            )

            if sym_input.strip():
                resolved, disp_name = resolve_ticker(sym_input)
                if resolved:
                    is_mcx_preview = resolved in MCX_COMMODITIES
                    badge_color = "#00C853" if not is_mcx_preview else "#FFD700"
                    badge_label = "NSE" if not is_mcx_preview else "MCX"
                    st.markdown(
                        f"<div style='background:rgba(0,200,83,.1);border:1px solid {badge_color};"
                        f"border-radius:4px;padding:5px 10px;font-size:.8rem;margin-bottom:6px;'>"
                        f"<span style='color:{badge_color};font-weight:700;'>[{badge_label}]</span> "
                        f"<span style='color:#E8F4FF;'>{disp_name}</span> "
                        f"<span style='color:#88B4CC;font-size:.72rem;'>({resolved})</span></div>",
                        unsafe_allow_html=True,
                    )
            else:
                resolved, disp_name = "", ""

            qty = st.number_input("Quantity", min_value=1, max_value=100_000, value=10)
            ep = st.number_input("Entry Price (₹)", min_value=0.01, max_value=10_000_000.0, value=100.0)

            if st.button("➕ Add Position", use_container_width=True, key="port_add_btn"):
                if not sym_input.strip():
                    st.error("Enter a symbol first.")
                elif resolved:
                    pid = f"{resolved}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    save_portfolio_position(pid, resolved, qty, ep)
                    st.success(f"✅ Added {disp_name} ({resolved})")
                    st.rerun()
                else:
                    st.error("Could not resolve symbol. Try: RELIANCE, TCS, GOLD, CRUDEOIL")

            all_wl = nse_watchlist + [k for k in mcx_watchlist if k in MCX_COMMODITIES]
            if all_wl:
                with st.expander("📋 Quick-add from watchlist"):
                    for t in all_wl:
                        dn = get_symbol_display_name(t)
                        if st.button(f"+ {dn}", key=f"qadd_{t}", use_container_width=True):
                            st.session_state["port_sym_search"] = dn
                            st.rerun()

        with col1:
            portfolio = load_portfolio()
            if portfolio:
                rows, total_pnl, total_inv = [], 0, 0
                for pid, pos in portfolio.items():
                    i = fetch_stock_data(pos["ticker"])
                    sig15p = _get_watchlist_signal_data(pos["ticker"])
                    is_n = ".NS" in pos["ticker"]
                    if i and i["current_price"]:
                        curr = i["current_price"]
                        ep = pos["entry_price"]
                        q = pos["quantity"]
                        pnl = (curr - ep) * q
                        inv = ep * q
                        pct = pnl / inv * 100 if inv else 0
                        total_pnl += pnl
                        total_inv += inv
                        _sig15p_label = (
                            "🟢 LONG"  if (sig15p and sig15p["signal"] == "LONG")  else
                            "🔴 SHORT" if (sig15p and sig15p["signal"] == "SHORT") else
                            ("〰️ " + (sig15p["daily_trend"] if sig15p else "—"))
                        )
                        rows.append({
                            "Symbol": get_symbol_display_name(pos["ticker"]),
                            "Qty": int(q),
                            "Entry": format_price(ep, is_n),
                            "Current": format_price(curr, is_n),
                            "Invested": format_price(inv, is_n),
                            "P&L": format_price(pnl, is_n),
                            "P&L %": f"{pct:+.2f}%",
                            "Trend": f"{i['trend_icon']} {i['trend']}",
                            "15M Signal": _sig15p_label,
                            "15M RSI":    f"{sig15p['rsi_15m']}" if (sig15p and sig15p["rsi_15m"] is not None) else "—",
                            "15M SL":     format_price(sig15p["sl"],     is_n) if (sig15p and sig15p["sl"])     else "—",
                            "15M Target": format_price(sig15p["target"], is_n) if (sig15p and sig15p["target"]) else "—",
                        })

                if rows:
                    df_p = pd.DataFrame(rows)
                    st.dataframe(df_p, use_container_width=True, hide_index=True)

                    m1, m2, m3 = st.columns(3)
                    m1.metric("💰 Total P&L", format_price(total_pnl, True),
                              delta=f"{total_pnl/total_inv*100:+.2f}%" if total_inv else None)
                    m2.metric("💵 Total Invested", format_price(total_inv, True))
                    m3.metric("📊 Positions", len(portfolio))

                    try:
                        import plotly.express as px
                        fig = px.bar(df_p, x="Symbol", y="P&L %", color="P&L %",
                                     color_continuous_scale=["#FF1744", "#FFD700", "#00C853"],
                                     title="Portfolio P&L %")
                        fig.update_layout(template="plotly_dark",
                                          paper_bgcolor="rgba(0,0,0,0)",
                                          plot_bgcolor="rgba(18,18,28,1)", height=280)
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        logging.warning(f"Portfolio chart: {e}")

                    st.markdown("**Remove Positions:**")
                    for pid in list(portfolio.keys()):
                        pos = portfolio[pid]
                        if st.button(f"❌ {get_symbol_display_name(pos['ticker'])}", key=f"del_pos_{pid}"):
                            delete_portfolio_position(pid)
                            st.rerun()
            else:
                st.info("No positions yet. Use the form →")

    # ── TRADE JOURNAL ──────────────────────────────────────────
    elif page == "📒 Trade Journal":
        st.markdown('<p class="main-header">📒 Trade Journal</p>', unsafe_allow_html=True)
        df = get_trade_history_df()

        if not df.empty:
            total = len(df)
            wins = (df["pnl"] > 0).sum()
            losses = (df["pnl"] < 0).sum()
            wr = wins / total * 100
            tpnl = df["pnl"].sum()
            avg_w = df[df["pnl"] > 0]["pnl"].mean() if wins > 0 else 0
            avg_l = df[df["pnl"] < 0]["pnl"].mean() if losses > 0 else 0
            rr = abs(avg_w / avg_l) if avg_l else 0

            c1, c2, c3, c4, c5, c6 = st.columns(6)
            c1.metric("Trades", total)
            c2.metric("Win Rate", f"{wr:.1f}%")
            c3.metric("Winners", wins)
            c4.metric("Losers", losses)
            c5.metric("Total P&L", format_price(tpnl, True))
            c6.metric("Avg R:R", f"{rr:.2f}")
            st.markdown("---")

            if "adx_at_entry" in df.columns:
                st.markdown("#### 📊 ADX Performance Analysis")
                adx_perf = df[df["adx_at_entry"].notna()].copy()
                if not adx_perf.empty:
                    adx_perf["adx_group"] = pd.cut(adx_perf["adx_at_entry"],
                                                     bins=[0, 20, 25, 40, 100],
                                                     labels=["<20 (Ranging)", "20-25 (Weak)", "25-40 (Trending)", ">40 (Strong)"])
                    adx_summary = adx_perf.groupby("adx_group").agg({
                        "pnl": ["count", "sum", "mean"],
                        "id": lambda x: (x > 0).sum()
                    }).round(2)
                    adx_summary.columns = ["Trades", "Total P&L", "Avg P&L", "Wins"]
                    adx_summary["Win Rate %"] = (adx_summary["Wins"] / adx_summary["Trades"] * 100).round(1)
                    st.dataframe(adx_summary[["Trades", "Win Rate %", "Total P&L", "Avg P&L"]], use_container_width=True)

            try:
                import plotly.graph_objects as go
                sdf = df.sort_values("id").copy()
                sdf["cum_pnl"] = sdf["pnl"].cumsum()
                color = "#00C853" if tpnl >= 0 else "#FF1744"
                fig = go.Figure(go.Scatter(
                    x=list(range(1, len(sdf)+1)), y=sdf["cum_pnl"],
                    mode="lines+markers", name="Equity",
                    line=dict(color=color, width=2),
                    fill="tozeroy",
                    fillcolor=f"rgba({'0,200,83' if tpnl>=0 else '255,23,68'},.12)",
                ))
                fig.update_layout(template="plotly_dark", title="Equity Curve",
                                  xaxis_title="Trade #", yaxis_title="Cumulative P&L (₹)",
                                  paper_bgcolor="rgba(0,0,0,0)",
                                  plot_bgcolor="rgba(18,18,28,1)", height=300)
                st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                logging.warning(f"Equity curve: {e}")

            disp = df[["symbol_name", "direction", "entry_price", "exit_price", "quantity",
                        "pnl", "entry_time", "exit_time", "pattern_name"]].copy()
            if "adx_at_entry" in df.columns:
                disp["ADX"] = df["adx_at_entry"]
            disp.columns = ["Symbol", "Dir", "Entry ₹", "Exit ₹", "Qty", "P&L ₹", "Entry Time", "Exit Time", "Pattern"] + (["ADX"] if "adx_at_entry" in df.columns else [])
            disp["P&L ₹"] = disp["P&L ₹"].round(2)
            st.dataframe(disp, use_container_width=True, hide_index=True)

            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("⬇️ Export Trade History (CSV)", data=csv,
                               file_name=f"trades_{datetime.now().strftime('%Y%m%d')}.csv",
                               mime="text/csv")
        else:
            st.info("No trades logged yet. Trades are automatically recorded when you close a position.")

        with st.expander("➕ Add Trade Manually"):
            with st.form("manual_trade"):
                ca, cb, cc = st.columns(3)
                with ca:
                    sym_m = st.text_input("Symbol")
                    dir_m = st.selectbox("Direction", ["LONG", "SHORT"])
                    qty_m = st.number_input("Quantity", 1, 100_000, 1)
                with cb:
                    ep_m = st.number_input("Entry Price", 0.01, 10_000_000.0, 100.0)
                    xp_m = st.number_input("Exit Price", 0.01, 10_000_000.0, 100.0)
                    pat_m = st.text_input("Pattern", placeholder="BULLISH PULLBACK")
                    adx_m = st.number_input("ADX at Entry", 0.0, 100.0, 30.0, 1.0)
                with cc:
                    et_m = st.text_input("Entry Time", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    xt_m = st.text_input("Exit Time", value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    regime_m = st.selectbox("Market Regime", ["TRENDING", "WEAK_TREND", "RANGING"])
                    nt_m = st.text_area("Notes", height=68)
                if st.form_submit_button("Log Trade"):
                    if sym_m:
                        log_trade(sym_m, sym_m, dir_m, ep_m, xp_m, qty_m, et_m, xt_m,
                                  pat_m, "BULLISH" if dir_m == "LONG" else "BEARISH", 0, 0, nt_m, adx_m, regime_m)
                        st.success("Trade logged!")
                        st.rerun()

    # ── FUTURES SCANNER ───────────────────────────────────────
    elif page == "⚡ Futures Scanner":
        render_futures_scanner_page(nse_watchlist)

    # ── SETTINGS ───────────────────────────────────────────────
    elif page == "⚙️ Settings":
        st.markdown('<p class="main-header">⚙️ Settings</p>', unsafe_allow_html=True)
        tab1, tab2, tab3 = st.tabs(["🗑️ Data Management", "📱 Telegram Alerts", "📊 ADX Filter"])

        with tab1:
            c1, c2 = st.columns(2)
            with c1:
                if st.button("🗑️ Clear NSE Watchlist", use_container_width=True):
                    save_watchlist_nse([])
                    st.success("NSE watchlist cleared!")
                    st.rerun()
                if st.button("🗑️ Clear MCX Watchlist", use_container_width=True):
                    save_watchlist_mcx([])
                    st.success("MCX watchlist cleared!")
                    st.rerun()
                if st.button("🗑️ Clear Alert History", use_container_width=True):
                    try:
                        conn = get_db()
                        conn.execute("DELETE FROM alert_history")
                        conn.execute("DELETE FROM signals_shown")
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        st.error(str(e))
                    st.session_state["alert_prev_states"] = {}
                    st.success("Alert history cleared!")
                    st.rerun()
            with c2:
                if st.button("🗑️ Clear Portfolio", use_container_width=True):
                    clear_portfolio()
                    st.success("Portfolio cleared!")
                    st.rerun()
                if st.button("🔄 Clear Re-entry States", use_container_width=True):
                    try:
                        conn = get_db()
                        conn.execute("DELETE FROM reentry_state")
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        st.error(str(e))
                    st.success("Re-entry states cleared!")
                    st.rerun()
                if st.button("🗑️ Clear Trade History", use_container_width=True):
                    try:
                        conn = get_db()
                        conn.execute("DELETE FROM trade_history")
                        conn.commit()
                        conn.close()
                    except Exception as e:
                        st.error(str(e))
                    st.success("Trade history cleared!")
                    st.rerun()
                if st.button("🔄 Reset All to Defaults", use_container_width=True):
                    save_watchlist_nse(DEFAULT_NSE_WATCHLIST.copy())
                    save_watchlist_mcx(["GC=F", "SI=F", "CL=F", "NG=F", "HG=F", "ZNC=F", "NG=F_MINI", "ZNC=F_MINI", "ALI=F_MINI"])
                    clear_portfolio()
                    _save_adx_threshold(DEFAULT_ADX_THRESHOLD)
                    _save_adx_weak(DEFAULT_ADX_WEAK)
                    try:
                        conn = get_db()
                        conn.execute("DELETE FROM reentry_state")
                        conn.execute("DELETE FROM signals_shown")
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass
                    st.session_state["alert_prev_states"] = {}
                    st.success("All reset to defaults!")
                    st.rerun()

        with tab2:
            st.markdown("""
            ### 📱 Telegram Bot Alerts
            Receive instant re-entry signal notifications on Telegram.

            **Setup steps:**
            1. Message **@BotFather** → `/newbot` → copy your token
            2. Start your bot, then get your Chat ID from **@userinfobot**
            3. Enter below and click **Test**
            """)
            tok = _get_setting("telegram_token")
            cid = _get_setting("telegram_chat_id")
            new_tok = st.text_input("Bot Token", value=tok, type="password", placeholder="123456:ABC-DEF...")
            new_cid = st.text_input("Chat ID", value=cid, placeholder="-100123456789")

            ca, cb = st.columns(2)
            with ca:
                if st.button("💾 Save Config", use_container_width=True):
                    if not new_tok.strip():
                        st.error("Bot token cannot be empty!")
                    elif not new_cid.strip():
                        st.error("Chat ID cannot be empty!")
                    else:
                        _save_setting("telegram_token", new_tok.strip())
                        _save_setting("telegram_chat_id", new_cid.strip())
                        st.success("✅ Config saved!")
            with cb:
                if st.button("🔔 Send Test Message", use_container_width=True):
                    if not new_tok.strip() or not new_cid.strip():
                        st.error("Fill in both Bot Token and Chat ID first!")
                    else:
                        _save_setting("telegram_token", new_tok.strip())
                        _save_setting("telegram_chat_id", new_cid.strip())
                        test_msg = (
                            f"Om NSE Tracker v20.8 - Connection Test\n"
                            f"Time: {datetime.now(IST).strftime('%d %b %Y %H:%M IST')}\n"
                            f"ADX Filter is ACTIVE — only trending markets generate alerts!"
                        )
                        ok, err = send_telegram_alert(test_msg)
                        if ok:
                            st.success("✅ Test message sent! Check your Telegram.")
                        else:
                            st.error(f"❌ Failed: {err}")

        with tab3:
            st.markdown("""
            ### 📊 ADX (Average Directional Index) Filter Configuration
            
            The ADX filter helps you avoid trading in ranging markets.
            
            **Recommended Settings:**
            - **Trade Threshold (ADX > X):** 25
            - **Weak Threshold (ADX < Y):** 20
            """)
            
            current_threshold = _get_adx_threshold()
            current_weak = _get_adx_weak_threshold()
            
            col_a, col_b = st.columns(2)
            with col_a:
                new_threshold = st.number_input(
                    "Trade Threshold (ADX > X)",
                    min_value=10,
                    max_value=50,
                    value=current_threshold,
                    step=1,
                )
            
            with col_b:
                new_weak = st.number_input(
                    "Skip Threshold (ADX < Y)",
                    min_value=5,
                    max_value=30,
                    value=current_weak,
                    step=1,
                )
            
            if st.button("💾 Save ADX Settings", use_container_width=True):
                _save_adx_threshold(new_threshold)
                _save_adx_weak(new_weak)
                st.success(f"✅ ADX settings saved! Trade when ADX > {new_threshold}, skip when ADX < {new_weak}")
                st.cache_data.clear()
                st.rerun()

    # ── OPTION SCANNER ─────────────────────────────────────────
    elif page == "🎯 Option Scanner":
        render_option_scanner_page()


if __name__ == "__main__":
    main()