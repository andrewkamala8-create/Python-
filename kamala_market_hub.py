#!/usr/bin/env python3
"""
KAMALA MARKET HUB
=================
A single-file, dependency-light Python tool for pulling CME Group futures,
spot FX, macro, and crypto data from multiple free/keyed sources — all from
the command line.

Sources wired in:
  - Yahoo Finance (yfinance)   -> CME/CBOT/NYMEX/COMEX futures + spot FX, no key needed
  - Frankfurter (ECB)          -> official daily FX reference rates, no key needed
  - Binance public API         -> crypto spot prices, no key needed
  - FRED                       -> macro series, needs FRED_API_KEY
  - Alpha Vantage              -> FX / commodities cross-check, needs ALPHAVANTAGE_API_KEY
  - Twelve Data                -> price cross-check, needs TWELVEDATA_API_KEY
  - FMP                        -> quote cross-check, needs FMP_API_KEY

NOTE ON "CME DATA": CME Group's own real-time / DataMine feeds are paid and
licensed. This tool pulls CME/CBOT/NYMEX/COMEX futures via Yahoo Finance,
which redistributes free delayed quotes for those contracts. If you later
get access to a licensed feed (CME DataMine, a broker API, etc.) you can
drop a new fetch_* function in next to the others below — the CLI and
output plumbing don't need to change.

Quick start:
    pip install -r requirements.txt
    python kamala_market_hub.py list
    python kamala_market_hub.py futures --symbols ES,6E,GC,CL
    python kamala_market_hub.py fx --pairs EURUSD,GBPUSD,USDJPY
    python kamala_market_hub.py macro --series DGS10,DFF
    python kamala_market_hub.py crypto --symbols BTCUSDT,ETHUSDT
    python kamala_market_hub.py quote --tickers AAPL,SPY,6L=F      # anything at all
    python kamala_market_hub.py all --export json --out snapshot.json
    python kamala_market_hub.py futures --symbols ES,NQ --watch 30 # loop every 30s

Optional API keys (set as environment variables, or put in a local
config.json — see README.md):
    FRED_API_KEY, ALPHAVANTAGE_API_KEY, TWELVEDATA_API_KEY, FMP_API_KEY
"""

import argparse
import concurrent.futures
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

try:
    import yfinance as yf
    YF_AVAILABLE = True
except ImportError:
    YF_AVAILABLE = False

HTTP_TIMEOUT = 10

# --------------------------------------------------------------------------
# Optional local config.json fallback for API keys (never commit this file)
# --------------------------------------------------------------------------
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
if os.path.exists(_CONFIG_PATH):
    try:
        with open(_CONFIG_PATH) as f:
            for k, v in json.load(f).items():
                os.environ.setdefault(k, str(v))
    except Exception:
        pass

FRED_API_KEY = os.environ.get("FRED_API_KEY", "")
ALPHAVANTAGE_API_KEY = os.environ.get("ALPHAVANTAGE_API_KEY", "")
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "")
FMP_API_KEY = os.environ.get("FMP_API_KEY", "")

# --------------------------------------------------------------------------
# Catalog: CME Group family futures (CME / CBOT / NYMEX / COMEX), reachable
# via Yahoo Finance continuous-contract tickers. RTY is ICE, not CME Group —
# kept here because people expect it next to ES/NQ, but labeled honestly.
# --------------------------------------------------------------------------
FUTURES_CATALOG = {
    # FX futures — CME
    "6E": {"yf": "6E=F", "name": "EUR/USD Futures", "exch": "CME", "cat": "fx"},
    "6B": {"yf": "6B=F", "name": "GBP/USD Futures", "exch": "CME", "cat": "fx"},
    "6J": {"yf": "6J=F", "name": "JPY/USD Futures", "exch": "CME", "cat": "fx"},
    "6A": {"yf": "6A=F", "name": "AUD/USD Futures", "exch": "CME", "cat": "fx"},
    "6C": {"yf": "6C=F", "name": "CAD/USD Futures", "exch": "CME", "cat": "fx"},
    "6S": {"yf": "6S=F", "name": "CHF/USD Futures", "exch": "CME", "cat": "fx"},
    "6N": {"yf": "6N=F", "name": "NZD/USD Futures", "exch": "CME", "cat": "fx"},
    "6M": {"yf": "6M=F", "name": "MXN/USD Futures", "exch": "CME", "cat": "fx"},
    # Equity index futures
    "ES": {"yf": "ES=F", "name": "E-mini S&P 500", "exch": "CME", "cat": "equity"},
    "NQ": {"yf": "NQ=F", "name": "E-mini Nasdaq 100", "exch": "CME", "cat": "equity"},
    "YM": {"yf": "YM=F", "name": "E-mini Dow", "exch": "CBOT", "cat": "equity"},
    "RTY": {"yf": "RTY=F", "name": "E-mini Russell 2000", "exch": "ICE (not CME Group)", "cat": "equity"},
    # Rates futures — CBOT
    "ZN": {"yf": "ZN=F", "name": "10-Year T-Note", "exch": "CBOT", "cat": "rates"},
    "ZB": {"yf": "ZB=F", "name": "30-Year T-Bond", "exch": "CBOT", "cat": "rates"},
    "ZF": {"yf": "ZF=F", "name": "5-Year T-Note", "exch": "CBOT", "cat": "rates"},
    "ZT": {"yf": "ZT=F", "name": "2-Year T-Note", "exch": "CBOT", "cat": "rates"},
    # Energy — NYMEX
    "CL": {"yf": "CL=F", "name": "WTI Crude Oil", "exch": "NYMEX", "cat": "energy"},
    "NG": {"yf": "NG=F", "name": "Natural Gas", "exch": "NYMEX", "cat": "energy"},
    "RB": {"yf": "RB=F", "name": "RBOB Gasoline", "exch": "NYMEX", "cat": "energy"},
    "HO": {"yf": "HO=F", "name": "Heating Oil", "exch": "NYMEX", "cat": "energy"},
    "BZ": {"yf": "BZ=F", "name": "Brent Crude (Last Day Financial)", "exch": "NYMEX", "cat": "energy"},
    # Metals — COMEX / NYMEX
    "GC": {"yf": "GC=F", "name": "Gold", "exch": "COMEX", "cat": "metals"},
    "SI": {"yf": "SI=F", "name": "Silver", "exch": "COMEX", "cat": "metals"},
    "HG": {"yf": "HG=F", "name": "Copper", "exch": "COMEX", "cat": "metals"},
    "PL": {"yf": "PL=F", "name": "Platinum", "exch": "NYMEX", "cat": "metals"},
    "PA": {"yf": "PA=F", "name": "Palladium", "exch": "NYMEX", "cat": "metals"},
    # Agriculture — CBOT
    "ZC": {"yf": "ZC=F", "name": "Corn", "exch": "CBOT", "cat": "ag"},
    "ZS": {"yf": "ZS=F", "name": "Soybeans", "exch": "CBOT", "cat": "ag"},
    "ZW": {"yf": "ZW=F", "name": "Wheat", "exch": "CBOT", "cat": "ag"},
    "ZM": {"yf": "ZM=F", "name": "Soybean Meal", "exch": "CBOT", "cat": "ag"},
    "ZL": {"yf": "ZL=F", "name": "Soybean Oil", "exch": "CBOT", "cat": "ag"},
    # Crypto futures — CME
    "BTC": {"yf": "BTC=F", "name": "Bitcoin Futures", "exch": "CME", "cat": "crypto"},
    "ETH": {"yf": "ETH=F", "name": "Ether Futures", "exch": "CME", "cat": "crypto"},
}

SPOT_FX = {
    "EURUSD": "EURUSD=X", "GBPUSD": "GBPUSD=X", "USDJPY": "USDJPY=X", "USDCHF": "USDCHF=X",
    "AUDUSD": "AUDUSD=X", "USDCAD": "USDCAD=X", "NZDUSD": "NZDUSD=X", "USDMXN": "USDMXN=X",
    "EURGBP": "EURGBP=X", "EURJPY": "EURJPY=X", "GBPJPY": "GBPJPY=X", "USDZAR": "USDZAR=X",
}

FRED_DEFAULT_SERIES = {
    "DGS10": "10-Year Treasury Yield", "DGS2": "2-Year Treasury Yield",
    "DFF": "Effective Fed Funds Rate", "DTWEXBGS": "Trade-Weighted Dollar Index (Broad)",
    "CPIAUCSL": "CPI, All Urban Consumers", "UNRATE": "Unemployment Rate",
    "T10Y2Y": "10Y-2Y Treasury Spread",
}


# --------------------------------------------------------------------------
# Fetchers — each returns a plain dict (or list of dicts). Every one is
# wrapped so a bad symbol or dead API never crashes a batch run.
# --------------------------------------------------------------------------
def _err(source, ident, e):
    return {"source": source, "symbol": ident, "error": str(e)}


def fetch_yf_quotes(tickers, period="5d", interval="1d"):
    """Bulk quote fetch via yf.download — one HTTP round-trip for many tickers."""
    if not YF_AVAILABLE:
        return [_err("yfinance", t, "yfinance not installed — pip install yfinance") for t in tickers]
    if not tickers:
        return []
    out = []
    try:
        data = yf.download(tickers, period=period, interval=interval,
                            group_by="ticker", threads=True, progress=False, auto_adjust=False)
    except Exception as e:
        return [_err("yfinance", t, e) for t in tickers]

    single = len(tickers) == 1
    for t in tickers:
        try:
            df = data if single else data[t]
            df = df.dropna(how="all")
            if df.empty:
                out.append(_err("yfinance", t, "no data returned (bad symbol or market closed with no history)"))
                continue
            last = df.iloc[-1]
            prev = df.iloc[-2] if len(df) > 1 else last
            price = float(last["Close"])
            prev_close = float(prev["Close"])
            change = price - prev_close
            pct = (change / prev_close * 100) if prev_close else 0.0
            out.append({
                "source": "yfinance", "symbol": t,
                "price": round(price, 5), "prev_close": round(prev_close, 5),
                "change": round(change, 5), "change_pct": round(pct, 3),
                "high": round(float(last["High"]), 5), "low": round(float(last["Low"]), 5),
                "volume": int(last["Volume"]) if "Volume" in last and last["Volume"] == last["Volume"] else None,
                "as_of": str(df.index[-1]),
            })
        except Exception as e:
            out.append(_err("yfinance", t, e))
    return out


def fetch_yf_history(ticker, period="1mo", interval="1d"):
    """Full OHLCV history for one ticker, as a list of row dicts (for --export)."""
    if not YF_AVAILABLE:
        return [_err("yfinance", ticker, "yfinance not installed")]
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=False)
        df = df.dropna(how="all")
        rows = []
        for idx, row in df.iterrows():
            rows.append({
                "date": str(idx), "open": float(row["Open"]), "high": float(row["High"]),
                "low": float(row["Low"]), "close": float(row["Close"]),
                "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else None,
            })
        return rows
    except Exception as e:
        return [_err("yfinance", ticker, e)]


def fetch_frankfurter(pairs):
    """ECB daily reference FX rates — free, no key. pairs like 'EURUSD'."""
    out = []
    for p in pairs:
        base, quote = p[:3].upper(), p[3:6].upper()
        try:
            r = requests.get("https://api.frankfurter.app/latest",
                              params={"from": base, "to": quote}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            j = r.json()
            out.append({"source": "frankfurter (ECB)", "symbol": p,
                        "rate": j["rates"].get(quote), "date": j.get("date")})
        except Exception as e:
            out.append(_err("frankfurter", p, e))
    return out


def fetch_binance(symbols):
    """Public Binance spot ticker — no key needed. symbols like 'BTCUSDT'."""
    out = []
    for s in symbols:
        try:
            r = requests.get("https://api.binance.com/api/v3/ticker/24hr",
                              params={"symbol": s.upper()}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            j = r.json()
            out.append({
                "source": "binance", "symbol": s.upper(),
                "price": float(j["lastPrice"]), "change_pct": float(j["priceChangePercent"]),
                "high": float(j["highPrice"]), "low": float(j["lowPrice"]),
                "volume": float(j["volume"]),
            })
        except Exception as e:
            out.append(_err("binance", s, e))
    return out


def fetch_fred(series_ids):
    if not FRED_API_KEY:
        return [_err("fred", s, "FRED_API_KEY not set") for s in series_ids]
    out = []
    for sid in series_ids:
        try:
            r = requests.get("https://api.stlouisfed.org/fred/series/observations",
                              params={"series_id": sid, "api_key": FRED_API_KEY, "file_type": "json",
                                       "sort_order": "desc", "limit": 1}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            obs = r.json().get("observations", [])
            if not obs:
                out.append(_err("fred", sid, "no observations returned"))
                continue
            o = obs[0]
            out.append({"source": "fred", "symbol": sid,
                        "name": FRED_DEFAULT_SERIES.get(sid, sid),
                        "value": o["value"], "date": o["date"]})
        except Exception as e:
            out.append(_err("fred", sid, e))
    return out


def fetch_alphavantage_fx(pairs):
    if not ALPHAVANTAGE_API_KEY:
        return [_err("alphavantage", p, "ALPHAVANTAGE_API_KEY not set") for p in pairs]
    out = []
    for p in pairs:
        base, quote = p[:3].upper(), p[3:6].upper()
        try:
            r = requests.get("https://www.alphavantage.co/query",
                              params={"function": "CURRENCY_EXCHANGE_RATE", "from_currency": base,
                                       "to_currency": quote, "apikey": ALPHAVANTAGE_API_KEY},
                              timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            j = r.json().get("Realtime Currency Exchange Rate", {})
            if not j:
                out.append(_err("alphavantage", p, r.json().get("Note") or r.json().get("Information") or "empty response"))
                continue
            out.append({"source": "alphavantage", "symbol": p,
                        "rate": j.get("5. Exchange Rate"), "as_of": j.get("6. Last Refreshed")})
        except Exception as e:
            out.append(_err("alphavantage", p, e))
    return out


def fetch_twelvedata(symbols):
    if not TWELVEDATA_API_KEY:
        return [_err("twelvedata", s, "TWELVEDATA_API_KEY not set") for s in symbols]
    out = []
    for s in symbols:
        try:
            r = requests.get("https://api.twelvedata.com/price",
                              params={"symbol": s, "apikey": TWELVEDATA_API_KEY}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            j = r.json()
            if "price" not in j:
                out.append(_err("twelvedata", s, j.get("message", "no price in response")))
                continue
            out.append({"source": "twelvedata", "symbol": s, "price": float(j["price"])})
        except Exception as e:
            out.append(_err("twelvedata", s, e))
    return out


def fetch_fmp_quote(symbols):
    if not FMP_API_KEY:
        return [_err("fmp", s, "FMP_API_KEY not set") for s in symbols]
    out = []
    for s in symbols:
        try:
            r = requests.get(f"https://financialmodelingprep.com/api/v3/quote/{s}",
                              params={"apikey": FMP_API_KEY}, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            j = r.json()
            if not j:
                out.append(_err("fmp", s, "empty response — check symbol"))
                continue
            q = j[0]
            out.append({"source": "fmp", "symbol": s, "price": q.get("price"),
                        "change_pct": q.get("changesPercentage"), "volume": q.get("volume")})
        except Exception as e:
            out.append(_err("fmp", s, e))
    return out


# --------------------------------------------------------------------------
# Output helpers
# --------------------------------------------------------------------------
def print_table(rows):
    if not rows:
        print("(no rows)")
        return
    cols = []
    for r in rows:
        for k in r:
            if k not in cols:
                cols.append(k)
    widths = {c: max(len(c), max((len(str(r.get(c, ""))) for r in rows), default=0)) for c in cols}
    header = "  ".join(c.ljust(widths[c]) for c in cols)
    print(header)
    print("-" * len(header))
    for r in rows:
        print("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))


def export_rows(rows, fmt, path):
    if fmt == "json":
        with open(path, "w") as f:
            json.dump(rows, f, indent=2, default=str)
    elif fmt == "csv":
        cols = []
        for r in rows:
            for k in r:
                if k not in cols:
                    cols.append(k)
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
    print(f"-> wrote {len(rows)} row(s) to {path}")


def resolve_futures_symbols(codes):
    """Map catalog codes (case-insensitive) to yfinance tickers; pass through
    anything not in the catalog as a raw ticker (so you can fetch literally
    any futures/stock/FX ticker Yahoo knows about)."""
    resolved = []
    for c in codes:
        c_up = c.upper()
        if c_up in FUTURES_CATALOG:
            resolved.append(FUTURES_CATALOG[c_up]["yf"])
        else:
            resolved.append(c)
    return resolved


def split_csv(s):
    return [x.strip() for x in s.split(",") if x.strip()] if s else []


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------
def cmd_list(args):
    print("== CME Group family futures (via Yahoo Finance) ==")
    rows = [{"code": k, "name": v["name"], "exchange": v["exch"], "category": v["cat"], "ticker": v["yf"]}
            for k, v in FUTURES_CATALOG.items()]
    print_table(rows)
    print("\n== Spot FX majors ==")
    print_table([{"pair": k, "ticker": v} for k, v in SPOT_FX.items()])
    print("\n== Default FRED macro series (needs FRED_API_KEY) ==")
    print_table([{"series_id": k, "name": v} for k, v in FRED_DEFAULT_SERIES.items()])
    print("\nAPI keys detected:")
    for name, val in [("FRED_API_KEY", FRED_API_KEY), ("ALPHAVANTAGE_API_KEY", ALPHAVANTAGE_API_KEY),
                       ("TWELVEDATA_API_KEY", TWELVEDATA_API_KEY), ("FMP_API_KEY", FMP_API_KEY)]:
        print(f"  {name}: {'set (' + val[:4] + '...)' if val else 'not set'}")


def cmd_futures(args):
    codes = split_csv(args.symbols) or list(FUTURES_CATALOG.keys())
    tickers = resolve_futures_symbols(codes)
    def run():
        rows = fetch_yf_quotes(tickers, period=args.period, interval=args.interval)
        print_table(rows)
        if args.export:
            export_rows(rows, args.export, args.out or f"futures_{int(time.time())}.{args.export}")
        return rows
    _maybe_watch(run, args.watch)


def cmd_fx(args):
    pairs = split_csv(args.pairs) or list(SPOT_FX.keys())
    def run():
        yf_tickers = [SPOT_FX.get(p.upper(), p.upper() + "=X") for p in pairs]
        rows = fetch_yf_quotes(yf_tickers, period="5d", interval="1d")
        for r, p in zip(rows, pairs):
            r["pair"] = p
        if args.cross_check:
            rows += fetch_frankfurter(pairs)
            if ALPHAVANTAGE_API_KEY:
                rows += fetch_alphavantage_fx(pairs)
        print_table(rows)
        if args.export:
            export_rows(rows, args.export, args.out or f"fx_{int(time.time())}.{args.export}")
        return rows
    _maybe_watch(run, args.watch)


def cmd_macro(args):
    series = split_csv(args.series) or list(FRED_DEFAULT_SERIES.keys())
    rows = fetch_fred(series)
    print_table(rows)
    if args.export:
        export_rows(rows, args.export, args.out or f"macro_{int(time.time())}.{args.export}")


def cmd_crypto(args):
    symbols = split_csv(args.symbols) or ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    def run():
        rows = fetch_binance(symbols)
        if args.with_cme_futures:
            rows += fetch_yf_quotes(["BTC=F", "ETH=F"])
        print_table(rows)
        if args.export:
            export_rows(rows, args.export, args.out or f"crypto_{int(time.time())}.{args.export}")
        return rows
    _maybe_watch(run, args.watch)


def cmd_quote(args):
    """Freeform: fetch literally any Yahoo Finance ticker(s)."""
    tickers = split_csv(args.tickers)
    if not tickers:
        print("Give at least one ticker with --tickers, e.g. --tickers AAPL,SPY,6L=F,BTC-USD")
        return
    def run():
        rows = fetch_yf_quotes(tickers, period=args.period, interval=args.interval)
        print_table(rows)
        if args.export:
            export_rows(rows, args.export, args.out or f"quote_{int(time.time())}.{args.export}")
        return rows
    _maybe_watch(run, args.watch)


def cmd_history(args):
    rows = fetch_yf_history(args.ticker, period=args.period, interval=args.interval)
    print_table(rows[:20])
    if len(rows) > 20:
        print(f"... ({len(rows)} rows total, showing first 20 — use --export to get all)")
    if args.export:
        export_rows(rows, args.export, args.out or f"history_{args.ticker.replace('=', '_')}.{args.export}")


def cmd_all(args):
    """One-shot comprehensive sweep across every wired source."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {
            "cme_futures": ex.submit(fetch_yf_quotes, [v["yf"] for v in FUTURES_CATALOG.values()]),
            "spot_fx": ex.submit(fetch_yf_quotes, list(SPOT_FX.values())),
            "crypto": ex.submit(fetch_binance, ["BTCUSDT", "ETHUSDT", "SOLUSDT"]),
            "macro": ex.submit(fetch_fred, list(FRED_DEFAULT_SERIES.keys())),
        }
        results = {name: f.result() for name, f in futs.items()}

    all_rows = []
    for section, rows in results.items():
        print(f"\n== {section} ==")
        print_table(rows)
        for r in rows:
            r["_section"] = section
        all_rows += rows

    if args.export:
        export_rows(all_rows, args.export, args.out or f"kamala_snapshot_{int(time.time())}.{args.export}")


def _maybe_watch(run_fn, watch_seconds):
    if not watch_seconds:
        run_fn()
        return
    try:
        while True:
            print(f"\n[{datetime.now(timezone.utc).isoformat(timespec='seconds')}]")
            run_fn()
            time.sleep(watch_seconds)
    except KeyboardInterrupt:
        print("\nStopped.")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def build_parser():
    p = argparse.ArgumentParser(
        prog="kamala_market_hub.py",
        description="Fetch CME Group futures, FX, macro, and crypto data from one CLI.")
    sub = p.add_subparsers(dest="command", required=True)

    def add_common(sp):
        sp.add_argument("--export", choices=["csv", "json"], help="export results to a file")
        sp.add_argument("--out", help="output path (default: auto-named in current dir)")

    sp = sub.add_parser("list", help="show the full symbol catalog and detected API keys")
    sp.set_defaults(func=cmd_list)

    sp = sub.add_parser("futures", help="CME/CBOT/NYMEX/COMEX futures quotes")
    sp.add_argument("--symbols", help="comma-separated catalog codes or raw tickers (default: all)")
    sp.add_argument("--period", default="5d")
    sp.add_argument("--interval", default="1d")
    sp.add_argument("--watch", type=int, default=0, help="refresh every N seconds")
    add_common(sp)
    sp.set_defaults(func=cmd_futures)

    sp = sub.add_parser("fx", help="spot FX rates")
    sp.add_argument("--pairs", help="comma-separated pairs like EURUSD,GBPUSD (default: majors)")
    sp.add_argument("--cross-check", action="store_true", help="also pull ECB + Alpha Vantage rates")
    sp.add_argument("--watch", type=int, default=0)
    add_common(sp)
    sp.set_defaults(func=cmd_fx)

    sp = sub.add_parser("macro", help="FRED macro series (needs FRED_API_KEY)")
    sp.add_argument("--series", help="comma-separated FRED series IDs (default: a useful shortlist)")
    add_common(sp)
    sp.set_defaults(func=cmd_macro)

    sp = sub.add_parser("crypto", help="crypto spot (Binance) and/or CME crypto futures")
    sp.add_argument("--symbols", help="comma-separated Binance symbols like BTCUSDT,ETHUSDT")
    sp.add_argument("--with-cme-futures", action="store_true", help="also include CME BTC/ETH futures")
    sp.add_argument("--watch", type=int, default=0)
    add_common(sp)
    sp.set_defaults(func=cmd_crypto)

    sp = sub.add_parser("quote", help="fetch ANY Yahoo Finance ticker(s), no catalog needed")
    sp.add_argument("--tickers", required=True, help="comma-separated tickers, e.g. AAPL,SPY,6L=F,BTC-USD")
    sp.add_argument("--period", default="5d")
    sp.add_argument("--interval", default="1d")
    sp.add_argument("--watch", type=int, default=0)
    add_common(sp)
    sp.set_defaults(func=cmd_quote)

    sp = sub.add_parser("history", help="full OHLCV history for one ticker")
    sp.add_argument("ticker")
    sp.add_argument("--period", default="1mo")
    sp.add_argument("--interval", default="1d")
    add_common(sp)
    sp.set_defaults(func=cmd_history)

    sp = sub.add_parser("all", help="one-shot sweep: CME futures + spot FX + crypto + macro")
    add_common(sp)
    sp.set_defaults(func=cmd_all)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
