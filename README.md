# KAMALA MARKET HUB

One Python file. No server, no build step. Pulls CME Group futures, spot FX,
macro, and crypto data from the command line, with every result printable,
watchable (auto-refresh), or exportable to CSV/JSON.

## Setup

```bash
pip install -r requirements.txt
```

Optional — add richer/cross-check sources by setting API keys as environment
variables, or by creating a `config.json` next to the script (never commit
this file):

```json
{
  "FRED_API_KEY": "...",
  "ALPHAVANTAGE_API_KEY": "...",
  "TWELVEDATA_API_KEY": "...",
  "FMP_API_KEY": "..."
}
```

Nothing above is required to get started — `futures`, `fx`, `quote`, `crypto`,
and `history` all work with zero keys via Yahoo Finance / Frankfurter (ECB) /
Binance's public endpoints.

## What "CME data" means here

CME Group's own real-time/DataMine feeds are paid and licensed — this tool
doesn't fake access to those. Instead it pulls CME/CBOT/NYMEX/COMEX futures
(FX futures, ES/NQ, Treasury futures, energy, metals, ag, CME crypto futures)
via Yahoo Finance, which redistributes free delayed quotes for those
contracts. That's honest, free, and needs no key. If you later get a licensed
feed or broker API (e.g. Interactive Brokers), drop a new `fetch_*` function
in next to the existing ones — the CLI, table printer, and exporter don't
need to change.

## Commands

```bash
# see every symbol the script knows about, plus which API keys are detected
python kamala_market_hub.py list

# CME Group futures — by catalog code or raw ticker, comma-separated
python kamala_market_hub.py futures --symbols ES,NQ,6E,GC,CL
python kamala_market_hub.py futures                       # all ~30 catalog symbols
python kamala_market_hub.py futures --symbols ES --watch 15   # refresh every 15s

# spot FX — majors by default, or your own pairs
python kamala_market_hub.py fx --pairs EURUSD,GBPUSD,USDJPY
python kamala_market_hub.py fx --cross-check              # add ECB + Alpha Vantage

# FRED macro series (needs FRED_API_KEY)
python kamala_market_hub.py macro --series DGS10,DFF,UNRATE

# crypto — Binance spot, optionally alongside CME BTC/ETH futures
python kamala_market_hub.py crypto --symbols BTCUSDT,ETHUSDT
python kamala_market_hub.py crypto --with-cme-futures

# literally any Yahoo Finance ticker — stocks, ETFs, other futures, crypto pairs
python kamala_market_hub.py quote --tickers AAPL,SPY,6L=F,BTC-USD

# full OHLCV history for one ticker
python kamala_market_hub.py history GC=F --period 6mo --interval 1d --export csv

# one-shot sweep across every wired source, in parallel
python kamala_market_hub.py all --export json --out snapshot.json
```

Any command that prints a table also accepts `--export csv` or `--export json`
plus `--out path.ext` to save results instead of (or in addition to) printing.

## Extending it

Every source is a small, isolated `fetch_*` function that returns a list of
plain dicts and never raises — bad symbols or dead APIs show up as an
`error` field in the row instead of crashing the batch. To wire in a new
source (say, Interactive Brokers via `ib_insync`, or a broker's REST API):

1. Write `fetch_yourthing(symbols) -> list[dict]` following the same pattern.
2. Add a subcommand in `build_parser()` that calls it.
3. Optionally fold it into `cmd_all()`'s thread pool.

The catalog (`FUTURES_CATALOG`, `SPOT_FX`, `FRED_DEFAULT_SERIES`) is just
dictionaries at the top of the file — add or edit entries freely.
