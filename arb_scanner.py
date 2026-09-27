#!/usr/bin/env python3
import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Tuple

import aiohttp

BINANCE_BASE = "https://api.binance.com"
BYBIT_BASE = "https://api.bybit.com"
OKX_BASE = "https://www.okx.com"
HTX_BASE = "https://api.huobi.pro"
KUCOIN_BASE = "https://api.kucoin.com"
MEXC_BASE = "https://api.mexc.com"

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
MIN_NET_SPREAD_PCT = float(os.getenv("MIN_NET_SPREAD_PCT", "0.20"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "1000000"))
TRADE_SIZE_USDT = float(os.getenv("TRADE_SIZE_USDT", "300"))
MAX_GROSS_SPREAD_PCT = float(os.getenv("MAX_GROSS_SPREAD_PCT", "8.0"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "90"))
MAX_ALERTS_PER_CYCLE = int(os.getenv("MAX_ALERTS_PER_CYCLE", "5"))

FEES_PCT = {
    "BINANCE": float(os.getenv("BINANCE_FEE_PCT", "0.10")),
    "BYBIT": float(os.getenv("BYBIT_FEE_PCT", "0.10")),
    "OKX": float(os.getenv("OKX_FEE_PCT", "0.10")),
    "HTX": float(os.getenv("HTX_FEE_PCT", "0.20")),
    "KUCOIN": float(os.getenv("KUCOIN_FEE_PCT", "0.10")),
    "MEXC": float(os.getenv("MEXC_FEE_PCT", "0.10")),
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
STABLE_BASES = {"USDT","USDC","FDUSD","TUSD","DAI","USDE","USDS","PYUSD"}

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("cex-arb")

@dataclass
class Quote:
    exchange: str
    symbol: str
    base: str
    ask: float
    bid: float
    quote_volume_24h: float

@dataclass
class Opportunity:
    base: str
    buy_exchange: str
    buy_symbol: str
    buy_ask: float
    sell_exchange: str
    sell_symbol: str
    sell_bid: float
    gross_pct: float
    fees_pct: float
    top_net_pct: float
    min_quote_volume_24h: float
    exec_buy_avg: float = 0.0
    exec_sell_avg: float = 0.0
    exec_net_pct: float = -999.0
    executable: bool = False

def fnum(x):
    try:
        return float(x)
    except Exception:
        return 0.0

async def get_json(session, url, params=None):
    timeout = aiohttp.ClientTimeout(total=12)
    async with session.get(url, params=params, timeout=timeout) as r:
        r.raise_for_status()
        return await r.json()

async def fetch_binance(session):
    info, ticks = await asyncio.gather(
        get_json(session, f"{BINANCE_BASE}/api/v3/exchangeInfo"),
        get_json(session, f"{BINANCE_BASE}/api/v3/ticker/24hr"),
    )
    allowed = {s["symbol"]: s["baseAsset"] for s in info.get("symbols", [])
               if s.get("status") == "TRADING"
               and s.get("quoteAsset") == "USDT"
               and s.get("isSpotTradingAllowed", True)}
    out = {}
    for t in ticks:
        sym = t.get("symbol"); base = allowed.get(sym)
        if not base or base in STABLE_BASES: continue
        ask = fnum(t.get("askPrice")); bid = fnum(t.get("bidPrice")); vol = fnum(t.get("quoteVolume"))
        if ask > 0 and bid > 0:
            out[base] = Quote("BINANCE", sym, base, ask, bid, vol)
    return out

async def fetch_bybit(session):
    info, ticks = await asyncio.gather(
        get_json(session, f"{BYBIT_BASE}/v5/market/instruments-info", {"category":"spot","limit":"1000"}),
        get_json(session, f"{BYBIT_BASE}/v5/market/tickers", {"category":"spot"}),
    )
    allowed = {s["symbol"]: s["baseCoin"] for s in info.get("result", {}).get("list", [])
               if s.get("status") == "Trading" and s.get("quoteCoin") == "USDT"}
    out = {}
    for t in ticks.get("result", {}).get("list", []):
        sym = t.get("symbol"); base = allowed.get(sym)
        if not base or base in STABLE_BASES: continue
        ask = fnum(t.get("ask1Price")); bid = fnum(t.get("bid1Price")); vol = fnum(t.get("turnover24h"))
        if ask > 0 and bid > 0:
            out[base] = Quote("BYBIT", sym, base, ask, bid, vol)
    return out


async def fetch_htx(session):
    symbols, ticks = await asyncio.gather(
        get_json(session, f"{HTX_BASE}/v1/common/symbols"),
        get_json(session, f"{HTX_BASE}/market/tickers"),
    )
    allowed = {}
    for s in symbols.get("data", []):
        if s.get("state") == "online" and s.get("quote-currency") == "usdt":
            allowed[s.get("symbol")] = str(s.get("base-currency", "")).upper()
    out = {}
    for t in ticks.get("data", []):
        sym = t.get("symbol")
        base = allowed.get(sym)
        if not base or base in STABLE_BASES:
            continue
        ask = fnum(t.get("ask"))
        bid = fnum(t.get("bid"))
        vol = fnum(t.get("amount")) * fnum(t.get("close"))
        if ask > 0 and bid > 0:
            out[base] = Quote("HTX", sym, base, ask, bid, vol)
    return out

async def fetch_kucoin(session):
    symbols, ticks = await asyncio.gather(
        get_json(session, f"{KUCOIN_BASE}/api/v2/symbols"),
        get_json(session, f"{KUCOIN_BASE}/api/v1/market/allTickers"),
    )
    allowed = {}
    for s in symbols.get("data", []):
        if s.get("enableTrading") and s.get("quoteCurrency") == "USDT":
            allowed[s.get("symbol")] = s.get("baseCurrency")
    out = {}
    ticker_rows = ticks.get("data", {}).get("ticker", [])
    for t in ticker_rows:
        sym = t.get("symbol")
        base = allowed.get(sym)
        if not base or base in STABLE_BASES:
            continue
        ask = fnum(t.get("sell"))
        bid = fnum(t.get("buy"))
        vol = fnum(t.get("volValue"))
        if ask > 0 and bid > 0:
            out[base] = Quote("KUCOIN", sym, base, ask, bid, vol)
    return out

async def fetch_mexc(session):
    info, ticks = await asyncio.gather(
        get_json(session, f"{MEXC_BASE}/api/v3/exchangeInfo"),
        get_json(session, f"{MEXC_BASE}/api/v3/ticker/24hr"),
    )
    allowed = {}
    for s in info.get("symbols", []):
        if s.get("status") in ("ENABLED", "TRADING", "1") and s.get("quoteAsset") == "USDT":
            allowed[s.get("symbol")] = s.get("baseAsset")
    out = {}
    for t in ticks:
        sym = t.get("symbol")
        base = allowed.get(sym)
        if not base or base in STABLE_BASES:
            continue
        ask = fnum(t.get("askPrice"))
        bid = fnum(t.get("bidPrice"))
        vol = fnum(t.get("quoteVolume"))
        if ask > 0 and bid > 0:
            out[base] = Quote("MEXC", sym, base, ask, bid, vol)
    return out

async def fetch_okx(session):
    inst, ticks = await asyncio.gather(
        get_json(session, f"{OKX_BASE}/api/v5/public/instruments", {"instType":"SPOT"}),
        get_json(session, f"{OKX_BASE}/api/v5/market/tickers", {"instType":"SPOT"}),
    )
    allowed = {s["instId"]: s["baseCcy"] for s in inst.get("data", [])
               if s.get("state") == "live" and s.get("quoteCcy") == "USDT"}
    out = {}
    for t in ticks.get("data", []):
        sym = t.get("instId"); base = allowed.get(sym)
        if not base or base in STABLE_BASES: continue
        ask = fnum(t.get("askPx")); bid = fnum(t.get("bidPx")); vol = fnum(t.get("volCcy24h"))
        if ask > 0 and bid > 0:
            out[base] = Quote("OKX", sym, base, ask, bid, vol)
    return out

def find_candidates(markets):
    bases = set()
    for m in markets.values(): bases.update(m.keys())
    found = []
    for base in bases:
        quotes = [m[base] for m in markets.values() if base in m]
        if len(quotes) < 2: continue
        for buy in quotes:
            for sell in quotes:
                if buy.exchange == sell.exchange or sell.bid <= buy.ask: continue
                gross = (sell.bid / buy.ask - 1.0) * 100.0
                if gross > MAX_GROSS_SPREAD_PCT: continue
                fees = FEES_PCT[buy.exchange] + FEES_PCT[sell.exchange]
                net = gross - fees
                liq = min(buy.quote_volume_24h, sell.quote_volume_24h)
                if net >= MIN_NET_SPREAD_PCT and liq >= MIN_24H_QUOTE_VOLUME:
                    found.append(Opportunity(base, buy.exchange, buy.symbol, buy.ask,
                        sell.exchange, sell.symbol, sell.bid, gross, fees, net, liq))
    return sorted(found, key=lambda x: x.top_net_pct, reverse=True)

async def fetch_book(session, exchange, symbol):
    if exchange == "BINANCE":
        d = await get_json(session, f"{BINANCE_BASE}/api/v3/depth", {"symbol":symbol,"limit":"100"})
        return d.get("bids", []), d.get("asks", [])
    if exchange == "BYBIT":
        d = await get_json(session, f"{BYBIT_BASE}/v5/market/orderbook",
                           {"category":"spot","symbol":symbol,"limit":"200"})
        r = d.get("result", {})
        return r.get("b", []), r.get("a", [])
    if exchange == "OKX":
        d = await get_json(session, f"{OKX_BASE}/api/v5/market/books", {"instId":symbol,"sz":"100"})
        rows = d.get("data", [])
        if rows: return rows[0].get("bids", []), rows[0].get("asks", [])
    return [], []

def buy_avg_from_asks(asks, quote_amount):
    remaining = quote_amount; base_bought = 0.0; spent = 0.0
    for row in asks:
        p, q = fnum(row[0]), fnum(row[1])
        if p <= 0 or q <= 0: continue
        take_quote = min(remaining, p*q)
        base_bought += take_quote/p
        spent += take_quote
        remaining -= take_quote
        if remaining <= 1e-9: break
    if remaining > 1e-6 or base_bought <= 0: return None, None
    return spent/base_bought, base_bought

def sell_avg_from_bids(bids, base_amount):
    remaining = base_amount; received = 0.0; sold = 0.0
    for row in bids:
        p, q = fnum(row[0]), fnum(row[1])
        if p <= 0 or q <= 0: continue
        take = min(remaining, q)
        received += take*p
        sold += take
        remaining -= take
        if remaining <= 1e-12: break
    if remaining > 1e-9 or sold <= 0: return None, None
    return received/sold, received

async def enrich_with_depth(session, o):
    try:
        (_, buy_asks), (sell_bids, _) = await asyncio.gather(
            fetch_book(session, o.buy_exchange, o.buy_symbol),
            fetch_book(session, o.sell_exchange, o.sell_symbol),
        )
        buy_avg, base_qty = buy_avg_from_asks(buy_asks, TRADE_SIZE_USDT)
        if buy_avg is None: return o
        sell_avg, quote_received = sell_avg_from_bids(sell_bids, base_qty)
        if sell_avg is None: return o
        buy_fee = TRADE_SIZE_USDT * FEES_PCT[o.buy_exchange] / 100.0
        sell_fee = quote_received * FEES_PCT[o.sell_exchange] / 100.0
        net_profit = quote_received - TRADE_SIZE_USDT - buy_fee - sell_fee
        o.exec_buy_avg = buy_avg
        o.exec_sell_avg = sell_avg
        o.exec_net_pct = net_profit / TRADE_SIZE_USDT * 100.0
        o.executable = o.exec_net_pct >= MIN_NET_SPREAD_PCT
    except Exception as e:
        log.warning("Depth check failed %s %s->%s: %s", o.base, o.buy_exchange, o.sell_exchange, e)
    return o

def fmt_price(v):
    if v >= 1000: return f"{v:,.2f}"
    if v >= 1: return f"{v:.6f}".rstrip("0").rstrip(".")
    return f"{v:.10f}".rstrip("0").rstrip(".")

def fmt_money(v):
    if v >= 1000000: return f"$" + f"{v/1000000:.2f}M"
    if v >= 1000: return f"$" + f"{v/1000:.1f}K"
    return f"$" + f"{v:.0f}"

def opp_text(o):
    profit = TRADE_SIZE_USDT * o.exec_net_pct / 100.0
    return (
        f"🚨 CEX ARBITRAGE\n{o.base}/USDT\n\n"
        f"🟢 BUY  {o.buy_exchange}\nTop ask: {fmt_price(o.buy_ask)}\n"
        f"Avg for $" + f"{TRADE_SIZE_USDT:.0f}: {fmt_price(o.exec_buy_avg)}\n\n"
        f"🔴 SELL {o.sell_exchange}\nTop bid: {fmt_price(o.sell_bid)}\n"
        f"Avg for size: {fmt_price(o.exec_sell_avg)}\n\n"
        f"Top-book gross: +{o.gross_pct:.3f}%\n"
        f"Fees est.: -{o.fees_pct:.3f}%\n"
        f"REAL NET after depth: +{o.exec_net_pct:.3f}%\n"
        f"Est. profit on $" + f"{TRADE_SIZE_USDT:.0f}: $" + f"{profit:.2f}\n"
        f"24h liquidity floor: {fmt_money(o.min_quote_volume_24h)}\n\n"
        f"⚠️ Verify same asset/network and deposit/withdraw status before trading."
    )

async def send_telegram(session, text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with session.post(url, json={"chat_id":TELEGRAM_CHAT_ID,"text":text,
                                           "disable_web_page_preview":True}, timeout=10) as r:
            if r.status >= 400: log.warning("Telegram error %s: %s", r.status, await r.text())
    except Exception as e:
        log.warning("Telegram exception: %s", e)

async def main():
    log.info("Starting CEX arbitrage scanner v3 — 6 exchanges")
    log.info("Trade size for depth check: $%.0f", TRADE_SIZE_USDT)
    log.info("NET threshold: %.3f%%", MIN_NET_SPREAD_PCT)
    log.info("Reject gross spread above: %.2f%%", MAX_GROSS_SPREAD_PCT)
    connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)
    headers = {"User-Agent":"cex-arb-scanner/railway-v2"}
    last_alert = {}
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        while True:
            started = time.time()
            try:
                results = await asyncio.gather(fetch_binance(session), fetch_bybit(session),
                                               fetch_okx(session), return_exceptions=True)
                names = ["BINANCE","BYBIT","OKX"]; markets = {}
                for name, result in zip(names, results):
                    if isinstance(result, Exception): log.warning("%s fetch failed: %s", name, result)
                    else: markets[name] = result
                candidates = find_candidates(markets)
                log.info("%s | candidates:%d",
                         " | ".join(f"{k}:{len(v)}" for k,v in markets.items()), len(candidates))
                checked = await asyncio.gather(*(enrich_with_depth(session,o) for o in candidates[:20]))
                opps = sorted([o for o in checked if o.executable],
                              key=lambda x:x.exec_net_pct, reverse=True)
                now = time.time(); sent = 0
                for o in opps:
                    if sent >= MAX_ALERTS_PER_CYCLE: break
                    key = (o.base,o.buy_exchange,o.sell_exchange)
                    if now-last_alert.get(key,0) < ALERT_COOLDOWN_SECONDS: continue
                    msg = opp_text(o)
                    log.info("\n%s", msg)
                    await send_telegram(session,msg)
                    last_alert[key] = now
                    sent += 1
            except Exception:
                log.exception("Unexpected cycle error")
            await asyncio.sleep(max(0.5, POLL_SECONDS-(time.time()-started)))

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Stopped")
