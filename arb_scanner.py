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

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "3"))
MIN_NET_SPREAD_PCT = float(os.getenv("MIN_NET_SPREAD_PCT", "0.20"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "1000000"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "90"))
MAX_ALERTS_PER_CYCLE = int(os.getenv("MAX_ALERTS_PER_CYCLE", "5"))

FEES_PCT = {
    "BINANCE": float(os.getenv("BINANCE_FEE_PCT", "0.10")),
    "BYBIT": float(os.getenv("BYBIT_FEE_PCT", "0.10")),
    "OKX": float(os.getenv("OKX_FEE_PCT", "0.10")),
}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

STABLE_BASES = {
    "USDT", "USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDS", "PYUSD"
}

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
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
    buy_ask: float
    sell_exchange: str
    sell_bid: float
    gross_pct: float
    fees_pct: float
    net_pct: float
    min_quote_volume_24h: float

def fnum(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

async def get_json(session: aiohttp.ClientSession, url: str, params=None):
    timeout = aiohttp.ClientTimeout(total=12)
    async with session.get(url, params=params, timeout=timeout) as r:
        r.raise_for_status()
        return await r.json()

async def fetch_binance(session) -> Dict[str, Quote]:
    info, ticks = await asyncio.gather(
        get_json(session, f"{BINANCE_BASE}/api/v3/exchangeInfo"),
        get_json(session, f"{BINANCE_BASE}/api/v3/ticker/24hr"),
    )

    allowed = {
        s["symbol"]: s["baseAsset"]
        for s in info.get("symbols", [])
        if s.get("status") == "TRADING"
        and s.get("quoteAsset") == "USDT"
        and s.get("isSpotTradingAllowed", True)
    }

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
            out[base] = Quote("BINANCE", sym, base, ask, bid, vol)

    return out

async def fetch_bybit(session) -> Dict[str, Quote]:
    info, ticks = await asyncio.gather(
        get_json(session, f"{BYBIT_BASE}/v5/market/instruments-info", {"category": "spot"}),
        get_json(session, f"{BYBIT_BASE}/v5/market/tickers", {"category": "spot"}),
    )

    allowed = {
        s["symbol"]: s["baseCoin"]
        for s in info.get("result", {}).get("list", [])
        if s.get("status") == "Trading" and s.get("quoteCoin") == "USDT"
    }

    out = {}
    for t in ticks.get("result", {}).get("list", []):
        sym = t.get("symbol")
        base = allowed.get(sym)
        if not base or base in STABLE_BASES:
            continue

        ask = fnum(t.get("ask1Price"))
        bid = fnum(t.get("bid1Price"))
        vol = fnum(t.get("turnover24h"))

        if ask > 0 and bid > 0:
            out[base] = Quote("BYBIT", sym, base, ask, bid, vol)

    return out

async def fetch_okx(session) -> Dict[str, Quote]:
    inst, ticks = await asyncio.gather(
        get_json(session, f"{OKX_BASE}/api/v5/public/instruments", {"instType": "SPOT"}),
        get_json(session, f"{OKX_BASE}/api/v5/market/tickers", {"instType": "SPOT"}),
    )

    allowed = {
        s["instId"]: s["baseCcy"]
        for s in inst.get("data", [])
        if s.get("state") == "live" and s.get("quoteCcy") == "USDT"
    }

    out = {}
    for t in ticks.get("data", []):
        inst_id = t.get("instId")
        base = allowed.get(inst_id)
        if not base or base in STABLE_BASES:
            continue

        ask = fnum(t.get("askPx"))
        bid = fnum(t.get("bidPx"))
        vol = fnum(t.get("volCcy24h"))

        if ask > 0 and bid > 0:
            out[base] = Quote("OKX", inst_id, base, ask, bid, vol)

    return out

def find_opportunities(markets: Dict[str, Dict[str, Quote]]) -> List[Opportunity]:
    bases = set()
    for market in markets.values():
        bases.update(market.keys())

    found = []

    for base in bases:
        quotes = [m[base] for m in markets.values() if base in m]
        if len(quotes) < 2:
            continue

        for buy in quotes:
            for sell in quotes:
                if buy.exchange == sell.exchange:
                    continue
                if buy.ask <= 0 or sell.bid <= buy.ask:
                    continue

                gross = (sell.bid / buy.ask - 1.0) * 100.0
                fees = FEES_PCT[buy.exchange] + FEES_PCT[sell.exchange]
                net = gross - fees
                liq = min(buy.quote_volume_24h, sell.quote_volume_24h)

                if net >= MIN_NET_SPREAD_PCT and liq >= MIN_24H_QUOTE_VOLUME:
                    found.append(
                        Opportunity(
                            base=base,
                            buy_exchange=buy.exchange,
                            buy_ask=buy.ask,
                            sell_exchange=sell.exchange,
                            sell_bid=sell.bid,
                            gross_pct=gross,
                            fees_pct=fees,
                            net_pct=net,
                            min_quote_volume_24h=liq,
                        )
                    )

    return sorted(found, key=lambda x: x.net_pct, reverse=True)

def fmt_price(v: float) -> str:
    if v >= 1000:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.6f}".rstrip("0").rstrip(".")
    return f"{v:.10f}".rstrip("0").rstrip(".")

def fmt_money(v: float) -> str:
    if v >= 1_000_000:
        return f"${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"${v / 1_000:.1f}K"
    return f"${v:.0f}"

def opp_text(o: Opportunity) -> str:
    return (
        f"🚨 CEX ARBITRAGE\n"
        f"{o.base}/USDT\n\n"
        f"🟢 BUY  {o.buy_exchange}: {fmt_price(o.buy_ask)}\n"
        f"🔴 SELL {o.sell_exchange}: {fmt_price(o.sell_bid)}\n\n"
        f"Gross: {o.gross_pct:.3f}%\n"
        f"Fees est.: {o.fees_pct:.3f}%\n"
        f"NET est.: +{o.net_pct:.3f}%\n"
        f"24h liquidity floor: {fmt_money(o.min_quote_volume_24h)}"
    )

async def send_telegram(session, text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    try:
        async with session.post(url, json=payload, timeout=10) as r:
            if r.status >= 400:
                body = await r.text()
                log.warning("Telegram error %s: %s", r.status, body)
    except Exception as e:
        log.warning("Telegram exception: %s", e)

async def main():
    log.info("Starting CEX arbitrage scanner")
    log.info("NET threshold: %.3f%%", MIN_NET_SPREAD_PCT)
    log.info("Min 24h quote volume: $%s", f"{MIN_24H_QUOTE_VOLUME:,.0f}")
    log.info("Poll interval: %.1fs", POLL_SECONDS)

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram is not configured. Alerts will only appear in Railway logs.")

    connector = aiohttp.TCPConnector(limit=40, ttl_dns_cache=300)
    headers = {"User-Agent": "cex-arb-scanner/railway"}

    last_alert: Dict[Tuple[str, str, str], float] = {}

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        while True:
            cycle_start = time.time()

            try:
                results = await asyncio.gather(
                    fetch_binance(session),
                    fetch_bybit(session),
                    fetch_okx(session),
                    return_exceptions=True,
                )

                names = ["BINANCE", "BYBIT", "OKX"]
                markets = {}

                for name, result in zip(names, results):
                    if isinstance(result, Exception):
                        log.warning("%s fetch failed: %s", name, result)
                    else:
                        markets[name] = result

                opps = find_opportunities(markets)

                counts = " | ".join(f"{name}:{len(m)}" for name, m in markets.items())
                log.info("%s | opportunities:%d", counts, len(opps))

                now = time.time()
                sent_this_cycle = 0

                for o in opps:
                    if sent_this_cycle >= MAX_ALERTS_PER_CYCLE:
                        break

                    key = (o.base, o.buy_exchange, o.sell_exchange)
                    if now - last_alert.get(key, 0) < ALERT_COOLDOWN_SECONDS:
                        continue

                    message = opp_text(o)
                    log.info("\n%s", message)
                    await send_telegram(session, message)
                    last_alert[key] = now
                    sent_this_cycle += 1

            except Exception:
                log.exception("Unexpected cycle error")

            elapsed = time.time() - cycle_start
            await asyncio.sleep(max(0.5, POLL_SECONDS - elapsed))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
