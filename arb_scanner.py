#!/usr/bin/env python3
import asyncio
import base64
import hashlib
import hmac
import logging
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlencode
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
MAX_TRADE_USDT = float(os.getenv("MAX_TRADE_USDT", "100"))
MIN_TRADE_USDT = float(os.getenv("MIN_TRADE_USDT", "10"))
MAX_GROSS_SPREAD_PCT = float(os.getenv("MAX_GROSS_SPREAD_PCT", "8.0"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "90"))
REARM_SECONDS = int(os.getenv("REARM_SECONDS", "120"))
REBALANCE_COST_USDT = float(os.getenv("REBALANCE_COST_USDT", "0.70"))
MIN_FINAL_PROFIT_USDT = float(os.getenv("MIN_FINAL_PROFIT_USDT", "1.00"))
NETWORK_REFRESH_SECONDS = int(os.getenv("NETWORK_REFRESH_SECONDS", "300"))
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

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")

OKX_API_KEY = os.getenv("OKX_API_KEY", "")
OKX_API_SECRET = os.getenv("OKX_API_SECRET", "")
OKX_API_PASSPHRASE = os.getenv("OKX_API_PASSPHRASE", "")
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
    exec_amount_usdt: float = 0.0
    exec_profit_usdt: float = 0.0
    final_profit_usdt: float = 0.0
    final_net_pct: float = -999.0
    network_name: str = ""
    network_verified: bool = False
    withdraw_fee_token: float = 0.0
    withdraw_fee_usdt: float = 0.0
    network_note: str = ""
    network_checked: bool = False
    network_route_available: bool = False
    alive_seconds: int = 0
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

def norm_network(name):
    raw = str(name or "").upper().strip()
    s = "".join(ch for ch in raw if ch.isalnum())

    rules = [
        (("INJECTIVE", "INJECTIVENETWORK", "INJ"), "INJ"),
        (("ETHEREUM", "ERC20", "ETH"), "ETH"),
        (("BNBSMARTCHAIN", "BEP20", "BSC"), "BSC"),
        (("TRON", "TRC20", "TRX"), "TRX"),
        (("ARBITRUMONE", "ARBITRUM", "ARB"), "ARBITRUM"),
        (("OPTIMISM",), "OPTIMISM"),
        (("SOLANA", "SOL"), "SOL"),
        (("POLYGON", "MATIC"), "POLYGON"),
        (("AVALANCHECCHAIN", "AVAXC"), "AVAXC"),
        (("HARMONY", "HARMONYONE", "ONE"), "ONE"),
        (("BASE",), "BASE"),
    ]
    for aliases, canonical in rules:
        for alias in aliases:
            a = "".join(ch for ch in alias if ch.isalnum())
            if s == a or s.startswith(a) or a in s:
                return canonical
    return s

def canonical_network(*names):
    known = {"INJ","ETH","BSC","TRX","ARBITRUM","OPTIMISM","SOL","POLYGON","AVAXC","ONE","BASE"}
    normalized = []
    for name in names:
        n = norm_network(name)
        if n:
            normalized.append(n)
    for n in normalized:
        if n in known:
            return n
    return normalized[0] if normalized else ""

async def fetch_binance_networks(session):
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return {}
    params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
    query = urlencode(params)
    signature = hmac.new(BINANCE_API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    url = f"{BINANCE_BASE}/sapi/v1/capital/config/getall?{query}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    timeout = aiohttp.ClientTimeout(total=12)
    async with session.get(url, headers=headers, timeout=timeout) as r:
        r.raise_for_status()
        data = await r.json()
    out = {}
    for coin in data:
        base = str(coin.get("coin", "")).upper()
        rows = []
        for n in coin.get("networkList", []):
            raw_name = n.get("name") or n.get("network")
            network = canonical_network(n.get("network"), n.get("name"))
            rows.append({
                "network": network,
                "raw": raw_name or "",
                "withdraw": bool(n.get("withdrawEnable")),
                "deposit": bool(n.get("depositEnable")),
                "fee": fnum(n.get("withdrawFee")),
                "confirms": int(fnum(n.get("minConfirm")))
            })
        if rows:
            out[base] = rows
    return out

async def fetch_bybit_networks(session):
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        return {}
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    query = ""
    payload = timestamp + BYBIT_API_KEY + recv_window + query
    signature = hmac.new(BYBIT_API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    headers = {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
        "X-BAPI-SIGN": signature
    }
    url = f"{BYBIT_BASE}/v5/asset/coin/query-info"
    timeout = aiohttp.ClientTimeout(total=12)
    async with session.get(url, headers=headers, timeout=timeout) as r:
        r.raise_for_status()
        d = await r.json()
    if d.get("retCode") not in (0, None):
        raise RuntimeError(f"Bybit retCode={d.get('retCode')} retMsg={d.get('retMsg')}")
    out = {}
    for coin in d.get("result", {}).get("rows", []):
        base = str(coin.get("coin", "")).upper()
        rows = []
        for ch in coin.get("chains", []):
            raw_name = ch.get("chainType") or ch.get("chain")
            network = canonical_network(ch.get("chainType"), ch.get("chain"))
            fee = fnum(ch.get("withdrawFee"))
            rows.append({
                "network": network,
                "raw": raw_name or "",
                "withdraw": str(ch.get("chainWithdraw")) == "1",
                "deposit": str(ch.get("chainDeposit")) == "1",
                "fee": fee,
                "confirms": int(fnum(ch.get("confirmation")))
            })
        if rows:
            out[base] = rows
    return out

def okx_iso_timestamp():
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")

async def fetch_okx_networks(session):
    if not OKX_API_KEY or not OKX_API_SECRET or not OKX_API_PASSPHRASE:
        return {}
    request_path = "/api/v5/asset/currencies"
    timestamp = okx_iso_timestamp()
    prehash = timestamp + "GET" + request_path
    digest = hmac.new(OKX_API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
    signature = base64.b64encode(digest).decode()
    headers = {
        "OK-ACCESS-KEY": OKX_API_KEY,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": OKX_API_PASSPHRASE
    }
    timeout = aiohttp.ClientTimeout(total=12)
    async with session.get(f"{OKX_BASE}{request_path}", headers=headers, timeout=timeout) as r:
        r.raise_for_status()
        d = await r.json()
    if d.get("code") not in ("0", 0, None):
        raise RuntimeError(f"OKX code={d.get('code')} msg={d.get('msg')}")
    out = {}
    for item in d.get("data", []):
        base = str(item.get("ccy", "")).upper()
        raw_chain = item.get("chain") or ""
        # OKX often returns values like "USDT-TRC20" or "ETH-ERC20".
        tail = raw_chain.split("-")[-1] if raw_chain else raw_chain
        network = canonical_network(tail, raw_chain)
        row = {
            "network": network,
            "raw": raw_chain,
            "withdraw": bool(item.get("canWd")),
            "deposit": bool(item.get("canDep")),
            "fee": fnum(item.get("fee")),
            "confirms": int(fnum(item.get("minDepArrivalConfirm")))
        }
        out.setdefault(base, []).append(row)
    return out
async def fetch_htx_networks(session):
    d = await get_json(session, f"{HTX_BASE}/v2/reference/currencies", {"authorizedUser":"false"})
    out = {}
    for coin in d.get("data", []):
        base = str(coin.get("currency", "")).upper()
        rows = []
        for ch in coin.get("chains", []):
            raw_name = ch.get("displayName") or ch.get("baseChain") or ch.get("baseChainProtocol") or ch.get("chain")
            network = canonical_network(
                ch.get("baseChain"),
                ch.get("baseChainProtocol"),
                ch.get("displayName"),
                ch.get("chain")
            )
            fee_type = str(ch.get("withdrawFeeType", "")).lower()
            fee = fnum(ch.get("transactFeeWithdraw")) if fee_type == "fixed" else fnum(ch.get("minTransactFeeWithdraw"))
            rows.append({"network": network, "raw": raw_name or ch.get("chain",""),
                         "withdraw": ch.get("withdrawStatus") == "allowed",
                         "deposit": ch.get("depositStatus") == "allowed",
                         "fee": fee, "confirms": int(fnum(ch.get("numOfConfirmations")))})
        if rows: out[base] = rows
    return out

async def fetch_kucoin_networks(session):
    out = {}

    # Current KuCoin public currency endpoint (UTA).
    try:
        d = await get_json(session, f"{KUCOIN_BASE}/api/ua/v2/asset/currencies")
        rows_data = d.get("data", [])
        for coin in rows_data:
            base = str(coin.get("currency", "")).upper()
            rows = []
            for ch in coin.get("list", []):
                raw_name = ch.get("chainName") or ch.get("chain")
                network = canonical_network(ch.get("chainName"), ch.get("chain"))
                fee = fnum(ch.get("minWithdrawFee") or ch.get("withdrawMinFee") or ch.get("withdrawalMinFee"))
                rows.append({
                    "network": network,
                    "raw": raw_name or "",
                    "withdraw": bool(ch.get("isWithdrawEnabled")),
                    "deposit": bool(ch.get("isDepositEnabled")),
                    "fee": fee,
                    "confirms": int(fnum(ch.get("confirms") or ch.get("preConfirms")))
                })
            if rows:
                out[base] = rows
    except Exception as e:
        log.warning("KuCoin UTA currency metadata failed: %s", e)

    # Fallback to the classic public endpoint and support both chains[] and list[].
    if not out:
        d = await get_json(session, f"{KUCOIN_BASE}/api/v3/currencies")
        for coin in d.get("data", []):
            base = str(coin.get("currency", "")).upper()
            rows = []
            chain_rows = coin.get("chains") or coin.get("list") or []
            for ch in chain_rows:
                raw_name = ch.get("chainName") or ch.get("chainId") or ch.get("chain")
                network = canonical_network(ch.get("chainName"), ch.get("chainId"), ch.get("chain"))
                fee = fnum(ch.get("withdrawMinFee") or ch.get("withdrawalMinFee") or ch.get("minWithdrawFee"))
                rows.append({
                    "network": network,
                    "raw": raw_name or "",
                    "withdraw": bool(ch.get("isWithdrawEnabled")),
                    "deposit": bool(ch.get("isDepositEnabled")),
                    "fee": fee,
                    "confirms": int(fnum(ch.get("confirms") or ch.get("preConfirms")))
                })
            if rows:
                out[base] = rows

    return out

async def fetch_mexc_networks(session):
    d = await get_json(session, "https://www.mexc.com/open/api/v2/market/coin/list")
    out = {}
    for item in d.get("data", []):
        base = str(item.get("currency", "")).upper()
        chain_rows = item.get("coins") or item.get("chains") or []
        if not chain_rows and item.get("chain"): chain_rows = [item]
        rows = []
        for ch in chain_rows:
            raw_name = ch.get("chain") or ch.get("netWork") or ch.get("network")
            w = str(ch.get("is_withdraw_enabled", ch.get("isWithdrawEnabled", ""))).lower() in ("true","1","yes")
            dpt = str(ch.get("is_deposit_enabled", ch.get("isDepositEnabled", ""))).lower() in ("true","1","yes")
            rows.append({"network": norm_network(raw_name), "raw": raw_name or "",
                         "withdraw": w, "deposit": dpt,
                         "fee": fnum(ch.get("fee") or ch.get("withdrawFee")),
                         "confirms": int(fnum(ch.get("deposit_minConfirm") or ch.get("minConfirm")))})
        if rows: out[base] = rows
    return out

async def refresh_network_catalog(session):
    funcs = {"BINANCE": fetch_binance_networks, "BYBIT": fetch_bybit_networks, "OKX": fetch_okx_networks, "HTX": fetch_htx_networks, "KUCOIN": fetch_kucoin_networks, "MEXC": fetch_mexc_networks}
    catalog = {}
    for name, fn in funcs.items():
        try:
            data = await fn(session)
            catalog[name] = data
            log.info("Network metadata %s: %d coins", name, len(data))
        except Exception as e:
            log.warning("Network metadata %s failed: %s", name, e)
    return catalog

def choose_common_network(o, catalog):
    src = catalog.get(o.buy_exchange, {}).get(o.base, [])
    dst = catalog.get(o.sell_exchange, {}).get(o.base, [])

    if not src or not dst:
        log.info(
            "NETWORK_DATA_MISSING %s %s->%s | src_count=%d | dst_count=%d",
            o.base, o.buy_exchange, o.sell_exchange, len(src), len(dst)
        )
        return None

    dst_by_net = {}
    for d in dst:
        if d.get("deposit"):
            dst_by_net.setdefault(d.get("network"), []).append(d)

    candidates = []
    for s in src:
        if not s.get("withdraw"):
            continue
        for d in dst_by_net.get(s.get("network"), []):
            candidates.append((s, d))

    if not candidates:
        log.info(
            "NETWORK_MISS %s %s->%s | src=%s | dst=%s",
            o.base, o.buy_exchange, o.sell_exchange,
            [(x.get("raw"), x.get("network"), x.get("withdraw"), x.get("fee")) for x in src],
            [(x.get("raw"), x.get("network"), x.get("deposit")) for x in dst]
        )
        return None

    candidates.sort(key=lambda x: x[0].get("fee", 0.0))
    return candidates[0]

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
        if rows:
            return rows[0].get("bids", []), rows[0].get("asks", [])
    if exchange == "HTX":
        d = await get_json(session, f"{HTX_BASE}/market/depth",
                           {"symbol":symbol,"type":"step0","depth":"20"})
        tick = d.get("tick", {})
        return tick.get("bids", []), tick.get("asks", [])
    if exchange == "KUCOIN":
        d = await get_json(session, f"{KUCOIN_BASE}/api/v1/market/orderbook/level2_20",
                           {"symbol":symbol})
        data = d.get("data", {})
        return data.get("bids", []), data.get("asks", [])
    if exchange == "MEXC":
        d = await get_json(session, f"{MEXC_BASE}/api/v3/depth",
                           {"symbol":symbol,"limit":"100"})
        return d.get("bids", []), d.get("asks", [])
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

def evaluate_trade_size(o, asks, bids, amount_usdt):
    buy_avg, base_qty = buy_avg_from_asks(asks, amount_usdt)
    if buy_avg is None:
        return None
    sell_avg, quote_received = sell_avg_from_bids(bids, base_qty)
    if sell_avg is None:
        return None

    buy_fee = amount_usdt * FEES_PCT[o.buy_exchange] / 100.0
    sell_fee = quote_received * FEES_PCT[o.sell_exchange] / 100.0
    profit = quote_received - amount_usdt - buy_fee - sell_fee
    net_pct = profit / amount_usdt * 100.0

    return buy_avg, sell_avg, net_pct, profit

async def enrich_with_depth(session, o):
    try:
        (_, buy_asks), (sell_bids, _) = await asyncio.gather(
            fetch_book(session, o.buy_exchange, o.buy_symbol),
            fetch_book(session, o.sell_exchange, o.sell_symbol),
        )

        # First check whether even the minimum size is worth doing.
        min_eval = evaluate_trade_size(o, buy_asks, sell_bids, MIN_TRADE_USDT)
        if min_eval is None or min_eval[2] < MIN_NET_SPREAD_PCT:
            return o

        # If the whole $100 (or configured max) fits profitably, use it.
        max_eval = evaluate_trade_size(o, buy_asks, sell_bids, MAX_TRADE_USDT)
        if max_eval is not None and max_eval[2] >= MIN_NET_SPREAD_PCT:
            chosen_amount = MAX_TRADE_USDT
            chosen = max_eval
        else:
            # Find the largest profitable amount to about $0.10 precision.
            lo = MIN_TRADE_USDT
            hi = MAX_TRADE_USDT
            chosen_amount = MIN_TRADE_USDT
            chosen = min_eval

            for _ in range(12):
                mid = (lo + hi) / 2.0
                ev = evaluate_trade_size(o, buy_asks, sell_bids, mid)
                if ev is not None and ev[2] >= MIN_NET_SPREAD_PCT:
                    chosen_amount = mid
                    chosen = ev
                    lo = mid
                else:
                    hi = mid

            chosen_amount = round(chosen_amount, 2)

        o.exec_buy_avg = chosen[0]
        o.exec_sell_avg = chosen[1]
        o.exec_net_pct = chosen[2]
        o.exec_profit_usdt = chosen[3]
        o.exec_amount_usdt = chosen_amount

        o.final_profit_usdt = o.exec_profit_usdt - REBALANCE_COST_USDT
        o.final_net_pct = (o.final_profit_usdt / o.exec_amount_usdt) * 100.0 if o.exec_amount_usdt > 0 else -999.0
        o.executable = chosen_amount >= MIN_TRADE_USDT and o.exec_net_pct >= MIN_NET_SPREAD_PCT

    except Exception as e:
        log.warning("Depth check failed %s %s->%s: %s",
                    o.base, o.buy_exchange, o.sell_exchange, e)
    return o

def apply_network_cost(o, catalog):
    src_supported = o.buy_exchange in catalog
    dst_supported = o.sell_exchange in catalog
    src_rows = catalog.get(o.buy_exchange, {}).get(o.base, [])
    dst_rows = catalog.get(o.sell_exchange, {}).get(o.base, [])

    o.network_checked = src_supported and dst_supported and bool(src_rows) and bool(dst_rows)
    match = choose_common_network(o, catalog)

    if match:
        src, dst = match
        o.network_name = src.get("raw") or src.get("network") or ""
        o.network_verified = True
        o.network_route_available = True
        o.withdraw_fee_token = fnum(src.get("fee"))
        o.withdraw_fee_usdt = o.withdraw_fee_token * o.exec_buy_avg
        o.network_note = "withdraw ✅ / deposit ✅"
        o.final_profit_usdt = o.exec_profit_usdt - o.withdraw_fee_usdt
        o.final_net_pct = (o.final_profit_usdt / o.exec_amount_usdt) * 100.0 if o.exec_amount_usdt > 0 else -999.0
    elif o.network_checked:
        # Both exchanges supplied network metadata for this asset, but no common enabled route exists.
        o.network_verified = True
        o.network_route_available = False
        o.network_note = "нет общей активной сети"
        o.final_profit_usdt = -999.0
        o.final_net_pct = -999.0
    else:
        o.network_verified = False
        o.network_route_available = False
        o.network_note = "сеть не проверена автоматически"
        o.final_profit_usdt = o.exec_profit_usdt - REBALANCE_COST_USDT
        o.final_net_pct = (o.final_profit_usdt / o.exec_amount_usdt) * 100.0 if o.exec_amount_usdt > 0 else -999.0

    o.executable = (
        o.executable
        and (not o.network_checked or o.network_route_available)
        and o.final_profit_usdt >= MIN_FINAL_PROFIT_USDT
    )
    return o

def fmt_price(v):
    if v >= 1000: return f"{v:,.2f}"
    if v >= 1: return f"{v:.6f}".rstrip("0").rstrip(".")
    return f"{v:.10f}".rstrip("0").rstrip(".")

def fmt_money(v):
    if v >= 1000000: return f"$" + f"{v/1000000:.2f}M"
    if v >= 1000: return f"$" + f"{v/1000:.1f}K"
    return f"$" + f"{v:.0f}"

def fmt_duration(seconds):
    if seconds < 60:
        return f"{seconds} сек"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m} мин {s} сек"
    h, rem = divmod(m, 60)
    return f"{h} ч {rem} мин"

def opp_text(o):
    if o.network_verified and o.network_route_available:
        network_block = (
            f"🌐 Сеть: {o.network_name}\n"
            f"Вывод с {o.buy_exchange}: ✅\n"
            f"Ввод на {o.sell_exchange}: ✅\n"
            f"Комиссия вывода: {o.withdraw_fee_token:g} {o.base} (~${o.withdraw_fee_usdt:.2f})\n"
        )
        cost_line = f"Комиссия сети: -${o.withdraw_fee_usdt:.2f}\n"
    elif o.network_checked and not o.network_route_available:
        network_block = (
            f"🌐 Сеть: ❌ нет общей активной сети\n"
            f"Связка заблокирована фильтром сети.\n"
        )
        cost_line = ""
    else:
        reason = "Для одной из бирж сеть пока не проверяется автоматически."
        network_block = (
            f"🌐 Сеть: ⚠️ не подтверждена\n"
            f"{reason}\n"
        )
        cost_line = f"Резерв на перевод/ребаланс: -${REBALANCE_COST_USDT:.2f}\n"
    return (
        f"🔥 ЛУЧШИЙ АРБИТРАЖ\n{o.base}/USDT\n\n"
        f"🟢 КУПИТЬ: {o.buy_exchange}\n"
        f"Цена: {fmt_price(o.exec_buy_avg)}\n\n"
        f"🔴 ПРОДАТЬ: {o.sell_exchange}\n"
        f"Цена: {fmt_price(o.exec_sell_avg)}\n\n"
        f"{network_block}\n"
        f"Спред: +{o.gross_pct:.3f}%\n"
        f"NET после торговых комиссий: +{o.exec_net_pct:.3f}%\n"
        f"Рабочий объём: ${o.exec_amount_usdt:.2f}\n"
        f"Прибыль до сети: ${o.exec_profit_usdt:.2f}\n"
        f"{cost_line}"
        f"ЧИСТАЯ прибыль: ${o.final_profit_usdt:.2f}\n"
        f"ЧИСТЫЙ NET: +{o.final_net_pct:.3f}%\n"
        f"Спред живёт: {fmt_duration(o.alive_seconds)}\n"
        f"24ч ликвидность: {fmt_money(o.min_quote_volume_24h)}"
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
    log.info("Starting CEX arbitrage scanner v14 — Binance Bybit OKX private network metadata")
    log.info("Capital cap: $%.0f | minimum useful size: $%.0f", MAX_TRADE_USDT, MIN_TRADE_USDT)
    log.info("Private network APIs configured | Binance:%s | Bybit:%s | OKX:%s", bool(BINANCE_API_KEY and BINANCE_API_SECRET), bool(BYBIT_API_KEY and BYBIT_API_SECRET), bool(OKX_API_KEY and OKX_API_SECRET and OKX_API_PASSPHRASE))
    log.info("Rebalance reserve: $%.2f | minimum final profit: $%.2f", REBALANCE_COST_USDT, MIN_FINAL_PROFIT_USDT)
    log.info("NET threshold: %.3f%%", MIN_NET_SPREAD_PCT)
    log.info("Reject gross spread above: %.2f%%", MAX_GROSS_SPREAD_PCT)
    connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)
    headers = {"User-Agent":"cex-arb-scanner/railway-v2"}
    last_alert = {}
    first_seen = {}
    alerted_bases = set()
    inactive_since = {}
    network_catalog = {}
    network_catalog_ts = 0.0
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        while True:
            started = time.time()
            try:
                results = await asyncio.gather(
                    fetch_binance(session),
                    fetch_bybit(session),
                    fetch_okx(session),
                    fetch_htx(session),
                    fetch_kucoin(session),
                    fetch_mexc(session),
                    return_exceptions=True
                )
                names = ["BINANCE","BYBIT","OKX","HTX","KUCOIN","MEXC"]; markets = {}
                for name, result in zip(names, results):
                    if isinstance(result, Exception): log.warning("%s fetch failed: %s", name, result)
                    else: markets[name] = result
                candidates = find_candidates(markets)
                now = time.time()

                if not network_catalog or now - network_catalog_ts >= NETWORK_REFRESH_SECONDS:
                    fresh_catalog = await refresh_network_catalog(session)
                    if fresh_catalog:
                        network_catalog = fresh_catalog
                        network_catalog_ts = now
                        log.info(
                            "Network catalog ready | BINANCE:%d | BYBIT:%d | OKX:%d | HTX:%d | KUCOIN:%d | MEXC:%d",
                            len(network_catalog.get("BINANCE", {})),
                            len(network_catalog.get("BYBIT", {})),
                            len(network_catalog.get("OKX", {})),
                            len(network_catalog.get("HTX", {})),
                            len(network_catalog.get("KUCOIN", {})),
                            len(network_catalog.get("MEXC", {}))
                        )

                active_keys = set()
                for o in candidates:
                    key = (o.base, o.buy_exchange, o.sell_exchange)
                    active_keys.add(key)
                    if key not in first_seen:
                        first_seen[key] = now

                for key in list(first_seen.keys()):
                    if key not in active_keys:
                        del first_seen[key]

                log.info("%s | candidates:%d",
                         " | ".join(f"{k}:{len(v)}" for k,v in markets.items()), len(candidates))

                checked = await asyncio.gather(
                    *(enrich_with_depth(session,o) for o in candidates[:30])
                )

                checked = [apply_network_cost(o, network_catalog) for o in checked]

                for o in checked:
                    if o.network_checked and not o.network_route_available:
                        log.info("NETWORK_REJECT %s %s->%s | %s",
                                 o.base, o.buy_exchange, o.sell_exchange, o.network_note)

                for o in checked:
                    key = (o.base, o.buy_exchange, o.sell_exchange)
                    o.alive_seconds = int(now - first_seen.get(key, now))

                opps = sorted([o for o in checked if o.executable],
                              key=lambda x:(x.final_profit_usdt, x.final_net_pct),
                              reverse=True)

                # Только одна лучшая связка на монету.
                best_by_base = {}
                for o in opps:
                    current = best_by_base.get(o.base)
                    if current is None or (o.final_profit_usdt, o.final_net_pct) > (current.final_profit_usdt, current.final_net_pct):
                        best_by_base[o.base] = o
                opps = sorted(best_by_base.values(),
                              key=lambda x:(x.final_profit_usdt, x.final_net_pct),
                              reverse=True)

                # Повтор по монете запрещён, пока прибыльная возможность не исчезла.
                active_bases = {o.base for o in opps}
                for base in list(alerted_bases):
                    if base in active_bases:
                        inactive_since.pop(base, None)
                    else:
                        if base not in inactive_since:
                            inactive_since[base] = now
                        elif now - inactive_since[base] >= REARM_SECONDS:
                            alerted_bases.discard(base)
                            inactive_since.pop(base, None)
                            last_alert.pop(base, None)

                for o in opps[:5]:
                    if o.network_verified:
                        fee_source = "REAL"
                        network = o.network_name or "?"
                        fee_desc = f"{o.withdraw_fee_token:g} {o.base} (~${o.withdraw_fee_usdt:.2f})"
                    else:
                        fee_source = "RESERVE"
                        network = "UNVERIFIED"
                        fee_desc = f"${REBALANCE_COST_USDT:.2f}"

                    log.info(
                        "TOP %s %s->%s | amount $%.2f | trade_net %.3f%% | network=%s | fee_source=%s | withdraw_fee=%s | trade_profit $%.2f | final_net %.3f%% | final_profit $%.2f | alive %ss",
                        o.base, o.buy_exchange, o.sell_exchange,
                        o.exec_amount_usdt, o.exec_net_pct,
                        network, fee_source, fee_desc,
                        o.exec_profit_usdt, o.final_net_pct,
                        o.final_profit_usdt, o.alive_seconds
                    )

                sent = 0
                for o in opps:
                    if sent >= MAX_ALERTS_PER_CYCLE:
                        break

                    key = o.base

                    # Если по этой монете уже был сигнал и возможность всё ещё жива —
                    # не отправляем повтор, даже если маршрут/процент немного изменился.
                    if key in alerted_bases:
                        continue

                    if now-last_alert.get(key,0) < ALERT_COOLDOWN_SECONDS:
                        continue

                    msg = opp_text(o)
                    log.info("\n%s", msg)
                    await send_telegram(session,msg)

                    last_alert[key] = now
                    alerted_bases.add(key)
                    inactive_since.pop(key, None)
                    sent += 1
            except Exception:
                log.exception("Unexpected cycle error")
            await asyncio.sleep(max(0.5, POLL_SECONDS-(time.time()-started)))

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log.info("Stopped")
