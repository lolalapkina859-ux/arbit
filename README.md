# CEX Arbitrage Scanner — Railway + Telegram

Manual CEX→CEX arbitrage scanner for Binance, Bybit and OKX.

It does **not place trades**. It only scans public SPOT/USDT markets and sends opportunities to Telegram.

## Features

- Binance + Bybit + OKX
- automatic discovery of active USDT spot pairs
- BUY = best ask
- SELL = best bid
- configurable taker fees
- minimum 24h quote-volume filter
- minimum estimated NET spread filter
- Telegram alerts
- cooldown / anti-spam
- Railway-ready worker
- no exchange trading API keys required

## Files

- `arb_scanner.py` — scanner
- `requirements.txt`
- `railway.json`
- `Procfile`
- `.env.example`

## 1. Create Telegram bot

1. Open Telegram and message `@BotFather`
2. Run `/newbot`
3. Copy the bot token
4. Send any message to your new bot
5. Open:

   `https://api.telegram.org/botYOUR_TOKEN/getUpdates`

6. Find your `chat.id`

## 2. Create GitHub repository

Create an empty repository and upload all files from this project.

## 3. Deploy to Railway

1. Open Railway
2. New Project
3. Deploy from GitHub repo
4. Select your repository
5. Railway should detect Python automatically

The start command is already configured:

`python arb_scanner.py`

## 4. Add Railway Variables

Add:

```env
TELEGRAM_BOT_TOKEN=YOUR_BOT_TOKEN
TELEGRAM_CHAT_ID=YOUR_CHAT_ID

MIN_NET_SPREAD_PCT=0.20
MIN_24H_QUOTE_VOLUME=1000000
POLL_SECONDS=3
ALERT_COOLDOWN_SECONDS=90
MAX_ALERTS_PER_CYCLE=5

BINANCE_FEE_PCT=0.10
BYBIT_FEE_PCT=0.10
OKX_FEE_PCT=0.10
```

Then redeploy.

## Telegram example

```text
🚨 CEX ARBITRAGE
SUI/USDT

🟢 BUY  BINANCE: 3.214
🔴 SELL BYBIT: 3.2297

Gross: 0.488%
Fees est.: 0.200%
NET est.: +0.288%
24h liquidity floor: $18.4M
```

## Important

This V1 uses top-of-book prices and 24h market volume.

It does not yet calculate:
- order-book depth for your actual trade size
- withdrawal fees
- common transfer networks
- deposit/withdraw status
- transfer time

Those should be added before treating every alert as executable.
