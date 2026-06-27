import asyncio
import json
import os
import websockets
import requests
from flask import Flask
from threading import Thread

# --- 1. WEB SERVER FOR DEPLOYMENT ---
app = Flask('')

@app.route('/', methods=['GET', 'HEAD'])
def home():
    return "Bot is running 24/7!", 200

def run_web_server():
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_web_server)
    t.daemon = True
    t.start()

# --- 2. CREDENTIALS ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = os.environ.get("CHAT_ID", "")

# Thresholds ($49k, $98k, $490k)
THRESHOLDS = [49000, 98000, 490000]

def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

# --- 3. LIQUIDATION ENGINE ---
async def track_binance():
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"
    while True:
        try:
            async with websockets.connect(url) as ws:
                print("Connected to Binance stream")
                while True:
                    data = await ws.recv()
                    event = json.loads(data)
                    o = event.get("o", {})
                    symbol = o.get("s", "")

                    if symbol == "BTCUSDT":
                        price = float(o.get("p", 0))
                        qty = float(o.get("q", 0))
                        usd_val = price * qty
                        is_yellow_bar = (o.get("S") == "SELL")
                        evaluate_liquidation(usd_val, is_yellow_bar)
        except Exception as e:
            print(f"Binance error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)

async def track_bybit():
    url = "wss://stream.bybit.com/v5/public/linear"
    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": ["liquidation.BTCUSDT"]}))
                print("Connected to Bybit stream")
                while True:
                    data = await ws.recv()
                    res = json.loads(data)
                    topic = res.get("topic", "")
                    if "liquidation" in topic:
                        d = res.get("data", {})
                        usd_val = float(d.get("size", 0)) * float(d.get("price", 0))
                        is_yellow_bar = (d.get("side") == "Sell")
                        evaluate_liquidation(usd_val, is_yellow_bar)
        except Exception as e:
            print(f"Bybit error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)

def evaluate_liquidation(usd_val, is_yellow_bar):
    if any(usd_val >= limit for limit in THRESHOLDS):
        if is_yellow_bar:
            msg = (
                f"🟨 **YELLOW BAR LIQUIDATION**\n"
                f"💰 **Amount:** ${usd_val:,.0f}\n"
                f"👉 **TRADE DIRECTION:** BUY (LONG) 📈"
            )
        else:
            msg = (
                f"🟪 **PINK BAR LIQUIDATION**\n"
                f"💰 **Amount:** ${usd_val:,.0f}\n"
                f"👉 **TRADE DIRECTION:** SELL (SHORT) 📉"
            )
        print(f"Alert: {msg}")
        send_telegram_alert(msg)

# --- 4. START ENGINE ---
async def main():
    print("Engine Started...")
    keep_alive()
    await asyncio.gather(track_binance(), track_bybit())

if __name__ == "__main__":
    asyncio.run(main())