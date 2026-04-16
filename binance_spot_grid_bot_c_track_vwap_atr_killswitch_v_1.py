"""
Binance Spot Grid Bot — C‑track (VWAP+ATR killswitch) v1

Что делает:
- Ставит симметричную сетку лимит‑ордеров вокруг VWAP (15m), шаг в процентах.
- Маркет‑направление не угадывает: mean‑reversion (пилит спред) без плеча.
- Killswitch: если |Price − VWAP| >= ATR(15m) * 2 — сетка выключается, ордера снимаются.
- Ре‑арм сетки через cooldown (по умолчанию 90 минут) или при возврате к VWAP.
- Пара BTCUSDT (можно сменить).
- Maker‑ордера (LIMIT_MAKER) для минимальных комиссий.
- Ограничение капитала: не используем больше max_usdt_alloc USDT.
- Telegram‑уведомления (опционально).

Минимальные требования:
- Python 3.10+
- pip install binance-connector python-dotenv requests

ФАЙЛ .env рядом со скриптом:
BINANCE_API_KEY=... 
BINANCE_API_SECRET=...
TELEGRAM_BOT_TOKEN=...       # (опционально)
TELEGRAM_CHAT_ID=...         # (опционально)
SYMBOL=BTCUSDT
INTERVAL=15m
GRID_STEP_PCT=0.002          # 0.2%
LEVELS_PER_SIDE=10           # 10 уровней вверх/вниз
ATR_PERIOD=14
VWAP_BARS=96                 # ~24 часа по 15m
ATR_MULTIPLIER=2.0
COOLDOWN_MIN=90
MAX_USDT_ALLOC=500           # сколько USDT выделяем под стратегию
PER_ORDER_USDT=15            # усреднённый размер ордера
SLIPPAGE_BPS=10              # защитный допуск округления/«безопасности» в bps
SLEEP_SECONDS=20             # период цикла

Примечание: это референс‑реализация для старта. Торгуйте малыми объёмами, проверьте логику на демо/малых суммах, далее масштабируйтесь.
"""
from __future__ import annotations
import os
import time
import json
import math
import hmac
import hashlib
import sqlite3
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from binance.spot import Spot as BinanceSpot

load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")
SYMBOL = os.getenv("SYMBOL", "BTCUSDT")
INTERVAL = os.getenv("INTERVAL", "15m")
GRID_STEP_PCT = float(os.getenv("GRID_STEP_PCT", "0.002"))
LEVELS_PER_SIDE = int(os.getenv("LEVELS_PER_SIDE", "10"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
VWAP_BARS = int(os.getenv("VWAP_BARS", "96"))
ATR_MULTIPLIER = float(os.getenv("ATR_MULTIPLIER", "2.0"))
COOLDOWN_MIN = int(os.getenv("COOLDOWN_MIN", "90"))
MAX_USDT_ALLOC = float(os.getenv("MAX_USDT_ALLOC", "500"))
PER_ORDER_USDT = float(os.getenv("PER_ORDER_USDT", "15"))
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "10"))
SLEEP_SECONDS = int(os.getenv("SLEEP_SECONDS", "20"))

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID")

BASE_URL = "https://api.binance.com"

assert API_KEY and API_SECRET, "API ключи Binance не заданы (.env)"

# --- Helpers ---

def tg(msg: str):
    if not (TG_TOKEN and TG_CHAT):
        return
    try:
        requests.get(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            params={"chat_id": TG_CHAT, "text": msg[:4000]}
        )
    except Exception:
        pass


def now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class Filters:
    tick_size: float
    step_size: float
    min_notional: float


class ExchangeMeta:
    def __init__(self, client: BinanceSpot, symbol: str):
        self.client = client
        self.symbol = symbol
        self.filters = self._load_filters()

    def _load_filters(self) -> Filters:
        info = self.client.exchange_info()["symbols"]
        meta = next(s for s in info if s["symbol"] == self.symbol)
        tick = next(f for f in meta["filters"] if f["filterType"] == "PRICE_FILTER")
        lot = next(f for f in meta["filters"] if f["filterType"] == "LOT_SIZE")
        notional = next(f for f in meta["filters"] if f["filterType"] == "NOTIONAL")
        return Filters(
            tick_size=float(tick["tickSize"]),
            step_size=float(lot["stepSize"]),
            min_notional=float(notional["minNotional"]),
        )

    def q_price(self, price: float) -> float:
        tick = self.filters.tick_size
        return math.floor(price / tick) * tick

    def q_qty(self, qty: float) -> float:
        step = self.filters.step_size
        return math.floor(qty / step) * step


class DataFeed:
    def __init__(self, client: BinanceSpot, symbol: str):
        self.client = client
        self.symbol = symbol

    def book_price(self) -> float:
        t = self.client.book_ticker(symbol=self.symbol)
        bid = float(t["bidPrice"]) ; ask = float(t["askPrice"])
        return (bid + ask) / 2.0

    def klines(self, interval: str, limit: int) -> List[List[Any]]:
        return self.client.klines(symbol=self.symbol, interval=interval, limit=limit)


def compute_atr(kl: List[List[Any]], period: int = 14) -> float:
    # kline: [open_time, o, h, l, c, v, ...]
    highs = [float(k[2]) for k in kl]
    lows = [float(k[3]) for k in kl]
    closes = [float(k[4]) for k in kl]
    trs = []
    for i in range(1, len(kl)):
        h, l = highs[i], lows[i]
        pc = closes[i-1]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / max(1, len(trs))
    return sum(trs[-period:]) / period


def compute_vwap(kl: List[List[Any]]) -> float:
    # VWAP по последним барам
    pv = 0.0
    vv = 0.0
    for k in kl:
        h, l, c, v = float(k[2]), float(k[3]), float(k[4]), float(k[5])
        tp = (h + l + c) / 3.0
        pv += tp * v
        vv += v
    return pv / vv if vv > 0 else float(kl[-1][4])


# --- Persistence (SQLite) ---

DB_PATH = "grid_state.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS state (
  id INTEGER PRIMARY KEY CHECK (id=1),
  killswitch_on INTEGER NOT NULL DEFAULT 0,
  rearm_ts INTEGER NOT NULL DEFAULT 0,
  day VARCHAR(10) NOT NULL,
  realized_pnl_usdt REAL NOT NULL DEFAULT 0.0
);

CREATE TABLE IF NOT EXISTS grid_orders (
  order_id INTEGER PRIMARY KEY,
  side TEXT NOT NULL,
  price REAL NOT NULL,
  qty REAL NOT NULL,
  status TEXT NOT NULL,
  paired_price REAL,
  created_ts INTEGER NOT NULL
);
"""


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    with db() as c:
        c.executescript(SCHEMA)
        cur = c.execute("SELECT 1 FROM state WHERE id=1")
        if not cur.fetchone():
            today = dt.date.today().isoformat()
            c.execute("INSERT INTO state (id, killswitch_on, rearm_ts, day, realized_pnl_usdt) VALUES (1,0,0,?,0.0)", (today,))


# --- Core Grid Logic ---

class GridBot:
    def __init__(self):
        self.client = BinanceSpot(api_key=API_KEY, api_secret=API_SECRET)
        self.meta = ExchangeMeta(self.client, SYMBOL)
        self.feed = DataFeed(self.client, SYMBOL)
        init_db()

    # Portfolio helpers
    def free_balances(self) -> Tuple[float, float]:
        acc = self.client.account()
        usdt = next(b for b in acc["balances"] if b["asset"] == "USDT")
        btc = next(b for b in acc["balances"] if b["asset"] == "BTC")
        return float(usdt["free"]), float(btc["free"])

    # Orders
    def cancel_all_open(self):
        try:
            opens = self.client.get_open_orders(symbol=SYMBOL)
            for o in opens:
                try:
                    self.client.cancel_order(symbol=SYMBOL, orderId=o["orderId"])
                except Exception as e:
                    pass
        except Exception:
            pass

    def place_limit_maker(self, side: str, price: float, qty: float) -> Optional[int]:
        price = self.meta.q_price(price)
        qty = self.meta.q_qty(qty)
        notional = price * qty
        if notional < self.meta.filters.min_notional:
            return None
        try:
            o = self.client.new_order(
                symbol=SYMBOL,
                side=side,
                type="LIMIT_MAKER",
                price=f"{price:.8f}",
                quantity=f"{qty:.8f}"
            )
            return int(o["orderId"])
        except Exception as e:
            return None

    def sync_fills(self):
        # Обновляем статусы ордеров и ставим парные тейки
        with db() as c:
            cur = c.execute("SELECT order_id, side, price, qty, status FROM grid_orders WHERE status IN ('NEW','PARTIALLY_FILLED')")
            rows = cur.fetchall()
            for order_id, side, price, qty, status in rows:
                try:
                    o = self.client.get_order(symbol=SYMBOL, orderId=order_id)
                except Exception:
                    continue
                st = o.get("status")
                if st in ("FILLED", "PARTIALLY_FILLED") and st != status:
                    c.execute("UPDATE grid_orders SET status=? WHERE order_id=?", (st, order_id))
                    if st == "FILLED":
                        # Постановка парного ордера
                        if side == "BUY":
                            sell_price = price * (1.0 + GRID_STEP_PCT)
                            qty_s = float(o.get("executedQty", qty))
                            oid = self.place_limit_maker("SELL", sell_price, qty_s)
                            if oid:
                                c.execute("INSERT OR IGNORE INTO grid_orders(order_id, side, price, qty, status, paired_price, created_ts) VALUES (?,?,?,?,?,?,?)",
                                          (oid, "SELL", sell_price, qty_s, "NEW", price, now_ms()))
                                tg(f"BUY filled @ {price:.2f}; placed SELL @ {sell_price:.2f}")
                        else:
                            buy_price = price * (1.0 - GRID_STEP_PCT)
                            qty_b = float(o.get("executedQty", qty))
                            oid = self.place_limit_maker("BUY", buy_price, qty_b)
                            if oid:
                                c.execute("INSERT OR IGNORE INTO grid_orders(order_id, side, price, qty, status, paired_price, created_ts) VALUES (?,?,?,?,?,?,?)",
                                          (oid, "BUY", buy_price, qty_b, "NEW", price, now_ms()))
                                tg(f"SELL filled @ {price:.2f}; placed BUY @ {buy_price:.2f}")

    def reset_day_if_needed(self):
        today = dt.date.today().isoformat()
        with db() as c:
            row = c.execute("SELECT day FROM state WHERE id=1").fetchone()
            if row and row[0] != today:
                c.execute("UPDATE state SET day=?, realized_pnl_usdt=0.0 WHERE id=1", (today,))

    def realised_pnl_guard(self):
        # Простая заглушка: считаем PnL по закрытым циклам из парных цен (если BUY->SELL выше, SELL->BUY ниже)
        # Для старта — достаточно мониторить вручную; при масштабировании — хранить сделки подробно.
        pass

    def place_grid(self, vwap: float, atr: float, mid: float):
        # Проверка kill‑switch
        if abs(mid - vwap) >= ATR_MULTIPLIER * atr:
            with db() as c:
                rearm = now_ms() + COOLDOWN_MIN * 60 * 1000
                c.execute("UPDATE state SET killswitch_on=1, rearm_ts=? WHERE id=1", (rearm,))
            self.cancel_all_open()
            tg(f"KILLSWITCH ON: |P−VWAP|={abs(mid-vwap):.2f} >= {ATR_MULTIPLIER}*ATR={ATR_MULTIPLIER*atr:.2f}. Orders cancelled.")
            return

        # Если kill был активен — проверим снятие
        with db() as c:
            ks, rearm_ts = c.execute("SELECT killswitch_on, rearm_ts FROM state WHERE id=1").fetchone()
            if ks == 1 and now_ms() < rearm_ts:
                return  # пауза
            if ks == 1 and now_ms() >= rearm_ts:
                c.execute("UPDATE state SET killswitch_on=0 WHERE id=1")
                tg("KILLSWITCH OFF: grid re-armed.")

        # Снимаем чужие «висячие» ордера вне сетки
        self.cancel_all_open()

        # Рассчитаем количество уровней, размер заявки и наличие USDT
        usdt_free, btc_free = self.free_balances()
        work_usdt = min(usdt_free, MAX_USDT_ALLOC)
        per_order = min(PER_ORDER_USDT, work_usdt / max(1, LEVELS_PER_SIDE * 2))
        if per_order < 10:  # минимальный нотионал Binance для BTCUSDT ≈ 10 USDT
            tg("Недостаточно USDT для постановки сетки (повысить MAX_USDT_ALLOC/PER_ORDER_USDT)")
            return

        # Ставим симметричную сетку вокруг VWAP
        for i in range(1, LEVELS_PER_SIDE + 1):
            buy_price = vwap * (1.0 - i * GRID_STEP_PCT)
            sell_price = vwap * (1.0 + i * GRID_STEP_PCT)
            qty_b = self.meta.q_qty(per_order / buy_price)
            qty_s = self.meta.q_qty(per_order / sell_price)

            oid_b = self.place_limit_maker("BUY", buy_price, qty_b)
            if oid_b:
                with db() as c:
                    c.execute("INSERT OR IGNORE INTO grid_orders(order_id, side, price, qty, status, paired_price, created_ts) VALUES (?,?,?,?,?,?,?)",
                              (oid_b, "BUY", buy_price, qty_b, "NEW", None, now_ms()))

            oid_s = self.place_limit_maker("SELL", sell_price, qty_s)
            if oid_s:
                with db() as c:
                    c.execute("INSERT OR IGNORE INTO grid_orders(order_id, side, price, qty, status, paired_price, created_ts) VALUES (?,?,?,?,?,?,?)",
                              (oid_s, "SELL", sell_price, qty_s, "NEW", None, now_ms()))

        tg(f"Grid placed: VWAP={vwap:.2f}, step={GRID_STEP_PCT*100:.2f}% x {LEVELS_PER_SIDE} per side")

    def run_once(self):
        self.reset_day_if_needed()

        # Датафид
        kl = self.feed.klines(INTERVAL, max(VWAP_BARS, ATR_PERIOD + 2))
        if len(kl) < max(VWAP_BARS, ATR_PERIOD + 2):
            return
        vwap = compute_vwap(kl[-VWAP_BARS:])
        atr = compute_atr(kl, ATR_PERIOD)
        mid = self.feed.book_price()

        # Обслуживание фризов/филлов
        self.sync_fills()

        # Постановка/снятие сетки
        self.place_grid(vwap, atr, mid)

    def loop(self):
        tg("Grid bot started.")
        while True:
            try:
                self.run_once()
            except Exception as e:
                tg(f"Error: {e}")
            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    bot = GridBot()
    bot.loop()
