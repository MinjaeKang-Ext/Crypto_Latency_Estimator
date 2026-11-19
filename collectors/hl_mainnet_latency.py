#!/usr/bin/env python3
"""Hyperliquid MAINNET Korean-stock perpetual ALO placement/cancel latency.

Install (Python >=3.10):
  python -m pip install hyperliquid-python-sdk aiohttp websockets
Read-only plan (no key):
  python hl_mainnet_latency.py --korea-stock skhynix
Live test (real orders; dedicated approved MAINNET API wallet required):
  export HL_ACCOUNT=0x...  # funded master wallet, NOT the ETH deposit address
  export HL_API_KEY=...   # API wallet private key, NOT the OKX wallet private key
  python hl_mainnet_latency.py --korea-stock skhynix --offset-ticks 5 --live --samples 1
Repeat after a successful first sample:
  python hl_mainnet_latency.py --korea-stock skhynix --offset-ticks 5 --live --iterations 100 --pause 2

Default target: Korean-listed SK hynix common-share perp on the xyz DEX.
Resolve its API name against live metadata (xyz:SKHYNIX, legacy xyz:SKHX).
Never select xyz:SKHY (the separate ADS instrument) for this Korean-stock target.
--list-korea lists the available names without signing or trading.

Defaults: 11 USD notional; one passive BUY five valid price ticks below best bid; isolated 1x;
cancel immediately after resting ACK; stop on rejection, fill, or uncertainty.
Cancel-by-client-ID is signed BEFORE order submission to avoid post-ACK signing delay.
Each iteration uses a fresh streaming order book, then confirms cancellation and
checks fills/positions before the next order. --pause is AFTER these checks.
Never transfers funds, never submits market orders, never auto-closes a fill.
Live mode changes the target coin's leverage to isolated 1x before measurement.
Spot assets are intentionally unsupported. ETH means the ETH PERPETUAL.

RTT starts immediately before websocket send, after signing and serialization.
Event timestamps are captured by an independent websocket reader, including
events arriving before the ACK. open_ms is NOT time-to-fill or one-way latency.
CSV is flushed after each attempt, including failed/rejected attempts.

This cannot guarantee no fill or cleanup during process/network failure. On
STOP/interrupt check the mainnet Open Orders and Positions manually.
"""

import argparse
import asyncio
import contextlib
import csv
import json
import math
import os
import re
import statistics
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import aiohttp
import websockets
from eth_account import Account
from hyperliquid.info import Info
from hyperliquid.utils.signing import sign_l1_action

REST = "https://api.hyperliquid.xyz"
WS = "wss://api.hyperliquid.xyz/ws"
D = Decimal
KOREA_STOCKS = {
    "skhynix": ("xyz:SKHYNIX", "xyz:SKHX"),
    "samsung": ("xyz:SAMSUNG",),
    "hyundai": ("xyz:HYUNDAI",),
}


def resolve_korea_stock(metadata, stock):
    available = metadata.coin_to_asset
    for coin in KOREA_STOCKS[stock]:
        if coin in available:
            return coin
    raise RuntimeError(f"Korean stock {stock} not found in live metadata. "
                       f"Expected one of {KOREA_STOCKS[stock]}. No order sent.")


def ms(start, end):
    return round((end - start) / 1e6, 3)


def wire(value):
    return format(value, "f").rstrip("0").rstrip(".") if "." in format(value, "f") else format(value, "f")


def adjacent_price(price, decimals, direction):
    """Next accepted perp price, including changes at powers of ten.

    HL permits five significant figures within 6-szDecimals places, and ALL
    integer prices. 'Tick' means an adjacent valid price, not L2 level spacing.
    """
    price = D(price)
    if price <= 0 or not 0 <= decimals <= 6 or direction not in (-1, 1):
        raise ValueError("Invalid price, size precision, or direction")
    quantum = D(10) ** -(6 - decimals)
    probe = price if direction == 1 else price - quantum / 10
    significant_tick = min(D(1), D(10) ** (probe.adjusted() - 4))
    tick = max(quantum, significant_tick)
    rounding = ROUND_FLOOR if direction == 1 else ROUND_CEILING
    result = ((price / tick).to_integral_value(rounding=rounding) + direction) * tick
    if result <= 0:
        raise ValueError("Offset reaches a nonpositive price")
    return result


def plan(reference, decimals, notional, offset_ticks, side="buy"):
    price = D(reference)
    direction = -1 if side == "buy" else 1
    for _ in range(offset_ticks):
        price = adjacent_price(price, decimals, direction)
    step = D(10) ** -decimals
    qty = (D(str(notional)) / price / step).to_integral_value(rounding=ROUND_CEILING) * step
    if price <= 0 or not D(10) <= price * qty <= D("12.5"):
        raise RuntimeError("Rounded order must be between $10 and $12.50; choose another market or size.")
    return price, qty


class Signer:
    def __init__(self, key):
        key = key.strip()
        if not re.fullmatch(r"(?:0x)?[0-9a-fA-F]{64}", key):
            raise ValueError("HL_API_KEY must be the generated API-wallet PRIVATE KEY: "
                             "64 hexadecimal characters, optionally prefixed with 0x. "
                             "Do not paste a wallet address, label, command, or Korean placeholder.")
        self.wallet = Account.from_key(key)
        self.nonce = 0

    def payload(self, action, expiry=None):
        self.nonce = max(self.nonce + 1, int(time.time() * 1000))
        signature = sign_l1_action(self.wallet, action, None, self.nonce, expiry, True)
        result = dict(action=action, nonce=self.nonce, signature=signature, vaultAddress=None)
        if expiry is not None:
            result["expiresAfter"] = expiry
        return result


def credentials():
    account = os.environ.get("HL_ACCOUNT", "").strip()
    if not re.fullmatch(r"0x[0-9a-fA-F]{40}", account):
        raise ValueError("HL_ACCOUNT must be your funded OKX MAIN wallet address: "
                         "0x followed by 40 hexadecimal characters.")
    signer = Signer(os.environ.get("HL_API_KEY", ""))
    if signer.wallet.address.lower() == account.lower():
        raise ValueError("Use a dedicated approved API-wallet key, not the funded wallet private key.")
    return account, signer


class Socket:
    def __init__(self, ws):
        self.ws, self.pending, self.events, self.fills = ws, {}, {}, set()
        self.subscribed = set()
        self.book = None
        self.book_received_ns = 0
        self.book_changed = asyncio.Event()
        self.failure = None
        self.reader = asyncio.create_task(self.read())
        self.heartbeat = asyncio.create_task(self.ping())

    async def ping(self):
        while True:
            await asyncio.sleep(20)
            await self.ws.send('{"method":"ping"}')

    async def read(self):
        try:
            async for raw in self.ws:
                received = time.perf_counter_ns()
                msg = json.loads(raw)
                channel, data = msg.get("channel"), msg.get("data")
                if channel == "post":
                    future = self.pending.get(data.get("id"))
                    if future is not None and not future.done():
                        future.set_result((data["response"], received))
                elif channel == "subscriptionResponse":
                    self.subscribed.add(data["subscription"]["type"])
                elif channel == "l2Book":
                    self.book, self.book_received_ns = data, received
                    self.book_changed.set()
                elif channel == "orderUpdates":
                    for item in data:
                        oid = item["order"]["oid"]
                        self.events.setdefault((oid, item["status"]), received)
                        order = item["order"]
                        if item["status"] == "filled" or (
                            "origSz" in order and D(order["sz"]) < D(order["origSz"])
                        ):
                            self.fills.add(oid)
                elif channel == "userFills" and not data.get("isSnapshot"):
                    self.fills.update(fill["oid"] for fill in data.get("fills", []))
        except Exception as exc:
            self.failure = exc
        finally:
            self.failure = self.failure or RuntimeError("Websocket closed")
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(self.failure)

    async def request(self, payload):
        if self.failure:
            raise self.failure
        request_id = uuid.uuid4().int % (2 ** 48)
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        message = json.dumps({"method": "post", "id": request_id,
                              "request": {"type": "action", "payload": payload}}, separators=(",", ":"))
        t0 = time.perf_counter_ns()
        try:
            await self.ws.send(message)
            sent = time.perf_counter_ns()
            response, received = await asyncio.wait_for(future, 8)
            return response, t0, sent, received
        finally:
            self.pending.pop(request_id, None)

    async def fresh_book(self, max_age_ms):
        deadline = time.monotonic() + 8
        while True:
            if self.failure:
                raise self.failure
            if self.book and ms(self.book_received_ns, time.perf_counter_ns()) <= max_age_ms:
                if self.book.get("levels") and all(self.book["levels"]):
                    return self.book, self.book_received_ns
            self.book_changed.clear()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Fresh order book not available; no new order sent")
            await asyncio.wait_for(self.book_changed.wait(), remaining)

    async def close(self):
        for task in (self.reader, self.heartbeat):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def info(session, body):
    async with session.post(REST + "/info", json=body) as response:
        response.raise_for_status()
        return await response.json()


def status(response):
    if response.get("type") != "action":
        raise RuntimeError("WS action error: " + str(response))
    value = response["payload"]
    if value.get("status") != "ok":
        raise RuntimeError("Exchange error: " + str(value))
    return value["response"]["data"]["statuses"][0]


async def assert_flat(session, account, dex, coin):
    state = await info(session, {"type": "clearinghouseState", "user": account, "dex": dex})
    positions = state.get("assetPositions", [])
    if any(p["position"]["coin"] == coin and D(p["position"]["szi"]) != 0 for p in positions):
        raise RuntimeError("Target market has an existing/filled position. Inspect Positions before continuing.")
    orders = await info(session, {"type": "openOrders", "user": account, "dex": dex})
    if any(o["coin"] == coin for o in orders):
        raise RuntimeError("Target market has open orders. Inspect/cancel those before continuing.")
    return state


async def recover(session, signer, asset, cloid, expiry):
    # Only this script's client ID is canceled; no account-wide cancel.
    # Wait past order expiry, then retry so a delayed placement cannot outlive cleanup.
    try:
        for attempt in range(2):
            payload = await asyncio.to_thread(signer.payload, {
                "type": "cancelByCloid", "cancels": [{"asset": asset, "cloid": cloid}]})
            async with session.post(REST + "/exchange", json=payload) as response:
                response.raise_for_status()
                result = await response.json()
                print("Cleanup response:", result, flush=True)
            if attempt == 0:
                await asyncio.sleep(max(0.1, (expiry - int(time.time() * 1000)) / 1000 + 0.5))
    except Exception as exc:
        print("Cleanup could not be confirmed:", type(exc).__name__, str(exc), flush=True)
    print("STOP: check mainnet Open Orders AND Positions; cleanup does not close fills.", flush=True)


async def sample(session, socket, signer, account, dex, coin, asset, decimals, args):
    book, book_received = await socket.fresh_book(args.max_book_age_ms)
    bid, ask = (D(book["levels"][i][0]["px"]) for i in (0, 1))
    if bid >= ask:
        raise RuntimeError("Crossed/locked book; no new order sent")
    reference = bid if args.side == "buy" else ask
    price, qty = plan(reference, decimals, args.notional, args.offset_ticks, args.side)
    cloid = "0x" + uuid.uuid4().hex
    expiry = int(time.time() * 1000) + 5000
    row = dict(utc=datetime.now(timezone.utc).isoformat(), environment="mainnet", coin=coin,
               side=args.side, offset_ticks=args.offset_ticks, best_bid=wire(bid), best_ask=wire(ask),
               book_age_ms="", ack_to_cancel_send_ms="", cancel_after_order_send_ms="",
               open_to_canceled_event_ms="",
               cloid=cloid, oid="", price=wire(price), quantity=wire(qty),
               notional=wire(price * qty), send_ms="", ack_ms="", open_ms="",
               cancel_ack_ms="", canceled_ms="", result="", error="")
    attempted = False
    try:
        action = {"type": "order", "orders": [{"a": asset, "b": args.side == "buy", "p": wire(price),
                  "s": wire(qty), "r": False, "t": {"limit": {"tif": "Alo"}}, "c": cloid}], "grouping": "na"}
        payload = await asyncio.to_thread(signer.payload, action, expiry)
        # Client ID is known now; pre-sign cancel so no signing work delays it after ACK.
        cancel = await asyncio.to_thread(signer.payload, {
            "type": "cancelByCloid", "cancels": [{"asset": asset, "cloid": cloid}]})
        row["book_age_ms"] = ms(book_received, time.perf_counter_ns())
        if row["book_age_ms"] > args.max_book_age_ms:
            raise RuntimeError("Book aged during signing; no order sent")
        attempted = True
        reply, t0, sent, received = await socket.request(payload)
        row.update(send_ms=ms(t0, sent), ack_ms=ms(t0, received))
        placed = status(reply)
        if "resting" not in placed:
            raise RuntimeError("Order did not return resting: " + str(placed))
        oid = placed["resting"]["oid"]
        row["oid"] = oid
        # Cancel before waiting for orderUpdates; the reader already timestamps them.
        order_ack = received
        reply, tc, _, cancel_ack = await socket.request(cancel)
        row.update(cancel_ack_ms=ms(tc, cancel_ack), ack_to_cancel_send_ms=ms(order_ack, tc),
                   cancel_after_order_send_ms=ms(t0, tc))
        if status(reply) != "success":
            raise RuntimeError("Cancel not successful: " + str(reply))
        # 24 info weight/iteration: status(2), fills(20), position(2).
        # No new order until server confirms THIS order canceled and checks complete.
        final = await info(session, {"type": "orderStatus", "user": account, "oid": oid})
        if final.get("status") != "order" or final["order"]["status"] != "canceled":
            raise RuntimeError("Canceled order status not confirmed: " + str(final))
        order = final["order"]["order"]
        if "origSz" in order and D(order["sz"]) < D(order["origSz"]):
            raise RuntimeError("Partial fill found in final order status. Inspect Positions.")
        fills = await info(session, {"type": "userFillsByTime", "user": account,
                                    "startTime": expiry - 15000})
        if oid in socket.fills or any(f["oid"] == oid for f in fills):
            raise RuntimeError("Order filled/partially filled. Inspect Positions now.")
        state = await info(session, {"type": "clearinghouseState", "user": account, "dex": dex})
        if any(p["position"]["coin"] == coin and D(p["position"]["szi"]) != 0
               for p in state.get("assetPositions", [])):
            raise RuntimeError("Nonzero position found. Inspect Positions now.")
        for _ in range(100):
            if (oid, "canceled") in socket.events:
                break
            if socket.failure:
                raise socket.failure
            await asyncio.sleep(0.01)
        for field, key, start in (("open_ms", (oid, "open"), t0), ("canceled_ms", (oid, "canceled"), tc)):
            if key in socket.events:
                row[field] = ms(start, socket.events[key])
        if oid in socket.fills:
            raise RuntimeError("Delayed fill event. Inspect Positions now.")
        if (oid, "open") in socket.events and (oid, "canceled") in socket.events:
            row["open_to_canceled_event_ms"] = ms(socket.events[(oid, "open")], socket.events[(oid, "canceled")])
        row["result"] = "resting_then_canceled_no_fill"
    except BaseException as exc:
        row.update(result="STOP", error=f"{type(exc).__name__}: {exc}")
        if attempted:
            await recover(session, signer, asset, cloid, expiry)
        if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt)):
            row["error"] = "Interrupted; inspect Open Orders and Positions"
    return row


def summary(rows):
    good = [r for r in rows if r["result"] == "resting_then_canceled_no_fill"]
    print(f"Successful samples: {len(good)}/{len(rows)}")
    for column in ("ack_ms", "open_ms", "cancel_ack_ms", "canceled_ms"):
        values = sorted(float(r[column]) for r in good if r[column] != "")
        if values:
            print(f"{column}: n={len(values)} median={statistics.median(values):.3f} "
                  f"p95={values[math.ceil(.95 * len(values))-1]:.3f} ms")
    print("Small-sample p95 is descriptive only; do not interpret 1-20 samples as a stable tail estimate.")


async def run(args):
    # Validate before any network or configuration action. Never print credentials.
    account, signer = credentials() if args.live else (None, None)
    dex = args.coin.split(":")[0] if args.coin and ":" in args.coin else ("" if args.coin else "xyz")
    if args.list_korea:
        dex = "xyz"
    metadata = await asyncio.to_thread(Info, REST, skip_ws=True, perp_dexs=[""] if not dex else ["", dex], timeout=10)
    if args.list_korea:
        for stock, aliases in KOREA_STOCKS.items():
            found = [coin for coin in aliases if coin in metadata.coin_to_asset]
            print(f"{stock}: {', '.join(found) if found else 'not found'}")
        print("READ ONLY: no signing or orders.")
        return
    if not args.coin:
        args.coin = resolve_korea_stock(metadata, args.korea_stock)
        print(f"Korean stock target: {args.korea_stock}; resolved API symbol: {args.coin}")
    try:
        asset = metadata.name_to_asset(args.coin)
        decimals = metadata.asset_to_sz_decimals[asset]
    except KeyError:
        raise RuntimeError("Unknown market. Use --list-korea to inspect live Korean-stock symbols.")
    if 10000 <= asset < 100000:
        raise RuntimeError("Only perpetual markets are supported")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        book = await info(session, {"type": "l2Book", "coin": args.coin})
        reference = book["levels"][0 if args.side == "buy" else 1][0]["px"]
        price, qty = plan(reference, decimals, args.notional, args.offset_ticks, args.side)
        print(f"MAINNET {args.coin}: {args.side.upper()} ALO {args.offset_ticks} ticks away "
              f"px={wire(price)} qty={wire(qty)} value={wire(price*qty)} USD; iterations={args.samples}")
        if not args.live:
            print("READ ONLY: no signing or orders. Add --live only after API-wallet setup.")
            return
        await assert_flat(session, account, dex, args.coin)
        async with websockets.connect(WS, ping_interval=20, open_timeout=10) as ws:
            socket = Socket(ws)
            try:
                for sub in ("orderUpdates", "userFills"):
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": sub, "user": account}}))
                await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": args.coin}}))
                deadline = time.monotonic() + 8
                while len(socket.subscribed) < 3:
                    if socket.failure:
                        raise socket.failure
                    if time.monotonic() > deadline:
                        raise RuntimeError("Subscription timeout; no orders sent")
                    await asyncio.sleep(.01)
                # Configuration action is excluded from measurements.
                payload = await asyncio.to_thread(signer.payload, {
                    "type": "updateLeverage", "asset": asset, "isCross": False, "leverage": 1})
                reply, *_ = await socket.request(payload)
                value = reply.get("payload", {})
                if reply.get("type") != "action" or value.get("status") != "ok":
                    raise RuntimeError("Cannot set isolated 1x; no order sent: " + str(reply))
                print("Target leverage set to isolated 1x. Real orders will now be sent.")
                rows = []
                # Exclusive creation prevents overwriting past measurements.
                with open(args.out, "x", newline="") as output:
                    writer = None
                    for index in range(args.samples):
                        row = await sample(session, socket, signer, account, dex, args.coin, asset, decimals, args)
                        rows.append(row)
                        if writer is None:
                            writer = csv.DictWriter(output, fieldnames=list(row))
                            writer.writeheader()
                        writer.writerow(row)
                        output.flush()
                        print(f"{index+1}/{args.samples}: {row['result']} px={row['price']} "
                              f"ack={row['ack_ms']}ms open={row['open_ms']}ms "
                              f"ACK->cancel={row['ack_to_cancel_send_ms']}ms "
                              f"cancel_ack={row['cancel_ack_ms']}ms", flush=True)
                        if row["result"] == "STOP":
                            print(row["error"], flush=True)
                            summary(rows)
                            print("CSV:", args.out)
                            raise RuntimeError("Measurement stopped; inspect Open Orders and Positions")
                        if index + 1 < args.samples:
                            await asyncio.sleep(args.pause)
                summary(rows)
                print("CSV:", args.out)
            finally:
                await socket.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--korea-stock", choices=sorted(KOREA_STOCKS), default="skhynix", help="Korean common-share perp; default skhynix")
    parser.add_argument("--coin", default=None, help="Optional explicit API symbol overriding --korea-stock")
    parser.add_argument("--list-korea", action="store_true", help="List current Korean-stock API symbols without orders")
    parser.add_argument("--notional", type=float, default=11)
    parser.add_argument("--side", choices=["buy", "sell"], default="buy")
    parser.add_argument("--offset-ticks", type=int, default=5, help="Valid price ticks below best bid (buy) or above best ask (sell)")
    parser.add_argument("--max-book-age-ms", type=float, default=500, help="Maximum locally observed book age before submission")
    parser.add_argument("--samples", "--iterations", dest="samples", type=int, default=1)
    parser.add_argument("--pause", type=float, default=2)
    parser.add_argument("--live", action="store_true", help="Submit REAL mainnet orders")
    parser.add_argument("--out", default="hl_mainnet_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + ".csv")
    args = parser.parse_args()
    if args.list_korea and args.live:
        parser.error("--list-korea is read-only and cannot be combined with --live")
    if not (math.isfinite(args.notional) and 10.5 <= args.notional <= 12):
        parser.error("notional must be between 10.5 and 12 USD")
    if not (1 <= args.samples <= 1000 and math.isfinite(args.pause) and args.pause >= 1.5):
        parser.error("samples/iterations 1..1000; pause >=1.5 seconds (REST verification rate limits)")
    if not 1 <= args.offset_ticks <= 1000:
        parser.error("offset-ticks must be 1..1000")
    if not (math.isfinite(args.max_book_age_ms) and 50 <= args.max_book_age_ms <= 1000):
        parser.error("max-book-age-ms must be 50..1000")
    if args.live and os.path.exists(args.out):
        parser.error("output file already exists; choose another --out")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("Interrupted. Check mainnet Open Orders and Positions.")
        raise SystemExit(130)
    except Exception as exc:
        print(f"STOP {type(exc).__name__}: {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
