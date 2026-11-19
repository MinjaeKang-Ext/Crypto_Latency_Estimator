#!/usr/bin/env python3
"""Binance USD-M LIMIT/GTX order and cancel latency; Python 3.10+.

Install: python -m pip install 'aiohttp>=3.9,<4' 'websockets>=13,<16'
Default is TESTNET and read-only. --live submits orders (simulated on testnet,
real on mainnet). Credentials are environment-specific HMAC key/secret pairs.
No deposit, withdrawal, leverage change, or automatic position closing.

Sources (checked 2026-11-07): developers.binance.com USD-M WebSocket API
General Info, Trade / order.place / order.cancel, User Data Streams.
"""
import argparse
import asyncio
import contextlib
import csv
import hashlib
import hmac
import json
import math
import os
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from urllib.parse import urlencode

import aiohttp
import websockets

D = Decimal
ENV = {
    'testnet': ('https://demo-fapi.binance.com',
                'wss://testnet.binancefuture.com/ws-fapi/v1',
                'wss://demo-fstream.binance.com'),
    'mainnet': ('https://fapi.binance.com',
                'wss://ws-fapi.binance.com/ws-fapi/v1',
                'wss://fstream.binance.com'),
}
FIELDS = ['iteration', 'environment', 'symbol', 'client_order_id', 'order_id',
          'price', 'quantity', 'notional', 'book_age_ms', 'send_ms',
          'ack_ms', 'open_ms', 'cancel_ack_ms', 'canceled_ms',
          'ack_to_cancel_send_ms', 'cancel_after_order_send_ms',
          'order_to_canceled_ms', 'order_send_unix_ns', 'cancel_send_unix_ns',
          'new_recv_unix_ns', 'new_E_ms', 'new_T_ms', 'new_order_T_ms',
          'new_recv_minus_E_ms', 'new_recv_minus_T_ms',
          'cancel_recv_unix_ns', 'cancel_E_ms', 'cancel_T_ms', 'cancel_order_T_ms',
          'cancel_recv_minus_E_ms', 'cancel_recv_minus_T_ms',
          'new_T_minus_send_estimate_ms', 'cancel_T_minus_send_estimate_ms',
          'clock_offset_estimate_ms', 'clock_probe_rtt_ms',
          'executed_quantity', 'final_status', 'result', 'error']


def stamp():
    return time.perf_counter_ns(), time.time_ns()


def ms(a, b):
    return (b - a) / 1e6


def wire(x):
    return format(x, 'f')


def grid(x, step, up=False):
    return (x / step).to_integral_value(rounding=ROUND_CEILING if up else ROUND_FLOOR) * step


class APIError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(f'Binance {code}: {message}')


class Rest:
    def __init__(self, session, base, key='', secret=''):
        self.session, self.base, self.key, self.secret = session, base, key, secret
        self.offset = 0.0
        self.rtt = None

    async def call(self, method, path, params=None, signed=False, keyed=False):
        p = dict(params or {})
        if signed:
            p.update(timestamp=int(time.time() * 1000 + self.offset), recvWindow=5000)
        query = urlencode(sorted(p.items()))
        if signed:
            signature = hmac.new(self.secret.encode('ascii'), query.encode('ascii'), hashlib.sha256).hexdigest()
            query += '&signature=' + signature
        url = self.base + path + ('?' + query if query else '')
        headers = {'X-MBX-APIKEY': self.key} if signed or keyed else {}
        try:
            async with self.session.request(method, url, headers=headers) as r:
                try:
                    data = await r.json(content_type=None)
                except (ValueError, UnicodeError):
                    raise RuntimeError(f'Non-JSON HTTP response: {r.status}') from None
                if r.status >= 400 or (isinstance(data, dict) and data.get('code', 0) < 0):
                    raise APIError(data.get('code', r.status), data.get('msg', 'HTTP error'))
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            # Do not print request URLs, signatures, keys or listenKeys.
            raise RuntimeError('REST transport error: ' + type(e).__name__) from None

    async def probe(self):
        probes = []
        for _ in range(3):
            a, aw = stamp()
            r = await self.call('GET', '/fapi/v1/time')
            b, bw = stamp()
            if abs(ms(aw, bw) - ms(a, b)) > 5:
                raise RuntimeError('Local clock stepped during clock probe')
            probes.append((ms(a, b), r['serverTime'] - (aw + bw) / 2e6))
        self.rtt, self.offset = min(probes)
        if abs(self.offset) > 1000:
            raise RuntimeError('Clock offset exceeds 1 second; check chrony/NTP first')

    def payload(self, method, params):
        p = dict(params, apiKey=self.key,
                 timestamp=int(time.time() * 1000 + self.offset), recvWindow=5000)
        q = urlencode(sorted(p.items()))
        p['signature'] = hmac.new(self.secret.encode('ascii'), q.encode('ascii'), hashlib.sha256).hexdigest()
        ident = uuid.uuid4().hex
        return ident, json.dumps({'id': ident, 'method': method, 'params': p}, separators=(',', ':'))


class TradeSocket:
    """One persistent socket, one outstanding request, dedicated receive task."""
    def __init__(self, ws, timeout):
        self.ws, self.timeout = ws, timeout
        self.pending = {}
        self.failure = None
        self.task = asyncio.create_task(self.reader())

    async def reader(self):
        try:
            async for raw in self.ws:
                received = stamp()  # Before JSON parsing and file I/O.
                obj = json.loads(raw)
                f = self.pending.get(obj.get('id'))
                if f is not None and not f.done():
                    f.set_result((obj, received))
            raise RuntimeError('Trading WebSocket closed')
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.failure = type(e).__name__
            for f in self.pending.values():
                if not f.done():
                    f.set_exception(RuntimeError('Trading WebSocket disconnected'))

    async def request(self, prepared):
        ident, encoded = prepared
        if self.failure:
            raise RuntimeError('Trading WebSocket unavailable')
        f = asyncio.get_running_loop().create_future()
        self.pending[ident] = f
        started = stamp()  # Signing and serialization are outside measured interval.
        try:
            await self.ws.send(encoded)
            sent = time.perf_counter_ns()
            obj, received = await asyncio.wait_for(f, self.timeout)
            return obj, started, sent, received
        finally:
            self.pending.pop(ident, None)

    async def close(self):
        self.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.task


class UserEvents:
    def __init__(self, ws):
        self.ws = ws
        self.watched = set()
        self.events = {}
        self.raw = []
        self.failure = None
        self.task = asyncio.create_task(self.reader())

    async def reader(self):
        try:
            async for raw in self.ws:
                mono, wall = stamp()  # Concurrent receiver, not drained after ACK.
                obj = json.loads(raw)
                if obj.get('e') == 'listenKeyExpired':
                    raise RuntimeError('User stream listenKey expired')
                if obj.get('e') != 'ORDER_TRADE_UPDATE':
                    continue
                o = obj.get('o', {})
                cid = o.get('c')
                if cid not in self.watched:
                    continue
                self.raw.append({'recv_mono_ns': mono, 'recv_unix_ns': wall, 'payload': obj})
                state = o.get('X')
                self.events.setdefault((cid, state), (obj, (mono, wall)))
                if D(str(o.get('z', '0'))) > 0 or o.get('x') == 'TRADE':
                    self.events.setdefault((cid, 'ANY_FILL'), (obj, (mono, wall)))
            raise RuntimeError('User WebSocket closed')
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.failure = type(e).__name__

    async def wait_for(self, cid, states, timeout):
        end = time.perf_counter() + timeout
        while time.perf_counter() < end:
            if all((cid, s) in self.events for s in states):
                return
            if self.failure:
                raise RuntimeError('User stream failed; check orders and positions')
            await asyncio.sleep(.005)
        raise RuntimeError('Missing NEW/CANCELED user event; result is incomplete')

    async def close(self):
        self.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.task


def order_result(obj):
    if obj.get('status') != 200 or not isinstance(obj.get('result'), dict):
        err = obj.get('error', {})
        raise APIError(err.get('code', obj.get('status')), err.get('msg', 'Invalid trading response'))
    return obj['result']


def build_order(spec, book, args):
    f = {x['filterType']: x for x in spec['filters']}
    pf, lot = f['PRICE_FILTER'], f['LOT_SIZE']
    tick, step = D(pf['tickSize']), D(lot['stepSize'])
    bid, ask = D(book['bids'][0][0]), D(book['asks'][0][0])
    if bid <= 0 or ask <= bid:
        raise RuntimeError('Invalid/crossed order book')
    distance = max(tick * args.offset_ticks, bid * D(str(args.offset_bps)) / 10000)
    price = grid(bid - distance, tick)
    if price <= 0 or not D(pf['minPrice']) <= price <= D(pf['maxPrice']):
        raise RuntimeError('Passive price outside PRICE_FILTER')
    qty = grid(max(D(str(args.notional)) / price, D(lot['minQty'])), step, True)
    value = price * qty
    nf = f.get('MIN_NOTIONAL', f.get('NOTIONAL', {}))
    minimum = D(str(nf.get('notional', nf.get('minNotional', '0'))))
    if value < minimum:
        raise RuntimeError(f'Notional {value} < minimum {minimum}; increase --notional explicitly')
    if value > D(str(args.max_notional)) or qty > D(lot['maxQty']):
        raise RuntimeError('Order exceeds --max-notional or LOT_SIZE; no order submitted')
    if nf.get('maxNotional') and value > D(str(nf['maxNotional'])):
        raise RuntimeError('Order exceeds NOTIONAL filter')
    # Remaining reference-price filters are enforced by the exchange; never
    # silently move a rejected order closer to the touch or retry its placement.
    return price, qty, value


async def assert_flat(rest, symbol):
    orders = await rest.call('GET', '/fapi/v1/openOrders', {'symbol': symbol}, signed=True)
    if orders:
        raise RuntimeError('Target symbol already has open orders; no existing orders were canceled')
    positions = await rest.call('GET', '/fapi/v2/positionRisk', {'symbol': symbol}, signed=True)
    if any(p.get('symbol') == symbol and D(p['positionAmt']) != 0 for p in positions):
        raise RuntimeError('Target symbol has a position; stop and inspect account')


async def verify_canceled(rest, symbol, cid, timeout=8):
    end = time.perf_counter() + timeout
    last = None
    while time.perf_counter() < end:
        try:
            last = await rest.call('GET', '/fapi/v1/order', {'symbol': symbol, 'origClientOrderId': cid}, signed=True)
        except APIError as e:
            if e.code != -2013:
                raise
            await asyncio.sleep(.4)
            continue
        if D(last.get('executedQty', '0')) > 0:
            raise RuntimeError('Fill detected. STOP: inspect Positions; no position is auto-closed')
        if last.get('status') == 'CANCELED':
            return last
        if last.get('status') in ('FILLED', 'EXPIRED', 'EXPIRED_IN_MATCH'):
            raise RuntimeError('Unexpected terminal order status: ' + last['status'])
        await asyncio.sleep(.4)
    raise RuntimeError('Cancellation not confirmed before deadline: ' + str((last or {}).get('status', 'not found')))


async def recover(rest, symbol, cid):
    """Only this run's client ID. Never retry a new-order request."""
    end = time.perf_counter() + 12
    final = None
    while time.perf_counter() < end:
        try:
            # Cancel first even when NEW response was lost; keyed by known ID.
            await rest.call('DELETE', '/fapi/v1/order', {'symbol': symbol, 'origClientOrderId': cid}, signed=True)
        except (APIError, RuntimeError):
            pass
        try:
            final = await rest.call('GET', '/fapi/v1/order', {'symbol': symbol, 'origClientOrderId': cid}, signed=True)
            if final.get('status') in ('CANCELED', 'FILLED', 'EXPIRED', 'EXPIRED_IN_MATCH'):
                return {'status': final['status'], 'executedQty': final.get('executedQty'),
                        'note': 'Cleanup does not close any fills'}
        except (APIError, RuntimeError):
            pass
        await asyncio.sleep(.7)
    return {'status': 'UNCONFIRMED', 'note': 'Check Open Orders AND Positions manually'}


def add_event(row, prefix, entry, started, offset):
    obj, (mono, wall) = entry
    row[prefix + '_recv_unix_ns'] = wall
    row[prefix + '_E_ms'] = obj.get('E', '')
    row[prefix + '_T_ms'] = obj.get('T', '')
    row[prefix + '_order_T_ms'] = obj.get('o', {}).get('T', '')
    for key in ('E', 'T'):
        if obj.get(key) is not None:
            row[f'{prefix}_recv_minus_{key}_ms'] = (wall - int(obj[key]) * 1000000) / 1e6
    if started and obj.get('T') is not None:
        row[f'{prefix}_T_minus_send_estimate_ms'] = obj['T'] - (started[1] / 1e6 + offset)
    if started:
        row['open_ms' if prefix == 'new' else 'canceled_ms'] = ms(started[0], mono)


async def sample(args, rest, trade, events, spec, index, rawlog):
    cid = 'lat_' + uuid.uuid4().hex[:28]
    row = dict.fromkeys(FIELDS, '')
    row.update(iteration=index, environment=args.env, symbol=args.symbol,
               client_order_id=cid, result='STOP', clock_offset_estimate_ms=rest.offset,
               clock_probe_rtt_ms=rest.rtt)
    events.watched.add(cid)
    attempted = False
    order_started = cancel_started = None
    interrupted = False
    records = []
    try:
        if events.failure:
            raise RuntimeError('User stream unavailable')
        book = await rest.call('GET', '/fapi/v1/depth', {'symbol': args.symbol, 'limit': 5})
        book_received = time.perf_counter_ns()
        price, qty, value = build_order(spec, book, args)
        row.update(price=wire(price), quantity=wire(qty), notional=wire(value))
        place = rest.payload('order.place', {'symbol': args.symbol, 'side': 'BUY',
                             'type': 'LIMIT', 'timeInForce': 'GTX', 'quantity': wire(qty),
                             'price': wire(price), 'newClientOrderId': cid, 'newOrderRespType': 'ACK'})
        cancel = rest.payload('order.cancel', {'symbol': args.symbol, 'origClientOrderId': cid})
        row['book_age_ms'] = ms(book_received, time.perf_counter_ns())
        if row['book_age_ms'] > 250:
            raise RuntimeError('Book aged >250ms while preparing order; no order submitted')
        attempted = True
        reply, order_started, sent, ack = await trade.request(place)
        row.update(order_send_unix_ns=order_started[1], send_ms=ms(order_started[0], sent),
                   ack_ms=ms(order_started[0], ack[0]))
        records.append({'kind': 'order_reply', 'recv_mono_ns': ack[0], 'recv_unix_ns': ack[1], 'payload': reply})
        placed = order_result(reply)
        row['order_id'] = placed.get('orderId', '')
        # Immediately cancel the known ID, before waiting for private NEW events.
        # Refresh the cancel signature if the initial response was slow.
        if ms(order_started[0], ack[0]) > 1500:
            cancel = rest.payload('order.cancel', {'symbol': args.symbol, 'origClientOrderId': cid})
        creply, cancel_started, _, cack = await trade.request(cancel)
        row.update(cancel_send_unix_ns=cancel_started[1], cancel_ack_ms=ms(cancel_started[0], cack[0]),
                   ack_to_cancel_send_ms=ms(ack[0], cancel_started[0]),
                   cancel_after_order_send_ms=ms(order_started[0], cancel_started[0]))
        records.append({'kind': 'cancel_reply', 'recv_mono_ns': cack[0], 'recv_unix_ns': cack[1], 'payload': creply})
        canceled = order_result(creply)
        row.update(final_status=canceled.get('status', ''), executed_quantity=canceled.get('executedQty', ''))
        if canceled.get('status') != 'CANCELED' or D(canceled.get('executedQty', '0')) != 0:
            raise RuntimeError('Cancel response is not CANCELED with zero executed quantity')
        if placed.get('status') not in (None, 'NEW') or D(placed.get('executedQty', '0')) > 0:
            raise RuntimeError('Initial order was not an unfilled NEW order')
        final = await verify_canceled(rest, args.symbol, cid)
        row.update(final_status=final['status'], executed_quantity=final['executedQty'])
        await events.wait_for(cid, ['NEW', 'CANCELED'], args.event_timeout)
        if (cid, 'ANY_FILL') in events.events:
            raise RuntimeError('Fill event detected; inspect Positions')
        await assert_flat(rest, args.symbol)
        row['result'] = 'resting_then_canceled_no_fill'
    except (Exception, asyncio.CancelledError, KeyboardInterrupt) as e:
        interrupted = isinstance(e, (asyncio.CancelledError, KeyboardInterrupt))
        row['error'] = 'Interrupted' if interrupted else str(e)
        if attempted:
            cleanup_task = asyncio.create_task(recover(rest, args.symbol, cid))
            try:
                cleanup = await asyncio.shield(cleanup_task)
                records.append({'kind': 'cleanup', 'client_order_id': cid, 'payload': cleanup})
                print('Cleanup:', cleanup, flush=True)
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await cleanup_task
        print('STOP: inspect Open Orders AND Positions. Cleanup does not close fills.', flush=True)
    finally:
        # Preserve event measurements even if later verification failed.
        for prefix, state, start in [('new', 'NEW', order_started), ('cancel', 'CANCELED', cancel_started)]:
            if (cid, state) in events.events:
                add_event(row, prefix, events.events[(cid, state)], start, rest.offset)
        if order_started and (cid, 'CANCELED') in events.events:
            row['order_to_canceled_ms'] = ms(order_started[0], events.events[(cid, 'CANCELED')][1][0])
        # File I/O is outside the measured order/cancel request intervals.
        for rec in records + events.raw:
            rawlog.write(json.dumps(rec, separators=(',', ':')) + '\n')
        rawlog.flush()
        events.raw.clear()
        events.watched.discard(cid)
    return row, interrupted


def summarize(rows):
    good = [r for r in rows if r['result'] == 'resting_then_canceled_no_fill']
    out = {'attempted_iterations': len(rows), 'successful_samples': len(good),
           'quantile_method': 'nearest rank; median averages two middle values',
           'note': 'Same-host monotonic elapsed time. Testnet is not mainnet. Small samples do not establish tail latency.',
           'metrics_ms': {}}
    for c in ('ack_ms', 'open_ms', 'cancel_ack_ms', 'canceled_ms', 'ack_to_cancel_send_ms'):
        v = sorted(float(r[c]) for r in good if r[c] != '')
        if v:
            out['metrics_ms'][c] = {'n': len(v), 'mean': statistics.mean(v),
                'median': statistics.median(v), 'std_sample': statistics.stdev(v) if len(v) > 1 else None,
                'min': v[0], 'p90': v[math.ceil(.9 * len(v)) - 1],
                'p95': v[math.ceil(.95 * len(v)) - 1], 'p99': v[math.ceil(.99 * len(v)) - 1], 'max': v[-1]}
    return out


async def keepalive(rest, events):
    while True:
        await asyncio.sleep(15 * 60)
        try:
            await rest.call('PUT', '/fapi/v1/listenKey', keyed=True)
        except Exception:
            events.failure = 'ListenKeyKeepaliveFailed'
            return


async def run(args):
    base, trade_url, stream_base = ENV[args.env]
    prefix = 'BINANCE_TESTNET' if args.env == 'testnet' else 'BINANCE_MAINNET'
    key = os.environ.get(prefix + '_API_KEY', '').strip() if args.live else ''
    secret = os.environ.get(prefix + '_API_SECRET', '').strip() if args.live else ''
    if args.live and (not key or not secret):
        raise RuntimeError(f'Set {prefix}_API_KEY and {prefix}_API_SECRET on the server')
    if args.live:
        if not key.isascii() or not secret.isascii():
            raise RuntimeError('API credentials must be ASCII; remove copied placeholders')
    rows = []
    out = Path(args.out)
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as session:
        rest = Rest(session, base, key, secret)
        await rest.probe()
        meta = await rest.call('GET', '/fapi/v1/exchangeInfo')
        spec = next((s for s in meta['symbols'] if s['symbol'] == args.symbol), None)
        if spec is None or spec.get('status') != 'TRADING' or spec.get('contractType') != 'PERPETUAL':
            raise RuntimeError('Symbol is not a trading perpetual in this environment; no automatic substitution')
        if spec.get('quoteAsset') != 'USDT':
            raise RuntimeError('Only USDT-quoted perpetuals are supported')
        book = await rest.call('GET', '/fapi/v1/depth', {'symbol': args.symbol, 'limit': 5})
        price, qty, value = build_order(spec, book, args)
        print(f'{args.env.upper()} {args.symbol}: BUY LIMIT GTX px={wire(price)} qty={wire(qty)} notional={wire(value)} USDT', flush=True)
        print(f'Clock probe: offset={rest.offset:.3f}ms RTT={rest.rtt:.3f}ms (heuristic, not exact one-way latency)', flush=True)
        if not args.live:
            print('READ ONLY: no orders sent. Add --live to submit orders in this environment.')
            return
        mode = await rest.call('GET', '/fapi/v1/positionSide/dual', signed=True)
        if mode.get('dualSidePosition'):
            raise RuntimeError('Hedge mode is unsupported; use an isolated test account in one-way mode')
        await assert_flat(rest, args.symbol)
        out.mkdir(parents=True, exist_ok=False)
        config = {'started_utc': datetime.now(timezone.utc).isoformat(), 'arguments': vars(args),
                  'instrument': spec, 'rest_url': base, 'trade_ws_url': trade_url,
                  'user_stream_path': args.user_stream_path,
                  'measurement': 'app send-start to app receive, signing/serialization excluded; CSV missing is not zero',
                  'status': 'running'}
        (out / 'run.json').write_text(json.dumps(config, indent=2), encoding='utf-8')
        listen = await rest.call('POST', '/fapi/v1/listenKey', keyed=True)
        listen_key = listen['listenKey']
        user_url = stream_base + ('/private/ws/' if args.user_stream_path == 'private' else '/ws/') + listen_key
        try:
            print('Connecting trading and user event sockets before sending any orders...', flush=True)
            async with websockets.connect(user_url, ping_interval=20, ping_timeout=20, open_timeout=10) as uws:
                events = UserEvents(uws)
                keeper = asyncio.create_task(keepalive(rest, events))
                try:
                    async with websockets.connect(trade_url, ping_interval=20, ping_timeout=20, open_timeout=10) as tws:
                        trade = TradeSocket(tws, args.timeout)
                        try:
                            await asyncio.sleep(.25)  # Settle connections; outside timed intervals.
                            with (out / 'latency.csv').open('w', newline='', encoding='utf-8') as fh, (out / 'raw.jsonl').open('w', encoding='utf-8') as raw:
                                writer = csv.DictWriter(fh, fieldnames=FIELDS)
                                writer.writeheader()
                                for i in range(1, args.samples + 1):
                                    if i > 1:
                                        await asyncio.sleep(args.pause)
                                        await rest.probe()
                                    row, interrupted = await sample(args, rest, trade, events, spec, i, raw)
                                    # Redact credentials defensively in any error from an external dependency.
                                    for sensitive in (key, secret, listen_key):
                                        if sensitive:
                                            row['error'] = row['error'].replace(sensitive, '[REDACTED]')
                                    rows.append(row)
                                    writer.writerow(row)
                                    fh.flush()
                                    parts = ' '.join(f'{k}={row[k]:.3f}ms' if isinstance(row[k], (int, float)) else f'{k}=missing' for k in ('ack_ms', 'open_ms', 'cancel_ack_ms', 'canceled_ms'))
                                    print(f'{i}/{args.samples} {row["result"]}: {parts}', flush=True)
                                    if row['result'] != 'resting_then_canceled_no_fill':
                                        print(row['error'], flush=True)
                                        config['status'] = 'interrupted' if interrupted else 'stopped'
                                        break
                                else:
                                    config['status'] = 'completed'
                        finally:
                            await trade.close()
                finally:
                    keeper.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await keeper
                    await events.close()
        except Exception as e:
            config['status'] = 'stopped'
            # URLs from connection exceptions may contain the listenKey.
            raise RuntimeError('Connection/run failed: ' + type(e).__name__ + '. See run.json; check endpoint path and API permissions.') from None
        finally:
            config['finished_utc'] = datetime.now(timezone.utc).isoformat()
            config['iterations_written'] = len(rows)
            (out / 'run.json').write_text(json.dumps(config, indent=2), encoding='utf-8')
            summary = summarize(rows)
            (out / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
            print(f'Successful samples: {summary["successful_samples"]}/{len(rows)}; output: {out}', flush=True)
            for field, stat in summary['metrics_ms'].items():
                print(f'{field}: median={stat["median"]:.3f} p95={stat["p95"]:.3f} p99={stat["p99"]:.3f}ms n={stat["n"]}')
            # Do not DELETE the account listenKey: another application may share it.
        if config['status'] != 'completed':
            raise RuntimeError('Run stopped; inspect latency.csv, Open Orders and Positions before retrying')


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--env', choices=ENV, default='testnet')
    p.add_argument('--symbol', required=True, help='Explicit USDT perpetual symbol; never substituted')
    p.add_argument('--samples', '--iterations', type=int, default=1)
    p.add_argument('--notional', type=float, default=110, help='Target order value, not margin (default 110 USDT)')
    p.add_argument('--max-notional', type=float, default=150, help='Hard maximum after quantity rounding')
    p.add_argument('--offset-bps', type=float, default=100, help='Passive buy distance in basis points; 100 = 1%%')
    p.add_argument('--offset-ticks', type=int, default=5, help='Minimum distance in ticks; larger of bps/ticks used')
    p.add_argument('--pause', type=float, default=2)
    p.add_argument('--timeout', type=float, default=8)
    p.add_argument('--event-timeout', type=float, default=5)
    p.add_argument('--user-stream-path', choices=['private', 'legacy'], default='private', help='Use legacy only if the selected testnet does not support /private/ws')
    p.add_argument('--live', action='store_true', help='Submit orders: simulated on testnet, REAL on mainnet')
    p.add_argument('--out', default='binance_orders_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    a = p.parse_args()
    a.symbol = a.symbol.upper()
    if not a.symbol.isascii() or not a.symbol.isalnum():
        p.error('symbol must be ASCII letters/digits')
    if not 1 <= a.samples <= 1000:
        p.error('samples must be 1..1000')
    for name in ('notional', 'max_notional', 'offset_bps', 'pause', 'timeout', 'event_timeout'):
        if not math.isfinite(getattr(a, name)):
            p.error(name + ' must be finite')
    if not 0 < a.notional <= a.max_notional or not 1 <= a.offset_ticks <= 100000 or not 1 <= a.offset_bps <= 500:
        p.error('require 0 < notional <= max-notional, offset-ticks 1..100000, offset-bps 1..500')
    if a.pause < 1 or not 2 <= a.timeout <= 15 or not 2 <= a.event_timeout <= 15:
        p.error('pause >=1, timeout/event-timeout 2..15 seconds')
    if a.live and Path(a.out).exists():
        p.error('output directory exists; choose a fresh --out')
    try:
        asyncio.run(run(a))
    except KeyboardInterrupt:
        print('Interrupted. Inspect Open Orders and Positions.', file=sys.stderr)
        return 130
    except Exception as e:
        message = str(e)
        for name in ('BINANCE_TESTNET_API_KEY', 'BINANCE_TESTNET_API_SECRET', 'BINANCE_MAINNET_API_KEY', 'BINANCE_MAINNET_API_SECRET'):
            value = os.environ.get(name, '').strip()
            if value:
                message = message.replace(value, '[REDACTED]')
        print('STOP ' + message, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
