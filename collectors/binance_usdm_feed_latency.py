#!/usr/bin/env python3
"""Receive Binance USD-M MAINNET market data; no keys, balances or orders.

Install: python -m pip install 'websockets>=13,<17' aiohttp
Example: python binance_usdm_feed_latency.py --symbol SKHYNIXUSDT --seconds 600
         python binance_usdm_feed_latency.py --symbol BTCUSDT --seconds 30

Three independent sockets: /public depth@100ms, /public bookTicker,
/market aggTrade. aggTrade is aggregated trade data, NOT individual fills.
Receive timestamps are application-level: immediately after ws.recv(), before
JSON parsing or file IO. They include OS/TLS/WebSocket/Python scheduling delay.
E and T are exchange millisecond timestamps; ns local storage does not make
the exchange timestamps nanosecond-accurate. E is event time, NOT a guaranteed
wire-transmit timestamp. recv-E is an apparent event delivery age, not RTT.

NTP/chrony synchronization is essential. /fapi/v1/time midpoint estimates are
logged as diagnostics only; REST clocks and market-data clocks may differ and
network paths may be asymmetric. Raw and heuristically adjusted ages are both
saved. The REST estimate is NOT a precise one-way latency calibration.

Output: latency.csv (one row/message), raw.jsonl (all raw event payloads),
run.json (symbol/config/clock samples/status), summary.json. Fresh directory per
run, fail on disconnect or full writer queue rather than silently omit messages.
L2 is raw incremental depth, not a reconstructed book. pu continuity is checked;
REST snapshot + official snapshot synchronization is required to rebuild L2.
No deduplication/resampling. Price and quantity strings are preserved in raw data.
"""

import argparse
import asyncio
import csv
import json
import math
import queue
import statistics
import threading
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import websockets

REST = "https://fapi.binance.com"
WSS = "wss://fstream.binance.com"
FIELDS = ["stream", "recv_unix_ns", "recv_mono_ns", "E_ms", "T_ms", "recv_minus_E_ms",
          "recv_minus_T_ms", "E_minus_T_ms", "http_offset_estimate_ms", "http_probe_rtt_ms",
          "rest_adjusted_recv_minus_E_ms", "interarrival_ms", "clock_step_ms", "writer_queue_ms",
          "depth_sequence_gap", "u", "U", "pu", "aggregate_trade_id", "first_trade_id", "last_trade_id",
          "price", "quantity", "best_bid", "bid_quantity", "best_ask", "ask_quantity"]


def summarize(values):
    v = sorted(values)
    if not v:
        return {"n": 0}
    return dict(n=len(v), min=min(v), median=statistics.median(v),
                p95=v[math.ceil(.95*len(v))-1], p99=v[math.ceil(.99*len(v))-1],
                max=max(v), negative_n=sum(x < 0 for x in v))


def extract(stream, raw, wall, mono, calibration, previous, last_depth_u):
    d = json.loads(raw)
    d = d.get("data", d)
    if "code" in d:
        raise RuntimeError("Exchange returned: " + str(d))
    kind = d.get("e")
    if kind not in ("depthUpdate", "bookTicker", "aggTrade"):
        return None, last_depth_u
    E, T = d.get("E"), d.get("T")
    # Subtract integer epoch timestamps before converting to float.
    age_e = (wall-int(E)*1_000_000)/1e6 if E is not None else ""
    age_t = (wall-int(T)*1_000_000)/1e6 if T is not None else ""
    old = previous.get(stream)
    previous[stream] = (wall, mono)
    row = dict.fromkeys(FIELDS, "")
    row.update(stream=stream, recv_unix_ns=wall, recv_mono_ns=mono, E_ms=E or "", T_ms=T or "",
               recv_minus_E_ms=age_e, recv_minus_T_ms=age_t,
               E_minus_T_ms=int(E)-int(T) if E is not None and T is not None else "",
               interarrival_ms=(mono-old[1])/1e6 if old else "",
               clock_step_ms=((wall-old[0])-(mono-old[1]))/1e6 if old else "",
               writer_queue_ms=(time.perf_counter_ns()-mono)/1e6)
    if calibration:
        row.update(http_offset_estimate_ms=calibration["offset_ms"], http_probe_rtt_ms=calibration["rtt_ms"],
                   rest_adjusted_recv_minus_E_ms=age_e+calibration["offset_ms"] if age_e != "" else "")
    if kind == "depthUpdate":
        row.update(U=d.get("U", ""), u=d.get("u", ""), pu=d.get("pu", ""),
                   depth_sequence_gap=int(last_depth_u is not None and d.get("pu") != last_depth_u))
        last_depth_u = d.get("u")
    elif kind == "bookTicker":
        row.update(u=d.get("u", ""), best_bid=d.get("b", ""), bid_quantity=d.get("B", ""),
                   best_ask=d.get("a", ""), ask_quantity=d.get("A", ""))
    else:
        row.update(aggregate_trade_id=d.get("a", ""), first_trade_id=d.get("f", ""),
                   last_trade_id=d.get("l", ""), price=d.get("p", ""), quantity=d.get("q", ""))
    return row, last_depth_u


class Writer:
    def __init__(self, folder):
        self.folder, self.q, self.done = folder, queue.Queue(maxsize=50000), threading.Event()
        self.failure, self.counts, self.stats = None, Counter(), {}
        self.gaps = self.steps = 0
        self.thread = threading.Thread(target=self.work, daemon=True)

    def work(self):
        previous, last_u = {}, None
        try:
            with (self.folder/"latency.csv").open("w", newline="") as cf, (self.folder/"raw.jsonl").open("w") as rf:
                writer=csv.DictWriter(cf, fieldnames=FIELDS);writer.writeheader()
                flushed=time.monotonic()
                while not self.done.is_set() or not self.q.empty():
                    try: stream,raw,wall,mono,cal = self.q.get(timeout=.2)
                    except queue.Empty: continue
                    # Always retain the raw input and its original receive timestamp.
                    rf.write(json.dumps(dict(stream=stream, recv_unix_ns=wall, recv_mono_ns=mono,
                                             payload=json.loads(raw)), separators=(",", ":"))+"\n")
                    row,last_u=extract(stream,raw,wall,mono,cal,previous,last_u)
                    if row:
                        writer.writerow(row);self.counts[stream]+=1
                        self.gaps+=int(row["depth_sequence_gap"] or 0)
                        self.steps+=int(row["clock_step_ms"] != "" and abs(row["clock_step_ms"])>5)
                        for field in ("recv_minus_E_ms","recv_minus_T_ms","E_minus_T_ms","interarrival_ms"):
                            if row[field] != "":self.stats.setdefault((stream,field),[]).append(row[field])
                    if time.monotonic()-flushed>=1:cf.flush();rf.flush();flushed=time.monotonic()
        except Exception as exc:
            self.failure=exc


async def get(session, path):
    async with session.get(REST+path) as response:
        if response.status != 200:
            text=(await response.text())[:300]
            raise RuntimeError(f"Binance HTTP {response.status}: {text}. No alternate-region bypass is attempted.")
        return await response.json()


async def probe(session, count=5):
    samples=[]
    for _ in range(count):
        w0=time.time_ns();m0=time.perf_counter_ns()
        d=await get(session,"/fapi/v1/time")
        m1=time.perf_counter_ns();w1=time.time_ns()
        rtt=(m1-m0)/1e6
        samples.append(dict(offset_ms=d["serverTime"]-(w0+w1)/2e6, rtt_ms=rtt,
                            measured_unix_ns=w1, clock_step_ms=((w1-w0)-(m1-m0))/1e6))
        await asyncio.sleep(.05)
    stable=[x for x in samples if abs(x["clock_step_ms"])<5]
    if not stable:raise RuntimeError("Local clock stepped during all time probes. Check chrony.")
    return min(stable,key=lambda x:x["rtt_ms"]),samples


async def receive(name,url,writer,clock):
    async with websockets.connect(url,ping_interval=20,ping_timeout=20,open_timeout=15,
                                  compression=None,max_queue=16) as ws:
        print("Connected:",name,flush=True)
        async for raw in ws:
            wall=time.time_ns();mono=time.perf_counter_ns()
            try:writer.q.put_nowait((name,raw,wall,mono,clock[0]))
            except queue.Full:raise RuntimeError("Writer queue full: stopped instead of dropping market data.")
            if writer.failure:raise writer.failure
    raise RuntimeError(f"{name} disconnected; stopped. Restart into a new directory.")


async def refresh_clock(session,clock,manifest):
    while True:
        await asyncio.sleep(60)
        try:
            best,samples=await probe(session,3);clock[0]=best
            manifest["clock_probes"].extend(samples)
        except Exception as exc:
            manifest.setdefault("clock_probe_errors",[]).append(str(exc))


async def run(args):
    folder=Path(args.out);folder.mkdir(parents=True,exist_ok=False)
    manifest=dict(symbol=args.symbol, environment="USD-M mainnet", application_receive_timestamp=True,
                  duration_seconds=args.seconds, start_utc=datetime.now(timezone.utc).isoformat(),
                  clock_probes=[], status="initializing", depth_is_incremental=True)
    writer=Writer(folder);tasks=[];started=False
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            metadata=await get(session,"/fapi/v1/exchangeInfo")
            symbol=next((s for s in metadata["symbols"] if s["symbol"]==args.symbol),None)
            if not symbol or symbol["status"]!="TRADING":
                candidates=[s["symbol"] for s in metadata["symbols"] if "SKH" in s["symbol"]]
                raise RuntimeError(f"Symbol is not currently TRADING. SK hynix candidates: {candidates}")
            manifest["instrument"]=symbol
            best,samples=await probe(session);manifest["clock_probes"].extend(samples);clock=[best]
            print(f"REST clock diagnostic: offset={best['offset_ms']:.3f}ms RTT={best['rtt_ms']:.3f}ms")
            print("These offsets are heuristic. Synchronize the EC2 clock using NTP/chrony.")
            writer.thread.start();started=True
            s=args.symbol.lower()
            streams={"l2":f"{WSS}/public/ws/{s}@depth@100ms",
                     "bbo":f"{WSS}/public/ws/{s}@bookTicker",
                     "aggtrade":f"{WSS}/market/ws/{s}@aggTrade"}
            manifest["streams"]=streams
            tasks=[asyncio.create_task(receive(name,url,writer,clock)) for name,url in streams.items()]
            clock_task=asyncio.create_task(refresh_clock(session,clock,manifest));tasks.append(clock_task)
            stop=asyncio.create_task(asyncio.sleep(args.seconds));tasks.append(stop)
            manifest["status"]="collecting"
            while not stop.done():
                done,_=await asyncio.wait(tasks,timeout=5,return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task is not stop:await task
                if writer.failure:raise writer.failure
                print("Received",dict(writer.counts),"writer_queue",writer.q.qsize(),flush=True)
            missing=set(streams)-set(writer.counts)
            manifest["missing_streams"]=sorted(missing)
            manifest["status"]="completed" if not missing else "completed_with_missing_streams"
    except BaseException as exc:
        manifest.update(status="stopped",error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        writer.done.set()
        if started:await asyncio.to_thread(writer.thread.join)
        if writer.failure:manifest["writer_error"]=str(writer.failure);manifest["status"]="writer_failed"
        manifest.update(counts=dict(writer.counts),depth_sequence_gaps=writer.gaps,
                        clock_steps_over_5ms=writer.steps,end_utc=datetime.now(timezone.utc).isoformat())
        report={stream:{field:summarize(values) for (st,field),values in writer.stats.items() if st==stream}
                for stream in writer.counts}
        (folder/"run.json").write_text(json.dumps(manifest,indent=2))
        (folder/"summary.json").write_text(json.dumps(report,indent=2))
        for stream,fields in report.items():print(stream,json.dumps(fields.get("recv_minus_E_ms",{})))
        print("Status:",manifest["status"],"Output:",folder.resolve())
        if manifest.get("missing_streams"):print("WARNING: missing streams may mean no activity, unsupported stream or an endpoint problem.")


def main():
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--symbol",default="BTCUSDT",type=str.upper)
    p.add_argument("--seconds",type=int,default=600)
    p.add_argument("--out",default="binance_feed_"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
    a=p.parse_args()
    if not 10<=a.seconds<=3600:p.error("seconds must be 10..3600")
    if not a.symbol.isascii() or not a.symbol.isalnum():p.error("symbol must be ASCII letters/digits")
    try:asyncio.run(run(a))
    except KeyboardInterrupt:print("Stopped by user; queued data flushed.")
    except Exception as exc:print(f"STOP {type(exc).__name__}: {exc}");raise SystemExit(1)


if __name__=="__main__":main()
