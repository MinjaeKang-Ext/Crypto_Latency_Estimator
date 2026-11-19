#!/usr/bin/env python3
"""Hyperliquid MAINNET public L2/BBO/trades recorder. No wallet, key or orders.

Install: python -m pip install 'websockets>=13,<17' aiohttp
Run: python hl_marketdata_latency.py --coin xyz:SKHX --seconds 600 --out hl_skhynix_01
Test: python hl_marketdata_latency.py --coin BTC --seconds 30 --out hl_btc_check

Three independent WebSockets. L2 default fast=true (5 levels/side);
--l2-mode full requests fast=false (20 levels/side). No price aggregation.
L2 messages are snapshots, NOT Binance-style depth diffs. No sequence number
is exposed here, so absence of drops/reconnects cannot prove full market history.
Native public feeds have their own throttling and block scheduling.

All recv times are captured immediately after recv(), before JSON parsing or
file I/O. recv-minus-time is an apparent exchange timestamp-to-app receive age,
NOT pure network latency or RTT. Native time semantics differ from Binance E/T.
NTP/chrony synchronization is required for meaningful cross-host comparisons.
Local nanosecond storage does not improve the exchange's millisecond precision.

First DATA message on EACH socket is flagged initial_message and excluded from
latency summaries, especially public trades' initial historical batch. Raw data
and CSV retain those rows. Duplicate trades are retained/flagged and excluded
from latency summaries using (time,coin,tid). No resampling is performed.

Outputs: raw.jsonl (one row/WS message, including controls), latency.csv
(one row/book or BBO; one row/individual trade), summary.json, run.json.
Independent bounded file writer; fail on queue overflow or disconnect rather
than silently drop data or merge reconnect replay into live timing statistics.
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

REST="https://api.hyperliquid.xyz/info"
WS="wss://api.hyperliquid.xyz/ws"
FIELDS=["stream","coin","message_id","recv_unix_ns","recv_mono_ns","exchange_time_ms",
        "recv_minus_exchange_ms","interarrival_message_ms","clock_step_ms","writer_queue_ms",
        "initial_message","duplicate_trade","included_in_latency_summary","batch_trade_index",
        "trades_in_batch","tid","hash","side","price","quantity","best_bid","bid_quantity",
        "best_ask","ask_quantity","bid_levels","ask_levels"]


def summarize(values):
    v=sorted(values)
    if not v:return {"n":0}
    return dict(n=len(v),min=v[0],median=statistics.median(v),p95=v[math.ceil(.95*len(v))-1],
                p99=v[math.ceil(.99*len(v))-1],max=v[-1],negative_n=sum(x<0 for x in v))


def rows_from_message(stream,msg,wall,mono,previous,seen_trades,message_id):
    channel,data=msg.get("channel"),msg.get("data")
    if channel not in ("l2Book","bbo","trades"):return []
    initial=stream not in previous
    old=previous.get(stream);previous[stream]=(wall,mono)
    gap=(mono-old[1])/1e6 if old else ""
    step=((wall-old[0])-(mono-old[1]))/1e6 if old else ""
    entries=data if channel=="trades" else [data]
    result=[]
    for index,item in enumerate(entries):
        timestamp=item.get("time")
        row=dict.fromkeys(FIELDS,"")
        row.update(stream=stream,coin=item.get("coin",""),message_id=message_id,
                   recv_unix_ns=wall,recv_mono_ns=mono,exchange_time_ms=timestamp,
                   recv_minus_exchange_ms=(wall-int(timestamp)*1_000_000)/1e6 if timestamp is not None else "",
                   interarrival_message_ms=gap if index==0 else "",clock_step_ms=step if index==0 else "",
                   writer_queue_ms=(time.perf_counter_ns()-mono)/1e6,
                   initial_message=int(initial),duplicate_trade=0,
                   included_in_latency_summary=int(not initial and timestamp is not None))
        if channel=="trades":
            identity=(item.get("time"),item.get("coin"),item.get("tid"))
            duplicate=identity in seen_trades
            seen_trades.add(identity)
            row.update(tid=item.get("tid",""),hash=item.get("hash",""),side=item.get("side",""),
                       price=item.get("px",""),quantity=item.get("sz",""),batch_trade_index=index,
                       trades_in_batch=len(entries),duplicate_trade=int(duplicate))
            if duplicate:row["included_in_latency_summary"]=0
        else:
            sides=item.get("levels") if channel=="l2Book" else item.get("bbo")
            bid=(sides[0][0] if sides[0] else None) if channel=="l2Book" else sides[0]
            ask=(sides[1][0] if sides[1] else None) if channel=="l2Book" else sides[1]
            row.update(best_bid=bid["px"] if bid else "",bid_quantity=bid["sz"] if bid else "",
                       best_ask=ask["px"] if ask else "",ask_quantity=ask["sz"] if ask else "")
            if channel=="l2Book":row.update(bid_levels=len(sides[0]),ask_levels=len(sides[1]))
        result.append(row)
    return result


class Writer:
    def __init__(self,folder):
        self.folder,self.q,self.done=folder,queue.Queue(maxsize=50000),threading.Event()
        self.failure,self.messages,self.rows,self.stats=None,Counter(),Counter(),{}
        self.controls,self.duplicates,self.clock_steps=0,0,0
        self.thread=threading.Thread(target=self.work,daemon=True)

    def work(self):
        previous,seen_trades={},set()
        try:
            with (self.folder/"latency.csv").open("w",newline="") as cf,(self.folder/"raw.jsonl").open("w") as rf:
                csv_writer=csv.DictWriter(cf,fieldnames=FIELDS);csv_writer.writeheader();flushed=time.monotonic()
                while not self.done.is_set() or not self.q.empty():
                    try:stream,raw,wall,mono,message_id=self.q.get(timeout=.2)
                    except queue.Empty:continue
                    msg=json.loads(raw)
                    rf.write(json.dumps(dict(stream=stream,message_id=message_id,recv_unix_ns=wall,
                                             recv_mono_ns=mono,payload=msg),separators=(",",":"))+"\n")
                    if msg.get("channel")=="error":raise RuntimeError("Subscription error: "+str(msg))
                    rows=rows_from_message(stream,msg,wall,mono,previous,seen_trades,message_id)
                    if msg.get("channel") in ("l2Book","bbo","trades"):self.messages[stream]+=1
                    else:self.controls+=1
                    for row in rows:
                        csv_writer.writerow(row);self.rows[stream]+=1
                        self.duplicates+=row["duplicate_trade"]
                        self.clock_steps+=int(row["clock_step_ms"]!="" and abs(row["clock_step_ms"])>5)
                        if row["included_in_latency_summary"]:
                            self.stats.setdefault((stream,"recv_minus_exchange_ms"),[]).append(row["recv_minus_exchange_ms"])
                        if row["interarrival_message_ms"]!="":
                            self.stats.setdefault((stream,"interarrival_message_ms"),[]).append(row["interarrival_message_ms"])
                    if time.monotonic()-flushed>=1:cf.flush();rf.flush();flushed=time.monotonic()
        except Exception as exc:self.failure=exc


async def heartbeat(ws):
    while True:
        await asyncio.sleep(20)
        await ws.send('{"method":"ping"}')


async def receive(stream,subscription,writer):
    async with websockets.connect(WS,open_timeout=15,ping_interval=20,ping_timeout=20,
                                  compression=None,max_queue=16) as ws:
        await ws.send(json.dumps(dict(method="subscribe",subscription=subscription)))
        ping=asyncio.create_task(heartbeat(ws));sequence=0;confirmed=False
        try:
            async for raw in ws:
                wall=time.time_ns();mono=time.perf_counter_ns();sequence+=1
                try:writer.q.put_nowait((stream,raw,wall,mono,f"{stream}-{sequence}"))
                except queue.Full:raise RuntimeError("Writer queue full; collection stopped without silently dropping data")
                msg=json.loads(raw)
                if msg.get("channel")=="error":raise RuntimeError(str(msg))
                if msg.get("channel")=="subscriptionResponse":
                    echoed=msg.get("data",{}).get("subscription",{})
                    if echoed.get("coin")!=subscription["coin"] or echoed.get("type")!=subscription["type"]:
                        raise RuntimeError("Unexpected subscription acknowledgement")
                    confirmed=True;print("Subscribed:",stream,echoed,flush=True)
                if writer.failure:raise writer.failure
                if ping.done():await ping
        finally:
            ping.cancel();await asyncio.gather(ping,return_exceptions=True)
    raise RuntimeError(f"{stream} disconnected (subscription confirmed={confirmed}); start a new collection segment")


async def post(session,body):
    async with session.post(REST,json=body) as response:
        response.raise_for_status();return await response.json()


async def resolve_coin(session,requested):
    dex=requested.split(":")[0] if ":" in requested else ""
    meta=await post(session,dict(type="meta",dex=dex))
    names={item["name"]:item for item in meta["universe"]}
    candidates=[requested]
    if requested in ("xyz:SKHX","xyz:SKHYNIX"):candidates=["xyz:SKHYNIX","xyz:SKHX"]
    for coin in candidates:
        if coin in names and not names[coin].get("isDelisted",False):return coin,names[coin]
    raise RuntimeError(f"Requested market not found/active: {requested}. No substitute market selected.")


async def run(args):
    folder=Path(args.out);folder.mkdir(parents=True,exist_ok=False)
    manifest=dict(venue="Hyperliquid mainnet",requested_coin=args.coin,l2_mode=args.l2_mode,
                  start_utc=datetime.now(timezone.utc).isoformat(),duration_seconds=args.seconds,
                  status="initializing",first_data_message_excluded=True,
                  timestamp_semantics="native time; not equivalent to Binance E or T; no REST clock correction")
    writer=Writer(folder);started=False;tasks=[]
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            coin,instrument=await resolve_coin(session,args.coin)
        manifest.update(coin=coin,instrument=instrument)
        subscriptions={"l2":dict(type="l2Book",coin=coin,fast=args.l2_mode=="fast"),
                       "bbo":dict(type="bbo",coin=coin),"trades":dict(type="trades",coin=coin)}
        manifest["subscriptions"]=subscriptions
        print("Public market-data collection:",coin,"L2",args.l2_mode,"No wallet/keys/orders.")
        writer.thread.start();started=True
        tasks=[asyncio.create_task(receive(name,sub,writer)) for name,sub in subscriptions.items()]
        stop=asyncio.create_task(asyncio.sleep(args.seconds));tasks.append(stop)
        manifest["status"]="collecting"
        while not stop.done():
            finished,_=await asyncio.wait(tasks,timeout=5,return_when=asyncio.FIRST_COMPLETED)
            for task in finished:
                if task is not stop:await task
            if writer.failure:raise writer.failure
            print("Messages",dict(writer.messages),"rows",dict(writer.rows),"queue",writer.q.qsize(),flush=True)
        manifest["status"]="completed"
    except BaseException as exc:
        manifest.update(status="stopped",error=f"{type(exc).__name__}: {exc}");raise
    finally:
        for task in tasks:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        writer.done.set()
        if started:await asyncio.to_thread(writer.thread.join)
        if writer.failure:manifest.update(status="writer_failed",writer_error=str(writer.failure))
        missing=sorted(set(("l2","bbo","trades"))-set(writer.messages))
        if missing and manifest["status"]=="completed":manifest["status"]="completed_with_missing_streams"
        report={stream:{field:summarize(values) for (s,field),values in writer.stats.items() if s==stream}
                for stream in writer.messages}
        manifest.update(message_counts=dict(writer.messages),row_counts=dict(writer.rows),missing_streams=missing,
                        duplicate_trades=writer.duplicates,clock_steps_over_5ms=writer.clock_steps,
                        end_utc=datetime.now(timezone.utc).isoformat())
        (folder/"run.json").write_text(json.dumps(manifest,indent=2))
        (folder/"summary.json").write_text(json.dumps(report,indent=2))
        for stream,metrics in report.items():print(stream,json.dumps(metrics))
        print("Status:",manifest["status"],"Output:",folder.resolve())
        if missing:print("No data messages for:",missing,"Check activity/subscriptions; empty trades may be normal.")


def main():
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--coin",default="xyz:SKHX")
    p.add_argument("--seconds",type=int,default=600)
    p.add_argument("--l2-mode",choices=["fast","full"],default="fast")
    p.add_argument("--out",default="hl_feed_"+datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S"))
    args=p.parse_args()
    if not 10<=args.seconds<=3600:p.error("seconds must be 10..3600")
    try:asyncio.run(run(args))
    except KeyboardInterrupt:print("Stopped by user; queued data flushed.")
    except Exception as exc:print(f"STOP {type(exc).__name__}: {exc}");raise SystemExit(1)


if __name__=="__main__":main()
