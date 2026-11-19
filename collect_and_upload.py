#!/usr/bin/env python3
"""Collect public feeds in separate bounded batches; upload closed files to S3."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from uuid import uuid4

ROOT = Path(__file__).resolve().parent
FILES = ('latency.csv', 'raw.jsonl', 'summary.json', 'run.json', 'collector.log')


def upload(folder, bucket, prefix):
    marker = folder / '.uploaded'
    if marker.exists():
        return True
    if not (folder / 'run.json').exists():
        print(f'Incomplete batch without run.json: {folder}; inspect manually', flush=True)
        return False
    target = f's3://{bucket}/{prefix}/{folder.parent.name}/{folder.name}'
    for name in FILES:
        p = folder / name
        if not p.is_file():
            print(f'Missing {p}; batch retained for inspection', flush=True)
            return False
        try:
            r = subprocess.run(['aws', 's3', 'cp', str(p), target + '/' + name,
                                '--only-show-errors', '--no-progress'], timeout=600)
        except (subprocess.TimeoutExpired, OSError) as e:
            print(f'Upload error {type(e).__name__}; retaining {folder}', flush=True)
            return False
        if r.returncode:
            print(f'Upload failed; retaining {folder}', flush=True)
            return False
    marker.write_text(datetime.now(timezone.utc).isoformat() + '\n', encoding='utf8')
    print(f'Uploaded {folder} -> {target}', flush=True)
    return True


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--venue', choices=['binance', 'hyperliquid'], required=True)
    p.add_argument('--symbol', required=True, help='Example BTCUSDT or xyz:SKHX; no silent substitution')
    p.add_argument('--bucket', required=True, help='S3 bucket name, not a URL')
    p.add_argument('--prefix', default='latency')
    p.add_argument('--seconds', type=int, default=600)
    p.add_argument('--data-dir', default='data')
    p.add_argument('--forever', action='store_true')
    a = p.parse_args()
    if not 10 <= a.seconds <= 3600: p.error('seconds must be 10..3600')
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', a.bucket): p.error('invalid bucket name')
    if not re.fullmatch(r'[A-Za-z0-9_/-]+', a.prefix) or '..' in a.prefix: p.error('invalid prefix')
    prefix = a.prefix.strip('/')
    base = Path(a.data_dir).resolve() / a.venue
    base.mkdir(parents=True, exist_ok=True)
    while True:
        for old in sorted(base.iterdir()):
            if old.is_dir() and not (old / '.uploaded').exists():
                if not upload(old, a.bucket, prefix):
                    print('Pending upload failed; stopping to prevent unbounded disk use.', file=sys.stderr)
                    return 1
        tag = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid4().hex[:8]
        folder = base / tag
        script = 'binance_usdm_feed_latency.py' if a.venue == 'binance' else 'hl_marketdata_latency.py'
        option = '--symbol' if a.venue == 'binance' else '--coin'
        command = [sys.executable, str(ROOT / 'collectors' / script), option, a.symbol,
                   '--seconds', str(a.seconds), '--out', str(folder)]
        # Collectors require an output directory that does not yet exist.
        logpath = base / (tag + '.log.tmp')
        print(f'Starting {a.venue} batch {tag}', flush=True)
        with logpath.open('w', encoding='utf8') as log:
            result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
        if not folder.exists():
            print(f'Collector failed before creating output. Log: {logpath}', file=sys.stderr)
            return 1
        os.replace(logpath, folder / 'collector.log')
        ok = upload(folder, a.bucket, prefix)
        if not ok: return 1
        try:
            manifest = json.loads((folder / 'run.json').read_text())
        except (OSError, ValueError):
            print('Invalid run.json; inspect uploaded batch', file=sys.stderr)
            return 1
        if result.returncode or manifest.get('status') not in ('completed', 'completed_with_missing_streams'):
            print(f'Collector stopped; status={manifest.get("status")}. Partial batch uploaded.', file=sys.stderr)
            return 1
        if manifest['status'] != 'completed':
            print('WARNING: missing streams. Inspect run.json.', file=sys.stderr)
        if not a.forever: return 0
        time.sleep(1)


if __name__ == '__main__':
    sys.exit(main())
