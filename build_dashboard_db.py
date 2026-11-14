#!/usr/bin/env python3
"""Build the small dashboard database (dashboard.db) from the raw history database (market.db).

    python3 build_dashboard_db.py [--src market.db] [--out dashboard.db]

The raw database (history_db.py: every change of every item, hourly) is never touched.  This script replays it and writes
everything the dashboard needs, already computed, so the page does not have to replay any history:

  meta(key, value)        latest_ts, first_ts, snapshots (count), window (points in the 24 h trade window), built_at
  item(item_id, hrid)     copied from the raw database
  candle                  one row per (item, level, UTC day) in which the item was listed.  ask_* / bid_* hold only quotes INSIDE the
                          estimated tradeable range (open, high, low, close, median of that day's hourly values); *_all hold the
                          extremes of every quote.  vwap = sum(price * volume) / sum(volume) over the day's hours, vol = units traded,
                          trade_hours = hours with trades, hours = hours listed, centre = estimated range centre at the end of the day.
  current                 one row per (item, level) listed in the newest snapshot: the values exactly as published (ask, bid, price,
                          volume), the in-range ask/bid, the range centre, trade hours in the last 24 points and the number of trade
                          reports in the whole history (seen).
  span_stats              level-0 items: min / max / median of the in-range ask and bid over the last 24 h, 3 d, 7 d, 14 d, 30 d, 90 d
                          and the whole history (span_secs = 0), exact, from the hourly data.
  item_recipe, item_ingredient_depth, item_craft_time, item_drop, loot_table, drink, item_category, item_vendor
                          copied from the raw database

The tradeable-range model (centre, range, grid steps) is the one the dashboard used to run in the browser (see the comments in
market_watch/index.html); it now runs here, once per build.  The whole database is rebuilt from the raw data on every run.
"""
import argparse
import math
import os
import sqlite3
import statistics
import sys
from collections import deque
from datetime import datetime, timezone

# ---- tradeable-range model (keep in sync with the constants documented in market_watch/index.html) -------------------------
CAP_PER_HOUR, TRADE_HOURS, DRY_HOURS, NEAR = 1.01, 24, 6, 1.2
TOL = 0.015
SPANS = [86400, 3 * 86400, 7 * 86400, 14 * 86400, 30 * 86400, 90 * 86400]
WINDOW = 24  # points in the "hours traded" window

STATIC_TABLES = ["item_ingredient_depth", "item_craft_time", "item_recipe", "item_drop", "loot_table", "drink",
                 "item_category", "item_vendor"]


def price_step(p):
    """Price grid: the step above price p (see priceStep in index.html)."""
    if p < 400:
        return 1
    if p < 800:
        return 2
    for s in (4, 5, 6, 8):
        if p < 300 * s:
            return s
    m = 1
    while True:
        for b in (10, 12, 16, 20, 25, 30, 40, 50, 60, 80):
            if p < 300 * b * m:
                return b * m
        m *= 10


def grid_near(p):
    st = price_step(p)
    return math.floor(p / st + 0.5) * st  # JS Math.round


def grid_up(p):
    st = price_step(p)
    return math.ceil(p / st) * st


def range_around(c):
    half = max(0.1 * c, 2 * price_step(c))
    return grid_near(c - half), grid_up(c + half)


def in_range(c, q):
    """q if it lies inside the estimated range around centre c (with the centre's tolerance), else None."""
    if c is None or q is None:
        return None
    lo, hi = range_around(c)
    return q if lo * (1 - TOL) <= q <= hi * (1 + TOL) else None


def bon(v):  # -1 = no order
    return None if v is None or v < 0 else v


class Series:
    __slots__ = ("c", "trades", "tsum", "last_tr", "seen", "win", "lo", "hi", "ts_l", "ask_l", "bid_l", "centre")

    def __init__(self):
        self.c = None
        self.trades = deque()
        self.tsum = 0
        self.last_tr = None
        self.seen = 0
        self.win = deque(maxlen=WINDOW)
        self.centre = None
        self.ts_l = self.ask_l = self.bid_l = None  # level-0 in-range series for span_stats


def median(a):
    return statistics.median(a) if a else None


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default="market.db")
    ap.add_argument("--out", default="dashboard.db")
    args = ap.parse_args()

    src = sqlite3.connect(args.src)
    snaps = src.execute("SELECT snapshot_id, ts FROM snapshot ORDER BY ts").fetchall()
    if not snaps:
        sys.exit("the raw database has no snapshots")
    obs = {}
    for r in src.execute("SELECT snapshot_id, item_id, level, ask, bid, price, volume FROM obs ORDER BY snapshot_id"):
        obs.setdefault(r[0], []).append(r[1:])

    S = {}                 # (item, level) -> Series
    state = {}             # (item, level) -> (ask, bid, price, volume) as stored; absent = not listed
    active = []            # series seen so far (stepped at every snapshot, like the browser did)
    candles = []
    day_acc = {}           # key -> accumulator of the current UTC day
    cur_day = None
    prev_ts = None
    last_ts = snaps[-1][1]

    def flush(day):
        for (i, l), a in day_acc.items():
            av, bv = a["av"], a["bv"]
            candles.append((i, l, day, a["n"], a["th"],
                            av[0] if av else None, max(av) if av else None, min(av) if av else None, av[-1] if av else None, median(av),
                            bv[0] if bv else None, max(bv) if bv else None, min(bv) if bv else None, bv[-1] if bv else None, median(bv),
                            a["alo"], a["ahi"], a["blo"], a["bhi"],
                            (a["pv"] / a["v"]) if a["v"] else None, a["v"], a["c"]))
        day_acc.clear()

    for idx, (sid, ts) in enumerate(snaps):
        fresh = set()
        for i, l, ask, bid, price, vol in obs.get(sid, ()):
            key = (i, l)
            if key not in S:
                S[key] = Series()
                if l == 0:
                    S[key].ts_l, S[key].ask_l, S[key].bid_l = [], [], []
                active.append(key)
            if ask is None and bid is None and price is None and vol is None:
                state.pop(key, None)
            else:
                state[key] = (ask, bid, price, vol)
                fresh.add(key)
        day = ts - ts % 86400
        if cur_day is not None and day != cur_day:
            flush(cur_day)
        cur_day = day
        cutoff = ts - TRADE_HOURS * 3600
        dt = max(1.0, (ts - prev_ts) / 3600) if prev_ts is not None else 1.0
        cap = CAP_PER_HOUR ** dt
        for key in active:
            s = S[key]
            v = state.get(key)
            traded = False
            if v is not None:
                ask, bid, price, vol = bon(v[0]), bon(v[1]), v[2], v[3]
                traded = vol is not None and vol > 0 and price is not None
                if traded and key in fresh:  # a new trade report (an unchanged value carried forward counts once)
                    s.trades.append((ts, price))
                    s.tsum += price
                    s.last_tr = ts
                    s.seen += 1
            else:
                ask = bid = price = vol = None
            while s.trades and s.trades[0][0] <= cutoff:
                s.tsum -= s.trades.popleft()[1]
            n_recent = len(s.trades)
            tg = (s.tsum / n_recent) if n_recent else None
            if not n_recent or s.last_tr <= ts - DRY_HOURS * 3600:  # no trade for a while: the quotes decide
                base = s.c if s.c is not None else ((ask if ask is not None else bid) if v is not None else None)
                if v is not None and base is not None:
                    q = [x for x in (ask, bid) if x is not None and base / NEAR <= x <= base * NEAR]
                    if q:
                        tg = sum(q) / len(q)
            if tg is not None:
                s.c = tg if s.c is None else min(max(tg, s.c / cap), s.c * cap)
            s.win.append(1 if traded else 0)
            c = s.c if v is not None else None
            s.centre = c
            if v is None:
                if key[1] == 0:
                    s.ts_l.append(ts); s.ask_l.append(None); s.bid_l.append(None)
                continue
            a_in, b_in = in_range(c, ask), in_range(c, bid)
            if key[1] == 0:
                s.ts_l.append(ts); s.ask_l.append(a_in); s.bid_l.append(b_in)
            a = day_acc.get(key)
            if a is None:
                a = day_acc[key] = {"n": 0, "th": 0, "av": [], "bv": [], "alo": None, "ahi": None, "blo": None, "bhi": None, "pv": 0, "v": 0, "c": None}
            a["n"] += 1
            a["c"] = c
            if a_in is not None: a["av"].append(a_in)
            if b_in is not None: a["bv"].append(b_in)
            if ask is not None:
                a["alo"] = ask if a["alo"] is None else min(a["alo"], ask)
                a["ahi"] = ask if a["ahi"] is None else max(a["ahi"], ask)
            if bid is not None:
                a["blo"] = bid if a["blo"] is None else min(a["blo"], bid)
                a["bhi"] = bid if a["bhi"] is None else max(a["bhi"], bid)
            if traded:
                a["th"] += 1
                a["pv"] += price * vol
                a["v"] += vol
        prev_ts = ts
    flush(cur_day)

    # ---- current values (newest snapshot) and span statistics
    current = []
    for key, v in state.items():
        s = S[key]
        ask, bid = bon(v[0]), bon(v[1])
        current.append((key[0], key[1], ask, bid, v[2], v[3], in_range(s.centre, ask), in_range(s.centre, bid),
                        s.centre, sum(s.win), s.seen))
    stats = []
    for (i, l), s in S.items():
        if l != 0 or (i, l) not in state:
            continue
        for field, arr in (("ask", s.ask_l), ("bid", s.bid_l)):
            for span in SPANS + [0]:
                since = last_ts - span if span else -1
                vals = [x for t, x in zip(s.ts_l, arr) if x is not None and t >= since]
                if vals:
                    stats.append((i, span, field, min(vals), max(vals), median(vals), len(vals)))

    # ---- write
    if os.path.exists(args.out):
        os.remove(args.out)
    out = sqlite3.connect(args.out)
    out.executescript("""
CREATE TABLE meta (key TEXT PRIMARY KEY, value) WITHOUT ROWID;
CREATE TABLE item (item_id INTEGER PRIMARY KEY, hrid TEXT NOT NULL UNIQUE);
CREATE TABLE candle (
  item_id INTEGER NOT NULL, level INTEGER NOT NULL, day INTEGER NOT NULL, hours INTEGER, trade_hours INTEGER,
  ask_o INTEGER, ask_h INTEGER, ask_l INTEGER, ask_c INTEGER, ask_med REAL,
  bid_o INTEGER, bid_h INTEGER, bid_l INTEGER, bid_c INTEGER, bid_med REAL,
  ask_lo_all INTEGER, ask_hi_all INTEGER, bid_lo_all INTEGER, bid_hi_all INTEGER,
  vwap REAL, vol INTEGER, centre REAL,
  PRIMARY KEY (item_id, level, day)
) WITHOUT ROWID;
CREATE TABLE current (
  item_id INTEGER NOT NULL, level INTEGER NOT NULL, ask INTEGER, bid INTEGER, price INTEGER, volume INTEGER,
  ask_in INTEGER, bid_in INTEGER, centre REAL, traded24 INTEGER, seen INTEGER,
  PRIMARY KEY (item_id, level)
) WITHOUT ROWID;
CREATE TABLE span_stats (
  item_id INTEGER NOT NULL, span_secs INTEGER NOT NULL, field TEXT NOT NULL, mn INTEGER, mx INTEGER, med REAL, n INTEGER,
  PRIMARY KEY (item_id, span_secs, field)
) WITHOUT ROWID;
""")
    out.executemany("INSERT INTO item VALUES (?,?)", src.execute("SELECT item_id, hrid FROM item").fetchall())
    out.executemany("INSERT INTO candle VALUES (" + ",".join("?" * 22) + ")", candles)
    out.executemany("INSERT INTO current VALUES (?,?,?,?,?,?,?,?,?,?,?)", current)
    out.executemany("INSERT INTO span_stats VALUES (?,?,?,?,?,?,?)", stats)
    out.executemany("INSERT INTO meta VALUES (?,?)", [
        ("latest_ts", last_ts), ("first_ts", snaps[0][1]), ("snapshots", len(snaps)), ("window", min(WINDOW, len(snaps))),
        ("built_at", datetime.now(timezone.utc).isoformat(timespec="seconds")), ("version", 1)])
    out.commit()
    out.close()
    out = sqlite3.connect(args.out)
    out.execute("ATTACH ? AS raw", (args.src,))
    for t in STATIC_TABLES:
        if src.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone():
            out.execute(f"CREATE TABLE main.{t} AS SELECT * FROM raw.{t}")
    out.commit()
    out.execute("DETACH raw")
    out.execute("VACUUM")
    out.close()
    print(f"candles {len(candles)}, current {len(current)}, span_stats {len(stats)}, snapshots {len(snaps)} -> {args.out} "
          f"({os.path.getsize(args.out) // 1024} KB)")


if __name__ == "__main__":
    main()
