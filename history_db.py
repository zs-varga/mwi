#!/usr/bin/env python3
"""Import market snapshots into a compact SQLite history database (deltas only).

    python3 history_db.py [--db market.db] [--snapshots snapshots] [--rebuild]

Schema
    item(item_id, hrid)                 one row per item, name stored once
    snapshot(snapshot_id, ts)           one row per imported snapshot (ts = the game's unix timestamp)
    obs(item_id, level, snapshot_id,    one row per (item, enhancement level) *whenever it changed*
        ask, bid, price, volume)        values are stored exactly as the game publishes them
                                        (ask/bid = -1 means "no order"; price/volume are NULL when missing)

A row is written only when its (ask, bid, price, volume) differ from the last row stored for that
(item, level).  If an item/level disappears from the market, a row with all four values NULL is written
as an end marker; if it comes back, the next differing row is written as usual.  The state at time T is
therefore the latest obs row per (item, level) with snapshot_id <= T (end markers = not listed).

Snapshots must be imported in time order; an older file than the newest imported one is refused
(use --rebuild to start over from all files).  Importing the same snapshot twice is a no-op.
"""
import argparse
import glob
import gzip
import json
import os
import re
import sqlite3
import sys

SCHEMA = """
CREATE TABLE IF NOT EXISTS item (
  item_id INTEGER PRIMARY KEY,
  hrid TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS snapshot (
  snapshot_id INTEGER PRIMARY KEY,
  ts INTEGER NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS obs (
  item_id INTEGER NOT NULL,
  level INTEGER NOT NULL,
  snapshot_id INTEGER NOT NULL,
  ask INTEGER,
  bid INTEGER,
  price INTEGER,
  volume INTEGER,
  PRIMARY KEY (item_id, level, snapshot_id)
) WITHOUT ROWID;
PRAGMA user_version = 1;
"""


def read_snapshot(path):
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def flatten(data):
    """{(hrid, level): (ask, bid, price, volume)} from a marketplace.json document."""
    out = {}
    for hrid, levels in data["marketData"].items():
        for lvl, v in levels.items():
            out[(hrid, int(lvl))] = (v.get("a"), v.get("b"), v.get("p"), v.get("v"))
    return out


def current_state(con):
    """Latest stored values per (item_id, level), as {(item_id, level): values-tuple}."""
    rows = con.execute(
        """SELECT o.item_id, o.level, o.ask, o.bid, o.price, o.volume
           FROM obs o JOIN (SELECT item_id, level, MAX(snapshot_id) AS m FROM obs GROUP BY item_id, level) l
             ON o.item_id = l.item_id AND o.level = l.level AND o.snapshot_id = l.m""")
    return {(r[0], r[1]): tuple(r[2:]) for r in rows}


def import_snapshot(con, data, ids, state):
    ts = data["timestamp"]
    new = flatten(data)
    cur = con.execute("INSERT INTO snapshot(ts) VALUES (?)", (ts,))
    sid = cur.lastrowid
    rows = []
    for (hrid, lvl), vals in new.items():
        iid = ids.get(hrid)
        if iid is None:
            iid = con.execute("INSERT INTO item(hrid) VALUES (?)", (hrid,)).lastrowid
            ids[hrid] = iid
        if state.get((iid, lvl)) != vals:
            rows.append((iid, lvl, sid) + vals)
            state[(iid, lvl)] = vals
    present = {(ids[h], l) for (h, l) in new}
    ended = 0
    for key, vals in list(state.items()):
        if key not in present and vals != (None, None, None, None):
            rows.append((key[0], key[1], sid, None, None, None, None))
            state[key] = (None, None, None, None)
            ended += 1
    con.executemany("INSERT INTO obs VALUES (?,?,?,?,?,?,?)", rows)
    return len(rows), ended


def snapshot_files(folder):
    files = glob.glob(os.path.join(folder, "marketplace_*.json*"))
    def key(p):
        m = re.search(r"marketplace_(\d+)\.json", os.path.basename(p))
        return int(m.group(1)) if m else 0
    return sorted(files, key=key)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="market.db")
    ap.add_argument("--snapshots", default="snapshots")
    ap.add_argument("--rebuild", action="store_true", help="delete the database and import everything again")
    args = ap.parse_args()

    if args.rebuild and os.path.exists(args.db):
        os.remove(args.db)
    con = sqlite3.connect(args.db)
    con.executescript(SCHEMA)

    done = {r[0] for r in con.execute("SELECT ts FROM snapshot")}
    newest = max(done) if done else 0
    ids = dict(con.execute("SELECT hrid, item_id FROM item"))
    state = current_state(con)

    imported = 0
    for path in snapshot_files(args.snapshots):
        data = read_snapshot(path)
        ts = data["timestamp"]
        if ts in done:
            continue
        if ts < newest:
            sys.exit(f"{path} (ts {ts}) is older than the newest imported snapshot ({newest}); use --rebuild")
        with con:
            changed, ended = import_snapshot(con, data, ids, state)
        done.add(ts)
        newest = ts
        imported += 1
        print(f"imported {os.path.basename(path)}: {changed} rows written ({ended} end markers)")
    if not imported:
        print("nothing new to import")
    con.close()


if __name__ == "__main__":
    main()
