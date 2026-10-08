#!/usr/bin/env python3
"""Import market snapshots into a compact SQLite history database (deltas only).

    python3 history_db.py [--db market.db] [--snapshots snapshots] [--rebuild]

Schema
    item_ingredient_depth(item_hrid, depth, ingredient_hrid, qty)
                                        recipe tree per depth, reloaded every run from --depths (see load_depths)
    item_recipe(item_hrid, ingredient_hrid, count, is_upgrade, output_count, skill, level_requirement)
                                        one row per recipe ingredient, reloaded every run from --recipes (see load_recipes)
    item_craft_time(item_hrid, secs)    crafting seconds per unit of each craftable item, reloaded every run from --craft-times
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
import csv
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


DEPTH_SCHEMA = """
DROP TABLE IF EXISTS item_ingredient_depth;
CREATE TABLE item_ingredient_depth (
  item_hrid TEXT NOT NULL,
  depth INTEGER NOT NULL,
  ingredient_hrid TEXT NOT NULL,
  qty REAL NOT NULL,
  PRIMARY KEY (item_hrid, depth, ingredient_hrid)
) WITHOUT ROWID;
"""


def load_depths(con, path):
    """(Re)load the recipe table from the CSV exported by the game-data project (build_db.py).

    item_ingredient_depth: for each craftable item, the ingredients at each depth of its recipe tree
    (depth 1 = direct inputs, 2 = the inputs of those, ...), per one unit of the item, quantities summed within a
    depth.  A base item appears only at the depth where it is reached.  Uses hrids (not item_id) because some
    ingredients, e.g. coin, are never on the market and so have no row in `item`.
    """
    if not os.path.exists(path):
        print(f"{path} not found - recipe table left as it is")
        return
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        rows = [(r["item_hrid"], int(r["depth"]), r["ingredient_hrid"], float(r["qty"])) for r in csv.DictReader(f)]
    with con:
        con.executescript(DEPTH_SCHEMA)
        con.executemany("INSERT INTO item_ingredient_depth VALUES (?,?,?,?)", rows)
    print(f"loaded {len(rows)} rows into item_ingredient_depth")


CRAFT_SCHEMA = """
DROP TABLE IF EXISTS item_craft_time;
CREATE TABLE item_craft_time (
  item_hrid TEXT NOT NULL PRIMARY KEY,
  secs REAL NOT NULL
) WITHOUT ROWID;
"""


def load_craft_times(con, path):
    """(Re)load item_craft_time from the CSV exported by the game-data project: crafting seconds per unit of each
    craftable item (action base time / output count, base speed, no gear).  Together with item_ingredient_depth
    it gives the crafting time of any recipe cut."""
    if not os.path.exists(path):
        print(f"{path} not found - craft time table left as it is")
        return
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        rows = [(r["item_hrid"], float(r["secs"])) for r in csv.DictReader(f)]
    with con:
        con.executescript(CRAFT_SCHEMA)
        con.executemany("INSERT INTO item_craft_time VALUES (?,?)", rows)
    print(f"loaded {len(rows)} rows into item_craft_time")


RECIPE_SCHEMA = """
DROP TABLE IF EXISTS item_recipe;
CREATE TABLE item_recipe (
  item_hrid TEXT NOT NULL,
  ingredient_hrid TEXT NOT NULL,
  count REAL NOT NULL,
  is_upgrade INTEGER NOT NULL,
  output_count REAL NOT NULL,
  skill TEXT NOT NULL,
  level_requirement INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (item_hrid, ingredient_hrid, is_upgrade)
) WITHOUT ROWID;
"""


def load_recipes(con, path):
    """(Re)load item_recipe from the CSV exported by the game-data project: one row per ingredient of each craftable
    item, exactly as the game defines the recipe.  count = consumed per craft, output_count = units one craft makes,
    is_upgrade = 1 for the upgrade item (the base item an upgrade recipe consumes; not reduced by the artisan buff),
    skill = the crafting skill (the gourmet buff only applies to cooking and brewing),
    level_requirement = the skill level the action needs (each level above it gives +1% efficiency; 0 if the CSV has no such column).
    The dashboard walks these rows to get the ingredients at every depth, applying artisan at each step."""
    if not os.path.exists(path):
        print(f"{path} not found - recipe table left as it is")
        return
    with gzip.open(path, "rt", encoding="utf-8", newline="") as f:
        rows = [(r["item_hrid"], r["ingredient_hrid"], float(r["count"]), int(r["is_upgrade"]), float(r["output_count"]), r["skill"], int(r.get("level_requirement") or 0))
                for r in csv.DictReader(f)]
    with con:
        con.executescript(RECIPE_SCHEMA)
        con.executemany("INSERT INTO item_recipe VALUES (?,?,?,?,?,?,?)", rows)
    print(f"loaded {len(rows)} rows into item_recipe")


def file_timestamp(path):
    m = re.search(r"marketplace_(\d+)\.json", os.path.basename(path))
    return int(m.group(1)) if m else 0


def snapshot_files(folder):
    return sorted(glob.glob(os.path.join(folder, "marketplace_*.json*")), key=file_timestamp)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default="market.db")
    ap.add_argument("--snapshots", default="snapshots")
    ap.add_argument("--depths", default="item_ingredient_depth.csv.gz", help="recipe depth table (CSV, gzip) to load")
    ap.add_argument("--craft-times", default="item_craft_time.csv.gz", help="crafting seconds per unit (CSV, gzip) to load")
    ap.add_argument("--recipes", default="item_recipe.csv.gz", help="recipes (CSV, gzip) to load")
    ap.add_argument("--rebuild", action="store_true", help="delete the database and import everything again")
    args = ap.parse_args()

    if args.rebuild and os.path.exists(args.db):
        os.remove(args.db)
    con = sqlite3.connect(args.db)
    con.executescript(SCHEMA)

    # The database mirrors the snapshot folder.  It stores only changes, so one snapshot cannot be cut out of it; if a file was
    # deleted on purpose (an old snapshot that should be forgotten), start over from the files that are left.
    files = snapshot_files(args.snapshots)
    have = {file_timestamp(p) for p in files}
    stale = {r[0] for r in con.execute("SELECT ts FROM snapshot")} - have
    if stale and have:  # never rebuild from an empty folder: that would be a broken checkout, not a deletion
        print(f"{len(stale)} snapshot(s) in the database have no file any more - rebuilding from the {len(have)} files")
        con.close()
        os.remove(args.db)
        con = sqlite3.connect(args.db)
        con.executescript(SCHEMA)

    done = {r[0] for r in con.execute("SELECT ts FROM snapshot")}
    newest = max(done) if done else 0
    ids = dict(con.execute("SELECT hrid, item_id FROM item"))
    state = current_state(con)

    imported = 0
    for path in files:
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
    load_depths(con, args.depths)
    load_craft_times(con, args.craft_times)
    load_recipes(con, args.recipes)
    con.close()


if __name__ == "__main__":
    main()
