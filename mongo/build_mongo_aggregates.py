"""
Build MongoDB import-ready JSONL from the processed Yelp CSVs.

Produces (in data/mongo/):
  businesses.jsonl  - business-centric aggregate:
                        base fields + geo + attributes/categories/hours
                        + review_count / tip_count (true totals)
                        + checkin_stats + checkin_monthly
                        + reviews_preview (last 50, newest first)
                        + tips_preview    (last 20, newest first)
  users.jsonl       - user-centric aggregate:
                        base fields + votes + compliments + elite
                        + friend_count (count, not the list)
                        + review_count / tip_count (computed) + review_count_total
                        + reviews_preview (last 50, business-oriented, newest first)
                        + tips_preview    (last 20, business-oriented, newest first)
  reviews.jsonl     - full flat reviews collection
  tips.jsonl        - full flat tips collection

Dates are emitted as MongoDB Extended JSON ({"$date": ...}) so mongoimport
creates real Date types.

Usage:
    py build_mongo_aggregates.py
"""

import os
import ast
import json
from datetime import datetime, timezone

import pandas as pd

PROC = "data/processed"
OUT = os.getenv("OUT_DIR", "data/mongo")
REVIEW_PREVIEW_N = 50
TIP_PREVIEW_N = 10

# --- what to do with the built documents ---------------------------------
WRITE_JSONL = False      # write data/mongo/*.jsonl
UPLOAD = True               # insert straight into MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "yelp")
BATCH = 10_000                                          # insert_many batch size

# Which collections to build/upload. Empty = all of them.
# Any subset of: "businesses", "users", "reviews", "tips".
DOCUMENTS = []

ALL_DOCS = ["businesses", "users", "reviews", "tips"]
_unknown = [d for d in DOCUMENTS if d not in ALL_DOCS]
if _unknown:
    raise SystemExit(f"Unknown DOCUMENTS {_unknown}; valid: {ALL_DOCS}")


def want(name):
    """True if `name` should be produced (DOCUMENTS empty = produce all)."""
    return not DOCUMENTS or name in DOCUMENTS


os.makedirs(OUT, exist_ok=True)

_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def to_date(s):
    """'2018-07-07 22:09:11' -> tz-aware datetime (native BSON date for pymongo)."""
    if not isinstance(s, str) or not s.strip():
        return None
    try:
        return datetime.strptime(s.strip(), _DATE_FMT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _ejson(o):
    """json.dumps default: datetime -> MongoDB Extended JSON {'$date': ...}."""
    if isinstance(o, datetime):
        return {"$date": o.strftime("%Y-%m-%dT%H:%M:%SZ")}
    raise TypeError(f"not JSON serializable: {type(o)}")


def dumps(doc):
    return json.dumps(doc, default=_ejson)


# --- optional MongoDB connection -----------------------------------------
mongo_db = None
if UPLOAD:
    from pymongo import MongoClient, ASCENDING, DESCENDING, GEOSPHERE

    print(f"Connecting to MongoDB {MONGO_URI} (db={MONGO_DB})...")
    mongo_db = MongoClient(MONGO_URI)[MONGO_DB]


SUMMARY = {}      # collection name -> docs written (only for collections built)


class Sink:
    """Fan-out for built docs: JSONL file (if WRITE_JSONL) and/or Mongo insert."""

    def __init__(self, name):
        self.name = name
        self.f = open(f"{OUT}/{name}.jsonl", "w", encoding="utf-8") if WRITE_JSONL else None
        self.coll = None
        if UPLOAD:
            mongo_db[name].drop()                    # idempotent re-runs
            self.coll = mongo_db[name]
        self.buf = []
        self.count = 0

    def write(self, doc):
        if self.f is not None:
            self.f.write(dumps(doc) + "\n")
        if self.coll is not None:
            self.buf.append(doc)
            if len(self.buf) >= BATCH:
                self._flush()
        self.count += 1

    def _flush(self):
        if self.buf:
            self.coll.insert_many(self.buf, ordered=False)
            self.buf = []

    def close(self):
        self._flush()
        if self.f is not None:
            self.f.close()
        SUMMARY[self.name] = self.count


def safe_literal(s, default):
    if not isinstance(s, str) or s.strip() in ("", "Unknown", "nan"):
        return default
    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return default


# ---------------------------------------------------------------- key sets ---
print("Loading users / businesses key sets...")
users = pd.read_csv(f"{PROC}/users.csv", usecols=["user_id", "name"])
user_ids = set(users["user_id"])
user_name = dict(zip(users["user_id"], users["name"]))
del users

biz_base = pd.read_csv(f"{PROC}/businesses.csv")
biz_ids = set(biz_base["business_id"])
biz_name = dict(zip(biz_base["business_id"], biz_base["name"]))
print(f"  users={len(user_ids):,}  businesses={len(biz_ids):,}")


# ------------------------------------------------------------------ reviews ---
# Needed if we upload the flat reviews, or need review previews for either aggregate.
review_count = {}            # per business  (for businesses aggregate)
user_review_count = {}       # per user      (for users aggregate)
reviews_preview = {}         # business -> last 50 reviews
user_reviews_preview = {}    # user -> last 50 reviews

if want("reviews") or want("businesses") or want("users"):
    print("Loading + filtering reviews...")
    rev = pd.read_csv(
        f"{PROC}/reviews.csv",
        usecols=["review_id", "user_id", "business_id", "stars",
                 "useful", "funny", "cool", "text", "date"],
    )
    rev = rev[rev["user_id"].isin(user_ids) & rev["business_id"].isin(biz_ids)]
    print(f"  reviews after filter: {len(rev):,}")

    # flat reviews collection (full) -- only when selected
    if want("reviews"):
        rev_sink = Sink("reviews")
        for r in rev.itertuples(index=False):
            rev_sink.write({
                "id": r.review_id, "business_id": r.business_id, "user_id": r.user_id,
                "stars": int(r.stars), "useful": int(r.useful),
                "funny": int(r.funny), "cool": int(r.cool),
                "text": r.text if isinstance(r.text, str) else "",
                "date": to_date(r.date),
            })
        rev_sink.close()

    if want("businesses"):
        review_count = rev.groupby("business_id").size().to_dict()
    if want("users"):
        user_review_count = rev.groupby("user_id").size().to_dict()

    # sort once by date, then take the last N per business AND/OR per user
    if want("businesses") or want("users"):
        rev_sorted = rev.sort_values("date")

        if want("businesses"):      # last 50 per business, newest first
            for r in rev_sorted.groupby("business_id").tail(REVIEW_PREVIEW_N).itertuples(index=False):
                reviews_preview.setdefault(r.business_id, []).append({
                    "review_id": r.review_id, "user_id": r.user_id,
                    "user_name": user_name.get(r.user_id),
                    "stars": int(r.stars), "useful": int(r.useful),
                    "funny": int(r.funny), "cool": int(r.cool),
                    "text": r.text if isinstance(r.text, str) else "",
                    "date": to_date(r.date),
                })
            for b in reviews_preview:
                reviews_preview[b].reverse()

        if want("users"):           # last 50 per user, newest first (business-oriented)
            for r in rev_sorted.groupby("user_id").tail(REVIEW_PREVIEW_N).itertuples(index=False):
                user_reviews_preview.setdefault(r.user_id, []).append({
                    "review_id": r.review_id, "business_id": r.business_id,
                    "business_name": biz_name.get(r.business_id),
                    "stars": int(r.stars), "useful": int(r.useful),
                    "funny": int(r.funny), "cool": int(r.cool),
                    "text": r.text if isinstance(r.text, str) else "",
                    "date": to_date(r.date),
                })
            for u in user_reviews_preview:
                user_reviews_preview[u].reverse()
        del rev_sorted
    del rev


# --------------------------------------------------------------------- tips ---
tip_count = {}               # per business  (for businesses aggregate)
user_tip_count = {}          # per user      (for users aggregate)
tips_preview = {}            # business -> last 20 tips
user_tips_preview = {}       # user -> last 20 tips

if want("tips") or want("businesses") or want("users"):
    print("Loading + filtering tips...")
    tip = pd.read_csv(
        f"{PROC}/tips.csv",
        usecols=["user_id", "business_id", "text", "date", "compliment_count"],
    )
    tip = tip[tip["user_id"].isin(user_ids) & tip["business_id"].isin(biz_ids)]
    print(f"  tips after filter: {len(tip):,}")

    if want("tips"):
        tip_sink = Sink("tips")
        for t in tip.itertuples(index=False):
            tip_sink.write({
                "business_id": t.business_id, "user_id": t.user_id,
                "text": t.text if isinstance(t.text, str) else "",
                "date": to_date(t.date),
                "compliment_count": int(t.compliment_count),
            })
        tip_sink.close()

    if want("businesses"):
        tip_count = tip.groupby("business_id").size().to_dict()
    if want("users"):
        user_tip_count = tip.groupby("user_id").size().to_dict()

    if want("businesses") or want("users"):
        tip_sorted = tip.sort_values("date")

        if want("businesses"):      # last 20 per business, newest first
            for t in tip_sorted.groupby("business_id").tail(TIP_PREVIEW_N).itertuples(index=False):
                tips_preview.setdefault(t.business_id, []).append({
                    "user_id": t.user_id, "user_name": user_name.get(t.user_id),
                    "text": t.text if isinstance(t.text, str) else "",
                    "date": to_date(t.date),
                    "compliment_count": int(t.compliment_count),
                })
            for b in tips_preview:
                tips_preview[b].reverse()

        if want("users"):           # last 20 per user, newest first (business-oriented)
            for t in tip_sorted.groupby("user_id").tail(TIP_PREVIEW_N).itertuples(index=False):
                user_tips_preview.setdefault(t.user_id, []).append({
                    "business_id": t.business_id, "business_name": biz_name.get(t.business_id),
                    "text": t.text if isinstance(t.text, str) else "",
                    "date": to_date(t.date),
                    "compliment_count": int(t.compliment_count),
                })
            for u in user_tips_preview:
                user_tips_preview[u].reverse()
        del tip_sorted
    del tip


# ----------------------------------------------------------------- checkins ---
checkin_stats = {}
checkin_monthly = {}
if want("businesses"):
    print("Loading checkins...")
    chk = pd.read_csv(f"{PROC}/checkins.csv")
    chk = chk[chk["business_id"].isin(biz_ids)]
    for c in chk.itertuples(index=False):
        ts = safe_literal(c.date, [])
        if not ts:
            continue
        ts_sorted = sorted(ts)
        checkin_stats[c.business_id] = {
            "total": len(ts_sorted),
            "first": to_date(ts_sorted[0]),
            "last": to_date(ts_sorted[-1]),
        }
        monthly = {}
        for t in ts_sorted:
            ym = t[:7]                              # 'YYYY-MM'
            monthly[ym] = monthly.get(ym, 0) + 1
        checkin_monthly[c.business_id] = [
            {"year": int(k[:4]), "month": int(k[5:7]), "count": v}
            for k, v in sorted(monthly.items())
        ]
    del chk


# ------------------------------------------------------- business documents ---
if want("businesses"):
    print("Writing business aggregate documents...")
    biz_sink = Sink("businesses")
    for b in biz_base.itertuples(index=False):
        bid = b.business_id
        doc = {
            "id": bid,
            "name": b.name,
            "location": {
                "address": None if pd.isna(b.address) else b.address,
                "city": None if pd.isna(b.city) else str(b.city).strip(),
                "state": None if pd.isna(b.state) else b.state,
                "postal_code": None if pd.isna(b.postal_code) else str(b.postal_code),
            },
            "stars": None if pd.isna(b.stars) else float(b.stars),
            "is_open": None if pd.isna(b.is_open) else int(b.is_open),
            "attributes": safe_literal(b.attributes, {}),
            "categories": safe_literal(b.categories, []),
            "hours": safe_literal(b.hours, {}),
            "review_count": 0 if pd.isna(b.review_count) else int(b.review_count),
            "loaded_review_count": int(review_count.get(bid, 0)),
            "tip_count": int(tip_count.get(bid, 0)),
            "checkin_stats": checkin_stats.get(bid),
            "checkin_monthly": checkin_monthly.get(bid, []),
            "reviews_preview": reviews_preview.get(bid, []),
            "tips_preview": tips_preview.get(bid, []),
        }
        if not pd.isna(b.longitude) and not pd.isna(b.latitude):
            doc["location"]["geo"] = {
                "type": "Point",
                "coordinates": [float(b.longitude), float(b.latitude)],
            }
        biz_sink.write(doc)
    biz_sink.close()


# ----------------------------------------------------------- user documents ---
def friend_count(s):
    if not isinstance(s, str) or s.strip() in ("", "None"):
        return 0
    return s.count(",") + 1


def ncount(v):
    return 0 if pd.isna(v) else int(v)


def nyear(v):
    """Year int, or None. Non-elite users store 'Unknown' (not NaN) here."""
    s = str(v).strip()
    return int(s) if s.isdigit() else None


if want("users"):
    print("Writing user aggregate documents...")
    user_full = pd.read_csv(f"{PROC}/users.csv")
    usr_sink = Sink("users")
    for u in user_full.itertuples(index=False):
        uid = u.user_id
        doc = {
            "id": uid,
            "name": u.name,
            "yelping_since": to_date(u.yelping_since),
            "average_stars": None if pd.isna(u.average_stars) else float(u.average_stars),
            "fans": ncount(u.fans),
            "friend_count": friend_count(u.friends),          # the count, not the list
            "votes": {"useful": ncount(u.useful), "funny": ncount(u.funny),
                      "cool": ncount(u.cool)},
            "compliments": {
                "hot": ncount(u.compliment_hot), "more": ncount(u.compliment_more),
                "profile": ncount(u.compliment_profile), "cute": ncount(u.compliment_cute),
                "list": ncount(u.compliment_list), "note": ncount(u.compliment_note),
                "plain": ncount(u.compliment_plain), "cool": ncount(u.compliment_cool),
                "funny": ncount(u.compliment_funny), "writer": ncount(u.compliment_writer),
                "photos": ncount(u.compliment_photos),
            },
            "elite": {
                "years_count": ncount(u.elite_years_count),
                "first_year": nyear(u.elite_first_year),
                "last_year": nyear(u.elite_last_year),
                "is_elite": bool(u.is_elite),
            },
            # computed from the (filtered) data actually loaded here:
            "review_count": int(user_review_count.get(uid, 0)),
            "tip_count": int(user_tip_count.get(uid, 0)),
            # stored Yelp lifetime total, for reference (may exceed embedded set):
            "review_count_total": ncount(u.review_count),
            "reviews_preview": user_reviews_preview.get(uid, []),   # last 50, newest first
            "tips_preview": user_tips_preview.get(uid, []),         # last 20, newest first
        }
        usr_sink.write(doc)
    usr_sink.close()


built = "  ".join(f"{k}={v:,}" for k, v in SUMMARY.items()) or "(nothing selected)"
print(f"\nDone. {built}")
if WRITE_JSONL:
    print(f"JSONL written to {OUT}/")

# --- indexes (only for uploaded collections) -----------------------------
if UPLOAD:
    print("Creating indexes...")
    if want("reviews"):
        mongo_db["reviews"].create_index([("business_id", ASCENDING), ("date", DESCENDING)])
        mongo_db["reviews"].create_index([("user_id", ASCENDING), ("date", DESCENDING)])
    if want("tips"):
        mongo_db["tips"].create_index([("business_id", ASCENDING), ("date", DESCENDING)])
        mongo_db["tips"].create_index([("user_id", ASCENDING), ("date", DESCENDING)])
    if want("businesses"):
        mongo_db["businesses"].create_index([("location.geo", GEOSPHERE)])
        mongo_db["businesses"].create_index([("id", ASCENDING)])
        mongo_db["businesses"].create_index([("categories", ASCENDING)])
    if want("users"):
        mongo_db["users"].create_index([("id", ASCENDING)])
        mongo_db["users"].create_index([("name", ASCENDING)])
    print(f"Uploaded to MongoDB db='{MONGO_DB}' at {MONGO_URI}")
