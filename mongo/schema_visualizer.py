"""
ER-style schema visualizer for MongoDB.

Samples documents from every collection in the database, infers a
compact field list per collection, detects relationships between
collections (e.g. reviews.business_id -> businesses.id), and renders
ALL of them together as ONE connected diagram (PNG).

Usage:
    python schema_visualizer.py
    python schema_visualizer.py --db yelp --sample 200
    python schema_visualizer.py --collections businesses users reviews tips
    python schema_visualizer.py --out schema.png

Requires:
    pip install pymongo graphviz
    Graphviz system binary (the 'dot' command) must be installed:
      - Ubuntu/Debian: sudo apt-get install graphviz
      - Mac: brew install graphviz
      - Windows: https://graphviz.org/download/
"""

import argparse
import html
import os
import re
from collections import defaultdict
from datetime import datetime

from pymongo import MongoClient
from graphviz import Digraph

MONGO_URI = "mongodb://localhost:27017"
DEFAULT_DB = "yelp"
DEFAULT_SAMPLE = 100
DEFAULT_OUT = "mongo/schema.png"
MAX_FIELDS_SHOWN = 14   # top-level fields to list per entity box, to keep it readable


def scalar_type_name(v):
    """Type name for a non-list value (lists are handled separately, see below)."""
    if v is None:
        return "None"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int):
        return "int"
    if isinstance(v, float):
        return "float"
    if isinstance(v, str):
        return "str"
    if isinstance(v, datetime):
        return "datetime"
    if isinstance(v, dict):
        return "object"
    return type(v).__name__


def summarize_collection(docs):
    """
    Top-level field -> merged type string, based on sampled docs.

    Lists are tracked separately from scalars so a field that's sometimes
    an empty list and sometimes a non-empty one doesn't produce a bogus
    "list | list<object>" -- it merges into a single "list<object>".
    """
    scalar_types = defaultdict(set)   # field -> set of non-list type names seen
    list_item_types = defaultdict(set)  # field -> set of item type names seen inside lists
    has_list = defaultdict(bool)      # field -> was a list ever seen
    total = 0
    for doc in docs:
        total += 1
        for k, v in doc.items():
            if k == "_id":
                continue
            if isinstance(v, list):
                has_list[k] = True
                for item in v:
                    list_item_types[k].add(scalar_type_name(item))
            else:
                scalar_types[k].add(scalar_type_name(v))

    all_fields = set(scalar_types) | set(has_list)
    result = {}
    for field in all_fields:
        parts = sorted(scalar_types.get(field, set()))
        if has_list.get(field):
            items = list_item_types.get(field)
            if items:
                parts.append(f"list<{' | '.join(sorted(items))}>")
            else:
                parts.append("list")
        result[field] = " | ".join(parts)
    return result, total


def detect_relationships(collections_fields):
    """
    Look for fields like 'business_id' / 'user_id' that reference the
    id field of another sampled collection, singular or plural match.
    Returns list of (from_collection, field, to_collection).
    """
    names = list(collections_fields.keys())
    rels = []
    for coll_name, fields in collections_fields.items():
        for field in fields:
            m = re.match(r"^(.+)_id$", field)
            if not m:
                continue
            stem = m.group(1)  # e.g. "business", "user"
            candidates = {stem, stem + "s", stem + "es"}
            for other in names:
                if other == coll_name:
                    continue
                if other.lower() in candidates and "id" in collections_fields[other]:
                    rels.append((coll_name, field, other))
    return rels


def build_label(coll_name, fields, total_docs):
    """Graphviz HTML-like label: a table with collection name as header."""
    rows = []
    shown = sorted(fields.items())[:MAX_FIELDS_SHOWN]
    for fname, ftype in shown:
        safe_name = html.escape(fname)
        safe_type = html.escape(ftype)
        rows.append(
            f'<TR><TD ALIGN="LEFT">{safe_name}</TD>'
            f'<TD ALIGN="LEFT"><FONT COLOR="gray30">{safe_type}</FONT></TD></TR>'
        )
    omitted = len(fields) - len(shown)
    if omitted > 0:
        rows.append(
            f'<TR><TD COLSPAN="2" ALIGN="LEFT"><FONT COLOR="gray50">'
            f'... +{omitted} more field(s)</FONT></TD></TR>'
        )

    header = (
        f'<TR><TD COLSPAN="2" BGCOLOR="#90caf9"><B>{coll_name}</B></TD></TR>'
    )
    return (
        '<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">'
        + header + "".join(rows) + "</TABLE>>"
    )


def render_er_diagram(collections_fields, collections_totals, out_path):
    dot = Digraph("schema", format="png")
    dot.attr(rankdir="LR", fontsize="11", nodesep="0.6", ranksep="1.0",margin="0.5")
    dot.attr("node", shape="plain")

    for coll_name, fields in collections_fields.items():
        label = build_label(coll_name, fields, collections_totals[coll_name])
        dot.node(coll_name, label)

    rels = detect_relationships(collections_fields)
    seen = set()
    for from_coll, field, to_coll in rels:
        key = (from_coll, field, to_coll)
        if key in seen:
            continue
        seen.add(key)
        dot.edge(from_coll, to_coll, label=field, fontsize="10",
                  color="#555555", fontcolor="#555555")

    root, ext = os.path.splitext(out_path)
    rendered = dot.render(root, format=ext.lstrip(".") or "png", cleanup=True)
    return rendered


def main():
    parser = argparse.ArgumentParser(description="Render a single ER-style diagram of a MongoDB database")
    parser.add_argument("--uri", default=MONGO_URI, help="MongoDB connection URI")
    parser.add_argument("--db", default=DEFAULT_DB, help="Database name")
    parser.add_argument("--sample", type=int, default=DEFAULT_SAMPLE,
                         help="Number of documents to sample per collection")
    parser.add_argument("--collections", nargs="*", default=None,
                         help="Specific collections to inspect (default: all)")
    parser.add_argument("--out", default=DEFAULT_OUT, help="Output image path (e.g. schema.png)")
    args = parser.parse_args()

    client = MongoClient(args.uri)
    db = client[args.db]

    collections = args.collections or db.list_collection_names()
    if not collections:
        print(f"No collections found in database '{args.db}'.")
        return

    collections_fields = {}
    collections_totals = {}

    for coll_name in sorted(collections):
        coll = db[coll_name]
        docs = list(coll.aggregate([{"$sample": {"size": args.sample}}]))
        if not docs:
            print(f"{coll_name}: empty collection, skipping.")
            continue
        fields, total = summarize_collection(docs)
        collections_fields[coll_name] = fields
        collections_totals[coll_name] = total
        print(f"{coll_name}: {total} docs sampled, {len(fields)} top-level fields")

    if not collections_fields:
        print("Nothing to render.")
        return

    out_path = render_er_diagram(collections_fields, collections_totals, args.out)
    print(f"\nSaved combined schema diagram -> {out_path}")


if __name__ == "__main__":
    main()