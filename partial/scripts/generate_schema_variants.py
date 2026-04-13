import json
import re
import hashlib
from pathlib import Path
from collections import defaultdict


INPUT_PATH = Path("partial/data/tables_subset.json")
SCHEMA_ROOT = Path("partial/schemas")
MAPPING_ROOT = Path("partial/mappings")

LEVELS = ["L0", "L1", "L2", "L3"]


# -----------------------------
# Utilities
# -----------------------------
def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def normalize_token(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def split_tokens(name: str):
    name = normalize_token(name)
    return [t for t in name.split("_") if t]


def short_hash(s: str, n: int = 6) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()[:n]


# -----------------------------
# L1 abbreviation
# -----------------------------
ABBREV_EXCEPTIONS = {
    "id": "id",
    "ids": "id",
    "no": "no",
    "num": "num",
    "number": "num",
    "date": "dt",
    "time": "tm",
    "year": "yr",
    "month": "mo",
    "day": "dy",
    "name": "nm",
}


def abbreviate_token(tok: str) -> str:
    if tok in ABBREV_EXCEPTIONS:
        return ABBREV_EXCEPTIONS[tok]
    if tok.isdigit():
        return tok
    return tok[:4] if len(tok) > 4 else tok


def l1_abbreviate(name: str) -> str:
    toks = split_tokens(name)
    if not toks:
        return normalize_token(name)
    return "_".join(abbreviate_token(t) for t in toks)


# -----------------------------
# L2 metonymic
# -----------------------------
METONYM_MAP = {
    "salary": "remuneration",
    "pay": "remuneration",
    "wage": "remuneration",
    "income": "earnings",
    "revenue": "economic_value",
    "profit": "net_yield",
    "cost": "expenditure",
    "price": "monetary_value",
    "amount": "magnitude",
    "total": "aggregate",
    "sum": "aggregate",
    "avg": "mean",
    "average": "mean",
    "age": "temporal_span",
    "date": "calendar_mark",
    "time": "temporal_mark",
    "year": "calendar_year",
    "month": "calendar_month",
    "day": "calendar_day",
    "name": "identifier",
    "title": "designation",
    "type": "category",
    "category": "class",
    "status": "state",
    "code": "symbol",
    "zip": "postal_mark",
    "postal": "postal_mark",
    "address": "location_mark",
    "city": "municipality",
    "country": "nation",
    "state": "region",
    "student": "learner",
    "teacher": "instructor",
    "employee": "staff_member",
    "manager": "supervisor",
    "department": "division",
    "company": "organization",
    "id": "key",
    "key": "key",
    "number": "index",
    "count": "tally",
    "rank": "ordering",
    "score": "metric",
    "grade": "evaluation",
}


def metonymize_token(tok: str) -> str:
    tok = normalize_token(tok)
    if tok in METONYM_MAP:
        return METONYM_MAP[tok]
    if len(tok) <= 2 or tok.isdigit():
        return tok
    return f"attr_{short_hash(tok, 6)}"


def l2_metonymic(name: str) -> str:
    toks = split_tokens(name)
    if not toks:
        return normalize_token(name)
    return "_".join(metonymize_token(t) for t in toks)


def get_table_names(db):
    return db.get("table_names_original", db.get("table_names", []))


def get_column_names(db):
    return db.get("column_names_original", db.get("column_names", []))


def build_old_variant_for_db(db, level: str):
    db_id = db["db_id"]
    table_names = [normalize_token(t) for t in get_table_names(db)]
    column_names = get_column_names(db)
    foreign_keys = db.get("foreign_keys", [])

    # Old scheme: keep table names stable for all levels
    table_map = {t: t for t in table_names}

    column_map = {}
    per_table_counter = defaultdict(int)

    # Build table -> columns in original order
    table_columns = defaultdict(list)

    for table_id, col_name in column_names:
        if table_id == -1 or col_name == "*":
            continue

        orig_table = table_names[table_id]
        orig_col = normalize_token(col_name)
        full_name = f"{orig_table}.{orig_col}"

        if level == "L0":
            new_col = orig_col
        elif level == "L1":
            new_col = l1_abbreviate(orig_col)
        elif level == "L2":
            new_col = l2_metonymic(orig_col)
        elif level == "L3":
            per_table_counter[table_id] += 1
            new_col = f"col_{per_table_counter[table_id]}"
        else:
            raise ValueError(f"Unknown level: {level}")

        column_map[full_name] = new_col
        table_columns[orig_table].append(new_col)

    tables = []
    for t in table_names:
        tables.append({
            "table_name": table_map[t],
            "columns": table_columns[t],
        })

    fk_pairs = []
    for src_idx, tgt_idx in foreign_keys:
        src_table_id, src_col = column_names[src_idx]
        tgt_table_id, tgt_col = column_names[tgt_idx]

        src_table = table_names[src_table_id]
        tgt_table = table_names[tgt_table_id]
        src_col_norm = normalize_token(src_col)
        tgt_col_norm = normalize_token(tgt_col)

        src_full = f"{src_table}.{src_col_norm}"
        tgt_full = f"{tgt_table}.{tgt_col_norm}"

        fk_pairs.append([
            f"{table_map[src_table]}.{column_map[src_full] if level != 'L0' else src_col_norm}",
            f"{table_map[tgt_table]}.{column_map[tgt_full] if level != 'L0' else tgt_col_norm}",
        ])

    schema = {
        "db_id": db_id,
        "level": level,
        "tables": tables,
        "foreign_keys": fk_pairs,
    }

    mapping = None
    if level in {"L1", "L2", "L3"}:
        mapping = {
            "db_id": db_id,
            "level": level,
            "rename_tables": False,
            "table_map": table_map,
            "column_map": column_map,
        }

    return schema, mapping


def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing input: {INPUT_PATH}")

    tables_data = load_json(INPUT_PATH)

    for level in LEVELS:
        for db in tables_data:
            schema, mapping = build_old_variant_for_db(db, level)
            save_json(SCHEMA_ROOT / level / f"{db['db_id']}.json", schema)

            if mapping is not None:
                save_json(MAPPING_ROOT / level / f"{db['db_id']}.json", mapping)

    print("Generated old-style schema variants:")
    print("  test/schemas/L0-L3/")
    print("  test/mappings/L1-L3/")


if __name__ == "__main__":
    main()