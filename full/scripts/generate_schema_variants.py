import json
import re
from pathlib import Path
from collections import defaultdict


INPUT_PATH = Path("data/tables_subset.json")
SCHEMA_ROOT = Path("full/schemas")
MAPPING_ROOT = Path("full/mappings")


def ensure_dirs():
    for level in ["L0", "L1", "L2", "L3"]:
        (SCHEMA_ROOT / level).mkdir(parents=True, exist_ok=True)
    for level in ["L1", "L2", "L3"]:
        (MAPPING_ROOT / level).mkdir(parents=True, exist_ok=True)


def normalize_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def abbreviate_token(token: str) -> str:
    special = {
        "name": "nm",
        "date": "dt",
        "result": "res",
        "location": "loc",
        "type": "typ",
        "note": "note",
        "killed": "kill",
        "injured": "inj",
        "bulgarian": "bulg",
        "latin": "lat",
        "commander": "cmdr",
        "disposition": "disp",
        "caused": "caus",
        "ship": "ship",
        "battle": "batt",
        "lost": "lost",
        "tonnage": "tonn",
        "of": "of",
        "by": "by",
        "in": "in",
        "id": "id",
    }
    if token in special:
        return special[token]

    if len(token) <= 4:
        return token

    return token[:4]


def abbreviate_column_name(col_name: str) -> str:
    parts = normalize_name(col_name).split("_")
    return "_".join(abbreviate_token(p) for p in parts)


def classify_semantic_type(col_name: str, spider_col_type: str, is_pk: bool, is_fk: bool) -> str:
    name = normalize_name(col_name)

    if is_pk:
        return "pk"
    if is_fk:
        return "fk"

    if spider_col_type in {"time", "date", "year"}:
        return "date"

    if spider_col_type == "number":
        return "num"

    if any(x in name for x in ["type", "category", "class", "status"]):
        return "cat"
    
    if "date" in col_name or "year" in col_name:
        return "date"

    return "text"


def build_schema_objects(db):
    table_names = db["table_names_original"]
    column_names = db["column_names_original"]
    column_types = db["column_types"]
    primary_keys = set(db.get("primary_keys", []))
    foreign_keys = db.get("foreign_keys", [])

    fk_source_cols = set(src for src, _ in foreign_keys)

    tables = []
    columns_by_table = defaultdict(list)

    for col_idx, ((table_id, col_name), col_type) in enumerate(zip(column_names, column_types)):
        if table_id == -1:
            continue
        columns_by_table[table_id].append(
            {
                "col_idx": col_idx,
                "table_id": table_id,
                "table_name": table_names[table_id],
                "column_name": normalize_name(col_name),
                "column_type": col_type,
                "is_pk": col_idx in primary_keys,
                "is_fk": col_idx in fk_source_cols,
            }
        )

    for table_id, table_name in enumerate(table_names):
        tables.append(
            {
                "table_id": table_id,
                "table_name": normalize_name(table_name),
                "columns": columns_by_table[table_id],
            }
        )

    return tables, foreign_keys, column_names


def make_l0_schema(db_id, tables, foreign_keys, column_names):
    schema_tables = []
    for table in tables:
        schema_tables.append(
            {
                "table_name": table["table_name"],
                "columns": [c["column_name"] for c in table["columns"]],
            }
        )

    fk_pairs = []
    for src_idx, tgt_idx in foreign_keys:
        src_table_id, src_col = column_names[src_idx]
        tgt_table_id, tgt_col = column_names[tgt_idx]
        src_table = normalize_name(tables[src_table_id]["table_name"])
        tgt_table = normalize_name(tables[tgt_table_id]["table_name"])
        fk_pairs.append(
            [
                f"{src_table}.{normalize_name(src_col)}",
                f"{tgt_table}.{normalize_name(tgt_col)}",
            ]
        )

    return {
        "db_id": db_id,
        "level": "L0",
        "tables": schema_tables,
        "foreign_keys": fk_pairs,
    }


def make_l1_mapping_and_schema(db_id, tables, foreign_keys, column_names):
    table_map = {t["table_name"]: t["table_name"] for t in tables}
    column_map = {}

    schema_tables = []
    for table in tables:
        mapped_cols = []
        for col in table["columns"]:
            full_name = f"{table['table_name']}.{col['column_name']}"
            mapped = abbreviate_column_name(col["column_name"])
            column_map[full_name] = mapped
            mapped_cols.append(mapped)
        schema_tables.append(
            {
                "table_name": table["table_name"],
                "columns": mapped_cols,
            }
        )

    fk_pairs = []
    for src_idx, tgt_idx in foreign_keys:
        src_table_id, src_col = column_names[src_idx]
        tgt_table_id, tgt_col = column_names[tgt_idx]

        src_table = normalize_name(tables[src_table_id]["table_name"])
        tgt_table = normalize_name(tables[tgt_table_id]["table_name"])
        src_col_norm = normalize_name(src_col)
        tgt_col_norm = normalize_name(tgt_col)

        fk_pairs.append(
            [
                f"{src_table}.{column_map[f'{src_table}.{src_col_norm}']}",
                f"{tgt_table}.{column_map[f'{tgt_table}.{tgt_col_norm}']}",
            ]
        )

    mapping = {
        "db_id": db_id,
        "level": "L1",
        "table_map": table_map,
        "column_map": column_map,
    }

    schema = {
        "db_id": db_id,
        "level": "L1",
        "tables": schema_tables,
        "foreign_keys": fk_pairs,
    }

    return mapping, schema


def make_l2_mapping_and_schema(db_id, tables, foreign_keys, column_names):
    table_map = {}
    column_map = {}

    schema_tables = []

    for i, table in enumerate(tables, start=1):
        orig_table = table["table_name"]
        mapped_table = f"table_{i}"
        table_map[orig_table] = mapped_table

        counters = defaultdict(int)
        mapped_cols = []

        for col in table["columns"]:
            semantic_type = classify_semantic_type(
                col["column_name"],
                col["column_type"],
                col["is_pk"],
                col["is_fk"],
            )
            counters[semantic_type] += 1
            mapped_col = f"{semantic_type}_{counters[semantic_type]}"
            full_name = f"{orig_table}.{col['column_name']}"
            column_map[full_name] = mapped_col
            mapped_cols.append(mapped_col)

        schema_tables.append(
            {
                "table_name": mapped_table,
                "columns": mapped_cols,
            }
        )

    fk_pairs = []
    for src_idx, tgt_idx in foreign_keys:
        src_table_id, src_col = column_names[src_idx]
        tgt_table_id, tgt_col = column_names[tgt_idx]

        src_table = normalize_name(tables[src_table_id]["table_name"])
        tgt_table = normalize_name(tables[tgt_table_id]["table_name"])
        src_col_norm = normalize_name(src_col)
        tgt_col_norm = normalize_name(tgt_col)

        fk_pairs.append(
            [
                f"{table_map[src_table]}.{column_map[f'{src_table}.{src_col_norm}']}",
                f"{table_map[tgt_table]}.{column_map[f'{tgt_table}.{tgt_col_norm}']}",
            ]
        )

    mapping = {
        "db_id": db_id,
        "level": "L2",
        "table_map": table_map,
        "column_map": column_map,
    }

    schema = {
        "db_id": db_id,
        "level": "L2",
        "tables": schema_tables,
        "foreign_keys": fk_pairs,
    }

    return mapping, schema


def make_l3_mapping_and_schema(db_id, tables, foreign_keys, column_names):
    table_map = {}
    column_map = {}

    schema_tables = []

    for i, table in enumerate(tables, start=1):
        orig_table = table["table_name"]
        mapped_table = f"table_{i}"
        table_map[orig_table] = mapped_table

        mapped_cols = []
        for j, col in enumerate(table["columns"], start=1):
            mapped_col = f"col_{j}"
            full_name = f"{orig_table}.{col['column_name']}"
            column_map[full_name] = mapped_col
            mapped_cols.append(mapped_col)

        schema_tables.append(
            {
                "table_name": mapped_table,
                "columns": mapped_cols,
            }
        )

    fk_pairs = []
    for src_idx, tgt_idx in foreign_keys:
        src_table_id, src_col = column_names[src_idx]
        tgt_table_id, tgt_col = column_names[tgt_idx]

        src_table = normalize_name(tables[src_table_id]["table_name"])
        tgt_table = normalize_name(tables[tgt_table_id]["table_name"])
        src_col_norm = normalize_name(src_col)
        tgt_col_norm = normalize_name(tgt_col)

        fk_pairs.append(
            [
                f"{table_map[src_table]}.{column_map[f'{src_table}.{src_col_norm}']}",
                f"{table_map[tgt_table]}.{column_map[f'{tgt_table}.{tgt_col_norm}']}",
            ]
        )

    mapping = {
        "db_id": db_id,
        "level": "L3",
        "table_map": table_map,
        "column_map": column_map,
    }

    schema = {
        "db_id": db_id,
        "level": "L3",
        "tables": schema_tables,
        "foreign_keys": fk_pairs,
    }

    return mapping, schema


def save_json(path: Path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Cannot find {INPUT_PATH}")

    ensure_dirs()

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        tables_data = json.load(f)

    for db in tables_data:
        db_id = db["db_id"]
        tables, foreign_keys, column_names = build_schema_objects(db)

        l0_schema = make_l0_schema(db_id, tables, foreign_keys, column_names)
        l1_mapping, l1_schema = make_l1_mapping_and_schema(db_id, tables, foreign_keys, column_names)
        l2_mapping, l2_schema = make_l2_mapping_and_schema(db_id, tables, foreign_keys, column_names)
        l3_mapping, l3_schema = make_l3_mapping_and_schema(db_id, tables, foreign_keys, column_names)

        save_json(SCHEMA_ROOT / "L0" / f"{db_id}.json", l0_schema)
        save_json(SCHEMA_ROOT / "L1" / f"{db_id}.json", l1_schema)
        save_json(SCHEMA_ROOT / "L2" / f"{db_id}.json", l2_schema)
        save_json(SCHEMA_ROOT / "L3" / f"{db_id}.json", l3_schema)

        save_json(MAPPING_ROOT / "L1" / f"{db_id}.json", l1_mapping)
        save_json(MAPPING_ROOT / "L2" / f"{db_id}.json", l2_mapping)
        save_json(MAPPING_ROOT / "L3" / f"{db_id}.json", l3_mapping)

    print(f"Generated schema variants for {len(tables_data)} DBs.")
    print("Saved to:")
    print("  schemas/L0-L3/")
    print("  mappings/L1-L3/")


if __name__ == "__main__":
    main()