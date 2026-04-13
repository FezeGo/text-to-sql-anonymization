import json
import re
from pathlib import Path
from collections import defaultdict


DEV_PATH = Path("spider/spider_data/dev.json")
TABLES_PATH = Path("spider/spider_data/tables.json")
OUTPUT_PATH = Path("data/spider_dev_labeled.json")


def normalize_sql(sql: str) -> str:
    sql = sql.lower()
    sql = re.sub(r"\s+", " ", sql).strip()
    return sql


def has_join(sql: str) -> bool:
    return " join " in sql


def has_aggregation(sql: str) -> bool:
    agg_funcs = ["count(", "sum(", "avg(", "max(", "min("]
    return any(func in sql for func in agg_funcs)


def has_group_by(sql: str) -> bool:
    return " group by " in sql


def has_order_by(sql: str) -> bool:
    return " order by " in sql


def has_limit(sql: str) -> bool:
    return " limit " in sql


def has_nested(sql: str) -> bool:
    return sql.count("select ") > 1


def has_set_op(sql: str) -> bool:
    return any(op in sql for op in [" union ", " intersect ", " except "])


def count_tables_used(sql: str) -> int:
    from_count = sql.count(" from ")
    join_count = sql.count(" join ")
    return from_count + join_count


def classify_query_type(tags: dict) -> str:
    if tags["has_set_op"]:
        return "set_op"
    if tags["has_nested"]:
        return "nested"
    if tags["has_join"] and (tags["has_aggregation"] or tags["has_group_by"]):
        return "join_agg"
    if tags["has_join"]:
        return "join"
    if tags["has_aggregation"] or tags["has_group_by"]:
        return "aggregation"
    return "simple"


def build_schema_stats(tables_json):
    schema_by_db = {}

    for db in tables_json:
        db_id = db["db_id"]
        table_names = db["table_names_original"]
        column_names = db["column_names_original"]  # [[table_id, col_name], ...]
        foreign_keys = db.get("foreign_keys", [])
        primary_keys = db.get("primary_keys", [])

        num_tables = len(table_names)

        # Exclude the special "*" column whose table_id is -1
        real_columns = [col for col in column_names if col[0] != -1]
        num_columns = len(real_columns)

        schema_by_db[db_id] = {
            "num_tables_in_schema": num_tables,
            "num_columns_in_schema": num_columns,
            "num_foreign_keys": len(foreign_keys),
            "num_primary_keys": len(primary_keys),
            "table_names_original": table_names,
        }

    return schema_by_db


def main():
    if not DEV_PATH.exists():
        raise FileNotFoundError(f"Cannot find {DEV_PATH}")

    if not TABLES_PATH.exists():
        raise FileNotFoundError(f"Cannot find {TABLES_PATH}")

    with open(DEV_PATH, "r", encoding="utf-8") as f:
        dev_data = json.load(f)

    with open(TABLES_PATH, "r", encoding="utf-8") as f:
        tables_data = json.load(f)

    schema_by_db = build_schema_stats(tables_data)

    labeled_examples = []

    for idx, ex in enumerate(dev_data):
        db_id = ex["db_id"]
        question = ex["question"]
        query = ex["query"]
        sql = normalize_sql(query)

        tags = {
            "has_join": has_join(sql),
            "has_aggregation": has_aggregation(sql),
            "has_group_by": has_group_by(sql),
            "has_order_by": has_order_by(sql),
            "has_limit": has_limit(sql),
            "has_nested": has_nested(sql),
            "has_set_op": has_set_op(sql),
        }

        num_tables_used = count_tables_used(sql)
        query_type = classify_query_type(tags)

        schema_stats = schema_by_db.get(db_id, {})

        record = {
            "example_id": idx,
            "db_id": db_id,
            "question": question,
            "query": query,
            "difficulty": ex.get("difficulty"),  # Spider dev often has this
            "num_tables_used": num_tables_used,
            "query_type": query_type,
            **tags,
            **schema_stats,
        }

        labeled_examples.append(record)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(labeled_examples, f, indent=2, ensure_ascii=False)

    # Print a tiny summary so you can sanity-check quickly
    by_type = defaultdict(int)
    by_db = defaultdict(int)

    for ex in labeled_examples:
        by_type[ex["query_type"]] += 1
        by_db[ex["db_id"]] += 1

    print(f"Saved {len(labeled_examples)} labeled examples to {OUTPUT_PATH}")
    print("\nQuery type distribution:")
    for k in sorted(by_type):
        print(f"  {k}: {by_type[k]}")

    print(f"\nNumber of unique DBs: {len(by_db)}")


if __name__ == "__main__":
    main()