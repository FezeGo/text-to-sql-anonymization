import json
from pathlib import Path

TABLES_PATH = Path("spider/spider_data/tables.json")
DB_LIST_PATH = Path("data/subset_db_ids.json")
OUTPUT_PATH = Path("data/tables_subset.json")

def main():
    if not TABLES_PATH.exists():
        raise FileNotFoundError(f"Cannot find {TABLES_PATH}")

    if not DB_LIST_PATH.exists():
        raise FileNotFoundError(f"Cannot find {DB_LIST_PATH}")

    with open(DB_LIST_PATH, "r", encoding="utf-8") as f:
        target_db_ids = set(json.load(f))

    with open(TABLES_PATH, "r", encoding="utf-8") as f:
        tables_data = json.load(f)

    subset = [db for db in tables_data if db["db_id"] in target_db_ids]

    found_db_ids = set(db["db_id"] for db in subset)

    print(f"Requested DBs: {len(target_db_ids)}")
    print(f"Found DBs: {len(found_db_ids)}\n")

    missing = target_db_ids - found_db_ids
    if missing:
        print("Missing DBs:")
        for db in missing:
            print(f"  {db}")

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(subset, f, indent=2, ensure_ascii=False)

    print(f"\nSaved subset tables to {OUTPUT_PATH}")

    print("\nSchema stats:")
    for db in subset:
        db_id = db["db_id"]
        num_tables = len(db["table_names_original"])
        num_columns = len([c for c in db["column_names_original"] if c[0] != -1])
        num_fks = len(db.get("foreign_keys", []))

        print(f"{db_id}: tables={num_tables}, columns={num_columns}, fks={num_fks}")


if __name__ == "__main__":
    main()