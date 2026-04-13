import json
from pathlib import Path

INPUT_PATH = Path("data/spider_subset_80.json")
OUTPUT_PATH = Path("data/subset_db_ids.json")


def main():
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Cannot find {INPUT_PATH}")

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    db_ids = sorted(list(set(ex["db_id"] for ex in data)))

    print(f"Found {len(db_ids)} unique DBs:\n")
    for db in db_ids:
        print(db)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(db_ids, f, indent=2, ensure_ascii=False)

    print(f"\nSaved DB list to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()