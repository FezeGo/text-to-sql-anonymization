import json
from pathlib import Path
from collections import defaultdict


INPUT_PATH = Path("data/spider_dev_labeled.json")
OUTPUT_PATH = Path("data/spider_candidate_filtered.json")


def keep_example(ex):
    # 1. Exclude set operations for now
    if ex["query_type"] == "set_op":
        return False

    # 2. Exclude overly large schemas
    if ex.get("num_tables_in_schema", 999) > 8:
        return False

    if ex.get("num_columns_in_schema", 999) > 40:
        return False

    # 3. Exclude very large query table usage if you want a cleaner first pass
    if ex.get("num_tables_used", 999) > 4:
        return False

    return True


def main():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    filtered = [ex for ex in data if keep_example(ex)]

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(filtered, f, indent=2, ensure_ascii=False)

    by_type = defaultdict(int)
    by_db = defaultdict(int)

    for ex in filtered:
        by_type[ex["query_type"]] += 1
        by_db[ex["db_id"]] += 1

    print(f"Saved {len(filtered)} filtered examples to {OUTPUT_PATH}")
    print("\nFiltered query type distribution:")
    for k in sorted(by_type):
        print(f"  {k}: {by_type[k]}")

    print(f"\nNumber of unique DBs after filtering: {len(by_db)}")
    print("\nExamples per DB:")
    for db_id, count in sorted(by_db.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {db_id}: {count}")


if __name__ == "__main__":
    main()