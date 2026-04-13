import json
import random
from pathlib import Path
from collections import defaultdict


INPUT_PATH = Path("data/spider_candidate_filtered.json")
OUTPUT_PATH = Path("data/spider_subset_80.json")

RANDOM_SEED = 42
MAX_PER_DB = 6

TARGET_COUNTS = {
    "simple": 12,
    "join": 16,
    "aggregation": 16,
    "join_agg": 20,
    "nested": 16,
}


def main():
    random.seed(RANDOM_SEED)

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    by_type = defaultdict(list)
    for ex in data:
        qtype = ex["query_type"]
        if qtype in TARGET_COUNTS:
            by_type[qtype].append(ex)

    for qtype in by_type:
        random.shuffle(by_type[qtype])

    selected = []
    db_counts = defaultdict(int)
    type_counts = defaultdict(int)
    selected_ids = set()

    def can_select(ex):
        if ex["example_id"] in selected_ids:
            return False
        if db_counts[ex["db_id"]] >= MAX_PER_DB:
            return False
        return True

    # Round 1: greedy sampling per type with db cap
    for qtype, target in TARGET_COUNTS.items():
        candidates = by_type[qtype]
        for ex in candidates:
            if type_counts[qtype] >= target:
                break
            if can_select(ex):
                selected.append(ex)
                selected_ids.add(ex["example_id"])
                db_counts[ex["db_id"]] += 1
                type_counts[qtype] += 1

    # Round 2: if any type is short, relax by drawing remaining candidates with db cap only
    for qtype, target in TARGET_COUNTS.items():
        if type_counts[qtype] >= target:
            continue
        for ex in by_type[qtype]:
            if type_counts[qtype] >= target:
                break
            if can_select(ex):
                selected.append(ex)
                selected_ids.add(ex["example_id"])
                db_counts[ex["db_id"]] += 1
                type_counts[qtype] += 1

    # Final sanity check
    total_target = sum(TARGET_COUNTS.values())
    if len(selected) != total_target:
        print(f"WARNING: selected {len(selected)} examples, target was {total_target}")

    selected = sorted(selected, key=lambda x: (x["query_type"], x["db_id"], x["example_id"]))

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(selected, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(selected)} examples to {OUTPUT_PATH}\n")

    print("Selected query type distribution:")
    for qtype in TARGET_COUNTS:
        print(f"  {qtype}: {sum(1 for ex in selected if ex['query_type'] == qtype)}")

    print("\nSelected examples per DB:")
    db_summary = defaultdict(int)
    for ex in selected:
        db_summary[ex["db_id"]] += 1
    for db_id, count in sorted(db_summary.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {db_id}: {count}")


if __name__ == "__main__":
    main()