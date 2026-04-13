import json
import sqlite3
from pathlib import Path
from collections import defaultdict

DB_ROOT = Path("spider/spider_data/database")
OUTPUT_ROOT = Path("full/outputs")
PROMPT_ROOT = Path("full/prompts")

LEVELS = ["L0", "L1", "L2", "L3"]


def execute_sql(db_path, sql):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        return result
    except Exception:
        return None


def analyze_level(level):
    stats = defaultdict(lambda: {"total": 0, "correct": 0})
    output_dir = OUTPUT_ROOT / level

    for f in output_dir.glob("*.json"):
        with open(f, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        example_id = data["example_id"]
        db_id = data["db_id"]

        meta_path = PROMPT_ROOT / level / f"{example_id}.json"
        with open(meta_path, "r", encoding="utf-8") as fh:
            meta = json.load(fh)

        qtype = meta["query_type"]

        gold = data["gold_sql"].strip().lower()
        pred = data["predicted_sql"].strip().lower()

        db_path = DB_ROOT / db_id / f"{db_id}.sqlite"

        gold_res = execute_sql(db_path, gold)
        pred_res = execute_sql(db_path, pred)

        stats[qtype]["total"] += 1

        if gold_res is not None and pred_res is not None:
            try:
                if sorted(gold_res) == sorted(pred_res):
                    stats[qtype]["correct"] += 1
            except Exception:
                pass

    print(f"\n=== OLD {level} by query type ===")
    for qtype, s in stats.items():
        acc = s["correct"] / s["total"] if s["total"] else 0.0
        print(f"{qtype}: {acc:.3f} ({s['correct']}/{s['total']})")


def main():
    for level in LEVELS:
        analyze_level(level)


if __name__ == "__main__":
    main()