import json
import sqlite3
from pathlib import Path
from collections import defaultdict


DB_ROOT = Path("spider/spider_data/database")
OUTPUT_ROOT = Path("full/outputs")
ERROR_ROOT = Path("full/errors")

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


def evaluate_level(level):
    output_dir = OUTPUT_ROOT / level
    files = list(output_dir.glob("*.json"))

    total = 0
    exact_match = 0
    strict_exec_match = 0
    relaxed_exec_match = 0

    for f in files:
        data = json.load(open(f))

        gold = data["gold_sql"].strip().lower()
        pred = data["predicted_sql"].strip().lower()

        db_id = data["db_id"]
        db_path = DB_ROOT / db_id / f"{db_id}.sqlite"

        total += 1

        # Exact match
        if gold == pred:
            exact_match += 1

        # Execution results
        gold_res = execute_sql(db_path, gold)
        pred_res = execute_sql(db_path, pred)

        if gold_res is not None and pred_res is not None:
            # Strict execution match
            if gold_res == pred_res:
                strict_exec_match += 1

            # Relaxed execution match: ignore row order
            try:
                if sorted(gold_res) == sorted(pred_res):
                    relaxed_exec_match += 1
            except Exception:
                pass

    return {
        "total": total,
        "exact_acc": exact_match / total,
        "strict_exec_acc": strict_exec_match / total,
        "relaxed_exec_acc": relaxed_exec_match / total,
    }

def collect_errors(level):
    output_dir = OUTPUT_ROOT / level
    files = list(output_dir.glob("*.json"))

    errors = []

    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        gold = data["gold_sql"]
        pred = data["predicted_sql"]

        db_id = data["db_id"]
        db_path = DB_ROOT / db_id / f"{db_id}.sqlite"

        gold_res = execute_sql(db_path, gold)
        pred_res = execute_sql(db_path, pred)

        if pred_res is None:
            error_type = "syntax_or_execution_error"
        elif gold_res is None:
            error_type = "gold_execution_error"
        elif gold_res != pred_res:
            error_type = "wrong_result"
        else:
            continue

        error_case = {
            "example_id": data.get("example_id"),
            "db_id": db_id,
            "level": level,
            "question": data.get("question"),
            "gold_sql": gold,
            "predicted_sql": pred,
            "error_type": error_type,
            "gold_result": gold_res,
            "pred_result": pred_res,
        }

        errors.append(error_case)

    return errors


def save_errors(level, max_examples=20):
    errors = collect_errors(level)

    full_path = ERROR_ROOT / f"errors_{level}_all.json"
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(errors, f, indent=2, ensure_ascii=False)

    short_errors = []
    for e in errors[:max_examples]:
        short_errors.append({
            "example_id": e["example_id"],
            "db_id": e["db_id"],
            "level": e["level"],
            "question": e["question"],
            "gold_sql": e["gold_sql"],
            "predicted_sql": e["predicted_sql"],
            "error_type": e["error_type"],
        })

    short_path = ERROR_ROOT / f"errors_{level}_sample.json"
    with open(short_path, "w", encoding="utf-8") as f:
        json.dump(short_errors, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(errors)} total errors to {full_path}")
    print(f"Saved first {min(len(errors), max_examples)} sampled errors to {short_path}")


def main():
    results = {}

    for level in LEVELS:
        stats = evaluate_level(level)
        results[level] = stats

    print("\n===== RESULTS =====")
    for level, stats in results.items():
        print(f"{level}:")
        print(f"  total: {stats['total']}")
        print(f"  exact_acc: {stats['exact_acc']:.3f}")
        print(f"  strict_exec_acc: {stats['strict_exec_acc']:.3f}")
        print(f"  relaxed_exec_acc: {stats['relaxed_exec_acc']:.3f}")

    results_path = "evaluation_summary.json"
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nSaved evaluation summary to {results_path}")

    print("\n===== SAVING ERROR CASES =====")
    for level in LEVELS:
        save_errors(level, max_examples=20)


if __name__ == "__main__":
    main()