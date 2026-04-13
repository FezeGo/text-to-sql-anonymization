import json
from pathlib import Path

DATA_PATH = Path("data/spider_subset_80.json")
SCHEMA_ROOT = Path("full/schemas")
PROMPT_ROOT = Path("full/prompts")

LEVELS = ["L0", "L1", "L2", "L3"]


def ensure_dirs():
    for level in LEVELS:
        (PROMPT_ROOT / level).mkdir(parents=True, exist_ok=True)


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def format_schema(schema_obj: dict) -> str:
    lines = []

    for table in schema_obj["tables"]:
        table_name = table["table_name"]
        columns = ", ".join(table["columns"])
        lines.append(f"{table_name}({columns})")

    if schema_obj.get("foreign_keys"):
        lines.append("")
        lines.append("Foreign keys:")
        for src, tgt in schema_obj["foreign_keys"]:
            lines.append(f"{src} = {tgt}")

    return "\n".join(lines)


def build_prompt(question: str, schema_text: str) -> str:
    prompt = f"""You are a Text-to-SQL system.

Given the following database schema, write one SQL query that answers the question.

Schema:
{schema_text}

You may use the foreign key relationships to determine valid joins.

Question:
{question}

Return only one executable SQL query in SQLite format. Do not include any explanation or markdown."""
    return prompt


def main():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Cannot find {DATA_PATH}")

    ensure_dirs()

    examples = load_json(DATA_PATH)

    count = 0
    for ex in examples:
        example_id = ex["example_id"]
        db_id = ex["db_id"]
        question = ex["question"]
        gold_sql = ex["query"]

        for level in LEVELS:
            schema_path = SCHEMA_ROOT / level / f"{db_id}.json"
            if not schema_path.exists():
                raise FileNotFoundError(f"Cannot find schema file: {schema_path}")

            schema_obj = load_json(schema_path)
            schema_text = format_schema(schema_obj)
            prompt_text = build_prompt(question, schema_text)

            prompt_txt_path = PROMPT_ROOT / level / f"{example_id}.txt"
            prompt_meta_path = PROMPT_ROOT / level / f"{example_id}.json"

            with open(prompt_txt_path, "w", encoding="utf-8") as f:
                f.write(prompt_text)

            metadata = {
                "example_id": example_id,
                "db_id": db_id,
                "level": level,
                "question": question,
                "gold_sql": gold_sql,
                "query_type": ex.get("query_type"),
                "difficulty": ex.get("difficulty"),
                "prompt_file": str(prompt_txt_path),
                "schema_file": str(schema_path),
            }

            with open(prompt_meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

        count += 1

    print(f"Generated prompts for {count} examples across {len(LEVELS)} levels.")
    print("Saved to prompts/L0-L3/")


if __name__ == "__main__":
    main()