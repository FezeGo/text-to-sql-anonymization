import json
import time
import re
from pathlib import Path
from google import genai


PROMPT_ROOT = Path("test/prompts")
OUTPUT_ROOT = Path("test/outputs")
LEVELS = ["L0", "L1", "L2", "L3"]

MODEL_NAME = "gemini-3-flash-preview"
SLEEP_TIME = 2.0
MAX_RETRIES = 8

client = genai.Client()


def ensure_dirs():
    for level in LEVELS:
        (OUTPUT_ROOT / level).mkdir(parents=True, exist_ok=True)


def call_gemini(prompt: str) -> str:
    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
    )
    return response.text.strip()


def extract_retry_delay(error_text: str) -> int:
    match = re.search(r"retryDelay': '(\d+)s'", error_text)
    if match:
        return int(match.group(1))
    return 5


def run_single(prompt_path: Path, output_path: Path, meta_path: Path):
    with open(prompt_path, "r", encoding="utf-8") as f:
        prompt_text = f.read()

    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    for attempt in range(MAX_RETRIES):
        try:
            prediction = call_gemini(prompt_text)

            print(f"[{meta['level']}] {meta['example_id']} -> {prediction[:80]}")

            result = {
                "example_id": meta["example_id"],
                "db_id": meta["db_id"],
                "level": meta["level"],
                "question": meta["question"],
                "gold_sql": meta["gold_sql"],
                "predicted_sql": prediction,
                "raw_response": prediction,
                "prompt_file": str(prompt_path),
            }

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False)

            return True

        except Exception as e:
            err_text = str(e)
            print(f"[Retry {attempt + 1}] Error: {err_text}")

            if "429" in err_text or "RESOURCE_EXHAUSTED" in err_text:
                wait_time = extract_retry_delay(err_text) + 2
                print(f"Rate limit hit. Sleeping {wait_time}s...")
                time.sleep(wait_time)
            elif "503" in err_text or "UNAVAILABLE" in err_text:
                wait_time = min(20, 5 * (attempt + 1))
                print(f"Model busy. Sleeping {wait_time}s...")
                time.sleep(wait_time)
            else:
                print("Other error. Sleeping 5s...")
                time.sleep(5)

    print(f"[FAILED] {prompt_path}")
    return False


def main():
    ensure_dirs()

    total = 0
    success = 0

    for level in LEVELS:
        prompt_dir = PROMPT_ROOT / level
        output_dir = OUTPUT_ROOT / level

        prompt_files = sorted(prompt_dir.glob("*.txt"))

        print(f"\n===== Running {level} ({len(prompt_files)} prompts) =====")

        for prompt_file in prompt_files:
            example_id = prompt_file.stem
            output_file = output_dir / f"{example_id}.json"
            meta_file = prompt_dir / f"{example_id}.json"

            if output_file.exists() and output_file.stat().st_size > 0:
                continue

            ok = run_single(prompt_file, output_file, meta_file)

            total += 1
            if ok:
                success += 1

            print(f"[{level}] {example_id} ✔" if ok else f"[{level}] {example_id} ✘")
            time.sleep(SLEEP_TIME)

    print("\n===== ALL DONE =====")
    print(f"Total processed: {total}")
    print(f"Success: {success}")


if __name__ == "__main__":
    main()