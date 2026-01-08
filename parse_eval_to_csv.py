import csv
import re
from pathlib import Path

base_folder = Path("attempt_fixed_baseline/no_min_pmi-1-112")  # adjust if needed
output_file = "attempt_fixed_baseline/spreadsheets/results_per_query_no_min_pmi_1-112v2.csv"

# regex for metrics
re_all = re.compile(r"^(\S+)\s+all\s+([0-9.\-]+)\s*$")
re_colon = re.compile(r"^([A-Za-z0-9 \-]+):\s*([0-9.\-]+)\s*$")

def normalize_name(s: str) -> str:
    return re.sub(r"\s+", "_", s.strip().lower())

def parse_file(filepath: Path):
    metrics = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if m := re_all.match(line):
                metrics[m.group(1)] = m.group(2)
            elif m2 := re_colon.match(line):
                metrics[normalize_name(m2.group(1))] = m2.group(2)
    return metrics

def parse_filename(filename: str):
    base = Path(filename).stem
    parts = base.split("_")
    # Expected: ['database', 'duckdb', 'english', '-1', '0', '0', 'eval']
    try:
        return {
            "mode": parts[1],
            "stopwords": parts[2],
            "min_freq": parts[4],
            "min_pmi": parts[5]
        }
    except IndexError:
        return {
            "mode": "unknown",
            "stopwords": "unknown",
            "min_freq": "unknown",
            "min_pmi": "unknown"
        }

rows = []
all_keys = set()

# Only include numeric folders, sort numerically. If there are none, fall back to
# flat files directly inside base_folder (e.g., aggregated results for queries 1-112).
numeric_folders = [f for f in base_folder.iterdir() if f.is_dir() and f.name.isdigit()]
numeric_folders.sort(key=lambda f: int(f.name))

if numeric_folders:
    for query_folder in numeric_folders:
        query_num = query_folder.name
        for file in query_folder.glob("*.txt"):
            if not re.match(r"database_.*_eval\.txt", file.name):
                continue

            metrics = parse_file(file)
            metadata = parse_filename(file.name)
            combined = {"query": query_num, **metadata, **metrics}
            rows.append(combined)
            all_keys.update(combined.keys())
else:
    for file in base_folder.glob("*.txt"):
        if not re.match(r"database_.*_eval\.txt", file.name):
            continue

        metrics = parse_file(file)
        metadata = parse_filename(file.name)
        combined = {"query": "all", **metadata, **metrics}
        rows.append(combined)
        all_keys.update(combined.keys())

# Write output CSV
all_keys = sorted(all_keys)
with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=all_keys)
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ Combined {len(rows)} result files from {len(numeric_folders)} folders into {output_file}")
