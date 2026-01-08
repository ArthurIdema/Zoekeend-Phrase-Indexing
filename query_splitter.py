#!/usr/bin/env python3
"""
Split each line of `cranfield_queries.tsv` into a separate file.
Files are written to a subdirectory `split_queries/` in the same folder as this script.
"""

from pathlib import Path
import sys

INPUT = Path("cranfield_queries.tsv")
OUTDIR = Path("split_queries")

def main():
  if not INPUT.exists():
    print(f"Input file {INPUT} not found", file=sys.stderr)
    sys.exit(1)

  OUTDIR.mkdir(exist_ok=True)

  with INPUT.open() as f:
    for i, line in enumerate(f, start=1):
      content = line.rstrip('\n')
      out_path = OUTDIR / f"{i}.tsv"
      out_path.write_text(content + "\n")

  print(f"Wrote files to {OUTDIR} (one file per query)")


if __name__ == "__main__":
  main()

    