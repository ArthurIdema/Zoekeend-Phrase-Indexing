#!/bin/bash
set -e

DB_BASE="database"
OUT_BASE="results"
DATASET="cranfield"
QUERY="cran"
INDEXER="phrase_index.py"

STOPWORDS_LIST=("english" "none")
MODE_LIST=("duckdb" "phrases")
LIMIT_LIST=(-1)
MIN_FREQ_LIST=(4 5 6 7 9 10 11)
MIN_PMI_LIST=(5 6 7 8 9 10 11 12 13 14)

cd ..

for STOPWORDS in "${STOPWORDS_LIST[@]}"; do
  for MODE in "${MODE_LIST[@]}"; do
    for LIMIT in "${LIMIT_LIST[@]}"; do
        for MIN_FREQ in "${MIN_FREQ_LIST[@]}"; do
          for MIN_PMI in "${MIN_PMI_LIST[@]}"; do
            # For duckdb mode, only run once per LIMIT/STOPWORDS (ignore min_freq/min_pmi except first)
            if [[ "$MODE" == "duckdb" && ( "$MIN_FREQ" != "${MIN_FREQ_LIST[0]}" || "$MIN_PMI" != "${MIN_PMI_LIST[0]}" ) ]]; then
              continue
            fi
            DB="${DB_BASE}_${MODE}_${STOPWORDS}_${LIMIT}_${MIN_FREQ}_${MIN_PMI}.db"
            OUT="${OUT_BASE}_${MODE}_${STOPWORDS}_${LIMIT}_${MIN_FREQ}_${MIN_PMI}.txt"

            # Remove old files if they exist
            [ -f "$DB" ] && rm "$DB"
            [ -f "$OUT" ] && rm "$OUT"
            [ -f eval.txt ] && rm eval.txt

            # Timestamped results directory
            RUN_ID=$(date +"%Y%m%d_%H%M%S")_${MODE}_${STOPWORDS}_${LIMIT}_${MIN_FREQ}_${MIN_PMI}
            RESULTS_DIR="results/$RUN_ID"
            mkdir -p "$RESULTS_DIR"

            # Step 1: Build the index
            python "$INDEXER" --db "$DB" --dataset "$DATASET" --stopwords "$STOPWORDS" --mode "$MODE" --limit "$LIMIT" --min-freq "$MIN_FREQ" --min-pmi "$MIN_PMI"

            # Step 2: Search
            ./zoekeend search "$DB" "$QUERY" -o "$OUT"

            # Step 3: Evaluate
            ./zoekeend eval "$OUT" "$QUERY" > eval.txt

            # Save all outputs and settings
            mkdir -p "$RESULTS_DIR"
            mv "$DB" "$RESULTS_DIR/"
            mv "$OUT" "$RESULTS_DIR/"
            mv eval.txt "$RESULTS_DIR/"
            
            # Save settings
            cat > "$RESULTS_DIR/settings.txt" <<EOF
DB: $DB
OUT: $OUT
DATASET: $DATASET
QUERY: $QUERY
STOPWORDS: $STOPWORDS
MODE: $MODE
LIMIT: $LIMIT
MIN_FREQ: $MIN_FREQ
MIN_PMI: $MIN_PMI
RUN_ID: $RUN_ID
EOF

            # Remove temporary files
            rm -f "$DB" "$OUT" eval.txt

            echo "Done. Results stored in $RESULTS_DIR"
            ls -lh "$RESULTS_DIR"
            echo "--------------------------------------"
          done
        done
    done
  done
done