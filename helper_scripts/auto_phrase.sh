#!/bin/bash
set -e

# Settings
DB="database.db"
OUT="results.txt"
DATASET="cranfield"
QUERY="cran"
STOPWORDS="english"
MODE="duckdb"
CODE="phrase_index.py"
EXTRACTOR="phrases_extractor.py"
LIMIT=-1
MIN_FREQ=9
MIN_PMI=4.0

# remove old if exists
[ -f ${DB} ] && rm ${DB}
[ -f ${OUT} ] && rm ${OUT}
[ -f eval.txt ] && rm eval.txt

# Timestamped results directory
RUN_ID=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="results/$RUN_ID"


# Step 1: Build the index
python $CODE --db "$DB" --dataset "$DATASET" --stopwords "$STOPWORDS" --mode "$MODE" --limit "$LIMIT" --min-freq "$MIN_FREQ" --min-pmi "$MIN_PMI"

# Step 2: Search
./zoekeend search "$DB" "$QUERY" -o "$OUT"

# Step 3: Evaluate
./zoekeend eval "$OUT" "$QUERY" | tee eval.txt

# Save all outputs and settings
mkdir -p "$RESULTS_DIR"
mv "$DB" "$RESULTS_DIR/"
mv "$OUT" "$RESULTS_DIR/"
mv eval.txt "$RESULTS_DIR/"
cp $CODE "$RESULTS_DIR/"
cp $EXTRACTOR "$RESULTS_DIR/"

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
rm -f ${DB} $OUT eval.txt

echo ""
echo "Done. Results stored in $RESULTS_DIR"
echo "duckdb -ui $RESULTS_DIR/$DB"
ls "$RESULTS_DIR"