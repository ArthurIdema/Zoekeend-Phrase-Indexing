#!/bin/bash
set -e

# Settings
DB="database.db"
OUT="results.txt"
DATASET="cranfield"
QUERY="cran"
STOPWORDS="english"

# remove old if exists
[ -f ${DB} ] && rm ${DB}
[ -f ${OUT} ] && rm ${OUT}
[ -f eval.txt ] && rm eval.txt

# Timestamped results directory
RUN_ID=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="resultszoekeend/$RUN_ID"


cd ..

# Step 1: Build the index
python ./zoekeend index $DB $DATASET -s "$STOPWORDS"

# Step 2: Search
./zoekeend search "$DB" "$QUERY" -o "$OUT"

# Step 3: Evaluate
./zoekeend eval "$OUT" "$QUERY" | tee eval.txt

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
RUN_ID: $RUN_ID
EOF

# Remove temporary files
rm -f ${DB} $OUT eval.txt

echo "Done. Results stored in $RESULTS_DIR"
ls -lh "$RESULTS_DIR"