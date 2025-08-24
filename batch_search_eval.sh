#!/bin/bash
# Batch search and evaluation for Zoekeend results
# Usage: ./batch_search_eval.sh <results_dir>

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <results_dir>"
    exit 1
fi

RESULTS_DIR="$1"
QUERIES_FILE="cranfield_queries.tsv"
QRELS_FILE="cranfield_qrels.tsv"
ZOEKEEND_PATH="./zoekeend"

find "$RESULTS_DIR" -name "*.db" | while read DB_FILE; do
    BASE=$(basename "$DB_FILE" .db)
    RESULTS_FILE="$RESULTS_DIR/${BASE}_results.txt"
    EVAL_FILE="$RESULTS_DIR/${BASE}_eval.txt"

    echo "Running search for $DB_FILE..."
    "$ZOEKEEND_PATH" search "$DB_FILE" "$QUERIES_FILE" -o "$RESULTS_FILE"

    echo "Running evaluation for $RESULTS_FILE..."
    "$ZOEKEEND_PATH" eval "$RESULTS_FILE" "$QRELS_FILE" > "$EVAL_FILE"

done

echo "All searches and evaluations completed."
