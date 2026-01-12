#!/bin/bash
# This script can be used to run search and evaluation over existing databases in a results directory
# Usage: ./batch_search_eval.sh <results_dir> <queries_dir> <qrels_file>

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <results_dir> <queries_dir> <qrels_file>"
    exit 1
fi

RESULTS_DIR="$1"
QUERIES_DIR="$2"
QRELS_FILE="$3"
ZOEKEEND_PATH="./zoekeend"

for QUERIES_FILE in "$QUERIES_DIR"/*.tsv; do
    QUERY_BASE=$(basename "$QUERIES_FILE" .tsv)
    OUTDIR="$RESULTS_DIR/$QUERY_BASE"
    mkdir -p "$OUTDIR"

    find "$RESULTS_DIR" -name "*.db" | while read DB_FILE; do
        BASE=$(basename "$DB_FILE" .db)
        RESULTS_FILE="$OUTDIR/${BASE}_results.txt"
        EVAL_FILE="$OUTDIR/${BASE}_eval.txt"

        echo "Running search for $DB_FILE with $QUERIES_FILE..."
        "$ZOEKEEND_PATH" search "$DB_FILE" "$QUERIES_FILE" -o "$RESULTS_FILE"

        echo "Running evaluation for $RESULTS_FILE..."
        "$ZOEKEEND_PATH" eval "$RESULTS_FILE" "$QRELS_FILE" > "$EVAL_FILE"
    done
done

echo "All searches and evaluations completed."