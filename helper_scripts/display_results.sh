#!/bin/bash

echo -e "RUN_ID\tMODE\tSTOPWORDS\tMIN_FREQ\tMIN_PMI\tMAP\tPOSTINGS_COST\tDICT_SIZE\tTERMS_SIZE\tNGRAMS\tAVGDL\tSUMDF"

for dir in ../results_new_postings/*; do
    [ -d "$dir" ] || continue
    SETTINGS="$dir/settings.txt"
    EVAL="$dir/eval.txt"
    DB=$(grep '^DB:' "$SETTINGS" | awk '{print $2}')
    DB="$dir/$(basename "$DB")"
    if [[ -f "$SETTINGS" && -f "$EVAL" && -f "$DB" ]]; then
        RUN_ID=$(grep '^RUN_ID:' "$SETTINGS" | awk '{print $2}')
        MODE=$(grep '^MODE:' "$SETTINGS" | awk '{print $2}')
        STOPWORDS=$(grep '^STOPWORDS:' "$SETTINGS" | awk '{print $2}')
        MIN_FREQ=$(grep '^MIN_FREQ:' "$SETTINGS" | awk '{print $2}')
        MIN_PMI=$(grep '^MIN_PMI:' "$SETTINGS" | awk '{print $2}')
        MAP=$(grep -E '^map[[:space:]]+all' "$EVAL" | awk '{print $3}')
        POSTINGS_COST=$(grep '^Average cost in postings:' "$EVAL" | awk '{print $5}')
        DICT_SIZE=$(duckdb "$DB" -csv -noheader "SELECT COUNT(*) FROM fts_main_documents.dict;")
        TERMS_SIZE=$(duckdb "$DB" -csv -noheader "SELECT COUNT(*) FROM fts_main_documents.terms;")
        NGRAMS=$(duckdb "$DB" -csv -noheader "SELECT COUNT(*) FROM fts_main_documents.dict WHERE term LIKE '% %';")
        AVGDL=$(duckdb "$DB" -csv -noheader "SELECT avgdl FROM fts_main_documents.stats;")
        SUMDF=$(duckdb "$DB" -csv -noheader "SELECT sumdf FROM fts_main_documents.stats;")
        echo -e "${RUN_ID}\t${MODE}\t${STOPWORDS}\t${MIN_FREQ}\t${MIN_PMI}\t${MAP}\t${POSTINGS_COST}\t${DICT_SIZE}\t${TERMS_SIZE}\t${NGRAMS}\t${AVGDL}\t${SUMDF}"
    fi
done
