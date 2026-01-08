#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${BASE_DIR}/attempt_fixed_baseline/results_new_postings_no_min_pmi"
EVAL_DIR_HALF1="${RESULTS_DIR}/cranfield_queries_half1"

if [[ ! -d "${RESULTS_DIR}" ]]; then
    echo "Results directory not found: ${RESULTS_DIR}" >&2
    exit 1
fi

echo -e "RUN_ID\tMODE\tSTOPWORDS\tMIN_FREQ\tMIN_PMI\tMAP\tPOSTINGS_COST\tDICT_SIZE\tTERMS_SIZE\tNGRAMS\tAVGDL\tSUMDF"

for dir in "${RESULTS_DIR}"/*; do
    [[ -d "${dir}" ]] || continue
    SETTINGS="${dir}/settings.txt"
    [[ -f "${SETTINGS}" ]] || continue

    DB_NAME=$(grep '^DB:' "${SETTINGS}" | awk '{print $2}')
    DB_PATH="${dir}/$(basename "${DB_NAME}")"
    [[ -f "${DB_PATH}" ]] || continue

    RUN_ID=$(grep '^RUN_ID:' "${SETTINGS}" | awk '{print $2}')
    MODE=$(grep '^MODE:' "${SETTINGS}" | awk '{print $2}')
    STOPWORDS=$(grep '^STOPWORDS:' "${SETTINGS}" | awk '{print $2}')
    MIN_FREQ=$(grep '^MIN_FREQ:' "${SETTINGS}" | awk '{print $2}')
    MIN_PMI=$(grep '^MIN_PMI:' "${SETTINGS}" | awk '{print $2}')
    DICT_SIZE=$(duckdb "${DB_PATH}" -csv -noheader "SELECT COUNT(*) FROM fts_main_documents.dict;")
    TERMS_SIZE=$(duckdb "${DB_PATH}" -csv -noheader "SELECT COUNT(*) FROM fts_main_documents.terms;")
    NGRAMS=$(duckdb "${DB_PATH}" -csv -noheader "SELECT COUNT(*) FROM fts_main_documents.dict WHERE term LIKE '% %';")
    AVGDL=$(duckdb "${DB_PATH}" -csv -noheader "SELECT avgdl FROM fts_main_documents.stats;")
    SUMDF=$(duckdb "${DB_PATH}" -csv -noheader "SELECT sumdf FROM fts_main_documents.stats;")

    DB_BASENAME=$(basename "${DB_PATH}" .db)
    EVAL_HALF1="${EVAL_DIR_HALF1}/${DB_BASENAME}_eval.txt"
    [[ -f "${EVAL_HALF1}" ]] || continue

    MAP=$(grep -E '^map[[:space:]]+all' "${EVAL_HALF1}" | awk '{print $3}')
    POSTINGS_COST=$(grep '^Average cost in postings:' "${EVAL_HALF1}" | awk '{print $5}')
    echo -e "${RUN_ID}\t${MODE}\t${STOPWORDS}\t${MIN_FREQ}\t${MIN_PMI}\t${MAP}\t${POSTINGS_COST}\t${DICT_SIZE}\t${TERMS_SIZE}\t${NGRAMS}\t${AVGDL}\t${SUMDF}"
done
