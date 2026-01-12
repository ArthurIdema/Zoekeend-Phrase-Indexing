## How to use
Run `python3 phrase_index.py` with any of the parameters listed below:
```
  -h, --help            show this help message and exit
  --db DB               Database file name
  --dataset DATASET     ir_datasets name (e.g., cranfield, msmarco-passage)
  --stopwords STOPWORDS Stopwords to use (english, none)
  --mode MODE           Indexing mode (duckdb, phrases)
  --min-freq MIN_FREQ   Minimum frequency for phrases (only for mode "phrases")
  --min-pmi MIN_PMI     Minimum PMI for phrases (only for mode "phrases")
```

## Helper scripts
- `./auto_phrase.sh` and `./auto_zoekeend.sh` can be used to automatically index, search and evaluate the results and store it in a results directory.  `auto_phrase` uses `phrase_index.py`, while `auto_zoekeend` uses `ze_index.py`.

- `./batch_phrase.sh` can be used to create the results using multiple different variables in one go.

- And `display_results.sh` can be used to display the evaluation metrics of all previous results. (So MAP, CiP, dictionary size, terms size, number of phrases, AVGDL and SUMDF)

### Statistical Analysis and Comparison
- **[compare_phrases_vs_duckdb.py](compare_phrases_vs_duckdb.py)** - Performs two-tailed pairwise sign test comparing MAP (Mean Average Precision) results between phrase-based and baseline approaches. Uses min_pmi=24 as baseline. Requires scipy for statistical testing.

- **[compare_postings_cost_vs_duckdb.py](compare_postings_cost_vs_duckdb.py)** - Similar to above but compares Cost in Postings (CiP) metric instead of MAP. Evaluates computational efficiency of different indexing approaches.
