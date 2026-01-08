import pandas as pd
from pathlib import Path

try:
	from scipy.stats import binomtest
	HAS_SCIPY = True
except Exception:
	HAS_SCIPY = False


def main(csv_path: str, out_csv: str = 'comparison_vs_minpmi24.csv'):
	df = pd.read_csv(csv_path)
	df = df.copy()
	if 'min_freq' in df.columns:
		df['min_freq'] = df['min_freq']
	if 'min_pmi' in df.columns:
		df['min_pmi'] = df['min_pmi']
	if 'map' in df.columns:
		df['map'] = df['map']

	group_fields = ['mode', 'stopwords', 'min_freq']

	results = []

	# iterate over groups keyed by (mode, stopwords, min_freq)
	grouped = df.groupby(group_fields)
	for key, group in grouped:
		mode, stopwords, min_freq = key

		# iterate over all min_pmi values present in this group
		for m in sorted(group['min_pmi'].unique()):
			# extract series for this min_pmi and baseline rows where min_pmi == 24 (same group)
			cip = pd.to_numeric(group[group['min_pmi'] == m].set_index('query')['total_postings_cost'], errors='coerce')

			# Compare to baseline with min_pmi == 24 and min_freq == 1 (same mode & stopwords)
			baseline = pd.to_numeric(
				df[
					(df['mode'] == mode)
					& (df['stopwords'] == stopwords)
					& (df['min_pmi'] == 24)
					& (df['min_freq'] == 1)
				].set_index('query')['total_postings_cost'],
				errors='coerce'
			)

			# align queries
			paired = pd.DataFrame({
				'cip': cip,
				'baseline': baseline
			}).dropna()

			better = int((paired['cip'] < paired['baseline']).sum())
			worse  = int((paired['cip'] > paired['baseline']).sum())
			equal  = int((paired['cip'] == paired['baseline']).sum())
			n_pairs = len(paired)
			n_sign = better + worse

			p_value = None
			if HAS_SCIPY and n_sign > 0:
				# pass number of positives (better) as k to binomtest
				p_value = float(binomtest(better, n_sign, p=0.5, alternative='two-sided').pvalue)

			results.append({
				'mode': mode,
				'stopwords': stopwords,
				'min_freq': min_freq,
				'compared_min_pmi': m,
				'n_pairs': n_pairs,
				'n_better': better,
				'n_worse': worse,
				'n_equal': equal,
				'p_value': p_value,
			})


	out_df = pd.DataFrame(results)
	out_df = out_df.sort_values(['mode', 'stopwords', 'min_freq', 'compared_min_pmi'])
	out_df.to_csv(out_csv, index=False)

	# Print a short summary
	total_comparisons = len(out_df)
	print(f"Wrote {out_csv} ({total_comparisons} comparisons)")
	if total_comparisons > 0:
		print(out_df.head(20).to_string(index=False))


if __name__ == '__main__':
	CSV = './spreadsheets/results_per_query-113-225.csv'
	OUT = './spreadsheets/p-values-CiP-q113-225.csv'
	if not Path(CSV).exists():
		print(f"Input CSV not found: {CSV}")
	else:
		if not HAS_SCIPY:
			print("scipy not found: binomial p-values will be omitted (set up scipy to get p-values)")
		main(CSV, OUT)


