# A super simple helper script
import pandas as pd

# Raw data as provided
data = """RUN_ID  MODE    STOPWORDS       MIN_FREQ        MIN_PMI MAP     POSTINGS_COST   DICT_SIZE       TERMS_SIZE      NGRAMS  AVGDL   SUMDF
20251202_124701_duckdb_english_-1_0_0   duckdb  english 0       0       0.3029  1175.6018       6639    128030  0       91.45   80317
20251202_124736_phrases_english_-1_0_0  phrases english 0       0       0.1528  1986.3274       22059   294974  17119   210.69571428571427 79556
20251202_124941_phrases_english_-1_1_0  phrases english 1       0       0.1528  1986.3274       22059   294974  17119   210.69571428571427 79556
20251202_125148_phrases_english_-1_2_0  phrases english 2       0       0.1694  1885.6283       10771   310491  6617    221.77928571428572 81204
20251202_125314_phrases_english_-1_4_0  phrases english 4       0       0.2008  544.4425        5398    108037  2445    77.16928571428572  77595
20251202_125423_phrases_english_-1_5_0  phrases english 5       0       0.2106  553.8850        4369    108237  1769    77.31214285714286  76576
20251202_125535_phrases_english_-1_6_0  phrases english 6       0       0.2265  595.4425        3705    108849  1355    77.74928571428572  75941
20251202_125641_phrases_english_-1_7_0  phrases english 7       0       0.2311  623.5929        3224    109284  1074    78.06   75467
20251202_125751_phrases_english_-1_8_0  phrases english 8       0       0.2350  651.3894        2890    109010  916     77.86428571428571  74604
20251202_125856_phrases_english_-1_9_0  phrases english 9       0       0.2363  663.6460        2586    108956  760     77.82571428571428  73877
20251202_130002_phrases_english_-1_10_0 phrases english 10      0       0.2345  659.1062        2384    108086  665     77.20428571428572  72858
20251202_130112_phrases_english_-1_11_0 phrases english 11      0       0.2456  674.3717        2199    107573  569     76.83785714285715  72070
20251202_130219_phrases_english_-1_16_0 phrases english 16      0       0.2566  736.9823        1636    105876  341     75.62571428571428  69335
20251202_130326_phrases_english_-1_24_0 phrases english 24      0       0.2625  818.8142        1164    102808  188     73.43428571428572  65298
20251202_130438_phrases_english_-1_48_0 phrases english 48      0       0.2776  982.8053        618     92510   63      66.07857142857142  55513
20251202_130556_duckdb_none_-1_0_0      duckdb  none    0       0       0.2753  6487.9558       7044    239151  0       170.82214285714286 120060
20251202_130628_phrases_none_-1_0_0     phrases none    0       0       0.1056  1834.7257       41549   271903  38602   194.21642857142857 113455
20251202_130815_phrases_none_-1_1_0     phrases none    1       0       0.1056  1834.7257       41549   271903  38602   194.21642857142857 113455
20251202_130959_phrases_none_-1_2_0     phrases none    2       0       0.1150  2822.3982       24994   308752  20739   220.53714285714287 132339
20251202_131109_phrases_none_-1_4_0     phrases none    4       0       0.1330  2399.0265       13170   186633  9931    133.30928571428572 139079
20251202_131155_phrases_none_-1_5_0     phrases none    5       0       0.1322  2826.0354       10622   196894  7723    140.63857142857142 140732
20251202_131247_phrases_none_-1_6_0     phrases none    6       0       0.1517  3127.3540       8967    205296  6318    146.64  142013
20251202_131339_phrases_none_-1_7_0     phrases none    7       0       0.1568  3355.3097       7755    212686  5310    151.91857142857143 142577
20251202_131426_phrases_none_-1_8_0     phrases none    8       0       0.1578  3690.0442       6834    219744  4572    156.96  142713
20251202_131515_phrases_none_-1_9_0     phrases none    9       0       0.1638  4123.8319       6083    227054  3970    162.18142857142857 143265
20251202_131605_phrases_none_-1_10_0    phrases none    10      0       0.1774  4309.0796       5514    233382  3517    166.70142857142858 142934
20251202_131654_phrases_none_-1_11_0    phrases none    11      0       0.1807  5016.9469       5005    240792  3104    171.99428571428572 143123
20251202_131745_phrases_none_-1_16_0    phrases none    16      0       0.2023  6999.1770       3510    269698  1970    192.64142857142858 143556
20251202_131836_phrases_none_-1_24_0    phrases none    24      0       0.2005  10562.2478      2347    303779  1166    216.985 141115
20251202_131929_phrases_none_-1_48_0    phrases none    48      0       0.2327  11640.1858      1155    304904  452     217.78857142857143 121287
20251203_150618_duckdb_english_-1_1_24  duckdb  english 1       24      0.3029  1175.6018       6639    128030  0       91.45   80317
20251203_150657_phrases_english_-1_1_24 phrases english 1       24      0.1528  1986.3274       22059   294974  17119   210.69571428571427 79556
20251203_150905_duckdb_none_-1_1_24     duckdb  none    1       24      0.2753  6487.9558       7044    239151  0       170.82214285714286 120060
20251203_150939_phrases_none_-1_1_24    phrases none    1       24      0.1056  1834.7257       41549   271903  38602   194.21642857142857 113455"""

# Split into rows and then by whitespace
rows = [line.split() for line in data.splitlines()]

# Create DataFrame
df = pd.DataFrame(rows)

# Save to CSV
csv_path = "no_min_pmi_results_table-q113-225.csv"
df.to_csv(csv_path, index=False, header=False)

csv_path