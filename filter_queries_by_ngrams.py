import csv

# Read ngrams from ngrams.csv (second column only)
# ngrams.csv format: id, term, frequency
ngrams = set()
with open('ngrams.csv', 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    for row in reader:
        # Only use the term (second column)
        if len(row) >= 2:
            ngrams.add(row[1].strip())

# Function to count how many ngrams are present in the query
def count_ngrams_in_query(query, ngrams):
    query_lower = query.lower()
    count = 0
    for ngram in ngrams:
        if ngram.lower() in query_lower:
            count += 1
    return count

# Open the input queries file and output file
with open('cranfield_queries.tsv', 'r', encoding='utf-8') as infile, \
     open('cranfield_queries_with_ngrams.tsv', 'w', encoding='utf-8', newline='') as outfile:
    reader = csv.reader(infile, delimiter='\t')
    writer = csv.writer(outfile, delimiter='\t')
    for row in reader:
        # Skip malformed lines
        if len(row) < 2:
            continue
        qid, query = row[0], row[1]
        # Write the query if it contains 2 or more ngrams
        if count_ngrams_in_query(query, ngrams) >= 2:
            writer.writerow([qid, query])
