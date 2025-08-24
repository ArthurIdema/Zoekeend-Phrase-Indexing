        CREATE OR REPLACE MACRO fts_main_documents.match_lm(query_string, fields := NULL, lambda := 0.3, conjunctive := 0) AS TABLE (
        WITH tokens AS (
            SELECT DISTINCT stem(unnest(fts_main_documents.tokenize(query_string)), 'none') AS t
        ),
        fieldids AS (
            SELECT fieldid
            FROM fts_main_documents.fields
            WHERE CASE WHEN ((fields IS NULL)) THEN (1) ELSE (field = ANY(SELECT * FROM (SELECT unnest(string_split(fields, ','))) AS fsq)) END
        ),
        qtermids AS (
            SELECT termid, df
            FROM fts_main_documents.dict AS dict, tokens
            WHERE (dict.term = tokens.t)
        ),
        qterms AS (
            SELECT termid, docid
            FROM fts_main_documents.terms AS terms
            WHERE (CASE WHEN ((fields IS NULL)) THEN (1) ELSE (fieldid = ANY(SELECT * FROM fieldids)) END
            AND (termid = ANY(SELECT qtermids.termid FROM qtermids)))
        ),
        term_tf AS (
            SELECT termid, docid, count_star() AS tf
            FROM qterms
            GROUP BY docid, termid
        ),
        cdocs AS (
            SELECT docid
            FROM qterms
            GROUP BY docid
            HAVING CASE WHEN (conjunctive) THEN ((count(DISTINCT termid) = (SELECT count_star() FROM tokens))) ELSE 1 END
        ),
        subscores AS (
           SELECT docs.docid, docs.len AS doc_len, term_tf.termid, term_tf.tf, qtermids.df, LN(1 + (lambda * tf * (SELECT ANY_VALUE(sumdf) FROM fts_main_documents.stats)) / ((1-lambda) * df * docs.len)) AS subscore
           FROM term_tf, cdocs, fts_main_documents.docs AS docs, qtermids
           WHERE ((term_tf.docid = cdocs.docid)
           AND (term_tf.docid = docs.docid)
           AND (term_tf.termid = qtermids.termid))
        ),
        scores AS (
           SELECT docs.name AS docname, LN(MAX(doc_len)) + sum(subscore) AS score FROM subscores, fts_main_documents.docs AS docs WHERE subscores.docid = docs.docid GROUP BY docs.name
        ),
        postings_cost AS (
           SELECT COUNT(*) AS cost FROM term_tf
        )
        SELECT docname, score, (SELECT cost FROM postings_cost) AS postings_cost FROM scores
        );