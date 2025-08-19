import pathlib
import os

import ir_datasets


class ir_dataset_test:
    class Doc:
        def __init__(self, doc_id, text):
            self.doc_id = doc_id
            self.text = text
    class Query:
        def __init__(self, query_id, text):
            self.query_id = query_id
            self.text = text
    class Qrel:
        def __init__(self, query_id, doc_id, relevance):
            self.query_id = query_id
            self.doc_id = doc_id
            self.relevance = relevance

    # Custom documents
    # Custom documents
    doc1  = Doc('d1', 'Custom document one about information retrieval.')
    doc2  = Doc('d2', 'Custom document two about machine learning.')
    doc3  = Doc('d3', 'Custom document three about artificial intelligence.')
    doc4  = Doc('d4', 'Custom-document FOUR about INFORMATION-RETRIEVAL and its applications.')
    doc5  = Doc('d5', 'Another custom document, artificial intelligence with punctuation! And special characters like @#$%.')
    doc6  = Doc('d6', 'Machine-learning is artificial amazing; it combines AI, data-science, and more.')
    doc7  = Doc('d7', 'Information retrieval is the backbone of search engines and academic research.')
    doc8  = Doc('d8', 'Machine learning has become a core part of artificial intelligence.')
    doc9  = Doc('d9', 'Artificial intelligence artificial kip saté and machine learning are fields with significant overlap.')
    doc10 = Doc('d10', 'Machine learning is a subfield of artificial intelligence focused on data.')
    doc11 = Doc('d11', 'The process of information retrieval includes indexing and ranking documents.')
    doc12 = Doc('d12', 'Many AI systems rely on both machine learning and information retrieval.')
    doc13 = Doc('d13', 'Artificial intelligence kip saté is widely used in natural language processing and robotics.')
    doc14 = Doc('d14', 'Information retrieval systems are essential for finding relevant documents.')
    doc15 = Doc('d15', 'Machine learning algorithms adapt based on data patterns.')
    doc16 = Doc('d16', 'Artificial intelligence kip saté applications range from games to healthcare.')
    doc17 = Doc('d17', 'Information retrieval helps systems return relevant search results.')
    doc18 = Doc('d18', 'Machine learning and artificial intelligence are driving modern technology.')
    doc19 = Doc('d19', 'Artificial intelligence is often combined with information retrieval to build smart assistants.')
    doc20 = Doc('d20', 'The in the over at on Advanced machine learning techniques artificial intelligence are part of the artificial intelligence stack.')

    docs  = [doc1,  doc2,  doc3,  doc4,  doc5,  doc6,  doc7,  doc8,  doc9,  doc10,
             doc11, doc12, doc13, doc14, doc15, doc16, doc17, doc18, doc19, doc20]

    # Custom queries
    query1 = Query('1', 'information retrieval')
    query2 = Query('2', 'machine learning')
    query3 = Query('3', 'artificial intelligence')
    queries = [query1, query2, query3]

    # Custom relevance judgments
    qrel1 = Qrel('1', 'd1', 2)
    qrel2 = Qrel('2', 'd2', 1)
    qrel3 = Qrel('3', 'd3', 1)
    qrels = [qrel1, qrel2, qrel3]

    def docs_count(self):
        return len(self.docs)

    def docs_iter(self):
        return self.docs

    def queries_iter(self):
        return self.queries

    def qrels_iter(self):
        return self.qrels


def file_exists(name_in):
    return pathlib.Path(name_in).is_file()


def get_qrels(experiment):
    if experiment == "custom":
        from ze_eval import ir_dataset_test
        qrel_file = "custom.qrels"
        if not pathlib.Path(qrel_file).is_file():
            with open(qrel_file, 'w') as file:
                for q in ir_dataset_test().qrels_iter():
                    line = q.query_id + ' Q0 ' + q.doc_id + " " + str(q.relevance)
                    file.write(line + '\n')
        return qrel_file
    if pathlib.Path(experiment).is_file(): # provide a qrels file directly...
        return experiment
    ir_dataset = ir_datasets.load(experiment) # ... or an ir_dataset
    ir_dataset_qrels = ir_dataset.qrels_iter()
    qrel_file = experiment + '.qrels'
    qrel_file = qrel_file.replace('/', '_')
    if not pathlib.Path(qrel_file).is_file():
        with open(qrel_file, 'w') as file:
            for q in ir_dataset_qrels:
                line = q.query_id + ' Q0 ' + q.doc_id + " " + str(q.relevance)
                file.write(line + '\n')
    return qrel_file

def trec_eval(run_name, experiment, complete_rel=False,
        ndcg=False, query_eval=False):
    qrel_file = get_qrels(experiment)
    switches = '-m official'
    if ndcg:
        switches += ' -m ndcg_cut'
    if complete_rel:
        switches += ' -c'
    if query_eval:
        switches += ' -q'
    command = f"trec_eval {switches} {qrel_file} {run_name}"
    print(command)
    os.system(command)
    # After running trec_eval, compute and print average postings cost if available in run file
    try:
        with open(run_name, 'r') as f:
            postings_costs = {}
            for line in f:
                parts = line.strip().split()
                if len(parts) >= 7:
                    query_id = parts[0]
                    try:
                        cost = float(parts[6])
                        if query_id not in postings_costs:
                            postings_costs[query_id] = cost
                    except Exception:
                        continue
            if postings_costs:
                avg_cost = sum(postings_costs.values()) / len(postings_costs)
                print(f"Average cost in postings: {avg_cost:.4f}")
                print(f"Total postings cost: {sum(postings_costs.values()):.4f}")
    except Exception:
        pass
