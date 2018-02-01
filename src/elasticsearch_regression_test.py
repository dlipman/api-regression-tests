from argparse import ArgumentParser

import elasticsearch
import pandas as pd
import numpy as np
import re
import scipy.stats
# DL (apparently unused) import codecs

from utils import parse_varlines_csv_file

QUERY_BODY_TEMPLATE_RAW = u"""
{
    "query": {
        "multi_match": {
            "type": "phrase",
            "slop": 1000,
            "fields": ["parsed_text*"],
            "query": "${query_string}",
            "tie_breaker": 0.001,
            "minimum_should_match": "3<80%"
        }
    }
}
"""

# DL - This template is to be added to the query for filtered searches (e.g. for testing negatives)
FILTER_TEMPLATE_RAW = u"""
    "post_filter": {
        "term": {
            "${field_to_test}": "${value_to_test}"
        }
    }"""


def escape_template(t):
    escaped = t.replace("{", "{{").replace("}", "}}")
    return re.sub("\$\{(\{.*?\})\}", "\\1", escaped)


QUERY_BODY_TEMPLATE = escape_template(QUERY_BODY_TEMPLATE_RAW)
# print QUERY_BODY_TEMPLATE

FILTER_TEMPLATE = escape_template(FILTER_TEMPLATE_RAW)

# DL - This function adds a post_filter to an already formatted query string
def add_filter(query_before, field, value):
    insert_string = FILTER_TEMPLATE.format(field_to_test=field, value_to_test=value)
    # insert_point = len(query_before) - 3
    insert_point = query_before[:query_before.rfind("}")].rfind("}") + 1
    query_after = query_before[:insert_point] + "," + insert_string + query_before[insert_point:]
    return query_after


class ElasticSearchRegressionTest(object):
    def __init__(self, input_fname, output_fname, es_host="localhost:9200"):
        self.es_host = es_host
        self.output_fname = output_fname
        self.input_fname = input_fname
        self._es = elasticsearch.Elasticsearch(es_host)
        # DLW - these are to be able to be overwritten by an overridden prepare function
        self.index_name = "tanakh"
        self.column_list = ["score", "parsed_text", "xml:id"]
        self.key_field = "xml:id"

    @staticmethod
    def get_parser():
        parser = ArgumentParser(description="this script performs a regression test on the ElasticSearch server")
        # ArgumentParser.add_argument(
        # name or flags...[, action][, nargs][, const][, default][, type][, choices][, required]
        #   [, help][, metavar][, dest])

        parser.add_argument("input_fname", help="csv file with the queries and their expected results")
        parser.add_argument("output_fname", help="file to write the query report into")
        parser.add_argument("-H", "--es_host", default="localhost:9200",
                            help="hostname:port for the ES server to upload to")

        return parser

# DL - Note the removal of the first step (creating the query string), now done by one of the above two functions
    def get_hits(self, query, fields_to_filter={}, doc_type="", size=10, verbose=False):
        body = QUERY_BODY_TEMPLATE.format(query_string=query)
        for field_to_filter, value_to_filter in fields_to_filter.iteritems():
            body = add_filter(body, field_to_filter, value_to_filter)
        if verbose:
            print "querying:", repr(query)
            print body

        doc_types = [doc_type] if len(doc_type) else ["small", "large"]
        res = []
        for doc_type in doc_types:
            if self.index_name != "tanakh":
                resd = self._es.search(index=self.index_name, body=body, size=size,
                                       _source_include=self.column_list) # DLW - changed index and removed doc_type
            else:
                resd = self._es.search(index="tanakh", doc_type=doc_type, body=body, size=size,
                                       _source_include=self.column_list)

            def hit2d(hit, copy_fields={"score": "_score"}):
                d = dict(hit["_source"])
                assert len(set(d) & set(copy_fields)) == 0,\
                    ValueError("hit fields include the following: {}".format(set(d) & set(copy_fields)))
                for fname, hitfname in copy_fields.iteritems():
                    d[fname] = hit[hitfname]
                return d

            res.extend(map(hit2d, resd["hits"]["hits"]))

        if verbose:
            print "ES responded with {} results".format(len(res))

        return sorted(res, key=lambda hit: hit["score"], reverse=True)[:size]

    def hits_to_df(self, hits):
        return pd.DataFrame.from_records(hits, columns=self.column_list)

    def _calculate_query_line_scores(self, qline,
                                     precision_weight=0.3, recall_weight=0.4, order_weight=0.1, negatives_weight=0.3):
        if self.index_name != "tanakh":
            qline.doc_type = self.index_name
        if qline.prepended_negatives.strip().isdigit():
            prepended_negatives_int = int(qline.prepended_negatives)
        else:
            prepended_negatives_int = 0
        if prepended_negatives_int > len(qline.all_results):
            prepended_negatives_int = len(qline.all_results)
        expected = np.array(qline.all_results[prepended_negatives_int:]) # DL - changed from 'expected_results'
        negatives = np.array(qline.all_results[0:prepended_negatives_int])
        expected_size = expected.size
        negatives_size = negatives.size
        hitsdf = self.hits_to_df(self.get_hits(qline.query, doc_type=qline.doc_type,
                                               size=expected_size if expected_size != 0 else 1))
        key_field = self.key_field
        hits = hitsdf[key_field].values
        recalled = np.in1d(expected, hits)
        # DL - The following segment creates the collection of negative result hits
        #       - first as a multiple-appended list, then as a df, then the values (all like the pos hits)
        neg_hits_list = []
        for neg_value in negatives:
            res_list = self.get_hits(qline.query,
                                     fields_to_filter={key_field: neg_value}, doc_type=qline.doc_type)
            for res in res_list: # DL - this loop forces the neg_hits_list to be 1d
                neg_hits_list.append(res)
        # DL - the addition made each time to neg_hits_list is either nothing, or exactly 1 match for the xml:id given
        # DL - this was for that: if len(neg_hitsdf) > 0: neg_hits.append(neg_hitsdf["xml:id"].values[0])
        neg_hitsdf = self.hits_to_df(neg_hits_list)
        neg_hits = neg_hitsdf[key_field].values
        negative_results = ~np.in1d(negatives,neg_hits) # DL - unary invert instead of old version with params
        # DL - End of segment to create negative results collection
        if negatives_size:
            negative_score = negative_results.mean()
        else:
            negative_score = 'N/A'
            negatives_weight = 0.
        combined_score = (negative_score * negatives_weight) if negatives_size else 0.
        if expected_size:
            recalled_score = recalled.mean()
            combined_score += (recalled_score * recall_weight)
            # beginning of segment to determine precise_score
            precise = np.in1d(hits, expected)
            if len(precise):
                precise_score = precise.mean()
                combined_score += (precise_score * precision_weight)
            else:
                precise_score = 'N/A'
                precision_weight = 0.
            # end of segment to determine precise_score
            # beginning of segment to determine order_score
            kt_score, _ = scipy.stats.kendalltau(
                hits[precise],
                expected[recalled]
            )
            if precise.sum() > 1:
                order_score = (kt_score + 1.) / 2.  # fix [-1, 1] -> [0, 1]
                combined_score += (order_score * order_weight)
            else:
                order_score = 'N/A'
                order_weight = 0.
            # end of segment to determine order_score
        elif negatives_size:
            recalled_score = 'N/A'
            recall_weight = 0.
            precise_score = 'N/A'
            precision_weight = 0.
            order_score = 'N/A'
            order_weight = 0.
        else:
            raise Exception("A query (" + qline.query
                            + ") was not accompanied by positive or negative expected results. At least one of the two is required.")
        combined_score /= (precision_weight + recall_weight + order_weight + negatives_weight)
        if abs(combined_score - 1.0) < 0.0000000000001: # DL - When all 4 scores are used there's a tiny imprecision
            combined_score = 1.0 # DL - (Such an imprecision must be corrected because we count PERFECT scores)
        res = dict(
            precision=precise_score,
            recall=recalled_score,
            order=order_score,
            negatives=negative_score,
            combined=combined_score
        )

        return res

    def calculate_query_lines_scores(self, query_lines):
        results = pd.DataFrame.from_records(query_lines.apply(self._calculate_query_line_scores, axis=1))
        results["query"] = query_lines["query"]
        return results[["query", "precision", "recall", "order", "negatives", "combined"]]

    @staticmethod
    def read_query_lines(fname, skip_header=True):
        return parse_varlines_csv_file(fname, ["query", "doc_type", "prepended_negatives"], "all_results", skip_header=skip_header)
    # DL - Note the added field "prepended negatives",
    #       and the changed name of the variable field from "expected_results" to "all_results"

    def prepare(self):
        return

    def run_regression_test(self):
        self.prepare()
        print "Reading input from file: {}".format(self.input_fname)
        query_lines = self.read_query_lines(self.input_fname)
        print "{} query lines were read from input".format(len(query_lines))
        print "Performing queries and calculating results"
        scores = self.calculate_query_lines_scores(query_lines)
        print "Total score: {}".format(scores.combined.mean())
        print "{} (out of {}) queries achieved perfect score".format((scores.combined == 1.0).sum(), len(scores))
        print "Writing results to file: {}".format(self.output_fname)
        scores.to_csv(self.output_fname, index=False, encoding="utf8")
        print "Done!"


if __name__ == "__main__":
    parser = ElasticSearchRegressionTest.get_parser()
    args = parser.parse_args()
    ElasticSearchRegressionTest(**vars(args)).run_regression_test()
