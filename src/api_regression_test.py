from argparse import ArgumentParser

import pandas as pd
import numpy as np
import re
import scipy.stats

from utils import parse_varlines_csv_file
import requests
import json

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

def escape_template(t):
    escaped = t.replace("{", "{{").replace("}", "}}")
    return re.sub("\$\{(\{.*?\})\}", "\\1", escaped)

QUERY_BODY_TEMPLATE = escape_template(QUERY_BODY_TEMPLATE_RAW)
# print QUERY_BODY_TEMPLATE


class APIRegressionTest(object):
    def __init__(self, input_fname, output_fname):
        self.output_fname = output_fname
        self.input_fname = input_fname

    class APIParamSet(object):
        def __init__(self,url,data):
            self.url = url
            self.data = data

    def api_post(self, param_set):
        response_data = requests.post(param_set.url,data=param_set.data)
        return response_data

    @staticmethod
    def get_parser():
        parser = ArgumentParser(description="This script performs a regression test by submitting text to an API")
        # ArgumentParser.add_argument(
        # name or flags...[, action][, nargs][, const][, default][, type][, choices][, required]
        #   [, help][, metavar][, dest])

        parser.add_argument("input_fname", help="csv file with the text and the expected results")
        parser.add_argument("output_fname", help="file to write the report into")

        return parser

    def preprocess_data(self,all_text):
        return json.dumps({"data": all_text})

    def postprocess_data(self, response_data):
        processed = response_data
        return processed

    def get_response(self, url, submission, verbose=False):
        prepared_data = self.preprocess_data(submission)
        # (the above step, when querying elasticsearch, required:) body = QUERY_BODY_TEMPLATE.format(query_string=query)
        if verbose:
            print(u'Prepared "data" to submit the text "' + submission + u'":')
            print("{}".format(prepared_data))
        param_set = self.APIParamSet(url,prepared_data)
        response_data = self.api_post(param_set)
        processed = self.postprocess_data(response_data)

        if verbose:
            print("API responded with: {}".format(processed))

        return processed

    def score_one_result(self, result, processed_response):
        if result == processed_response:
            return 1.0
        else:
            return 0.0

    def compare_results(self, result_array, processed_response):
        total_score = 0.0
        count = 0
        for result in result_array:
            count += 1
            total_score += self.score_one_result(result, processed_response)
        score = total_score / count
        return score

    def get_url(self):
        return "http://nakdanserver.dicta.org.il:8080/simplemodernnakdan"

    def generate_score_dict(self, expected, processed_response):
        score = self.compare_results(expected, processed_response)
        res = dict(
            final_score=score,
        )
        return res

    def deal_with_special_columns(self, s_line):
        return

    def _calculate_one_line_scores(self, s_line):
        expected = np.array(s_line.all_results)
        self.deal_with_special_columns(s_line)
        expected_size = expected.size  # these 2 steps done here in case we need to submit the # of expected results
        if expected_size == 0:
            raise Exception("No expected results given for submission: " + s_line.submission)
        url = self.get_url()
        processed_response = self.get_response(url, s_line.submission)
        res = self.generate_score_dict(expected, processed_response)
        return res

    def choose_result_columns(self, results):
        return results[["submission", "final_score"]]

    def calculate_all_lines_scores(self, submission_lines):
        results = pd.DataFrame.from_records(submission_lines.apply(self._calculate_one_line_scores, axis=1))
        results["submission"] = submission_lines["submission"]
        chosen_result_columns = self.choose_result_columns(results)
        return chosen_result_columns

    @staticmethod
    def read_submission_lines(fname, skip_header=True):
        return parse_varlines_csv_file(fname, ["submission", "placeholder"], "all_results", skip_header=skip_header)

    def run_regression_test(self):
        print("Reading input from file: {}".format(self.input_fname))
        submission_lines = self.read_submission_lines(self.input_fname)
        print("{} submission lines were read from input".format(len(submission_lines)))
        print("Performing submissions and calculating results")
        scores = self.calculate_all_lines_scores(submission_lines)
        print("Total score: {}".format(scores.final_score.mean()))
        print("{} (out of {}) queries achieved perfect score".format((scores.final_score == 1.0).sum(), len(scores)))
        print("Writing results to file: {}".format(self.output_fname))
        scores.to_csv(self.output_fname, index=False, encoding="utf8")
        print("Done!")


if __name__ == "__main__":
    parser = APIRegressionTest.get_parser()
    args = parser.parse_args()
    APIRegressionTest(**vars(args)).run_regression_test()
