from argparse import ArgumentParser

import pandas as pd
import numpy as np
import re
import scipy.stats

from utils import parse_varlines_csv_file
import requests
import json
from api_regression_test import APIRegressionTest
import editdistance as ed


class NakdanAPIRegressionTest(APIRegressionTest):
    # def api_post(self, param_set):
        # (overriding for testing) response_data = requests.post(param_set.url,data=param_set.data)
        # print(param_set.data)
        # response_data = u"\u05d4\u05e0\u05e7\u05d3\u05df \u05d4\u05d6\u05d4 \u05d4\u05d5\u05d0 \u05e0\u05e7\u05d3\u05df \u05d8\u05d5\u05d1"
        # response_data = response_data.encode()
        # print(response_data)
        # (note: the above "print" commands may not work from the command line!)
        # return response_data

    def postprocess_data(self, response_data):
        # for response_item in response_data.content:
        #     print(response_item)
        if response_data.status_code == 200:
            processed = response_data.json()
        else:
            raise Exception(response_data.text)
        return processed

    def get_words(self, json_list):
        word_list = []
        for item in json_list:
            if not item["sep"]:
                word_list.append(item)
        return word_list

    def compare_word_characters(self, word1, word2):
        edit_distance = ed.eval(word1, word2)
        length = len(word1) if len(word1) >= len(word2) else len(word2)
        score = 1.0 - (float(edit_distance) / float(length))
        return score

    def compare_vowelized_words(self, word1, word2):
        # print("Comparing " + word1 + " with " + word2 + ". ".encode("utf-8"))
        if word1 == word2:
            score = 1.0
        else:
            score = 0.0
        print("Score = %f" % score)
        return score

    def score_one_result(self, result, processed_response):
        response_word_list = self.get_words(processed_response) # note: processed_response is a json-read list
        expected_word_list = result.split()
        if len(response_word_list) == len(expected_word_list):
            score_tally = 0
            for i in range(len(response_word_list)):
                print("Comparing word #%d of current line." % (i+1))
                score_tally += self.compare_vowelized_words(expected_word_list[i], response_word_list[i]["options"][0])
            score = score_tally / len(response_word_list)
        else:
            score = 0.0
        return score


if __name__ == "__main__":
    parser = APIRegressionTest.get_parser()
    args = parser.parse_args()
    NakdanAPIRegressionTest(**vars(args)).run_regression_test()
