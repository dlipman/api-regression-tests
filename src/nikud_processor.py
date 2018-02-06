from argparse import ArgumentParser
import pandas as pd
import re
import textwrap

LETTER_LIST = [u'\u05d0', u'\05d1', u'\u05d2', u'\05d3', u'\u05d4', u'\05d5', u'\u05d6', u'\05d7',
               u'\u05d8', u'\05d9', u'\u05da', u'\05db', u'\u05dc', u'\05dd', u'\u05de', u'\05df',
               u'\u05e0', u'\05e1', u'\u05e2', u'\05e3', u'\u05e4', u'\05e5', u'\u05e6', u'\05e7',
               u'\u05e8', u'\05e9', u'\u05ea']


class NikudProcessor(object):
    def __init__(self, input_fname, output_fname):
        self.output_fname = output_fname
        self.input_fname = input_fname
        self.letter_list = LETTER_LIST

    @staticmethod
    def get_parser():
        parser = ArgumentParser(description="This script performs a regression test by submitting text to an API")
        # ArgumentParser.add_argument(
        # name or flags...[, action][, nargs][, const][, default][, type][, choices][, required]
        #   [, help][, metavar][, dest])

        parser.add_argument("input_fname", help="utf-8 encoded text file")
        parser.add_argument("output_fname", help="csv file to generate the regression test sample file into")

        return parser

    def sep_into_blocks(self, all_text):
        block_size = 140 # default for now
        block_list = textwrap.wrap(all_text, block_size, break_long_words=False)
        return block_list

    def strip_nikud(self, block):
        ublock = block.decode("utf-8")
        words = ublock.split()
        no_nikud = ""
        for word in words:
            no_nikud += re.sub(ur'[^-\u05d0-\u05ea]','',word) + " "
        return no_nikud[:-1] # strip last space

    def remove_extra_letters(self, block):
        block = re.sub(r'[",."]',"",block)
        ublock = block.decode("utf-8")
        while ublock.find(u'\u05bd') != -1:
            meteg_pos = ublock.find(u'\u05bd')
            ublock = ublock[:meteg_pos-1] + ublock[meteg_pos+1:]
        return ublock

    def single_spaces(self, block):
        while block.find('  ') != -1:
            block = re.sub('  ', ' ', block)
        return block

    def convert_one_block(self, block):
        block = self.single_spaces(block)
        no_nikud = self.strip_nikud(block)
        with_nikud = self.remove_extra_letters(block)
        res = dict(
            submission=no_nikud,
            placeholder='placeholder',
            expected_results=with_nikud
        )
        return res

    def convert_to_regression_sample_format(self, block_list):
        # block_set_df = pd.DataFrame.from_records(block_list)
        # results = pd.DataFrame.from_records(block_set_df.apply(self.convert_one_block, axis=1))
        dict_list = []
        for block in block_list:
            dict_list.append(self.convert_one_block(block))
        results = pd.DataFrame.from_records(dict_list)
        chosen_result_columns = results[["submission", "placeholder", "expected_results"]]
        return chosen_result_columns

    def generate_regression_test_file(self):
        print("Reading input from file: {}".format(self.input_fname))
        file = open(self.input_fname)
        all_text = file.read()
        all_text = re.sub(r'[A-Za-z0-9*\[\]*!:();?\''r']','',all_text)
        all_text = re.sub(ur'\xc2\xba','',all_text)
        block_list = self.sep_into_blocks(all_text)
        print("{} blocks of text were read from input".format(len(block_list)))
        print("Converting into unvowelized and vowelized versions")
        results = self.convert_to_regression_sample_format(block_list)
        print("Conversion successful") # i.e. no special results to output here
        print("Writing results to file: {}".format(self.output_fname))
        results.to_csv(self.output_fname, index=False, encoding="utf8")
        print("Done!")


if __name__ == "__main__":
    parser = NikudProcessor.get_parser()
    args = parser.parse_args()
    NikudProcessor(**vars(args)).generate_regression_test_file()
