from elasticsearch_regression_test import ElasticSearchRegressionTest


class ESWordsRegressionTest(ElasticSearchRegressionTest):
    def prepare(self):
        self.index_name = "words"
        self.column_list = ["score", "parsed_text_rep"]
        self.key_field = "parsed_text_rep"


if __name__ == "__main__":
    parser = ElasticSearchRegressionTest.get_parser()
    args = parser.parse_args()
    ESWordsRegressionTest(**vars(args)).run_regression_test()
