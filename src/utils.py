# coding=utf-8
import csv
import re

import pandas as pd


def parse_varlines_csv_file(fname, const_field_names=[], variable_field_name="", skip_header=False, encoding="utf8",
                            verbose=False):
    """
    parse a csv file with variable-length lines of the form:
    <field 1>,<field 2>,...,<field N>,[comma-separated values of last field]
    the file will be parsed into a Pandas DataFrame, with the last field as a list of the values
    :param fname: input file name
    :param const_field_names: list of names of expected constant fields (namely: field 1 to N)
    :param variable_field_name: name for the variable-length field
    :param skip_header: whether to skip the first line of the file
    :param encoding: file encoding
    :param verbose: enable printing of line parsing
    :return: pandas.DataFrame of the parsed file, with columns according to the given field names

    example:
    given the file 'f.csv' with the following content:
    a,b, 1,<TAB>2,3
    c,d
    e,f,bla,blabla
    g,h,
    <EOF>

    the invocation:
    parse_varlines_csv_file(
        fname="f.csv",
        const_field_names=["field1", "field2"],
        variable_field_name="varfield"
    )
    will produce the following DataFrame:
      field1 field2           varfield
    0      a      b    ['1', '2', '3']
    1      c      d                 []
    2      e      f  ['bla', 'blabla']
    3      g      h               ['']
    """
    const_size = len(const_field_names)
    parsed_lines = []
    with open(fname, "r") as inp:
        reader = csv.reader(inp)
        if skip_header:
            next(reader)
        for i, line in enumerate(reader, 1):
            if encoding is not None:
                line = [s.decode(encoding) for s in line]
            line = [s.strip() for s in line]
            assert len(line) >= const_size,\
                "line {} in file '{}' is too short! (must have at least {} fields)".format(
                    i, fname, const_size)
            lineres = dict(zip(const_field_names, line))
            varfield_value = line[const_size:]
            if verbose:
                print "parsed line", i
                for name, value in lineres.iteritems():
                    print name, ":", value
                print variable_field_name, ":"
                for v in varfield_value:
                    print "\t", v

            lineres[variable_field_name] = varfield_value
            parsed_lines.append(lineres)
    return pd.DataFrame.from_records(parsed_lines, columns=[const_field_names + [variable_field_name]])


def split_letters(s):
    """
    splits each word to pairs of letter and vocalization
    """
    spl = re.split(u"([א-ת ־])", s)[1:]
    assert len(spl) % 2 == 0
    return spl[0::2], spl[1::2]

# split_letters(u"בְּרֵאשִׁית")


def remove_vocalization(s):
    return re.sub(u"[\u05B0-\u05BC\u05C1\u05C2\u05C7]", "", s)


def remove_teamim(s):
    return re.sub(u"[\u0591-\u05AF\u05BD\u05BF\u05C0\u05C4]", "", s)


def remove_both(s):
    return remove_vocalization(remove_teamim(s))


def strip_lemma(lemma):
    bad_chars = list("/=[]") + ["_aramaic"]
    for bc in bad_chars:
        lemma = lemma.replace(bc, "")
    return lemma


class IllegalArgumentException(Exception):
    pass