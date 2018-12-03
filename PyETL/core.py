"""
MIT License

Copyright (c) 2018 PyETL

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import copy as cp
from collections import OrderedDict


class Utils(object):
    """ Collection of the helper methods """
    def __init__(self):
        pass

    @classmethod
    def read_file(cls, filename):
        """ Reads the input file """
        with open(filename, 'r') as fd:
            rows = fd.readlines()
        return [row.strip() for row in rows]

    @classmethod
    def extract_headers(cls, raw_data):
        """ Extracts the headers from the
        content of the data file """
        return raw_data[0]

    @classmethod
    def extract_data(cls, raw_data):
        """ Extracts the data from the
        content of the data file """
        return raw_data[1:]

    @classmethod
    def parse_headers(cls, header_row, header_delimiter=";"):
        """ Given the delimiter, splits the column values """
        def header_udf(x):
            if len(x) > 0:
                return x.strip()
            else:
                return 'unnamed'
        return list(map(lambda x: header_udf(x), header_row.split(header_delimiter)))

    @classmethod
    def map_columns(cls, original_columns):
        """ Finds the indices of the specified columns """
        index_array = list()
        for col in schema.keys():
            col_index = original_columns.index(col)
            index_array.append(col_index)
        return index_array

    @classmethod
    def linearize(cls, raw_data):
        """ A mini-pipeline to transform the raw_data into a list of line objects """
        header_row = Utils.extract_headers(raw_data)
        data_rows = Utils.extract_data(raw_data)
        original_columns = Utils.parse_headers(header_row, header_delimiter=";")
        specified_columns_indices = Utils.map_columns(original_columns)
        return list(map(lambda data_row: Line(data_row, specified_columns_indices), data_rows))

    @classmethod
    def int_code_generator(cls):
        """ A generator that generates integers for each unique label """
        it = 0
        while True:
            yield it
            it += 1

    @classmethod
    def integer_encoder(cls, val, generator):
        """ If the value appears for the first time, assigns it a unique integer,
         otherwise, returns the integer that is already assigned """
        code = encoder_map.get(val, None)
        if code is not None:
            return code
        else:
            new_code = next(generator)
            encoder_map[val] = new_code
            return new_code


class Line(object):
    """ Wrapper class that parses a raw line and flags
     those which any of the specified columns is null

     Parameters:
         raw_line: raw line as it is read from the data file
         col_indices: indices of the specified columns
         parsed_raw_line: specified column values, subject to transformations
         has_null: if any of the specified columns is null
     """
    def __init__(self, raw_line, col_indices):
        self.raw_line = raw_line
        self.col_indices = col_indices
        self.parsed_raw_line = None
        self.has_null = False
        self._process_line()

    def _parse(self, field_delimiter=";"):
        """ Parses the line string according to the delimiter provided """
        split_raw_line = self.raw_line.split(field_delimiter)
        parsed_raw_line = [split_raw_line[col_idx] for col_idx in self.col_indices]
        self.parsed_raw_line = list(map(lambda x: x.strip(), parsed_raw_line))
        return self

    def _check_null(self, null_char='-'):
        """ Checks whether the column values contain null """
        self.has_null = True if null_char in self.parsed_raw_line else False
        return self

    def _flag(self, params):
        """ Looks for a specific value in a given column
         e.g. aspiration: turbo"""
        for field, val in params.items():
            field_idx = list(schema.keys()).index(field)
            temp = self.parsed_raw_line[field_idx]
            self.parsed_raw_line[field_idx] = 1 if temp == val else 0
        return self

    def _encode(self, fields, encoder='integer_encoder'):
        """ Replaces the unique column values with their corresponding labels"""
        _generator = None
        spec_cols = list(schema.keys())
        assert (encoder in encoders.keys())
        for field in fields:
            assert (field in spec_cols)
        if encoder == 'integer_encoder':
            _generator = generators.get('int_code_generator')
        for field in fields:
            field_idx = spec_cols.index(field)
            field_val = self.parsed_raw_line[field_idx]
            code = encoders.get(encoder)(field_val, _generator)
            self.parsed_raw_line[field_idx] = code
        return self

    def _str_2_int(self, fields):
        """ Convert word numbers to numbers e.g. three -> 3 """
        for field in fields:
            field_idx = list(schema.keys()).index(field)
            self.parsed_raw_line[field_idx] = num_words.get(self.parsed_raw_line[field_idx], None)
        return self

    def _enforce_type(self):
        """ Casts specified columns to given data types as described in the schema """
        for field, _type in schema.items():
            field_idx = list(schema.keys()).index(field)
            temp = self.parsed_raw_line[field_idx]
            if _type == int:
                self.parsed_raw_line[field_idx] = _type(temp)
            elif _type == str:
                self.parsed_raw_line[field_idx] = _type(temp).encode('utf-8')
            elif _type == float:
                if isinstance(temp, str) is True:
                    self.parsed_raw_line[field_idx] = _type(temp.replace(",", "."))
                else:
                    self.parsed_raw_line[field_idx] = _type(temp)
            else:
                self.parsed_raw_line[field_idx] = _type(temp)
        return self

    def _scale(self, params):
        """ Scales the given column by a certain factor e.g. 'price' by 0.01 """
        numeric_types = [int, float]
        for field, factor in params.items():
            assert(sum([isinstance(factor, _type) for _type in numeric_types]) >= 1)
            field_type = schema.get(field)
            assert(field_type in numeric_types)
            field_idx = list(schema.keys()).index(field)
            temp = self.parsed_raw_line[field_idx]
            assert(sum([isinstance(temp, _type) for _type in numeric_types]) >= 1)
            self.parsed_raw_line[field_idx] = temp * factor
        return self

    def _process_line(self):
        """ On the fly parsing and null checking of the raw line """
        self._parse(field_delimiter=";")._check_null(null_char='-')

    def __repr__(self):
        """ Overriding the built-in function, to describe the class
        in a human friendly fashion"""
        return "{0}".format(self.parsed_raw_line)


class Transformer(object):
    """ Implements transformers to be applied on the list of line objects.

     Transformers are designed to be chainable i.e. transformer.func1().func2()

     Parameters:
         orig_data: X data in the f(X) = y
         transformed_data: Y data in the f(X) = y
     """
    def __init__(self):
        self.orig_data = None
        self.transformed_data = None

    def fit(self, data):
        """ Setter for orig_data """
        self.orig_data = data
        return self

    def chain(func):
        """ If the transformer in chained i.e. transformer is applied on the
        output of the previous transformer, orig_data becomes the transformed_data,
        so that the new transformation can be applied on the previously-transformed data"""
        @functools.wraps(func)
        def decorator(self, *args, **kwargs):
            if self.transformed_data is not None:
                self.orig_data, self.transformed_data = self.transformed_data, None
            return func(self, *args, **kwargs)
        return decorator

    @chain
    def dropna(self):
        """ Dropns the entry if any of the specified columns is null  """
        self.transformed_data = list(
            filter(lambda line_obj: line_obj.has_null is False, self.orig_data))
        return self

    @chain
    def encode(self, fields, encoder='integer_encoder'):
        """ Encodes labels to integers, can be decoded to
        recover the original data via the internal map.

        Refer to _encode method in the Line Class """
        self.transformed_data = list(
            map(lambda line_obj: line_obj._encode(fields, encoder=encoder), self.orig_data))
        return self

    @chain
    def str_2_int(self, fields):
        """ Convert word numbers to numbers e.g. 'three' -> 3

        Refer to _str_2_int method in the Line Class """
        self.transformed_data = list(
            map(lambda line_obj: line_obj._str_2_int(fields), self.orig_data))
        return self

    @chain
    def flag(self, params):
        """ Looks for a specific value in a given column e.g. 'aspiration': 'turbo'
         Replaces the column values with 1 in case of a match, 0 otherwise

         Refer to _flag method in the Line Class """
        self.transformed_data = list(
            map(lambda line_obj: line_obj._flag(params), self.orig_data))
        return self

    @chain
    def enforce_type(self):
        """ Casts column data types to those described in the schema.

         Refer to _enforce_type method in the Line Class """
        self.transformed_data = list(map(lambda line_obj: line_obj._enforce_type(), self.orig_data))
        return self

    @chain
    def scale(self, params):
        """ Scales the given column by a specific factor.

         Refer to _scale method in the Line Class """
        self.transformed_data = list(map(lambda line_obj: line_obj._scale(params), self.orig_data))
        return self

    chain = staticmethod(chain)

    def collect(self):
        """ Collects the payload from the transformed
        line objects and inserts the headers as a first entry """
        headers = list(schema.keys())
        out = list(map(lambda x: x.parsed_raw_line, self.transformed_data))
        return [headers] + out

    def copy(self):
        """ Returns the exact copy of the transformer """
        return cp.deepcopy(self)


# word numbers
num_words = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
             'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10}

# output data schema, order matters
schema = OrderedDict([('engine-location', int),
                      ('num-of-cylinders', int),
                      ('engine-size', int),
                      ('weight', int),
                      ('horsepower', float),
                      ('aspiration', int),
                      ('price', float),
                      ('make', str)])

# label to integer mapping
encoder_map = dict()

# generators registrar
int_code_generator = Utils.int_code_generator()
generators = {'int_code_generator': int_code_generator}

# encoders registrar
encoders = {'integer_encoder': Utils.integer_encoder}


def load(dataFile):
    """ Read the input file """
    return Utils.read_file(dataFile)


def transform(raw_data):
    """ Sample pipeline

    Parameters:
        raw_data: list of strings, output of the load() method
    Returns:
        transformed data: list of list
    """
    line_objects = Utils.linearize(raw_data)
    transformer = Transformer()
    return transformer.fit(line_objects)\
        .dropna()\
        .encode(['engine-location'], encoder='integer_encoder')\
        .str_2_int(['num-of-cylinders'])\
        .flag({'aspiration': 'turbo'})\
        .enforce_type()\
        .scale({'price': 0.01})\
        .collect()


# if __name__ == '__main__':
#     raw_data = load('../Challenge_me.txt')
#     print(transform(raw_data))

