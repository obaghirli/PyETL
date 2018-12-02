from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import functools
import copy as cp
from collections import OrderedDict


class Utils(object):
    def __init__(self):
        pass

    @classmethod
    def read_file(cls, filename):
        with open(filename, 'r') as fd:
            rows = fd.readlines()
        return [row.strip() for row in rows]

    @classmethod
    def extract_headers(cls, header_row, header_delimiter=";"):
        def header_udf(x):
            if len(x) > 0:
                return x.strip()
            else:
                return 'unnamed'
        return list(map(lambda x: header_udf(x), header_row.split(header_delimiter)))

    @classmethod
    def map_columns(cls, original_columns):
        index_array = list()
        for col in schema.keys():
            col_index = original_columns.index(col)
            index_array.append(col_index)
        return index_array

    @classmethod
    def int_code_generator(cls):
        it = 0
        while True:
            yield it
            it += 1

    @classmethod
    def integer_encoder(cls, val, generator):
        code = encoder_map.get(val, None)
        if code is not None:
            return code
        else:
            new_code = next(generator)
            encoder_map[val] = new_code
            return new_code


# wrapper class for each entry in the data file
class Line(object):
    def __init__(self, raw_line, col_indices):
        self.raw_line = raw_line
        self.col_indices = col_indices
        self.parsed_raw_line = None
        self.has_null = False
        self._process_line()

    def _parse(self, field_delimiter=";"):
        split_raw_line = self.raw_line.split(field_delimiter)
        parsed_raw_line = [split_raw_line[col_idx] for col_idx in self.col_indices]
        self.parsed_raw_line = list(map(lambda x: x.strip(), parsed_raw_line))
        return self

    def _flag(self, params):
        for field, val in params.items():
            field_idx = list(schema.keys()).index(field)
            temp = self.parsed_raw_line[field_idx]
            self.parsed_raw_line[field_idx] = 1 if temp == val else 0
        return self

    def _check_null(self, null_char='-'):
        self.has_null = True if null_char in self.parsed_raw_line else False
        return self

    def _encode(self, fields, encoder='integer_encoder'):
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
        for field in fields:
            field_idx = list(schema.keys()).index(field)
            self.parsed_raw_line[field_idx] = num_words.get(self.parsed_raw_line[field_idx], None)
        return self

    def _enforce_type(self):
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
        self._parse(field_delimiter=";")._check_null(null_char='-')

    def __repr__(self):
        return "{0}".format(self.parsed_raw_line)


class Transformer(object):
    def __init__(self):
        self.orig_data = None
        self.transformed_data = None

    def set_orig_data(self, orig_data):
        self.orig_data = orig_data

    def set_transformed_data(self, transformed_data):
        self.transformed_data = transformed_data

    def fit(self, data):
        self.orig_data = data
        return self

    def chain(func):
        @functools.wraps(func)
        def decorator(self, *args, **kwargs):
            if self.transformed_data is not None:
                self.orig_data, self.transformed_data = self.transformed_data, None
            return func(self, *args, **kwargs)
        return decorator


    @chain
    def dropna(self):
        self.transformed_data = list(filter(lambda line_obj: line_obj.has_null is False, self.orig_data))
        return self

    @chain
    def encode(self, fields, encoder='integer_encoder'):
        self.transformed_data = list(map(lambda line_obj: line_obj._encode(fields, encoder=encoder), self.orig_data))
        return self

    @chain
    def str_2_int(self, fields):
        self.transformed_data = list(map(lambda line_obj: line_obj._str_2_int(fields), self.orig_data))
        return self

    @chain
    def flag(self, params):
        self.transformed_data = list(map(lambda line_obj: line_obj._flag(params), self.orig_data))
        return self

    @chain
    def enforce_type(self):
        self.transformed_data = list(map(lambda line_obj: line_obj._enforce_type(), self.orig_data))
        return self

    @chain
    def scale(self, params):
        self.transformed_data = list(map(lambda line_obj: line_obj._scale(params), self.orig_data))
        return self

    chain = staticmethod(chain)

    def collect(self):
        headers = list(schema.keys())
        out = list(map(lambda x: x.parsed_raw_line, self.transformed_data))
        return [headers] + out

    def copy(self):
        return cp.deepcopy(self)


# number words
num_words = {'one': 1, 'two': 2, 'three': 3, 'four': 4, 'five': 5,
             'six': 6, 'seven': 7, 'eight': 8, 'nine': 9, 'ten': 10}

# order matters
schema = OrderedDict([('engine-location', int),
                      ('num-of-cylinders', int),
                      ('engine-size', int),
                      ('weight', int),
                      ('horsepower', float),
                      ('aspiration', int),
                      ('price', float),
                      ('make', str)])

# label to int map
encoder_map = dict()

# register generators
int_code_generator = Utils.int_code_generator()
generators = {'int_code_generator': int_code_generator}

# register encoders
encoders = {'integer_encoder': Utils.integer_encoder}


def load(dataFile):
    return Utils.read_file(dataFile)


def transform(data):
    header_row = data[0]
    data_rows = data[1:]
    original_columns = Utils.extract_headers(header_row, header_delimiter=";")
    specified_columns_indices = Utils.map_columns(original_columns)
    line_objects = list(map(lambda data_row: Line(data_row, specified_columns_indices), data_rows))
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
#     data = load('Challenge_me.txt')
#     print(transform(data))


