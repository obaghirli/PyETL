import os
import sys
import copy as cp

# order matters
SPECIFIED_COLUMNS = {'engine-location': int,
                     'num-of-cylinders': int,
                     'engine-size': int,
                     'weight': float,
                     'horsepower': float,
                     'aspiration': int,
                     'price': float,
                     'make': str}


class Utils(object):
    def __init__(self):
        pass

    def read_file(self, filename='Challenge_me.txt'):
        with open(filename) as fd:
            rows = fd.readlines()
        return [row.strip() for row in rows]

    def extract_headers(self, header_row, header_delimiter=";"):
        def header_udf(x):
            if len(x) > 0:
                return x.encode('utf-8').strip()
            else:
                return 'unnamed'
        return list(map(lambda x: header_udf(x), header_row.split(header_delimiter)))

    def map_columns(self, original_columns):
        index_array = list()
        for col in SPECIFIED_COLUMNS.keys():
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
            print new_code
            return new_code


# label to int map
encoder_map = dict()

# register generators
int_code_generator = Utils.int_code_generator()
generators = {'int_code_generator': int_code_generator}

# register encoders
encoders = {'integer_encoder': Utils.integer_encoder}


# wrapper class for each entry in the textfile
class Line(object):
    def __init__(self, raw_line, col_indices):
        self.raw_line = raw_line
        self.col_indices = col_indices

        self.parsed_raw_line = None
        self.has_null = False

        self.process_line()

    def parse(self, field_delimiter=";"):
        split_raw_line = self.raw_line.split(field_delimiter)
        parsed_raw_line = [split_raw_line[col_idx] for col_idx in self.col_indices]
        self.parsed_raw_line = list(map(lambda x: x.strip(), parsed_raw_line))
        return self

    def check_null(self, null_char='-'):
        self.has_null = True if null_char in self.parsed_raw_line else False
        return self

    def _encode(self, fields, encoder='integer_encoder'):
        _generator = None
        spec_cols = SPECIFIED_COLUMNS.keys()
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

    def process_line(self):
        self.parse(field_delimiter=";").check_null(null_char='-')

    def __repr__(self):
        return "{0}:{1}".format(self.parsed_raw_line, self.has_null)


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
        def decorator(self, *args, **kwargs):
            if self.transformed_data:
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

    def copy(self):
        return cp.deepcopy(self)


if __name__ == '__main__':
    sample_line = """14;std;sedan;3.31;20;9.00;3055;rwd;front;164;ohc;mpfi;gas;55.70;25;121,76;189.00;bmw;-;- ;four;4250;2456500;3.19;1;103.50;66.90"""

    utils = Utils()
    transformer = Transformer()

    rows = utils.read_file(filename='Challenge_me.txt')
    header_row = rows[0]
    data_rows = rows[1:]
    original_columns = utils.extract_headers(header_row, header_delimiter=";")
    specified_columns_indices = utils.map_columns(original_columns)
    # line = Line(sample_line, specified_columns_indices)
    # print line.parse(field_delimiter=";").check_null(null_char="-")

    line_objects = list(map(lambda data_row: Line(data_row, specified_columns_indices), data_rows))
    transformer.fit(line_objects).dropna().encode(['engine-location'], encoder='integer_encoder')

    print len(line_objects)
    print len(transformer.transformed_data)

    print transformer.transformed_data
    print encoder_map


