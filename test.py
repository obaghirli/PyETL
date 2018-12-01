from collections import OrderedDict

vals = ['front', 'end', 'front', 'end', 'back', 'jack', 'jack']
encoder_map = dict()


def int_code_generator():
    it = 0
    while True:
        yield it
        it += 1

generator = int_code_generator()


def integer_encoder(val, generator):
    # global encoder_map
    code = encoder_map.get(val, None)
    if code:
        pass
    else:
        encoder_map[val] = next(generator)


for val in vals:
    integer_encoder(val, generator)

print encoder_map
print integer_encoder.func_name

def te(**kwargs):
    print type(kwargs.get('copy'))

te(copy=True)

# order matters
schema = OrderedDict([('engine-location', int),
                      ('num-of-cylinders', int),
                      ('engine-size', int),
                      ('weight', int),
                      ('horsepower',float),
                      ('aspiration', int),
                      ('price', float),
                      ('make', str)])