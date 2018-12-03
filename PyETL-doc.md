
### | PyETL
_python3.5 package for ETL jobs_

***

### Install
```sh
# create a python3.5 environment and install the package
$ pip install -i https://test.pypi.org/simple/ PyETL==1.0.4
# start the python shell and execute the test command
# if the test command completed successfully, proceed
# with the Quickstart
$ python
>>> import PyETL
```

### Quick Start
```sh
# pre-built pipeline: PyETL.transform
>>> from PyETL import load
>>> from PyETL import transform
>>> raw_data = load("/Users/abra.cadabra/data/Challenge_me.txt")
>>> transformed_data = transform(raw_data)
>>> transformed_data
```

```sh
# custom pipeline
>>> from PyETL import Utils
>>> from PyETL import Transformer
>>> from PyETL import load
>>> raw_data = load("/Users/abra.cadabra/data/Challenge_me.txt")
>>> line_objects = Utils.linearize(raw_data)
>>> transformer = Transformer()
>>> transformed_data = transformer\
        .fit(line_objects)\
        .dropna()\
        .encode(['engine-location'], encoder='integer_encoder')\
        .str_2_int(['num-of-cylinders'])\
        .flag({'aspiration': 'turbo'})\
        .enforce_type()\
        .scale({'price': 0.01})\
        .collect()
>>> transformed_data
```

### Architecture
+ Datafile is read and each line is stored in a list producing `list of strings`
+ `Class Line` wraps each line in the list and creates `list of line objects`
+ Extracting specified column values and flagging `line objects` containing null values are done on the fly during the initialization phase of the Line objects
+ Transformations are applied on the `list of line objects`
+ Transformers utilize `map()`, `filter()` and `lambda` functions for performance
+ Transformers are chainable via the help of `decorators` to achieve flexibility and generalization e.g. `transformer.func1().func2().func3()`
+ Integer encoding is done with the help of `generators`

['engine-location', 'num-of-cylinders', 'engine-size', 'weight', 'horsepower', 'aspiration', 'price', 'make']

## Schema
| Field | Type |
| ------ | ----------- |
| engine-location   | int |
| num-of-cylinders | int |
| engine-size    | int |
| weight    | int |
| horsepower    | float |
| aspiration    | int |
| price    | float |
| make    | byte (utf encoded str) |

### Reference
| **Utils** - class Utils(object)
| **Line**  - class Line(object)
| **Transformer**  - class Transformer(object)
| **load**  - def load(dataFile)
| **transform**  - def transform(raw_data)


