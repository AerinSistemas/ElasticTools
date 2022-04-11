import numpy 
import pandas as pd
from elastictools.logsconf.log_conf import logging_config
import logging
from logging.config import dictConfig

dictConfig(logging_config)
logger = logging.getLogger("CONSOLE")


panda_elastic = {numpy.dtype('int64'): "long", numpy.dtype('int32'): "long", numpy.dtype('int32'): "integer",
                 numpy.dtype('int16'): "short", numpy.dtype('int8'): "byte", numpy.dtype('float16'): "half_float",
                 numpy.dtype('float32'): "float", numpy.dtype('float64'): "double", numpy.dtype('bool'): "boolean",
                 numpy.dtype('object'): "text"}
                 
elastic_pandas = {"long": 'int64', "integer": 'int64', "short": 'int64', "byte": 'int64',
                 "half_float" : 'float64', "float": 'float64', "double": 'float64',
                  "boolean": 'bool', "text": numpy.dtype('object')}

elastic_pandas_lambda = {"long": lambda value : pd.to_numeric(value) , "integer": lambda value : pd.to_numeric(value)
    , "int64": lambda value : pd.to_numeric(value), "byte": lambda value : pd.to_numeric(value), 
    "float": lambda value : pd.to_numeric(value, 'float'), "double": lambda value : pd.to_numeric(value, 'float'),
    "boolean": lambda value : pd.to_numeric(value, 'unsigned'), "date": lambda value : pd.to_datetime(value),
    "datetime": lambda value : pd.to_datetime(value), "text": lambda value: value }

