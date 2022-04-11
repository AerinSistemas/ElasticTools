import numpy
panda_elastic = {numpy.dtype('int64'): "long", numpy.dtype('int32'): "long", numpy.dtype('int32'): "integer",
                 numpy.dtype('int16'): "short", numpy.dtype('int8'): "byte", numpy.dtype('float16'): "half_float",
                 numpy.dtype('float32'): "float", numpy.dtype('float64'): "double", numpy.dtype('bool'): "boolean",
                 numpy.dtype('object'): "text", numpy.dtype('datetime64[ns]'): "date", numpy.dtype('<M8[ns]'): "date"}

csv_map = {'real': "float", 'decimal': "float", 'integer': "long", 'int': "long", 'entero':
     "long", 'varchar': "text", 'nvarchar': "text", 'text': "text", 'datetime': "date", 'date': "date"}
