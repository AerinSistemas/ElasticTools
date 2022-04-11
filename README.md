# ElasticTools

![Python version](https://img.shields.io/badge/Python-3.8-yellow.svg?style=flat)

ElasticTools is a package created to insert small and big data with pandas or dask and can insert data with multiprocess.

* [Starting](#starting)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
  * [Data Protection](#data-protection)
* [Examples](#examples)
  * [ElasticIndex](#examples-with-elasticindex)
  * [ElasticInsert](#examples-with-elasticinsert)
  * [ElasticMultiprocessInsertData](#examples-with-elasticmultiprocessinsertdata)
    * [DASK](#dask)
    * [CSV](#csv)
    * [EXCEL/ODS](#excelods)
  * [ElasticQuery](#examples-with-elasticquery)
* [License](#license)
* [Contact](#contact)

## Starting

This instructions will allow you to get the program and use it in your project.

### Prerequisites

From pip will need to have installed Python and pre-install few packages.

```pip
pip install pandas
pip install xlrd
pip install odfpy
pip install numpy
pip install elasticsearch
```

### Installation

Now, to use the program, it will need to be installed from pip.

```pip
pip install elastictools
```

### Data protection

Elastictools is a tool for managing indexes in a database. For security reasons, to prevent unauthorized access, it is strongly recommended that private data be passed through an .env file, within the elastictools directory. This type of files must be included in the .gitignore file, which contains the files that the user does not want to be visible in the repository._
_The .env file must include: ELASTIC_HOST, ELASTIC_PORT, ELASTIC_USER, ELASTIC_PASS, which are connection data to user's database account.

## Examples

### Examples with ElasticIndex

Here, it will be showed a few examples about how to use the package.

First of all, we are going to create an instance.

```python
from elastictools import ElasticIndex

elas = ElasticIndex(HOST, PORT, USER, PASS)

# GET THE MAP TYPES
types = elas.get_maptypes(index)

# GET ALL INDEX
indexes = elas.get_all_index()

# INSERT AN INDEX
elas.set_index(index, mapping)

# SAVE MAPPING TO FILE
file = "ABSOLUTE/RELATIVE PATH"
elas.save_map(index, file)

# LOAD MAPPING FROM FILE
file = "ABSOLUTE/RELATIVE PATH"
elas.load_map(file)
```

### Examples with ElasticInsert

Here we will show a few examples about how to use the package.

An instance must be created.

```python
from elastictools import ElasticInsert
import pandas as pd

elasid = ElasticInsert(HOST, PORT, USER, PASS)
index = "name_index"
file = "ABSOLUTE/RELATIVE PATH"

# CHECK INDEX 
res,mapp = elasid.check_index(index)
```

`check_index` returns two parameters: response and mapping, in the case the mapping exists.

```python
# IF INDEX EXISTS
res, mapp = True, {"mapping"}

# IF NOT
res, mapp == False, None
```

You can create a new index using `ElasticInsert`.

```python
if res == False:
    header = pd.read_excel(file,nrow=1)
    elasid.set_index_from_pandas(index,header)
```

Finally, you can insert data from EXCEL/ODS/CSV.

```python
# INSERT DATA FROM EXCEL
elasid.insert_excel(index,file,rows=100)

# INSERT DATA FROM ODS
elasid.insert_excel(index,file,rows=100)

# INSERT DATA FROM CSV
elasid.insert_csv(index,file,separator=",",chunk_size=100)
```

All of them return a response.

```python
# EVERYTHING OKAY
res = (0, "Bulk done.")

# SOMETHING WRONG
res = (1, "Error to bulk data.")
```

### Examples with ElasticMultiprocessInsertData

To use multiprocess you will need a dask dataframe, csv file or excel/ods file.

As always, objects must be instantiated.

```python
from elastictools import InsertData, MultiprocessInsertData
import dask as dd
import pandas as pd

index = "index_name"
file = "ABSOLUTE/RELATIVE PATH"

elasid = InsertData(HOST, PORT, USER, PASS)
ingest = MultiprocessInsertData(HOST, PORT, USER, PASS, index, file)
```

#### DASK

If you want to use dask to insert data here is an example with csv file.

```python
data = pd.read_csv(FILE_PATH)
ddata = dd.dataframe.from_pandas(data, npartitions=10)
```

And check if the index exists and insert data.

```python
res,mapp = elasid.check_index(index)
if res == False:
    datahead = ddata.head()
    elasid.set_index_from_pandas(index,datahead)

ingest.run_insert(dask_dataframe)
ingest.stop_process
```

#### CSV

Inserting data from csv is very simple. You will need to use `run_csv()`

Like before, it is recommended to use `check_index`.

```python
if res == False:
    datahead = pd.read_csv(file, nrows=1)
    elasid.set_index_from_pandas(index, datahead)

ingest.run_csv(file, chunk_size = 100, nprocess = 4)
ingest.stop_process
```

#### EXCEL/ODS

Inserting data from excel/ods it is just like csv, but you will need to use `run_excel()`.

Like before, it is recommended to use `check_index`.

```python
if res == False:
    datahead = pd.read_excel(file, nrows=1)
    elasid.set_index_from_pandas(index,datahead)

ingest.run_excel(archivo, nrows = 500, nprocess = 4)
ingest.stop_process()
```

### Examples with ElasticQuery

Here are some examples of how to use queries.

Like every example, first will need to create an instance.

```python
from elastictools import ElasticQuery

index = "index_name"

query = ElasticQuery(HOST, PORT, USER, PASS)
```

We can get types from a query:

```python
q = {"query": {"match": {"field": "example"}}}
res = query.get_types_from_query(index,q)
```

Using `get_types_from_query` returns a dictionary response.

```python
res == dict
for i in res:
    i == str
```

Using `raw_query` returns a count of the rows and a pandas dataframe.

```python
q = {
        "query": {
            "range": {
                "field": {
                    "gte": 200,
                    "lte": 300
                }
            }
        }
    }

count, data = query.raw_query(ind_excel, q)
```

Using `raw_query` returns a count of the rows and a pandas dataframe.
Here is the print:

```bash
<class 'int'>
<class 'pandas.core.frame.DataFrame'>
COUNT: 284
                        DATO  NUMEROS COLORES     NOMBRE  APELLIDO
66vm73kBvQjBDM42zm-6   Dato1      230   verde  Alejandro      Cruz
7Kvm73kBvQjBDM42zm-6   Dato2      231    rojo      Maria   Cabello
7avm73kBvQjBDM42zm-6   Dato3      232    azul      Pablo  Gonzalez
7qvm73kBvQjBDM42zm-6   Dato4      233   verde  Alejandro      Cruz
76vm73kBvQjBDM42zm-6   Dato5      234    rojo      Maria   Cabello
...                      ...      ...     ...        ...       ...
5avo73kBvQjBDM42VntH  Dato67      296    azul      Pablo  Gonzalez
5qvo73kBvQjBDM42VntH  Dato68      297   verde  Alejandro      Cruz
56vo73kBvQjBDM42VntH  Dato69      298    rojo      Maria   Cabello
6Kvo73kBvQjBDM42VntH  Dato70      299    azul      Pablo  Gonzalez
6avo73kBvQjBDM42VntH  Dato71      300   verde  Alejandro      Cruz
```

## License

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

## Contact

Aerin Sistemas - info@aerin.es

Project Link: [GitHub](https://github.com/AerinSistemas/ElasticTools)
