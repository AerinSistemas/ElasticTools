from dotenv import load_dotenv
import os
from sklearn import datasets
import dask as dk
import pandas as pd
import numpy as np

from elastictools.aerastic.elastic_query import ElasticQuery
from elastictools.aerastic.elastic_index import ElasticIndex, InsertData, MultiprocessInsertData

from pathlib import Path
dotenv_path = Path('../.env')
load_dotenv(dotenv_path)

HOST = os.getenv("ELASTIC_HOST")
PORT = os.getenv("ELASTIC_PORT")
USER = os.getenv("ELASTIC_USER")
PASS = os.getenv("ELASTIC_PASS")


def test_insert_dask_process():
    """
    Test insert big dask dataframe, the value of the _id is the index
    """
    dataset = pd.DataFrame(np.random.randint(0, 10000,size=(10000, 17)), columns=list('ABCDEFGHIJKMLNOPQ'))
    elas = ElasticIndex(HOST, PORT, USER, PASS)
    dask_dataframe = dk.dataframe.from_pandas(dataset, npartitions = 8)
    pandas_head = dask_dataframe.head()
    elas.set_index_from_pandas("prueba_dask_multi_1", pandas_head)
    ingest = MultiprocessInsertData(HOST, PORT, USER, PASS, "prueba_dask_multi_1", "dask")
    ingest.run_insert(dask_dataframe)
    ingest.stop_process()


def test_insert_dask():
    """
    Test insert big dask dataframe, the value of the _id is the index
    """
    dataset = pd.DataFrame(np.random.randint(0, 10000,size=(10000, 17)), columns=list('ABCDEFGHIJKMLNOPQ'))
    elas = ElasticIndex(HOST, PORT, USER, PASS)
    dask_dataframe = dk.dataframe.from_pandas(dataset, npartitions = 8)
    pandas_head = dask_dataframe.head()
    elas.set_index_from_pandas("prueba_dask_multi_1", pandas_head)
    multi_ingest = InsertData(HOST, PORT, USER, PASS)
    multi_ingest.insert_dask(dask_dataframe, "prueba_dask_multi_1", "dask", index_id=False)

def test_insert_multi_dask():
    """
    Test to insert DASK dataframe using multriprocess.
    """
    ind_dask = "prueba_dask_3"
    dataset = pd.DataFrame(np.random.randint(
         0, 10000, size=(20000, 17)), columns=list('ABCDEFGHIJKMLNOPQ'))
    elas = ElasticIndex(HOST, PORT, USER, PASS)
    dask_dataframe = dk.dataframe.from_pandas(dataset, npartitions=10)
    pandas_head = dask_dataframe.head()
    elas.set_index_from_pandas(ind_dask, pandas_head)
    ingest = MultiprocessInsertData(HOST, PORT, USER, PASS, ind_dask, "dask")
    ingest.run_insert(dask_dataframe)
    ingest.stop_process

