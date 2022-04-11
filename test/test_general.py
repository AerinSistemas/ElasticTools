from dotenv import load_dotenv
import os
from sklearn import datasets
import dask as dk

from elastictools.aerastic.elastic_query import ElasticQuery
from elastictools.aerastic.elastic_index import ElasticIndex, InsertData
from pathlib import Path

dotenv_path = Path('../.env')
load_dotenv(dotenv_path)

HOST = os.getenv("ELASTIC_HOST")
PORT = os.getenv("ELASTIC_PORT")
USER = os.getenv("ELASTIC_USER")
PASS = os.getenv("ELASTIC_PASS")

def test_query_dask():
    """Test query to dask, the dask index is the elastic _id value
    """
    query = ElasticQuery(host=HOST, port=PORT, user=USER,password=PASS, dask=True)
    q = {"query": {"match": {"alcohol": "14.23"}}}
    count, result = query.raw_query("prueba_dask_3", q)

def test_query_pandas():
    """Test query to pandas, the pandas index is the elastic _id value
    """
    query = ElasticQuery(
        host=HOST, port=PORT, user=USER,
        password=PASS, dask=False)
    q = {"query": {"match": {"alcohol": "14.23"}}}
    count, result = query.raw_query("prueba_dask_3", q)


def test_query_dask_id():
    """Test query to pandas, the pandas index is the elastic _id value
    """
    query = ElasticQuery(
        host=HOST, port=PORT, user=USER,
        password=PASS, dask=True)
    q =  {"query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        },
        {
          "range": {
            "alcohol": {
              "gte": "14.0",
              "lte": "14.5"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }}}
    count, result = query.raw_query("prueba_dask_3", q) # count, result = query.raw_query("prueba_dask_*", q)
    assert count == 120