# -*- coding:utf-8 -*-
'''
 @File Name: ElasticQuery.py
 @Author: Aerin Sistemas. <correo para estas cosas>
 @Description: Makes queries and transform them into a pandas dataframe
 @Created 2019-08-08T10:42:26.354Z+02:00
 @License:     MIT License
    
    
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
'''
import elasticsearch
from elasticsearch import client
import pandas as pd
import subprocess
import csv
import os
import time
import datetime
import dask.dataframe as dd
from urllib3.exceptions import ReadTimeoutError
from socket import timeout
import numpy as np
import joblib


from elastictools.aerastic.mapcasting import elastic_pandas
from elastictools.logsconf.log_conf import logging_config
import logging
from logging.config import dictConfig


dictConfig(logging_config)
logger = logging.getLogger("CONSOLE")


class ElasticQuery(object):
    def __init__(self, host, port, user, password, dask=False, divisions=4, timeout=50):
        """Class to manage elastic queries.

        Args:
            host (str): ElasticSearch host IP/Name
            port (int): ElasticSearch port number
            user (str): ElasticSearch user
            password (str): ElasticSearch password
            dask (bool, optional): Uses dask. Defaults to False.
            divisions (int, optional): The number of partitions of the index. Defaults to 4.
            time_out (int, optional): Timeout. Defaults to 50.
        """

        port = str(port)
        if host[-1] == ':':
            self.host_connection = host + port
        else:
            self.host_connection = host + ':' + port
        try:
            self.connection = elasticsearch.Elasticsearch(
                self.host_connection, http_auth=(user, password), scheme="http")
        except Exception as err:
            logger.critical("Error in conextion: %s", err)
        self.count = 0
        self.connection_ok, self.listIndex = self.check_connection(timeout)
        self.dask = dask
        self.divisions = divisions

    def check_connection(self, timeout):
        """
        Checks elasticsearch connection and get all index
        :return: False, None error. True, index_list ok
        """
        try:
            subprocess.check_output(
                ['curl', '-XGET', self.host_connection], timeout=timeout)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as err:
            logger.error(err)
            logger.error("Failed to connect to %s." % (self.host_connection))
            return False, None
        else:
            logger.info("There is an elastic listening in %s",
                        self.host_connection)
            try:
                index = self.connection.indices.get("*")
            except Exception as err:
                logger.error("Failed to connect to %s. %s",
                             self.host_connection, err)
                return False, None
            logger.debug("Connected to host")
            return True, index

    def get_types(self, index):
        """Gets types in the index map

        Args:
            index (str): Index name.

        Returns:
            dict: mapping of the index
        """
        if self.connection_ok:
            if self.connection.indices.exists(index):
                elasticclient = client.IndicesClient(self.connection)
                try:
                    index_map = elasticclient.get_mapping(index)
                except elasticsearch.exceptions.NotFoundError as err:
                    index_map = dict()
                keys_maps = list(index_map.keys())
                if len(keys_maps) == 0:
                    logger.error("Error, no matching index %s", keys_maps)
                    return None
                elif len(keys_maps) >= 1:
                    imap = index_map[keys_maps[0]]
                    logger.info("Total index matching %s ", keys_maps)
                try:
                    names = imap['mappings'][list(imap['mappings'].keys())[0]]
                except IndexError as err:
                    logger.error("No map in index %s", err)
                    return None
                return names
        else:
            logger.error(
                "Error in the connection, is elasticsearch running in %s",
                self.host_connection)
            return None

    def get_types_from_query(self, index, query):
        """Get mapping from query.

        Args:
            index (str): Index name.
            query (dict): Query body (DSL or Lucene)

        Returns:
            dict: result of the query.
        """
        all_map = self.get_types(index)
        dict_types = dict()
        try:
            sources = query['_source']
        except KeyError:
            dict_types = self.dict_all_values(all_map)
            return dict_types
        else:
            if len(sources) == 0:
                dict_types = self.dict_all_values(all_map)
                return dict_types
            if isinstance(sources, list):
                for source in sources:
                    dict_types[source] = all_map[source]["type"]
            else:
                dict_types[sources] = all_map[sources]["type"]
        return dict_types

    def dict_all_values(self, all_map):
        dict_types = dict()
        for key, value in all_map.items():
                dict_types[key] = value["type"]
        return dict_types

    def scan(
            self, index, client, query,  delete, datetime_format, scroll = '1h', raise_on_error = True,
            size = 10000, request_timeout = 60, clear_scroll = True,
            scroll_kwargs = None, **kwargs):
        """
        Makes a query, split the return in pages and transform them  in a pandas dataframe
        :param index: Elasticsearch index (now single index)
        :param client: Elasticsearch client         #NO ENTENDI LA PRIMERA VEZ(ES CONFUSO)
        :param query: Query body (DSL or Lucene)
        :param scroll:  Specify how long a consistent view of the index should be maintained
        :param raise_on_error: Launch exception if an error is encountered
        :param size: Size for batch
        :param request_timeout: Explicit timeout
        :param clear_scroll: Clear scroll
        :param scroll_kwargs: Scroll args (future)
        :param delete: Delete queries
        :param kwargs: Other args (future)
        :return: Count and pandas dataframe
        """
        dataframe = None
        dict_types = self.get_types_from_query(index, query)
        if self.connection_ok:
            scroll_kwargs = scroll_kwargs or {}
            result_list = []
            index_list = []
            if isinstance(query, dict):
                time_sleep = 0.7
                if delete:
                    count = 1
                    total_deleted = 0
                    while not count == 0:
                        try:
                            resp = client.delete_by_query(
                                index=index, body=query, scroll=scroll,
                                size=size, request_timeout=request_timeout)
                        except Exception as err:
                            logger.warning(
                                'Autotune, the process time is increased by 0.1 s')
                            time_sleep += 0.1
                        count = resp['deleted']
                        total_deleted += count
                        time.sleep(time_sleep)
                    return count, 'deleted'
                else:
                    resp = client.search(
                        index=index, body=query, scroll=scroll, size=size,
                        batched_reduce_size=512,
                        request_timeout=request_timeout, typed_keys=True, allow_no_indices=False)
                    count = resp['hits']['total']
            else:
                if delete:
                    time_sleep = 0.7
                    count = 1
                    total_deleted = 0
                    while not count == 0:
                        try:
                            resp = client.delete_by_query(
                                index=index, q=query, scroll=scroll, size=size,
                                batched_reduce_size=512,
                                request_timeout=request_timeout,
                                typed_keys=True)
                        except Exception as err:
                            time_sleep += 0.1
                        count = resp['deleted']
                        total_deleted += count
                        time.sleep(time_sleep)
                    return count, delete
                else:
                    resp = client.search(
                        index=index, q=query, scroll=scroll, size=size,
                        batched_reduce_size=512,
                        request_timeout=request_timeout, typed_keys=True, allow_no_indices=False)
                    count = resp['hits']['total']
            logger.info("Total hits = %s in query: %s", count['value'], query)
            scroll_id = resp.get('_scroll_id')
            cont = 1
            if scroll_id is None:
                return
            try:
                first_run = True
                while True:
                    timestamp = []
                    if first_run:
                        first_run = False
                    else:
                        resp = client.scroll(
                            scroll_id=scroll_id, scroll=scroll,
                            request_timeout=request_timeout, **scroll_kwargs)
                    for hit in resp['hits']['hits']:
                        cont += 1
                        if self.dask:
                            if count['value'] < size:
                                size = count['value']
                            if cont % size == 0:
                                pandas_dataframe = pd.DataFrame.from_dict(
                                    result_list, orient='columns')
                                pandas_dataframe.index = index_list
                                if dataframe is None:
                                    dataframe = dd.from_pandas(
                                        pandas_dataframe, npartitions=1)
                                else:
                                    dataframe = dd.concat(
                                        [dataframe, dd.from_pandas(pandas_dataframe, npartitions=1)])
                                result_list = []
                                index_list = []
                        index_list.append(hit['_id'])
                        result_list.append(hit['_source'])
                    if scroll_id is None or not resp['hits']['hits']:
                        if result_list and self.dask:
                            pandas_dataframe = pd.DataFrame.from_dict(
                                result_list, orient='columns')
                            pandas_dataframe.index = index_list
                            dataframe = dd.concat(
                                [dataframe, dd.from_pandas(pandas_dataframe, npartitions=1)])
                        break
            finally:
                if scroll_id and clear_scroll:
                    client.clear_scroll(
                        body={'scroll_id': [scroll_id]},
                        ignore=(404,))
            if not self.dask:
                dataframe = pd.DataFrame.from_dict(
                    result_list, orient='columns')
                dataframe.index = index_list    
            if count['value'] > 0:
                for att, att_type in dict_types.items():
                    try:
                        if att_type == 'date':
                            dataframe[att] = dd.to_datetime(
                                dataframe[att], format=datetime_format)
                        else:
                            dataframe[att] = dataframe[att].astype(
                                elastic_pandas[att_type])
                    except Exception as err:
                        if att_type == 'date':
                            final_type = 'date'
                        else:
                            final_type = elastic_pandas[att_type]
                        logger.debug(
                            "Error to convert type. %s to %s %s", att_type, final_type, err)
                if not kwargs['dataframe_index'] is None:
                    try:
                        dataframe = dataframe.set_index(
                            kwargs['dataframe_index'])
                    except Exception as err:
                        logger.debug("Error to set index, %s", err)
                if self.dask:
                    dataframe = dataframe.repartition(partition_size='100MB')
                return count['value'], dataframe
            else:
                logger.debug("Empty dataframe")
                return None, None
        else:
            logger.error(
                "Error in the connection, is elasticsearch running in %s",
                self.host_connection)
            return None, None

    def raw_query(self, index, query, delete=False, dataframe_index=None, datetime_format="%Y-%m-%d %H:%M:%S"):
        """Send raw query (in DLS or in Lucene format)

        Args:
            index (str): Index name
            query (DLS or Lucene): Body query (dictionary for DLS or text for lucene)
            delete (bool, optional): Delete query. Defaults to False.
            dataframe_index (str, optional): ?. Defaults to None.
            datetime_format (str, optional): Date time format. Defaults to "%Y-%m-%d %H:%M:%S".

        Returns:
            int , pandas.dataFrame: count,result
        """
        dataframe = None
        result = None
        if self.connection_ok:
            if not self.connection.indices.exists(index):
                logger.error("Index %s does not exist", index)
                logger.info("Index list %s", list(
                    self.connection.indices.get('*').keys()))
                return 0, None
            if isinstance(query, dict):
                if delete:
                    logger.info('Delete query in index')
                    counts = self.scan(index, self.connection, delete=True)
                else:
                    count, result = self.scan(
                        index, self.connection, query=query, delete=delete,
                        dataframe_index=dataframe_index, datetime_format=datetime_format)
            else:
                count, result = self.scan(
                    index, self.connection, query=query, delete=delete,
                    dataframe_index=dataframe_index, datetime_format=datetime_format)
            return count, result
        else:
            logger.error(
                "Error in the connection, is elasticsearch running in %s?",
                self.host_connection)
            return 0, None

    # RESULTADO DE UNA QUERY GUARDAR DATOS EN UN CSV
    def insert_to_csv(self, data, file, ntimes=1, separator=","):
        """Save the result of a query in a CSV file.

        Args:
            data (str): The result of the raw query.
            file (str): Relative/absolute path of a file.
            ntimes (int, optional): How many times split the data. Defaults to 1.
            separator (str, optional): Separator will use. Defaults to ",".
        """
        # CABECERA
        hcount = 0
        for i in data.columns:
            with open(file, "a") as f:
                if hcount < len(data.columns) - 1:
                    f.write(i + ",")
                    hcount += 1
                else:
                    f.write(i + "\n")
                f.close()
        # DATOS
        for i in np.array_split(data, ntimes):
            i.to_csv(file, header=False, sep=separator, mode="a")

    def insert_to_excel(self, data, file, ntimes=1):
        """Save the result of a query in EXCEL/ODS file.

        Args:
            data (pandas.dataframe): Result of the query.
            file (str): Relative/Absolute path of a file.
            ntimes (int, optional): How many times split the data. Defaults to 1.
        """
        for i in np.array_split(data, ntimes):
            if "xls" in file:
                # HAY QUE INSTALAR XLSXWRITER PARA QUE FUNCIONE
                writer = pd.ExcelWriter(file, engine="xlsxwriter")
            elif "ods" in file:
                writer = pd.ExcelWriter(file)
            i.to_excel(writer)
            print(i.info(verbose=False, memory_usage=True))
            writer.save()

    def insert_to_hdf(self,data,file, ntimes=1):
        ddata = dd.from_pandas(data,ntimes)
        dd.to_hdf(ddata,file)