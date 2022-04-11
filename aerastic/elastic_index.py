# -*- coding:utf-8 -*-
'''
  @ Author: Aerin Sistemas <aerin_proyectos@aerin.es>
  @ Create Time: 2020-12-05 11:21:16
  @ Modified time: 2021-07-08 18:54:17
  @ Project: ElasticTools-Aerastic
  @ Description:
  @ License: MIT License
 '''

from os import sep
import subprocess
import json
import time
import csv
import codecs
from multiprocessing import Process


import pandas as pd
import dask as dd
import socket
import numpy as np
import elasticsearch
from elasticsearch import helpers
from elasticsearch import client
import re

from elastictools.logsconf.log_conf import logging_config
import logging
from logging.config import dictConfig
import elastictools.aerastic.map_lambda as maptypes

dictConfig(logging_config)
logger = logging.getLogger("CONSOLE")


class ElasticIndex(object):
    def __init__(self, host, port, user, password, timeout=30, dask=False):
        """Class to manage elastic index

        Args:
            host (str): ElasticSearch host IP/Name
            port (int): ElasticSearch port number
            user (str): ElasticSearch user
            password (str): ElasticSearch password
            timeout (int, optional): Timeout. Defaults to 30.
            dask (bool, optional): Uses dask. Defaults to False.
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
        else:
            self.types_dictionary = {}
            self.connection_ok, self.listIndex = self.check_connection(timeout)
            self.last_field = None

    def recursive_dic(self, dic):  # Funcion interna
        for key in dic.keys():
            name = dic[key]
            if key == 'type':
                self.types_dictionary[self.last_field] = dic[key]
                break
            else:
                self.last_field = key
            if isinstance(dic[key], dict):
                self.recursive_dic(dic[key])

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

    def get_all_index(self):
        """Get all index

        Returns:
            dict: index list
        """
        if not self.connection_ok:
            logger.error(
                "Error in the connection, is elasticsearch running in %s?",
                self.host_connection)
            return None
        index_list = self.listIndex
        info = self.connection.cat.indices().split('\n')
        for index, inf in zip(index_list, info):
            info_list = inf.split(' ')
            info_list = [value for value in info_list if not value == '']
            status = info_list[2]
            uuid = info_list[3]
            size = info_list[-1]
            try:
                doc = list(index_list[index]['mappings'].keys())[0]
            except Exception:
                doc = ""
            logger.info(
                "Index name: %s, Doc: %s, Status: %s, uuid: %s, Size: %s" %
                (index, doc, status, uuid, size))
        return self.listIndex

    def get_index(self, index_name):
        """Gets types of a map

        Args:
            index_name (str): Name of the index
        Returns:
            json: Includes index name and the properties
        """
        index_map = {'empty': 'empty'}
        if not self.connection_ok:
            logger.error(
                "Error in the connection, is elasticsearch running in %s?",
                self.host_connection)
            return -1
        if self.connection.indices.exists(index_name):
            elasticclient = client.IndicesClient(self.connection)
            try:
                index_map = elasticclient.get_mapping(index_name)
            except elasticsearch.exceptions.NotFoundError as err:
                logger.error("Index not exist %s", err)
        self.recursive_dic(index_map)
        return index_map, self.types_dictionary

    def set_index_from_pandas(self, index_name, dataframe, geopos_field=None):
        """Create a new index from pandas

        Args:
            index_name (str): Index name
            dataframe (DataFrame): Pandas dataframe with the data
            geopos_field ((?), optional): [description]. Defaults to None.
        """
        # COMPROBAR LOS PUNTOS
        pandas_map = self.map_from_pandas(dataframe, geopos_field)
        self.set_index(index_name, pandas_map)

    def set_index(self, index_name, map):  # LIMITES AL CREAR INDEX limit at 1000
        """Create a new index

        Args:
            index_name (str): Index name
            map (dict): Data mapping

        Returns:
            int: response if error
            dict: response if success
        """
        if not self.connection_ok:
            logger.error(
                "Error in the connection, is elasticsearch running in %s?",
                self.host_connection)
            return -1
        if self.connection.indices.exists(index_name):
            logger.debug("There is already an index with same name")
            return -1
        else:
            try:
                elasticclient = client.IndicesClient(self.connection)
                response = elasticclient.create(index_name, map)
            except Exception as err:
                logger.error("The index could not be created: %s", err)
                return -1
            else:
                logger.debug("Index %s created!!", index_name)
        return response

    def get_maptypes(self, index_name):
       """
       Gets types of a map
       :param index_name: Name index
       :return: JSON map
       """
       index_map = {'empty': 'empty'}
       if not self.connection_ok:
           logger.error(
               "Error in the connection, is elasticsearch running in %s?",
               self.host_connection)
           return -1
       if self.connection.indices.exists(index_name):
           elasticclient = client.IndicesClient(self.connection)
           try:
               index_map = elasticclient.get_mapping(index_name)
           except elasticsearch.exceptions.NotFoundError as err:
               logger.error("Index not exist %s", err)
       self.recursive_dic(index_map)
       return index_map, self.types_dictionary


    def map_from_pandas(self, dataframe, geopos_field=None):  # Funcion interna
        # def map_from_pandas(self, dataframe, mapname, geopos_field=None)
        """
        Set a map from a pandas dataframe
        :param dataframe: Pandas dataframe
        :param mapname: Map name (not index name or doc name)
        :param geopos_field: (?)
        :return: Elasticsearch valid map
        """
        mapping = dict()
        properties = dict()
        data = dict()
        try:
            logger.info("We're changing the dots ('.') for low bar ('_')")
            heads = dataframe.columns
            datatypes = dataframe.dtypes
        except Exception as err:
            logger.error(
                "Error to read heads and types in dataframe: %s" % err)
        for head, typeval in zip(heads, datatypes):
            if "." in head:
                head = head.replace(".", "_")
            if head == "":
                head = "empty_name"
            pandas_type = maptypes.panda_elastic[typeval]
            if pandas_type == "text":
                if head == geopos_field:
                    data[head] = {"type": "geo_point"}
                else:
                    key_dict = {"keyword": {
                        "type": "keyword", "ignore_above": 750}}
                    data[head] = {
                        "type": maptypes.panda_elastic[typeval],
                        "fields": key_dict}
            else:
                data[head] = {"type": maptypes.panda_elastic[typeval]}
        index = dataframe.index
        if (isinstance(index, pd.core.indexes.datetimes.DatetimeIndex)):
            data['timestamp'] = {"type": 'date'}
        properties['properties'] = data
        mapping['mappings'] = properties
        return mapping

    def save_map(self, index_name, file_name=None):
        """Save index map in a file

        Args:
            index_name (str): Index name
            file_name (str, optional): File path. Defaults to None.

        Returns:
            dict: return map

        """
        if not self.connection_ok:
            logger.error(
                "Error in the connection, is elasticsearch running in %s?",
                self.host_connection)
            return -1
        if self.connection.indices.exists(index_name):
            elasticclient = client.IndicesClient(self.connection)
            elas_map = elasticclient.get_mapping(index_name)
        else:
            logger.error("No map in %s", index_name)
            return None
        if file_name is None:
            return elas_map
        else:
            with open(file_name, 'w') as fw:
                json.dump(elas_map, fw)
            return elas_map

    def load_map(self, file_name):
        """
        Load a map from a file
        :param file_name: File map name
        :return: Map in a dictionary
        """
        try:
            with open(file_name, 'r') as fr:
                map_dictionary = json.load(fr)
        except FileNotFoundError as err:
            logger.error("Error to read map file: %s" % err)
            return {}
        return map_dictionary[list(map_dictionary.keys())[0]]

    def reindex(self, source, target):
        """Reindex, the index target must exist

        Args:
            source (str): Source index
            target (str): Target index

        """
        if not self.connection_ok:
            logger.error(
                "Error in the connection, is elasticsearch running in %s?",
                self.host_connection)
            return -1
        if self.connection.indices.exists(source):
            if self.connection.indices.exists(target):
                logger.warning("Reindex over exist index. Please wait....")
                helpers.reindex(self.connection, source, target)
                logger.info("End reindex")
            else:
                self.set_index(target, None)
                logger.info("Reindex with map %s. Please wait....", map)
                helpers.reindex(self.connection, source, target)
                logger.warning("End reindex")
        else:
            logger.error("Source does not exist")

    def delete_index(self, index):
        """Delete an index (remove all data)

        Args:
            index (str)): Index name
        """
        if not self.connection_ok:
            logger.error(
                "Error in the connection, is elasticsearch running in %s?",
                self.host_connection)
            return -1
        if self.connection.indices.exists(index):
            response = self.connection.indices.delete(index)
            logger.info("Delete response: %s" % response)
        else:
            logger.info("The index does not exist")


class InsertData(ElasticIndex):
    def __init__(self, host, port, user, password, timeout=30):
        """Class to insert data

        Args:
            host (str): ElasticSearch host IP/Name
            port (int): ElasticSearch port number
            user (str): ElasticSearch user
            password (str): ElasticSearch password
            timeout (int, optional): Timeout. Defaults to 30.
        """
        super().__init__(host, port, user, password, timeout)

    def _generate_data(self, pandasframe, indexname, dateindex=False, index_id=False):
        datetimefield = None
        json_dict = dict.fromkeys(list(pandasframe.columns))
        head = [indexname, indexname]
        status, mapping = self.check_index(indexname)
        mapping_fields = mapping['mappings']['properties']
        for key in mapping_fields.keys():
            if mapping_fields[key]['type'] == 'date':
                datetimefield = key
        if status:
            for index, value in pandasframe.iterrows():
                for key in json_dict:
                    if not key == 'timestamp':
                        isnull = pd.isnull(value[key])
                    else:
                        isnull = False
                    if not key == 'Serial' and not key == 'timestamp':
                        try:
                            isnan = np.isnan(value[key])
                        except Exception as err:
                            isnan = False
                        if isnan or isnull:
                            one_value = None
                        else:
                            one_value = value[key]
                        json_dict[key] = one_value
                if dateindex:
                    if isinstance(index, pd.DatetimeIndex):
                        json_dict['timestamp'] = index
                if index_id:
                    yield {"_index": indexname, "_id": index, "_source": json_dict}
                else:
                    yield {"_index": indexname, "_source": json_dict}

    def check_index(self, index_name, mapping=None):
        """Checks elasticsearch index

        Args:
            index_name (str): Index name
            mapping ([type], optional): [description]. Defaults to None.

        Returns:
            bool, dict: response, mapping
        """
        p = '(?:http.*://)?(?P<host>[^:/ ]+).?(?P<port>[0-9]*).*'
        m = re.search(p, self.host_connection)
        host, port = m.group('host'), m.group('port')
        matching = False
        try:
            s = socket.create_connection((host, port), 2)
        except ConnectionRefusedError as err:
            logger.error(
                "Connection error. Is ElasticSearch running?, check the host name and port number")
            return False, None
        else:
            if self.connection.indices.exists(index_name):
                elas_client = client.IndicesClient(self.connection)
                index_mapping = elas_client.get_mapping(index_name)[index_name]
                if not mapping is None:
                    matching = mapping == index_mapping
                    msg_match = "Maps equal: " + str(matching)
                else:
                    msg_match = "Not map to compare"
                logger.info("The index exist. %s", msg_match)
                return True, index_mapping
            else:
                logger.error("The index does not exist")
                return False, None

    def insert_csv(self, index_name, file_name, separator=',',
                   chunk_size=1, field_type={}):
        """Inserts data from CSV

        Args:
            index_name (str): Index name
            file_name (str): Absolute or relative path of the file
            separator (str, optional): Character used as delimiter. Defaults to ','.
            chunk_size (int, optional): Size of the chunk. Default to 1
            field_type (dict, optional): Default to {}:
        Returns:
            int,str: response status
        """
        if len(field_type) != 0:
            data = pd.read_csv(file_name, sep=separator,
                               dtype=field_type, chunksize=chunk_size)
        else:
            data = pd.read_csv(file_name, sep=separator, chunksize=chunk_size)
        time_init = time.time()
        total_len = 0
        try:
            for chunk in data:
                self.insert_pandas(chunk, index_name, file_name)
                total_len += len(chunk)
        except Exception as err:
            logger.error("Error to bulk data %s", err)
            return 1, "Error to bulk data." + str(err)
        else:
            logger.info("Total time multiprocess insertions in %s partitions: %s",
                        total_len, str(time.time() - time_init))
            return 0, "Bulk done."

    def insert_excel(self, index_name, file_name, rows):
        """Insert data from Excel or Ods

        Args:
            index_name (str): Index name
            file_name (str): Absolute or relative path of the file
            rows (int): How many rows want to read.

        Returns:
            int,str: response status
        """
        # PARA QUE FUNCIONE HAY DE INSTALAR xlrd y odfpy
        try:
            header = pd.read_excel(file_name, nrows=0)
            columns = {i: col for i, col in enumerate(
                header.columns.tolist())}
            skiprows = 1
            while True:
                chunk = pd.read_excel(
                    file_name, nrows=rows, skiprows=skiprows, header=None)
                chunk.rename(columns=columns, inplace=True)
                if not chunk.shape[0]:
                    break
                self.insert_pandas(chunk, index_name, file_name)
                skiprows += rows
        except Exception as err:
            logger.error("Error to bulk data %s", err)
            return 1, "Error to bulk data." + str(err)
        else:
            return 0, "Bulk done."

    def unicode_dict_reader(self, utf8_data, **kwargs):  # Funcion interna INUTILIZADA
        csv_reader = csv.DictReader(utf8_data, **kwargs)
        for row in csv_reader:
            yield {str(key): str(value).replace(',', '.') for key, value in row.items()}

    def insert_pandas(self, pandasframe, indexname, docname, dateindex=False, index_id=False):  # Funcion interna
        """Inserts data from dataframe pandas to ElasticSearch index
        (for little and medium dataframe, for big files uses other method)

        Args:
            pandasframe (pandas.dataframe): Dataframe.
            indexname (str): Index name.
            docname (str): File name.
            dateindex (bool, optional): Uses the index as timestamp. Defaults to False.
            index_id (bool, optional): [description]. Defaults to False.
        """
        now = time.time()
        helpers.bulk(
            self.connection, actions=self._generate_data(
                pandasframe, indexname, dateindex, index_id),
            raise_on_exception=False, raise_on_error=False, stats_only=True)
        logger.info("Total time insertions: %s", str(time.time()-now))

    def insert_dask(self, daskdataframe, indexname, docname, dateindex=False, index_id=False):
        """Insert data from dask dataframe (for big dataframe)

        Args:
            daskdataframe (dask.dataframe): Data
            indexname (str): Index name
            docname (str): File name
            dateindex (bool, optional): [description]. Defaults to False.
            index_id (bool, optional): Uses the index as timestamp.. Defaults to False.
        """
        time_init = time.time()
        partitons = daskdataframe.npartitions
        for n_part in range(0, partitons):
            pandas = daskdataframe.get_partition(n_part).compute()
            self.insert_pandas(pandas, indexname, docname, dateindex, index_id)
        logger.info("Total time insertions in %s partitions: %s",
                    partitons, str(time.time()-time_init))

    # Porque pide DOCNAME si nunca lo lee.
    def insert(self, daskdataframe, indexname, docname, dateindex=False, index_id=False, multiprocessor=False):
        """Insert data from dask dataframe.

        Args:
            daskdataframe (dask.dataframe): Data
            indexname (str): Index name
            docname (str): File name
            dateindex (bool, optional): [description]. Defaults to False.
            index_id (bool, optional): Uses the index as timestamp. Defaults to False.
            multiprocessor (bool,optional): If comes from MultiprocessInsertData. Default to False.
        """
        if multiprocessor == False:
            pandas = daskdataframe.compute()
            self.insert_pandas(pandas, indexname,
                               docname, dateindex, index_id)
        else:
            self.insert_pandas(daskdataframe, indexname,
                               docname, dateindex, index_id)


class MultiprocessInsertData(object):
    def __init__(self, host, port, user, password, indexname, docname,
                 dateindex=False, index_id=False, timeout=30):
        """Class to insert data in multriprocess

        Args:
            host (str): ElasticSearch host IP/Name
            port (int): ElasticSearch port number
            user (str): ElasticSearch user
            password (str): ElasticSearch password
            indexname (str): Index name
            docname (str): File name
            dateindex (bool, optional): ?. Defaults to False.
            index_id (bool, optional): Uses the index as timestamp. Defaults to False.
            timeout (int, optional): Timeout. Defaults to 30.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.indexname = indexname
        self.docname = docname
        self.dateindex = dateindex
        self.index_id = index_id
        self.process_list = []

    def run_insert(self, alldaskdataframe):
        """ Insert dask dataframe multiprocess.

        Args:
            alldaskdataframe (dask.dataframe): Data.
        """
        time_init = time.time()
        partitons = alldaskdataframe.npartitions
        for n_part in range(0, partitons):
            # No siempre da los mismos resultados, CREO QUE SOLUCIONADO
            # HACER MAS PRUEBAS
            insert_handler = InsertData(
                self.host, self.port, self.user, self.password)
            dask_dataframe = alldaskdataframe.get_partition(n_part)
            pandas_dataframe = dask_dataframe.compute()
            p = Process(target=self.multi_insert, args=(
                pandas_dataframe, insert_handler, ))
            p.start()
            self.process_list.append(p)
        logger.info(
            "Total time multiprocess insertions in %s partitions: %s",
            partitons, str(time.time() - time_init))

    def run_csv(self, file_name, separator=',',
                chunk_size=1, field_type={}, nprocess=4):
        """Insert CSV with multiprocess. For large files.

        Args:
            file_name (str): Absolute or relative path of the file
            separator (str, optional): Character used as delimiter. Defaults to ','.
            chunk_size (int, optional): Size of the chunk. Defaults to 1.
            field_type (dict, optional): Dtype of the column . Defaults to {}.
            nprocess (int, optional): How many process to use. Defaults to 4.
        """
        if len(field_type) != 0:
            data = pd.read_csv(file_name, sep=separator,
                               dtype=field_type, chunksize=chunk_size)
        else:
            data = pd.read_csv(file_name, sep=separator, chunksize=chunk_size)
        time_init = time.time()
        total = 0
        for chunk in data:
            total += len(chunk)
            lista = np.array_split(chunk, nprocess)
            for n in range(0, nprocess):
                insert_handler = InsertData(
                    self.host, self.port, self.user, self.password)
                p = Process(target=self.multi_insert, args=(
                    lista[n], insert_handler, ))
                p.start()
                self.process_list.append(p)
        logger.info("Total time multiprocess insertions in %s partitions: %s",
                    total, str(time.time() - time_init))

    def run_excel(self, file_name, rows, nprocess=4):
        """Insert EXCEL/ODS with multiprocess. For large files.

        Args:
            file_name (str): Absolute or relative path of the file
            rows (int): How many rows to load.
            nprocess (int, optional): How many process to use. Defaults to 4.
        """
        header = pd.read_excel(file_name, nrows=0)
        columns = {i: col for i, col in enumerate(
            header.columns.tolist())}
        skiprows = 1
        time_init = time.time()
        total = 0
        while True:
            chunk = pd.read_excel(
                file_name, nrows=rows, skiprows=skiprows, header=None)
            chunk.rename(columns=columns, inplace=True)
            if not chunk.shape[0]:
                break
            total += len(chunk)
            lista = np.array_split(chunk, nprocess)
            for n in range(0, nprocess):
                insert_handler = InsertData(
                    self.host, self.port, self.user, self.password)
                p = Process(target=self.multi_insert, args=(
                    lista[n], insert_handler, ))
                p.start()
                self.process_list.append(p)
            skiprows += rows
        logger.info("Total time multiprocess insertions in %s partitions: %s",
                    total, str(time.time() - time_init))

    def multi_insert(self, daskdataframe, insert_handler):  # Funcion interna
        """[summary]

        Args:
            daskdataframe ([type]): [description]
            insert_handler ([type]): [description]
        Returns:
            [type]: [description]
        """
        insert_handler.insert(daskdataframe, self.indexname,
                              self.docname, self.dateindex, self.index_id, multiprocessor=True)
        return True

    def stop_process(self):
        """Finish all process
        """
        for p in self.process_list:
            p.terminate()
