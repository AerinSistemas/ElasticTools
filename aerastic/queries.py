# -*- coding:utf-8 -*-
'''
 @File Name: queries.py
 @Author: Jose Luis Blanco
 @Description: Elastic queries maker
 @Created 2019-08-08T11:07:20.022Z+02:00
 @Last-modified 2019-08-08T11:07:20.022Z+02:00
 @License:     MIT License
    
    Copyright (c) 2019 Jose Luis Blanco
    
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


from elastictools.logsconf.log_conf import logging_config
import logging
from logging.config import dictConfig


dictConfig(logging_config)
logger = logging.getLogger("CONSOLE")


def range(**kwargs):
    ranges = {}
    for key in kwargs.keys():
        ranges["range"] = {key: {"gte": kwargs[key][0], "lte": kwargs[key][1]}}
    query = {"query": ranges}
    return query


def boolq(**kwargs):
    logical = {}
    for key in kwargs.keys():
        match = []
        for values in kwargs[key]:
            match.append({"match": {values[0]: values[1]}})
        logical[key] = match
    query = {"query": {"bool": logical}}
    return query


def createQuery(**kwargs):
    q_dictionary = dict()
    bool_dictionary = dict()
    if 'all' in kwargs.keys():
        q_dictionary = {"query": {"match_all": {}}}
    if 'limit' in kwargs.keys():
        q_dictionary["from"] = 0
        q_dictionary["size"] = kwargs['limit']
    '''for boolkey, boolvalues in kwargs.items():
        temp = []
        for key, values in boolvalues.items():
            if isinstance(values, list):
                temp.append({"range": {key: {"gte": values[0], "lte": values[1]}}})
            elif isinstance(values, set):
                values = list(values)
                match = []
                for val in values:
                    temp.append({"match": {key: val}})
            else:
                temp.append({"match": {key: values}})
        bool_dictionary[boolkey] = temp
    if bool(bool_dictionary):  
        q_dictionary["query"] = bool_dictionary'''
    print(q_dictionary)
    return q_dictionary


def createQueryString(**kwargs):
    stringq = ''
    for value in kwargs['lucene']:
        stringq += ' ' + value
    q_dictionary = {"query": stringq}
    return {"query": {"query_string": q_dictionary}}


def addSource(q, source):
    q["_source"] = source
    return q
