# -*- coding:utf-8 -*-
'''
 @File Name: log_conf.py
 @Author: Jose Luis Blanco
 @Description: Logging configuration file
 @Created 2019-07-26T14:10:59.839Z+02:00
 @Last-modified 2019-07-26T14:10:59.839Z+02:00
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

logging_config = {
    "version": 1,
    "disable_existing_loggers": 0,
    "loggers": {
    },
    "formatters": {
        "precise": {
            "format": "%(asctime)s %(name)-15s %(levelname)-8s %(message)s"
        },
        "data": {
            "format": "%(levelname)-8s: %(message)s"
        },
        "report": {
            "format": "%(message)s"
        },
        "node": {
            "format": "NODERED-GREYMATTER-PYTHON %(levelname)-8s: %(message)s"
        },
    },
    "handlers": {
        "debugfile": {
            "class": "logging.FileHandler",
            "formatter": "precise",
            "mode": "a",
            "level": "DEBUG",
            "filename": "debug.log"
        },
        "console": {
            "formatter": "precise",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "level": "DEBUG"
        },
        "data": {
            "formatter": "data",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "level": "INFO",
        },
        "report": {
            "formatter": "report",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "level": "INFO",
        },
        "node": {
            "formatter": "node",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "level": "DEBUG"
        }

    },
    'loggers': {
        'VERBOSE': {
            'handlers': ['console', 'debugfile'],
            'level': 'DEBUG',
            'propagate': True
        },
        'TOFILE': {
            'handlers': ['debugfile'],
            'level': 'DEBUG',
            'propagate': True
        },
        'CONSOLE': {
            'handlers': ['console'],
            'level': 'DEBUG',
            'propagate': True
        },
        'REPORT': {
            'handlers': ['report'],
            'level': 'DEBUG',
            'propagate': False
        },
        'NODERED': {
            'handlers': ['node'],
            'level': 'DEBUG',
            'propagate': False
            }

    }
}