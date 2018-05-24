# -*- coding: utf-8 -*-
"""
Created on Thu Dec 07 12:23:37 2017

@author: RAVITHEJA
"""

import os
import logging
import time
import requests
import zipfile
import urllib2
import json
from config import mongo_config
from MongodbConnector import mongodbConnector
from os import walk
from datetime import datetime


DEFAULT_DATA_PATH = os.path.abspath(os.path.join(
    os.path.dirname('__file__'), '', 'Quandl'))
mongo = mongodbConnector()


def getCodesInCSVsForAllDatasets(quandl_apikey):
    logging.basicConfig(format='%(asctime)s %(levelname)s \
                        %(module)s.%(funcName)s :: %(message)s',
                        level=logging.INFO)

    q_db_base_url = "https://www.quandl.com/api/v3/databases"
    q_databases_url = q_db_base_url + "?api_key={0}&page={1}"
    q_codes_url = q_db_base_url + "/{0}/codes.json?api_key={1}"

    page = 0
    database_codes = {}
    premium_codes = []
    prev_codes_count = -1
    total_codes = 0
    json_data = {}

    """while prev_codes_count != total_codes:
        prev_codes_count = total_codes
        try:
            page += 1
            q_db_URL = q_databases_url.format(quandl_apikey, str(page))

            time.sleep(2)
            json_data = (requests.get(q_db_URL)).json()

            for d in json_data['databases']:
                if d['premium']:
                    premium_codes.append(d['database_code'])
                if not d['premium']:
                    database_codes[d['database_code']] = d['name']
    
            total_codes = len(database_codes) + len(premium_codes)
        except:
            print "Errorrrrr..."
            continue
    print database_codes"""

    with open('q_database_codes.json') as q_database_codes:    
        database_codes = json.load(q_database_codes)

    for code in database_codes.keys():
    
        zip_filename = code + '-datasets-codes.zip'

        code_colln = mongo.initialize_mongo(code)
        code_colln_count = code_colln.count({})

        if not code_colln_count:
            
            try:
                time.sleep(1)
                resp = urllib2.urlopen(q_codes_url.format(code, quandl_apikey))
                zipcontent = resp.read()
        
                with open(zip_filename, 'wb') as zfw:
                    zfw.write(zipcontent)
        
                with zipfile.ZipFile(zip_filename, "r") as zfr:
                    zfr.extractall(DEFAULT_DATA_PATH)
        
                saveCodesInMongo(database_codes[code])
                
            except:
                print "Errorrrrr..."
                continue
            finally:
                if os.path.isfile(zip_filename):
                    os.remove(zip_filename)

    logging.info(str(len(database_codes))
                 + " datasets should be downloaded to Mongo")


def saveCodesInMongo(qcode_name):

    quandl_codes_colln_name = mongo_config.get('quandl_codes_colln_name')

    q_data_base_URL = "https://www.quandl.com/api/v3/datasets/{0}"

    filenamesList = []
    for (dirpath, dirnames, filenames) in walk(DEFAULT_DATA_PATH):
        filenamesList.extend(filenames)

    qcodes_colln = mongo.initialize_mongo(quandl_codes_colln_name)
    for fn in filenamesList:
        try:
            dataset_qcodes = []
            logging.info(fn + " extracted.")
            codesFile = os.path.abspath(os.path.join(DEFAULT_DATA_PATH, fn))
            dataset = fn.replace('-datasets-codes.csv', '')

            qcode_cursor = qcodes_colln.find_one({'dataset': dataset})
            if not qcode_cursor:
                with open(codesFile, 'r') as csv_file:
                    csvlines = csv_file.readlines()
    
                    for num, line in enumerate(csvlines):
                        codeline = line.split(',')
                        if len(codeline) > 1:
                            dataset_code = codeline[0]
                            dataset_descrpn = codeline[1]
                            created_time = datetime.now().strftime("%Y-%m-%d")
    
                            code_doc = {"dataset": dataset,
                                        "dataset_code": dataset_code,
                                        "description": dataset_descrpn,
                                        "base_url": q_data_base_URL.format(dataset_code),
                                        "created_time":	created_time,
                                        "name": qcode_name,
                                        "_id": dataset_code,
                                        }
                            dataset_qcodes.append(code_doc)
                        
            if qcode_cursor:
                mongo.bulk_mongo_update(qcodes_colln, dataset_qcodes)
            else:
                mongo.bulk_mongo_insert(qcodes_colln, dataset_qcodes)

        except:
            raise
            print "Errorrrrr..."
            continue
        finally:
            os.remove(codesFile)
