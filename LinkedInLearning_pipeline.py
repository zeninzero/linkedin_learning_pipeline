import requests
import json
from accessToken import access_token, expires_on
from subprocess import call
import os
import logging
from datetime import datetime, timedelta
import pytz
import asyncio
import argparse
import sys
import io
import zipfile
import shutil
import pandas as pd
import glob
from pyspark.sql import SparkSession, functions
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import copy
import json
import sys
import collections
import json.decoder
from avro import schema
import time
import uuid
import traceback
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import yaml
import csv
import argparse
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from kafka import KafkaConsumer, TopicPartition
from google.cloud import bigquery
from time import sleep
import logging
import numpy as np
import math

def oAuth():
    client_id = 'XXXXXXXXXXX'
    client_secret = 'YYYYYYYYYYY'

    url = "https://www.linkedin.com/oauth/v2/accessToken"

    querystring = {"grant_type":"client_credentials","client_id":client_id,"client_secret":client_secret}
    payload = "<share>\n    <comment>Comment</comment>\n    <content>\n        <title>title"
    headers = {'Content-Type': "text/plain", 'Cache-Control': "no-cache", 'Host': "www.linkedin.com",
               'Accept-Encoding': "gzip, deflate", 'Connection': "keep-alive", 'cache-control': "no-cache"}

    response = requests.request("POST", url, data=payload, headers=headers, params=querystring,verify=False)
    response = json.loads(response.text)
    token = response['access_token']

    current_time = int(time.time() * 1000)
    expiration = current_time + response['expires_in']

    exp = str(expiration)

    new_token = "access_token = %a \nexpires_on = '%s'" % (token, exp)
    print("new expiration:", new_token)

    response = {}
    response['token'] = token
    response['exp'] = exp
    return response

data_array = []

def report_import(url, headers, querystring = ''):

    response = requests.request("GET", url, headers=headers, params=querystring,verify=False)

    data = json.loads(response.text)

    print(url)
    print(data)
    data_array.append(data['elements'])

    print("=============================================")

    if  len(data['paging']['links']) == 1 and data['paging']['links'][0]['rel'] == 'prev':
        print("last_batch_data::End of results for request")
        print("=============================================")
    else:
        print("current_batch_data::")

        url = ''
        if data['paging']['links'][0]['rel'] == 'prev':
            url = 'https://api.linkedin.com/' + data['paging']['links'][1]['href']
        elif data['paging']['links'][0]['rel'] == 'next':
            url = 'https://api.linkedin.com/' + data['paging']['links'][0]['href']
        report_import(url, headers)

    return data


def main():

    # API reference: https://docs.microsoft.com/en-us/linkedin/learning/integrations/learning-activity-reports

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    oAuthToken = oAuth()
    bearerToken = "Bearer " + oAuthToken['token']

    # current_time = int(time.time() * 1000)
    # if current_time > int(oAuthToken['exp']):
    #     oAuthToken = oAuth()

    headers = {
        'Authorization': bearerToken,
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Host': "api.linkedin.com",
        'cache-control': "no-cache"
    }

    numberOfResults = 20
    offsetUnit = "DAY"
    offsetDuration = 14  # maximum offset is 14 days, larger calls will return 500 error
    # converting current time from seconds since epoch to milliseconds since epoch
    currentTime = int(time.time() * 1000)
    # startedAt is milliseconds since epoch, adjusted based upon the offset duration
    startedAt = currentTime - (86400000 * offsetDuration)  # 86400000 is number of milliseconds in a day
    primaryAggregation = "INDIVIDUAL"
    secondaryAggregation = "CONTENT"
    contentSource = "EXTERNAL"
    assetType = "COURSE"

    querystring = {"q": "criteria",
                   "count": numberOfResults,
                   "startedAt": startedAt,
                   "timeOffset.unit": offsetUnit,
                   "timeOffset.duration": offsetDuration,
                   "aggregationCriteria.primary": primaryAggregation,
                   "aggregationCriteria.secondary": secondaryAggregation,
                   "locale.language": "en",
                   # scopeFilterType:scopeFilter,
                   "contentSource": contentSource,
                   "assetType": assetType
                   }

    url = "https://api.linkedin.com/v2/learningActivityReports"

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    data = report_import(url, headers,querystring)
    df_test = pd.json_normalize(data["elements"])

    df_stg_stg = []
    for arr in data_array:
        df_stg_stg.append(pd.json_normalize(arr))

    appended_data = pd.concat(df_stg_stg)
    df_stg = appended_data

    print("appended_data::")
    print(len(appended_data.index))
    #print(appended_data)


    df = (pd.concat({i: pd.DataFrame(x) for i, x in df_stg.pop('activities').items()})
           .reset_index(level=1, drop=True)
           .join(df_stg)
           .reset_index(drop=True))

    df.columns = [x.replace(".","_") for x in df.columns]

    pd.set_option("display.max_rows", None, "display.max_columns", None)

    df.columns = df.columns.str.replace(' ', '_')
    df = df.reset_index(drop=True)

    ######## WRITE TO KAFKA ################

    def get_name_type_pairs(header):
        return ",\n".join(
            ['\t\t{"name": "%s", "type": "string"}' % x.replace(' ', '_').replace('(', '_').replace(')', '_') for x in
             header])

    def generate_schema(header):
        schema_str = """
    {
    	"namespace": "%s",
    	"type": "record",
    	"name": "Log",
    	"fields": [
    %s
    	]
    }""" % ("result", get_name_type_pairs(header))
        return avro.schema.parse(schema_str)

    cfg_d = {}

    
    compute_project = os.environ.get('COMPUTE_PROJECT')

    from google.cloud import storage
    client = storage.Client(project=compute_project)
    bucket = client.get_bucket(compute_project)
    blob = bucket.get_blob('config.yaml')

    print(blob.download_as_string())

    cfg_d = yaml.safe_load(blob.download_as_string())

    ssl_cafile = '/certs/ca-int.pem'
    ssl_certfile = '/certs/client.pem'
    ssl_keyfile = '/certs/client.key'

    print("loaded yaml")

    kafka_url = ''
    broker_list = ''

    kafka_url = 'SCHEMA_REGISTRY_URL'
    broker_list = 'BROKERS_LIST'

    schema_registry = CachedSchemaRegistryClient(url=kafka_url, ca_location=ssl_cafile,
                                                 cert_location=ssl_certfile, key_location=ssl_keyfile)

    kafka_params = {'bootstrap.servers': broker_list, 'default.topic.config': {'message.timeout.ms': 300000},
                    'acks': 'all', 'compression.codec': 'snappy',
                    'security.protocol': 'ssl',
                    'ssl.ca.location': ssl_cafile,
                    'ssl.certificate.location': ssl_certfile,
                    'ssl.key.location': ssl_keyfile,
                    'queue.buffering.max.messages': 1000000,
                    'queue.buffering.max.ms': 5000,
                    'batch.num.messages': 1000,
                    'schema.registry.url': kafka_url,
                    'schema.registry.ssl.certificate.location': ssl_certfile,
                    'schema.registry.ssl.key.location': ssl_keyfile,
                    'schema.registry.ssl.ca.location': ssl_cafile
                    }

    from avro import schema

    current_milli_time = lambda: int(round(time.time() * 1000))

    schema_id, sschema_value, schema_version = schema_registry.get_latest_schema('linkedinlearning-value')
    avroProducer = AvroProducer(kafka_params, default_value_schema=sschema_value)

    counter = 1

    print("data::")
    for index, row in df.iterrows():

        # data massaging
        row['learnerDetails_enterpriseGroups'] = str(",".join(row['learnerDetails_enterpriseGroups']) or '')

        if math.isnan(row['firstEngagedAt']):
            row['firstEngagedAt'] = 0
        else: row['firstEngagedAt'] = int(round(row['firstEngagedAt']))
                        
        if math.isnan(row['lastEngagedAt']):
            row['lastEngagedAt'] = 0
        else: row['lastEngagedAt'] = int(round(row['lastEngagedAt']))
                        
        if math.isnan(row['engagementValue']):
            row['engagementValue'] = 0
        else: row['engagementValue'] = int(round(row['engagementValue']))               

        row['engagementType'] = str(row['engagementType'] or '')
        row['engagementMetricQualifier'] = str(row['engagementMetricQualifier'] or '')
        row['assetType'] = str(row['assetType'] or '')
        row['learnerDetails_name'] = str(row['learnerDetails_name'] or '')
        row['learnerDetails_email'] = str(row['learnerDetails_email'] or '')
        row['learnerDetails_entity_profileUrn'] = str(row['learnerDetails_entity_profileUrn'] or '')
        row['learnerDetails_uniqueUserId'] = str(row['learnerDetails_uniqueUserId'] or '')
        row['contentDetails_name'] = str(row['contentDetails_name'] or '')
        row['contentDetails_contentSource'] = str(row['contentDetails_contentSource'] or '')
        row['contentDetails_contentUrn'] = str(row['contentDetails_contentUrn'] or '')
        row['contentDetails_locale_country'] = str(row['contentDetails_locale_country'] or '')
        row['contentDetails_locale_language'] = str(row['contentDetails_locale_language'] or '')

        eventRecord = {}
        eventRecord['header'] = {}

        uuid_id = str(uuid.uuid4()) + "_"

        eventMetadataDetailRecord = {}
        eventMetadataDetailRecord['micro_batch_uuid'] = str(uuid.uuid1())
        eventMetadataDetailRecord['uuid'] = 'linkedinlearning' + uuid_id + "_" + str(current_milli_time())
        eventMetadataDetailRecord['type'] = 'linkedinlearning'
        eventMetadataDetailRecord['producer_timestamp'] = current_milli_time()
        eventMetadataDetailRecord['consumer_timestamp'] = 0
        eventMetadataDetailRecord['target_timestamp'] = 0
        eventMetadataDetailRecord['source'] = 'linkedinlearning'
        eventMetadataDetailRecord['version_id'] = 1
        eventMetadataDetailRecord['application_id'] = 'linkedinlearning'

        eventRecord["header"]["event_metadata"] = eventMetadataDetailRecord

        eventRecord['body'] = row.to_dict()

        print("----------------")
        print(row.to_json())

        try:
            avroProducer.produce(topic="linkedinlearning", value=eventRecord, value_schema=sschema_value)
            avroProducer.poll(0)
        except BufferError as e:
            print("Buffer full, waiting for free space on the queue")
            print(e)
            avroProducer.poll(10)
            #  putting the poll() first to block until there is queue space available.
            #  This blocks for 10 seconds, but you should block for as long as makes sense for your application
            #  (preferably longer) since message delivery can take some time if there are temporary errors
            #  on the broker (e.g., leader failover).

            avroProducer.produce(topic="linkedinlearning", value=eventRecord, value_schema=sschema_value)
            # sleep(0.01)

        print("sent..")

        counter = counter + 1
        if counter % 100000:
            #     avroProducer.flush()
            print("counter::")
            print(counter)

    avroProducer.flush()

    print("Finished writing to Kafka")

    return "Finished"


if __name__ == "__main__":
    main()
