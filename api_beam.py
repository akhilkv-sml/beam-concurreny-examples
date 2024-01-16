import json
import logging
import argparse
import io
import math
import multiprocessing as mp



import apache_beam as beam
import requests
import time
from apache_beam.io import ReadFromText

from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.io import to_csv
from apache_beam.dataframe.convert import to_pcollection
# from apache_beam.dataframe.convert import t

from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import csv
from collections import namedtuple
import re




def textsearch_url(address, api_key):
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json?query="
    url += address
    url += "&key={}".format(api_key)
    return url
def findplace_url (address, api_key):
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?inputtype=textquery&input="
    url += "place at "
    url += address
    url += "&key={}".format(api_key)
    return url

def api_resp(address, api_key,session):
    url = textsearch_url(address, api_key=api_key)
    # params = {"input":"Mexican food", "inputtype": "textquery", "fields":"name,geometry,place_id"}
    params = {}
    payload = {}
    headers = {}
    res = session.get(url, params=params)
    results = json.loads(res.content)
    return results

def api_resp2(address, api_key):
    url = findplace_url(address, api_key=api_key)
    params = { "inputtype": "textquery", "fields":"name,place_id,geometry,type,icon,permanently_closed,business_status"}
    # params = {}
    payload = {}
    headers = {}
    res = requests.get(url, params=params)
    results = json.loads(res.content)
    return results

class textapi_call(beam.DoFn):
    def __init__(self, api_key):
        self.api_key = api_key
        # self.session = requests.session()

    def setup(self):
        self.session = requests.session()

    def process(self, element):
        address = element[3] + ", " + element[4] + ", " + element[5] + ", " + element[6] + ", " + element[7]
        url = findplace_url(address, api_key=self.api_key)
        params = {"inputtype": "textquery",
                  "fields": "name,place_id,geometry,type,icon,permanently_closed,business_status"}
        start = time.time()
        res = self.session.get(url, params=params)
        results = json.loads(res.content)
        time_taken = time.time() - start

        return [[element[0], address, str(results), time_taken]]


    def teardown(self):
        self.session.close()


def call_textapi(element,api_key,session):

    address = element[3]+", "+element[4]+", "+element[5]+", "+element[6]+", "+element[7]

    start = time.time()
    res = api_resp(address,api_key,session)
    time_taken = time.time() - start

    return [element[0],address,str(res),time_taken]


def call_findplace(element, api_key,session):

    address = element[1]

    start = time.time()
    res = api_resp2(address,api_key,session)
    time_taken = time.time() - start

    element.extend([res,time_taken])
    return [element[0],element[1],element[3],time_taken,element[2],str(res)]


# Parsing CSV
def parse_csv(element):
    for line in csv.reader([element], delimiter=','):
        return line



def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()



    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
             "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--input_file",
        help="file location of input data "
             '"GCS path"',
    )


    known_args, pipeline_args = parser.parse_known_args(argv)
    input_file = known_args.input_file
    output_table = known_args.output_table
    # file_delimiter = None



    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session


    api_key = "AIzaSyCm7RVrEx1JuBrbwfXVtsaIpUW4hCIiCUU"

    headers = ['Agent_ID', 'address','Textsearch_res','time_taken']

    # session = requests.session()
    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | ReadFromText(input_file,skip_header_lines=1)
        lines_list = lines | "to list" >> beam.Map(parse_csv)

        first_call = lines_list | "ext add" >> beam.ParDo(textapi_call(api_key))
        # second_call = first_call | "call findplace" >> beam.Map(lambda x: call_findplace(x,api_key))
        dict_val = first_call | "to dict" >> beam.Map(lambda x: dict(zip(headers,x)))
        dict_val | "Write" >> WriteToText(output_table)




if __name__ == '__main__':
  startjob = time.time()
  logging.getLogger().setLevel(logging.INFO)
  run()
  # logging.info("This is the job time:"+time.time() - startjob)
  print(time.time() - startjob)

