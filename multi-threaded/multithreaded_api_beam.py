import asyncio
import json
import logging
import argparse
import io
import math
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed

import apache_beam as beam

import requests
import aiohttp
import time
from apache_beam.io import ReadFromText

from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.io import to_csv
from apache_beam.dataframe.convert import to_pcollection

from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import csv
from collections import namedtuple
import re

from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.windowed_value import WindowedValue


def findplace_url (address, api_key):
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json?inputtype=textquery&input="
    url += "place at "
    url += address
    url += "&key={}".format(api_key)
    return url




class textapi_call(beam.DoFn):
    def __init__(self, api_key):
        # self.buffer = []
        self.api_key = api_key
        # self.session = requests.session()

    def start_bundle(self):
        self.buffer = []


    def process(self, element):
        self.buffer.append(element)
        if len(self.buffer) ==3000:
            yield from self._flush()
            self.buffer = []


    def finish_bundle(self):
        if self.buffer:
            for element in self._flush():
                yield WindowedValue(element, MIN_TIMESTAMP,[GlobalWindow])

    def _flush(self):

        ses = requests.session()

        def _request(element):
            address = element[3] + ", " + element[4] + ", " + element[5] + ", " + element[6] + ", " + element[7]
            geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
            if self.api_key is not None:
                geocode_url = geocode_url + "&key={}".format(self.api_key)
            start = time.time()
            results = ses.get(geocode_url)
            res = results.json()
            status = res['status']
            while(status=='OVER_QUERY_LIMIT'):
                results = ses.get(geocode_url)
                res = results.json()
                status = res['status']
            time_taken = time.time() - start
            # res = results.json()
            return [element[0], address, str(res), time_taken]


        with ThreadPoolExecutor(max_workers=32) as executor:
            future_to_index = {executor.submit(_request, i): i for i in self.buffer}
            for future in as_completed(future_to_index):
                yield future.result()




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



    api_key = "AIzaSyDjeLapyd9_9a8PIc71hVlzqXmMDtyPJ4w"



    headers = ['Agent_ID', 'address','Textsearch_res','time_taken']

    # session = requests.session()
    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | ReadFromText(input_file,skip_header_lines=1)
        lines_list = lines | "to list" >> beam.Map(parse_csv)

        first_call = lines_list | "API calls transform" >> beam.ParDo(textapi_call(api_key))
        dict_val = first_call | "to dict" >> beam.Map(lambda x: dict(zip(headers,x)))
        dict_val | "Write" >> WriteToText(output_table)




if __name__ == '__main__':
  startjob = time.time()
  logging.getLogger().setLevel(logging.INFO)
  run()
  # logging.info("This is the job time:"+time.time() - startjob)
  print(time.time() - startjob)



