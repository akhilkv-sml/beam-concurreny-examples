import asyncio
import concurrent
import json
import logging
import argparse
import io
import math
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed

import apache_beam as beam

import requests
import asyncio
import aiohttp
import time
from apache_beam.io import ReadFromText


from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

import csv
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
        self.api_key = api_key
      

    def start_bundle(self):
        self.buffer = []


    def process(self, element):
        self.buffer.append(element)
        if len(self.buffer) == 3000:
            yield from self._flush()
            self.buffer = []


    def finish_bundle(self):
        if self.buffer:
            for element in self._flush():
                yield WindowedValue(element, MIN_TIMESTAMP,[GlobalWindow])


    def _flush(self):
        # use aiohttp
        results=[]

        async def fetch(session,element):
           url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(element['address'])
           url = url + "&key={}".format(self.api_key)
           start = time.time()

           async with session.get(url) as response:
               res=await response.json()
               time_taken = time.time() - start
               element['time_taken'] = time_taken

           while(res['status'] =='OVER_QUERY_LIMIT'):
               async with session.get(url) as response:
                   res = await response.json()
                   time_taken = time.time() - start
                   element['time_taken'] = time_taken

           element['geocode_response'] = str(res)
           return element

        async def by_aiohttp_concurrency(elements):
           # use aiohttp

            async with aiohttp.ClientSession() as session:
               tasks = []
               for element in elements:
                   tasks.append(asyncio.create_task(fetch(session,element)))

               original_result = await asyncio.gather(*tasks)
               for res in original_result:
                    results.append(res)

        asyncio.run(by_aiohttp_concurrency(self.buffer))
        # asyncio.run(by_aiohttp_concurrency(errors))
        for i in results:
           yield i








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

    headers = ['Agent_ID', 'address']

    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | ReadFromText(input_file,skip_header_lines=1)
        lines_list = lines | "to list" >> beam.Map(parse_csv) | "ID, address" >> beam.Map(lambda element: [element[0],element[3] + ", " + element[4] + ", " + element[5] + ", " + element[6] + ", " + element[7]])

        dict_val = lines_list | "to dict" >> beam.Map(lambda x: dict(zip(headers, x)))
        first_call = dict_val | "ext add" >> beam.ParDo(textapi_call(api_key))
        # second_call = first_call | "call findplace" >> beam.Map(lambda x: call_findplace(x,api_key))
        # dict_val = lines_list | "to dict" >> beam.Map(lambda x: dict(zip(headers2,x)))
        first_call | "Write" >> WriteToText(output_table)




if __name__ == '__main__':
  startjob = time.time()
  logging.getLogger().setLevel(logging.INFO)
  run()
  # logging.info("This is the job time:"+time.time() - startjob)
  print(time.time() - startjob)
