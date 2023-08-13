# Dataflow Flex template job to upload xml data from GCS to BQ
# Author: Rajathithan Rajasekar 
# Version : 1.0
# Date: 08/05/2023

import sys
import json
import logging
import argparse
import xmltodict
import apache_beam as beam
from google.cloud import storage  
from tableschema import table_schema
from apache_beam.options.pipeline_options import PipelineOptions


# setup python logging
logging.basicConfig(format='[%(levelname)-8s] [%(asctime)s] [%(module)-35s][%(lineno)04d] : %(message)s', level=logging.INFO)
logger = logging


def convertXMLtoJSON(gcsfile):
  print("Inside the convertXMLtoJSON function with "+ gcsfile )  
  storage_client = storage.Client()
  bucket = storage_client.get_bucket('xmltestpoc')
  # get bucket data as blob
  blob = bucket.get_blob(gcsfile)
  with blob.open("r") as f:
      data_dict = xmltodict.parse(f.read())	
      json_data = json.dumps(data_dict)
      json_without_slash = json.loads(json_data)
  logger.info(json_without_slash)
  return json_without_slash


def run():
    parser = argparse.ArgumentParser(description="Uploading XML data to BigQuery from GCS")
    parser.add_argument('--input', 
                        type=str,
                        default='xml/test1xml.xml',
                        help='Input GCS Folder Path')

    parser.add_argument('--output',
                        type=str,
                        default='xmldata.table0001',
                        help='BQ-Dataset.BQ-table')

    known_args, beam_args = parser.parse_known_args(sys.argv)

    options = PipelineOptions(beam_args, save_main_session=True, streaming=False)

    with beam.Pipeline(options=options) as p:
        # Convert XML to JSON and Push it to BQ
        logger.info("Converting XML data to JSON and Pushing it to BQ")
        uploadtoBQ = (p
        | 'getfilename' >> beam.Create([known_args.input])
        | 'parse' >> beam.Map(convertXMLtoJSON)
        | 'tobq' >> beam.io.WriteToBigQuery(known_args.output,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

if __name__ == '__main__':
    run()


        

