# Dataflow Job to upload xml data from GCS to BQ
# Author: Rajathithan Rajasekar 
# Version : 1.0
# Date: 07/30/2023

import sys
import json
import logging
import argparse
import xmltodict
import apache_beam as beam
from google.cloud import storage  
from tableschema import table_schema
from apache_beam.options.pipeline_options import SetupOptions
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

    parser.add_argument('--project', 
                        dest='project', 
                        required=False, 
                        default='mercurial-smile-386805',
                        help='Project name')

    parser.add_argument('--input', 
                        type=str,
                        default='xml/test1xml.xml',
                        help='Input GCS Folder Path')

    parser.add_argument('--output',
                        type=str,
                        required=False,
                        default='xmldata.table0001',
                        help='BQ-Dataset.BQ-table')

    parser.add_argument('--region', 
                        dest='region', 
                        required=False, 
                        default='us-central1',
                        help='Region')

    parser.add_argument('--staging_location', 
                        dest='staging_location', 
                        required=False,
                        default='gs://xmltestpoc/staging',
                        help='staging gcs location')

    parser.add_argument('--temp_location', 
                        dest='temp_location', 
                        required=False,
                        default='gs://xmltestpoc/temp',
                        help='temp gcs location')

    parser.add_argument('--jobname', 
                        dest='job_name', 
                        required=False, 
                        default='gcstobqjob',
                        help='jobName')

    parser.add_argument('--runner', 
                        dest='runner', 
                        required=False, 
                        default='DataflowRunner',
                        help='Runner Name')
    
    parser.add_argument('--subnetwork', 
                        dest='subnetwork', 
                        required=False, 
                        default='https://www.googleapis.com/compute/v1/projects/mercurial-smile-386805/regions/us-central1/subnetworks/dataflow-network',
                        help='Subnetwork Name')

    parser.add_argument('--experiments', 
                default="use_runner_v2",
                required=False,
                help='For Batch python pipeline')

    parser.add_argument('--sdk_container_image', 
                default="us-central1-docker.pkg.dev/mercurial-smile-386805/docker-worker/dataflowworker:v1",
                required=False,
                help='Dataflow worker container image')

    parser.add_argument('--sdk_location', 
                default="container",
                required=False,
                help='Dataflow worker container image')


    known_args, _ = parser.parse_known_args(sys.argv)

    pipeline_options = {
    'input' : known_args.input,
    'output': known_args.output,
    'project': known_args.project,
    'staging_location': known_args.staging_location,
    'runner': known_args.runner,
    'region': known_args.region,
    'temp_location': known_args.temp_location,
    'job_name': known_args.job_name,
    'subnetwork': known_args.subnetwork,
    'experiments':known_args.experiments,
    'sdk_container_image': known_args.sdk_container_image,
    'sdk_location': known_args.sdk_location,
    }

    options = PipelineOptions.from_dictionary(pipeline_options)
    options.view_as(SetupOptions).save_main_session = True

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


        

