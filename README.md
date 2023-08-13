# gcstobq
Dataflow Job for uploading xml data from GCS to BQ.

* Pipeline-run.py -
To run the dataflow job from your local.

* Pipeline-template.py - 
To create custom dataflow job template in GCS bucket.

* JSONSCHEMA.JSON - 
Json schema Reference using during bq load.

* tableschema.py - 
Variable holding the json schema data.

* Dockerfile -
Dockerfile to create the dataflow worker container image

* flex / Dockerfile -
Dockerfile to create the dataflow flex template container image
  
* flex / metadata.json -
Metadata.json to validate the input / outpur parameters

* fles / pipeline-run-flex.py -
Pipeline file to run after the flex template launch

* flex / requirements.txt -
Flex template Python package dependencies

* cloudfunction / main.py -
2nd Gen Cloud Function code to send a REST API call to the dataflow flex template launch API

* cloudfunction / requirements.txt -
cloudfunction Python package dependencies
