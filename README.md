# hello_dataflow
hello dataflow - https://cloud.google.com/dataflow

## Quick Start with Python

https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python

```
$ python3 -m venv venv
$ source ./venv/bin/activate # or source ./venv/bin/activate.fish
$ pip install apache-beam[gcp]
```

set GOOGLE_APPLICATION_CREDENTIALS environment variable

```
export GOOGLE_APPLICATION_CREDENTIALS=...
```

<img src="https://user-images.githubusercontent.com/1106556/97177107-4f420180-17d9-11eb-8648-8dfe75a7ee26.png" alt="Dataflow pipeline example" title="Dataflow pipeline example" height="600">

## Example: schema test

using DirectRunner
```sh
$ python bigquery_schema.py --output <PROJECT_ID:DATASET_NAME.TABLE_NAME>
```

using DataflowRunner

```sh
$ python bigquery_schema.py \
  --region $REGION \
  --output <PROJECT_ID:DATASET_NAME.TABLE_NAME> \
  --runner DataflowRunner \
  --project $PROJECT
```

## Example: streaming wordcount

```
$ python streaming_wordcount.py \
  --project $PROJECT \
  --region $REGION \
  --runner DataflowRunner \
  --input_topic projects/<project>/topics/<input_topic> \
  --output_topic projects/<project>/topics/<output_topic> \
  --setup_file ./setup.py
```

## Example: tagged output

![image](https://user-images.githubusercontent.com/1106556/100439053-59139900-30e6-11eb-95a3-8e2c076499cd.png)

```sh
$ python tagged_output.py \
  --output $GCS_OUTPUT_PATH \
  --project $PROJECT \
  --runner DataflowRunner \
  --temp_location $GCS_TEMP_LOCATION
```

## Example: pubsub_stream_to_bigquery.py

```
$ python pubsub_stream_to_bigquery.py \
  --subscription "projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}" \
  --output_table {PROJECT_ID}:{DATASET_ID}.{TABLE_ID} \
  --temp_location gs://{GCS_LOCATION}/ \
  --runner DataflowRunner \
  --project {PROJECT_ID} \
  --experiments=disable_runner_v2 \
  --experiments=disable_streaming_engine \
  --experiments=allow_non_updatable_job
```


