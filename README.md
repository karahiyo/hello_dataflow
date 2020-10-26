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
$ python src/bigquery_schema.py --output <PROJECT_ID:DATASET_NAME.TABLE_NAME>
```

using DataflowRunner

```sh
$ python src/bigquery_schema.py \
  --region $REGION \
  --output <PROJECT_ID:DATASET_NAME.TABLE_NAME> \
  --runner DataflowRunner \
  --project $PROJECT \
```
