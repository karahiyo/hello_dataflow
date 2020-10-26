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
PROJECT=PROJECT_ID
BUCKET=GCS_BUCKET
REGION=DATAFLOW_REGION
```

![image](https://user-images.githubusercontent.com/1106556/97177107-4f420180-17d9-11eb-8648-8dfe75a7ee26.png)

## Example: schema test

```sh
python src/bigquery_schema.py --output <PROJECT_ID:DATASET_NAME.TABLE_NAME>
```

