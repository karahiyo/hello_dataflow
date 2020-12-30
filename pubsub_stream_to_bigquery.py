import argparse
import json
import logging
import apache_beam as beam
import apache_beam.io.gcp.bigquery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.trigger import AccumulationMode, AfterProcessingTime
from apache_beam.transforms.window import FixedWindows


def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--subscription',
            help='Pub/Sub pull subscription',
            required=True,
            type=str
            )

    parser.add_argument(
            '--output_table',
            help=(
                'Output BigQuery table for results specified as: '
                'PROJECT:DATASET.TABLE or DATASET.TABLE.'),
            required=True,
            type=str
            )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    from apache_beam.io.gcp.internal.clients import bigquery
    table_schema = bigquery.TableSchema()
    msg_schema = bigquery.TableFieldSchema()
    msg_schema.name = "message"
    msg_schema.type = "string"
    msg_schema.mode = "nullable"
    table_schema.fields.append(msg_schema)

    with beam.Pipeline(options=pipeline_options) as p:

        (p
         | "Read input from PubSub" >>
         beam.io.gcp.pubsub.ReadFromPubSub(subscription=known_args.subscription)
         | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
         | "Parse json" >> beam.Map(json.loads)
         | "File load to BigQuery" >> beam.io.gcp.bigquery.WriteToBigQuery(
                    table=known_args.output_table,
                    method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                    schema=table_schema,
                    triggering_frequency=10,
                    write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
         )


if __name__ == '__main__':
    print("Streaming Pub/Sub messages to BigQuery...")
    logging.getLogger().setLevel(logging.INFO)
    run()
