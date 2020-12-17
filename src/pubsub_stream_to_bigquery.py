import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument(
            '--subscription',
            dest='subscription',
            help='Pub/Sub pull subscription',
            required=True,
            type=str
            )

    parser.add_argument(
            '--output_table',
            dest='tableId',
            help=(
                'Output BigQuery table for results specified as: '
                'PROJECT:DATASET.TABLE or DATASET.TABLE.'),
            required=True,
            type=str
            )

    known_args, pipeline_args = parser.parse_known_args(argv)

    with beam.Pipeline(argv=pipeline_args) as p:
        (p 
                | "Read input from PubSub" >>
                beam.io.gcp.pubsub.ReadFromPubSub(subscription=options.subscription) 
                | "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
                | "Parse json" >> beam.Map(json.loads)
                | "File load to BigQuery" >> beam.io.gcp.bigquery.WriteToBigQuery(
                    table=known_args.output_table,
                    method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                    triggering_frequency=10,
                    write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_NEVER)
                )


if __name__ == '__main__':
    print("Streaming Pub/Sub messages to BigQuery...")
    logging.getLogger().setLevel(logging.DEBUG)
    run()
