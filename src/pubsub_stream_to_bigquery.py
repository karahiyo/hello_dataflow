import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# Parses messages from Pubsub client
class ParseTweets(beam.DoFn):
    def process(self, element):
        tweet = json.loads(element.decode('utf-8'))
        yield tweet

def run(argv=None):
    class MyOptions(PipelineOptions):
        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                '--subscription',
                dest='subscription',
                help='Pub/Sub pull subscription',
                required=True,
                type=str
            )

            parser.add_argument(
                '--projectId',
                dest='projectId',
                help='project ID',
                required=True,
                type=str
            )

            parser.add_argument(
                '--datasetId',
                dest='datasetId',
                help='bigquery dataset name',
                required=True,
                type=str
            )

            parser.add_argument(
                '--tableId',
                dest='tableId',
                help='bigquery table name',
                required=True,
                type=str
            )

    options = MyOptions(flags=argv)
    with beam.Pipeline(options=options) as p:
        (p 
                | "Read input from PubSub" >>
                beam.io.gcp.pubsub.ReadFromPubSub(subscription=options.subscription) 
                | "Parse tweet" >> beam.ParDo(ParseTweets())
                | "File load to BigQuery" >> beam.io.gcp.bigquery.WriteToBigQuery(
                    project=options.projectId,
                    dataset=options.datasetId,
                    table=options.tableId,
                    method="FILE_LOADS",
                    triggering_frequency=1,
                    write_disposition=beam.io.gcp.bigquery.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.gcp.bigquery.BigQueryDisposition.CREATE_NEVER)
                )


if __name__ == '__main__':
    print("Streaming Pub/Sub messages to BigQuery...")
    run()
