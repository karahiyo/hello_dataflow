import argparse
import logging

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import SetupOptions, PipelineOptions, StandardOptions


class WordLengthMonitorDoFn(beam.DoFn):
    def __init__(self):
        self.words_length_monitor = Metrics.distribution(self.__class__, 'length')

    def process(self, element: str, *args, **kwargs):
        self.words_length_monitor.update(len(element))
        return element


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        messages = (p
                    | beam.io.ReadFromPubSub(topic=known_args.input_topic).with_output_types(bytes)
                    | 'Decode' >> beam.Map(lambda x: x.decode('utf-8')))

        output = (
                messages
                | '_Monitoring' >> beam.ParDo(WordLengthMonitorDoFn())
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
