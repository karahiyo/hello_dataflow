import argparse
import logging
import re

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import SetupOptions, PipelineOptions, StandardOptions


class WordLengthMonitorDoFn(beam.DoFn):
    def __init__(self):
        # TODO(BEAM-6158): Revert the workaround once we can pickle super() on py3.
        # super(WordLengthMonitorDoFn, self).__init__()
        beam.DoFn.__init__(self)
        self.short_words_counter = Metrics.counter(self.__class__, 'short_words_count')
        self.long_words_counter = Metrics.counter(self.__class__, 'long_words_count')
        self.words_length_monitor = Metrics.distribution(self.__class__, 'length')

    def process(self, element: str, *args, **kwargs):
        text_line = element.strip()
        if not text_line:
            yield

        words = re.findall(r'[\w\']+', text_line, re.UNICODE)
        for w in words:
            self.words_length_monitor.update(len(w))
            if len(w) <= 3:
                self.short_words_counter.inc()
            else:
                self.long_words_counter.inc()
        return words


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

        (messages
         | '_Monitoring' >> beam.ParDo(WordLengthMonitorDoFn())
         | 'Print' >> beam.Map(lambda x: logging.info(x)))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
