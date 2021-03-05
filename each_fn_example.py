import argparse
import logging
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CustomFn(beam.DoFn):
    def __init__(self, name):
        self.name = name

    def process(self, elem):
        print(self.name, elem)
        yield elem


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(argv=pipeline_args, options=pipeline_options) as p:
        inputs = (p
                | beam.Create([
                    {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
                    {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
                    {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
                    {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
                    {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'}
                    ]))

        grouped = (inputs
                | beam.Map(lambda v: (v.get("duration"), v)) 
                | beam.GroupByKey())

        output = (grouped
                | beam.Map(lambda x: print(x)))



if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
