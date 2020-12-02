import argparse
import logging
import re

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CategorizeFn(beam.DoFn):
  TAGS = ["A", "B", "C"]

  def process(self, element):
      if element.startswith("A"):
          yield pvalue.TaggedOutput("A", element)
      elif element.startswith("B"):
          yield pvalue.TaggedOutput("B", element)
      elif element.startswith("C"):
          yield pvalue.TaggedOutput("C", element)
      else:
          yield None

      yield element


class AddSuffix(beam.PTransform):
    def __init__(self, label=None):
        super(AddSuffix, self).__init__(label=label)

    def expand(self, pcoll):
        return pcoll | f'Print {self.label}' >> beam.Map(lambda x: f"{x}.xxx")



def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  with beam.Pipeline(argv=pipeline_args, options=pipeline_options) as p:

    lines = (p | beam.Create([
        "Aaa", "Bbb" , "Ccc", "no match", "Abcc"
        ]))

    tagged_result = (
        lines
        | beam.ParDo(CategorizeFn()).with_outputs())

    for t in CategorizeFn.TAGS:
        tagged_result[t] | f"add suffix {t}" >> AddSuffix() | f'Print {t}' >> beam.Map(lambda x: print(x))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
