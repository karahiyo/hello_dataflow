import argparse
import logging
import os

import apache_beam as beam
from apache_beam.transforms.core import RestrictionProvider
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker


class ReadFilesProvider(RestrictionProvider):
  def __init__(self, split_size):
    super(ReadFilesProvider, self).__init__()
    self.split_size = split_size

  def initial_restriction(self, element):
    size = os.path.getsize(element)
    logging.warning(f"{element=}, {size=}")
    return OffsetRange(0, size)

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(restriction)

  def restriction_size(self, element, restriction):
    return restriction.size()

  def split(self, file_name, restriction):
    split_size = self.split_size
    i = restriction.start
    while i < restriction.stop - split_size:
      yield OffsetRange(i, i + split_size)
      i += split_size
    yield OffsetRange(i, restriction.stop)


class ReadFiles(beam.DoFn):
  def __init__(self, split_size, *unused_args, **unused_kwargs):
      super().__init__(*unused_args, **unused_kwargs)
      self.split_size = split_size

  def process(
      self,
      element,
      restriction_tracker=beam.DoFn.RestrictionParam(ReadFilesProvider(128)),
      *args,
      **kwargs):
    logging.info(f"{element=}, {restriction_tracker.current_restriction().start=}")
    file_name = element

    with open(file_name, 'rb') as file:
      pos = restriction_tracker.current_restriction().start
      if restriction_tracker.current_restriction().start > 0:
        file.seek(restriction_tracker.current_restriction().start - 1)
        line = file.readline()
        pos = pos - 1 + len(line)

      output_count = 0
      while restriction_tracker.try_claim(pos):
        line = file.readline()
        yield line.strip()

        output_count += 1
        pos += len(line)

  def initial_restriction(self, element):
    size = os.path.getsize(element)
    logging.warning(f"{element=}, {size=}")
    return OffsetRange(0, size)

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(restriction)

  def restriction_size(self, element, restriction):
    return restriction.size()

  def split(self, file_name, restriction):
    split_size = self.split_size
    i = restriction.start
    while i < restriction.stop - split_size:
      yield OffsetRange(i, i + split_size)
      i += split_size
    yield OffsetRange(i, restriction.stop)



def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    file_names = ['requirements.txt', 'README.md', 'bigquery_schema.py']
    with beam.Pipeline(argv=pipeline_args) as p:
        (p
        | 'Create1' >> beam.Create(file_names)
        | 'SDF' >> beam.ParDo(ReadFiles(split_size=128)))

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
