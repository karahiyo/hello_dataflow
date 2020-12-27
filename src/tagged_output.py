import argparse
import logging
import re

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class SplitLinesToWordsFn(beam.DoFn):
  """A transform to split a line of text into individual words.
  This transform will have 3 outputs:
    - main output: all words that are longer than 3 characters.
    - short words output: all other words.
    - character count output: Number of characters in each processed line.
  """

  # These tags will be used to tag the outputs of this DoFn.
  OUTPUT_TAG_SHORT_WORDS = 'tag_short_words'
  OUTPUT_TAG_CHARACTER_COUNT = 'tag_character_count'

  def __init__(self, *unused_args, **unused_kwargs):
    super(SplitLinesToWordsFn, self).__init__(*unused_args, **unused_kwargs)
    self.long_words_counter = Metrics.counter(self.__class__, 'long_words_count')
    self.short_words_counter = Metrics.counter(self.__class__, 'short_words_count')

  def process(self, element):
    """Receives a single element (a line) and produces words and character
    counts.
    Important things to note here:
      - For a single element you may produce multiple main outputs:
        words of a single line.
      - For that same input you may produce multiple outputs, potentially
        across multiple PCollections
      - Outputs may have different types (count) or may share the same type
        (words) as with the main output.
    Args:
      element: processing element.
    Yields:
      words as main output, short words as tagged output, line character count
      as tagged output.
    """
    # yield a count (integer) to the OUTPUT_TAG_CHARACTER_COUNT tagged
    # collection.
    yield pvalue.TaggedOutput(self.OUTPUT_TAG_CHARACTER_COUNT, len(element))

    words = re.findall(r'[A-Za-z\']+', element)
    for word in words:
      if len(word) <= 3:
        # yield word as an output to the OUTPUT_TAG_SHORT_WORDS tagged
        # collection.
        self.short_words_counter.inc()
        yield pvalue.TaggedOutput(self.OUTPUT_TAG_SHORT_WORDS, word)
      else:
        # yield word to add it to the main collection.
        self.long_words_counter.inc()
        yield word


class CountWords(beam.PTransform):
  """A transform to count the occurrences of each word.
  A PTransform that converts a PCollection containing words into a PCollection
  of "word: count" strings.
  """
  def expand(self, pcoll):
    def count_ones(word_ones):
      (word, ones) = word_ones
      return (word, sum(ones))

    def format_result(word_count):
      (word, count) = word_count
      return '%s: %s' % (word, count)

    return (
        pcoll
        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(count_ones)
        | 'format' >> beam.Map(format_result))


def run(argv=None, save_main_session=True):
  """Runs the workflow counting the long words and short words separately."""

  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      default='gs://dataflow-samples/shakespeare/kinglear.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      required=True,
      help='Output prefix for files to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | ReadFromText(known_args.input)

    # with_outputs allows accessing the explicitly tagged outputs of a DoFn.
    split_lines_result = (
        lines
        | beam.ParDo(SplitLinesToWordsFn()).with_outputs(
            SplitLinesToWordsFn.OUTPUT_TAG_SHORT_WORDS,
            SplitLinesToWordsFn.OUTPUT_TAG_CHARACTER_COUNT,
            main='words'))

    # split_lines_result is an object of type DoOutputsTuple. It supports
    # accessing result in alternative ways.
    words, _, _ = split_lines_result
    short_words = split_lines_result[SplitLinesToWordsFn.OUTPUT_TAG_SHORT_WORDS]
    character_count = split_lines_result.tag_character_count

    # pylint: disable=expression-not-assigned
    (
        character_count
        | 'pair_with_key' >> beam.Map(lambda x: ('chars_temp_key', x))
        | beam.GroupByKey()
        | 'count chars' >> beam.Map(lambda char_counts: sum(char_counts[1]))
        | 'write chars' >> WriteToText(known_args.output + '-chars'))

    # pylint: disable=expression-not-assigned
    (
        short_words
        | 'count short words' >> CountWords()
        |
        'write short words' >> WriteToText(known_args.output + '-short-words'))

    # pylint: disable=expression-not-assigned
    (
        words
        | 'count words' >> CountWords()
        | 'write words' >> WriteToText(known_args.output + '-words'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
