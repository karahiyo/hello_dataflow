import argparse
import logging

import apache_beam as beam


def by_duration(plant, num_partitions):
  return durations.index(plant['duration'])

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    durations = ['annual', 'biennial', 'perennial']

    with beam.Pipeline(argv=pipeline_args) as pipeline:
        annuals, biennials, perennials = (
                pipeline
                | 'Gardening plants' >> beam.Create([
                    {'icon': '🍓', 'name': 'Strawberry', 'duration': 'perennial'},
                    {'icon': '🥕', 'name': 'Carrot', 'duration': 'biennial'},
                    {'icon': '🍆', 'name': 'Eggplant', 'duration': 'perennial'},
                    {'icon': '🍅', 'name': 'Tomato', 'duration': 'annual'},
                    {'icon': '🥔', 'name': 'Potato', 'duration': 'perennial'},
                    ])
                | 'Partition' >> beam.Partition(by_duration, len(durations))
                )

        annuals | 'Annuals' >> beam.Map(lambda x: print('annual: {}'.format(x)))
        biennials | 'Biennials' >> beam.Map(
              lambda x: print('biennial: {}'.format(x)))
        perennials | 'Perennials' >> beam.Map(
              lambda x: print('perennial: {}'.format(x)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
