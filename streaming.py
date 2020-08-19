from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def __init__(self):
    self.empty_line_counter = Metrics.counter('main', 'empty_lines')
    self.word_length_counter = Metrics.counter('main', 'word_lengths')
    self.word_counter = Metrics.counter('main', 'total_words')
    self.word_lengths_dist = Metrics.distribution('main', 'word_len_dist')

  def process(self, element):
    text_line = element.properties.get('content', '')
    if not text_line:
      self.empty_line_counter.inc()
      return None

    words = re.findall(r'[A-Za-z\']+', text_line)
    for w in words:
      self.word_length_counter.inc(len(w))
      self.word_lengths_dist.update(len(w))
      self.word_counter.inc()
    return words

def run(argv=None, save_main_session=True):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic',
      required=True,
      help=(
          'Output PubSub topic of the form '
          '"projects/<PROJECT>/topics/<TOPIC>".'))
  parser.add_argument(
      '--input_topic',
      required=True,
      help=(
          'Input PubSub subscription of the form '
          '"projects/<PROJECT>/topics/<TOPIC>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  pipeline_options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=pipeline_options) as p:

    messages = (
        p
        | beam.io.ReadFromPubSub(topic=known_args.input_topic).
        with_output_types(bytes))

    lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

    # Count the occurrences of each word.
    def count_ones(word_ones):
      (word, ones) = word_ones
      return (word, sum(ones))

    counts = (
        lines
        | 'split' >>
        (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
        | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
        | beam.WindowInto(window.FixedWindows(15, 0))
        | 'group' >> beam.GroupByKey()
        | 'count' >> beam.Map(count_ones))

    # Format the counts into a PCollection of strings.
    def format_result(word_count):
      (word, count) = word_count
      return '%s: %d' % (word, count)

    output = (
        counts
        | 'format' >> beam.Map(format_result)
        | 'encode' >>
        beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes))

    # Write to PubSub.
    output | beam.io.WriteToPubSub(known_args.output_topic)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
