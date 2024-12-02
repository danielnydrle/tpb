import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import (FileSource, StreamFormat, FileSink, OutputFileConfig,
                                           RollingPolicy)

def letter_count(input_path, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    fs = FileSource \
        .for_record_stream_format(StreamFormat.text_line_format(), input_path) \
        .monitor_continuously(Duration.of_seconds(1)) \
        .build()

    if input_path is not None:
        ds = env.from_source(
            source=fs,
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source"
        )
    else:
        print("Executing word count example with default input data")
        word_count_data = ["To be, or not to be, that is the question"]
        ds = env.from_collection(word_count_data)

    def first_letter(line):
        for word in line.lower().split():
            if word[0].isalpha():
                yield word[0]

    ds = ds.flat_map(first_letter) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))

    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("letter_stream")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()

    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        help='Output file to write results to.')

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    letter_count(known_args.input, known_args.output)