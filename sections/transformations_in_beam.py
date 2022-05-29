import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import DebugOptions


logger = logging.getLogger('transformations_in_beam')
logger.setLevel(logging.DEBUG)


def write_to_text_examples():
    p2 = beam.pipeline.Pipeline()

    lines = (
        p2
        | beam.Create(['Element ' + str(i) for i in range(10)])
        | beam.io.WriteToText('data/create_lines.txt')
    )
    p2.run()

    p3 = beam.pipeline.Pipeline()
    lines_with_tuple = (
        p3
        | beam.Create([('Element ' + str(i), 'tuple ' + str(i)) for i in range(10)])
        | beam.io.WriteToText('data/created_tuples.txt')
    )
    p3.run()

    p4 = beam.pipeline.Pipeline()
    lines_with_tuple = (
        p4
        | beam.Create([{'element': 'Element ' + str(i), 'tuple': 'tuple ' + str(i)} for i in range(10)])
        | beam.io.WriteToText('data/created_dicts.txt')
    )
    p4.run()


def map_flatmap_filter_part_1():
    # creating the pipeline
    p1 = beam.pipeline.Pipeline()
    attendance_count = (
        p1
        | beam.io.ReadFromText('data/dept_data.txt')
        | beam.Map(lambda record: record.split(','))
        | beam.io.WriteToText('data/mapoutput.txt')
    )
    # run the pipeline
    p1.run()

    p2 = beam.pipeline.Pipeline()
    attendance_count = (
        p2
        | beam.io.ReadFromText('data/dept_data.txt')
        | beam.FlatMap(lambda record: record.split(','))
        | beam.io.WriteToText('data/flatmapoutput.txt')
    )
    # run the pipeline
    p2.run()

    p3 = beam.pipeline.Pipeline()
    attendance_count = (
        p3
        # create initial p collection
        | beam.io.ReadFromText('data/dept_data.txt')
        | beam.Map(lambda record: record.split(','))
        | beam.Filter(lambda record: record[4] == '2-01-2019')
        | beam.io.WriteToText('data/filteroutput.txt')
        # | beam.Filter(lambda record: record[3] == 'Accounts')
        # | beam.Map(lambda record: (record[1], 1))
        # | beam.CombinePerKey(sum)
        # | beam.Map(lambda employee_count: str(employee_count))
        # # write p collection to external source
        # | beam.io.WriteToText('data/attendance_output.txt')
    )
    # run the pipeline
    p3.run()

    p4 = beam.pipeline.Pipeline()
    attendance_count = (
        p4
        # create initial p collection
        | beam.io.ReadFromText('data/dept_data.txt')
        | beam.Map(lambda record: record.split(','))
        | beam.Filter(lambda record: record[3] == 'Accounts')
        | beam.Map(lambda record: (record[1], 1))
        | beam.CombinePerKey(sum)
        | beam.io.WriteToText('data/finalfilteroutput.txt')
    )
    # run the pipeline
    p4.run()


def map_flatmap_and_filter_part_2():
    p1 = beam.pipeline.Pipeline()
    attendance_count = (
        p1
        # create initial p collection
        | beam.io.ReadFromText('data/dept_data.txt')
        | beam.Map(lambda record: record.split(','))
        | beam.Filter(lambda record: record[3] == 'Accounts')
        | beam.Map(lambda record: (record[1], 1))
        | beam.CombinePerKey(sum)
        | beam.io.WriteToText('data/finalfilteroutput.txt')
    )
    # run the pipeline
    p1.run()


def branching_pipelines():
    p1 = beam.pipeline.Pipeline(options=DebugOptions())
    branches_base = (
        p1
        # create initial p collection
        | beam.io.ReadFromText('data/dept_data.txt')
        | beam.Map(lambda record: record.split(','))
    )
    accounts_filepath = 'data/branched-account-attendance.txt'
    accounts_count = (
        branches_base
        | 'get Accounts dept employees' >> beam.Filter(lambda record: record[3] == 'Accounts')
        | 'pair each Accounts employee with 1' >> beam.Map(lambda record: ('Accounts - ' + record[1], 1))
        | 'group and sum Accounts' >> beam.CombinePerKey(sum)
    )
    hr_filepath = 'data/branched-hr-attendance.txt'
    hr_count = (
        branches_base
        | 'get HR dept employees' >> beam.Filter(lambda record: record[3] == 'HR')
        | 'pair each HR employeer with 1' >> beam.Map(lambda record: ('HR - ' + record[1], 1))
        | 'group and sum HR' >> beam.CombinePerKey(sum)
    )
    flattened_filepath = 'data/branched-flattened-attendance.txt'
    merged = (
        (accounts_count, hr_count)
        | 'flatten accounts and hr into 1 node' >> beam.Flatten()
        | f'write combined output to {flattened_filepath}' >> beam.io.WriteToText(f'{flattened_filepath}')
    )
    # run the pipeline
    p1.run()


def assignment_1():
    p1 = beam.pipeline.Pipeline()
    input_filpath = 'assignments/assignment_1/data.txt'
    output_filepath = 'assignments/assignment_1/output.txt'
    word_count = (
        p1
        | f'Read {input_filpath}' >> beam.io.ReadFromText(f'{input_filpath}')
        | 'Extract words' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        | 'Count number of occurrences' >> beam.combiners.Count.PerElement()
        | 'Make tuple of word and count' >> beam.MapTuple(lambda word, count: f'{word}: {count}')
        | f'Write to {output_filepath}' >> beam.io.WriteToText(f'{output_filepath}')
    )
    # run the pipeline
    p1.run()


class SplitRow(beam.DoFn):

    def process(self, element, *args, **kwargs):
        return [element.split(',')]


class FilterColumn(beam.DoFn):

    def process(self, element, *args, **kwargs):
        if element[3] == 'Accounts':
            return [element]
        else:
            return None


class Counting(beam.DoFn):

    def process(self, element, *args, **kwargs):
        (key, values) = element
        return [(key, sum(values))]


def pardo():
    p1 = beam.pipeline.Pipeline()
    output_filepath = 'data/pardo_output.txt'
    attendance_count = (
        p1
        # create initial p collection
        | beam.io.ReadFromText('data/dept_data.txt')
        | beam.ParDo(SplitRow())
        | beam.ParDo(FilterColumn())
        | beam.Map(lambda record: (record[1], 1))
        | beam.GroupByKey()
        | beam.ParDo(Counting())
        | beam.io.WriteToText(f'{output_filepath}')
    )
    # run the pipeline
    p1.run()


def combiner():
    # only works for both associative and commutative in nature
    p1 = beam.pipeline.Pipeline()
    output_filepath = 'data/combiner_output.txt'
    attendance_count = (
            p1
            # create initial p collection
            | beam.io.ReadFromText('data/dept_data.txt')
            | beam.ParDo(SplitRow())
            | beam.ParDo(FilterColumn())
            | beam.Map(lambda record: (record[1], 1))
            | beam.GroupByKey()
            | beam.ParDo(Counting())
            | beam.io.WriteToText(f'{output_filepath}')
    )
    # run the pipeline
    p1.run()


class AverageFn(beam.CombineFn):

    def create_accumulator(self, *args, **kwargs):
        return 0.0, 0

    def add_input(self, mutable_accumulator, element, *args, **kwargs):
        sum, count = mutable_accumulator
        return sum + element, count + 1

    def merge_accumulators(self, accumulators, *args, **kwargs):
        total_sums, total_counts = zip(*accumulators)
        return sum(total_sums), sum(total_counts)

    def extract_output(self, accumulator, *args, **kwargs):
        sum, count = accumulator
        return sum / count if count else float('NaN')


def custom_accumulator():
    # only works for both associative and commutative in nature
    p1 = beam.pipeline.Pipeline()
    output_filepath = 'data/accumulator_output.txt'
    attendance_count = (
            p1
            # create initial p collection
            | beam.Create([15, 71, 84, 39, 90, 27, 18, 5, 68, 44]) # avg is 46.1
            | beam.CombineGlobally(AverageFn())
            | beam.io.WriteToText(f'{output_filepath}')
    )
    # run the pipeline
    p1.run()


def filter_on_count(element):
    name, count = element
    if count > 30:
        return element


def format_output(element):
    name, count = element
    return ", ".join((name, str(count), 'Regular employee'))


class MyTransform(beam.PTransform):

    def expand(self, input_collection):
        a = (
            input_collection
            | 'group and sum Accounts' >> beam.CombinePerKey(sum)
            | 'count filter accounts' >> beam.Filter(filter_on_count)
            | 'format accounts' >> beam.Map(format_output)
        )
        return a


def composite_transforms():
    p1 = beam.pipeline.Pipeline(options=DebugOptions())
    branches_base = (
        p1
        # create initial p collection
        | beam.io.ReadFromText('data/dept_data.txt')
        | beam.Map(lambda record: record.split(','))
    )
    accounts_filepath = 'data/composite-transform-attendance.txt'
    accounts_count = (
        branches_base
        | 'get Accounts dept employees' >> beam.Filter(lambda record: record[3] == 'Accounts')
        | 'pair each Accounts employee with 1' >> beam.Map(lambda record: ('Accounts - ' + record[1], 1))
        | 'composite transform for accounts' >> MyTransform()
        | f'write combined output to {accounts_filepath}' >> beam.io.WriteToText(f'{accounts_filepath}')
    )
    # run the pipeline
    p1.run()


def parse_data(element):
    this_tuple = element.split(',')
    return this_tuple[0], this_tuple[1:]


def cogroup_by_key():
    p1 = beam.Pipeline()

    dep_rows = (
        p1
        | 'reading dept_data' >> beam.io.ReadFromText('data/dept_data.txt')
        | 'pair each employee with key' >> beam.Map(parse_data)
    )

    loc_rows = (
        p1
        | 'reading loc_data' >> beam.io.ReadFromText('data/location.txt')
        | 'pair each location with key' >> beam.Map(parse_data)
    )

    cogroupby_filepath = 'data/cogroupby_results.txt'
    results = (
        {'dep_data': dep_rows, 'loc_data': loc_rows}
        | beam.CoGroupByKey()
        | 'write cogroupby output' >> beam.io.WriteToText(f'{cogroupby_filepath}')
    )
    p1.run()


def transformations_in_beam():
    print("write to text examples...")
    write_to_text_examples()
    print("map flatmap and filter part 1 examples...")
    map_flatmap_filter_part_1()
    print("map flatmap and filter part 2 examples...")
    map_flatmap_and_filter_part_2()
    print("branched example")
    branching_pipelines()

    print("doing assignment 1")
    assignment_1()

    print("doing pardo")
    pardo()

    print("doing combiner")
    combiner()

    print("custom accumulator")
    custom_accumulator()

    print("composite transforms")
    composite_transforms()

    print("cogroupby for joins")
    cogroup_by_key()
