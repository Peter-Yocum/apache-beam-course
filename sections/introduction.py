
import apache_beam as beam


# section 1 - introduction
def introduction():
    print("Counting attendance...")
    # creating the pipeline
    p1 = beam.pipeline.Pipeline()
    attendance_count = (
        p1
        # create initial p collection
        | beam.io.ReadFromText('data/dept_data.txt')
        # apply p transforms using the pipe operating instead of apply()
        | beam.Map(lambda record: record.split(','))
        | beam.Filter(lambda record: record[3] == 'Accounts')
        | beam.Map(lambda record: (record[1], 1))
        | beam.CombinePerKey(sum)
        | beam.Map(lambda employee_count: str(employee_count))
        # write p collection to external source
        | beam.io.WriteToText('data/attendance_output.txt')
    )
    # run the pipeline
    p1.run()
