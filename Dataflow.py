#!/usr/bin/env python
import apache_beam as beam
import re
import sys
from google.cloud import bigquery

# def my_grep(line, term):
#     if re.match( r'^' + re.escape(term), line):
#         yield line

PROJECT='streamingtest1-186611'
BUCKET='streamingtest1'
DATASET='test1'

def run():
    argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=examplejob2',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--runner=DataflowRunner'
    ]   
    p = beam.Pipeline(argv=argv)
    input = 'gs://{0}/Bookmark.json'.format(BUCKET)
    # dataset= '{0}:{1}'.format(PROJECT) .format(DATASET)
    # output = bigquery.Dataset(dataset)
    # searchTerm = 'IN_PROGRESS'
    (p
        | 'Getfile' >> beam.io.ReadFromText(input) #input file
        # # | beam. FlatMap (lambda line: count_number(line)) # perform processing
        # # | beam. Bucket.by(SlidingWindows.of(24, HOURS)) #streaming window
        # # | beam. ParDo.of(new Filter1())
        # # | beam. newGroup1()
        # # | beam. Pardo.of(new Filter2())
        # # | beam. newTransform1()
        # # | 'Grep' >> beam.FlatMap(lambda line: my_grep(line, searchTerm) )
        # | 'Write' >> beam.io.WritetoText(output) # write output
    )
    # lines = p | ...
    # sizes = lines | 'Length' >> beam.Map(lambda line: len(line))
    p.run()

if __name__ == '__main__':    
    run() # run the pipeline