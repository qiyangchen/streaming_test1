#!/usr/bin/env python
import apache_beam as beam
import re
import sys

def my_grep(line, term):
    if re.match( r'^' + re.escape(term), line):
        yield line

# PROJECT='kf'
# BUCKET='coach'

# def run():
#    argv = [
#       '--project={0}'.format(kf),
#       '--job_name=examplejob2',
#       '--save_main_session',
#       '--staging_location=gs://{0}/staging/'.format(coach),
#       '--temp_location=gs://{0}/staging/'.format(coach),
#       '--runner=DataflowRunner'
#    ]
        

if __name__ == '__main__':
    p = beam.Pipeline(argv=sys.argv);
    input = 'C:\\Users\\qiyang.chen\\Documents\\SpiDIY\\GCP\\Streaming\\Bookmark.json'
    output = 'tmp/output.json'
    searchTerm = 'IN_PROGRESS'
    (p
        | 'Getfile' >> beam.io.ReadFromText(input) #input file
        # | beam. FlatMap (lambda line: count_number(line)) # perform processing
        # | beam. Bucket.by(SlidingWindows.of(24, HOURS)) #streaming window
        # | beam. ParDo.of(new Filter1())
        # | beam. newGroup1()
        # | beam. Pardo.of(new Filter2())
        # | beam. newTransform1()
        | 'Grep' >> beam.FlatMap(lambda line: my_grep(line, searchTerm) )
        | 'Write' >> beam.io.WriteToText(output) # write output
    )
    # lines = p | ...
    # sizes = lines | 'Length' >> beam.Map(lambda line: len(line))
    p.run() # run the pipeline