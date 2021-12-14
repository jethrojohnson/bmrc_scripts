"""
===========================
Pipeline bmrc_check
===========================

The most basic of ruffus pipelines. 
Written to ensure tasks can be submitted via UGE without any issues.

"""
import sys
import os
from ruffus import *
from cgatcore import pipeline as P

# load options from the config file
PARAMS = P.get_parameters(
    ["%s/pipeline.yml" % os.path.splitext(__file__)[0],
     "pipeline.yml"])


###############################################################################
# Pipeline tasks
###############################################################################
@originate(['task_%i.sentinel' % n for n in range(1, 11)])
def create_dummy_files(outfile):
    '''A set of dummy files'''
    open(outfile, 'w').close()


@transform(create_dummy_files,
           regex("(.+).sentinel"),
           r"\1.complete")
def submit_jobs(infile, outfile):
    '''Submit jobs to cluster'''

    statement = ("sleep `shuf -i 10-120 -n 1`;"
                 " env > %(outfile)s")
    P.run(statement)

    open(outfile, 'w').close()



###############################################################################
# Generic pipeline tasks
###############################################################################
@follows(submit_jobs)
def full():
    pass


def main(argv=None):
    if argv is None:
        argv = sys.argv
    P.main(argv)


if __name__ == "__main__":
    sys.exit(P.main(sys.argv))    
