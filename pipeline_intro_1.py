###############################################################################
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.  
###############################################################################
"""
pipeline_hello_world2.py

A pipeline to introduce the basic decorators for building ruffus tasks.

INPUTS: There are no inputs required to run this pipeline. All files are
        originally created with the ruffus decorator function @originate

OUTPUT: Each ruffus task creates a simple text file that is written to 
        the run directory. 

TO RUN: Within the run directory type:
        python <path-to-pipeline_dir>/pipeline_hello_world2.py -v6 -p2 make full 
"""

########## This pipeline is written following the PEP 8 style guide ###########
# Yours should be too. 
# Have a look at https://www.python.org/dev/peps/pep-0008/


####################### Import essential python modules #######################
# There are two essential modules for creating a ruffus pipeline from a python
# script. The first is the ruffus python module (and all its contents). The
# second is cgatcore.pipeline which contains all our customised code to handle
# submitting jobs to helix as well as utility functions, for example P.snip().

# ruffus module
import ruffus
from ruffus import *

# Custom toolkit to facilitate running of ruffus pipelines
import cgatcore.pipeline as P

# other python libraries that are used in the code below
import os
import sys

########## Read essential configuration parameters from pipeline.ini ##########
# Within the source directory containing a pipeline, there should ALWAYS be a 
# directory named after the pipeline. For example pipeline_intro_1.py will
# have a directory pipeline_intro_1/ that contains an file 
# called pipeline.yml. This file is in YAML format and parameters specified in
# this file are parsed into a python dictionary called 'PARAMS', using code in
# the cgatcore.Pipeline module imported above:

PARAMS = P.get_parameters(["{}/pipeline.yml".format(os.path.splitext(__file__)[0]),
                          "pipeline.yml"],)


######################### The pipeline itself #################################
# This section contains the actual ruffus tasks that consitute the pipeline, 
# the decorator functions (e.g. @originate, @transform etc.) come from the 
# ruffus module imported above. Each ruffus decorator has its own syntax for
# specifying: i) the input file, ii) a regular expression that the input file
# should match, iii) the output file. Details of specific functions are given
# in the docstrings below; however, full details of all functions are in the
# ruffus man pages:   http://www.ruffus.org.uk/

# If you are confused about what code belongs to ruffus and what code belongs
# to your pipeline, I would also recommend reading about python decorators. 
# Essentially, decorators receive arguments, that they pass to the function
# below. They also perform additional tasks. In the case of ruffus decorators, 
# they i) receive arguments, which they pass to the function below, and 
# ii) they then excecute additional code that handles running this function
# as part of a ruffus pipeline.

@originate('Task01_output_file.txt') 
def taskOne(outfile):
    '''@originate: The most basic ruffus function. 
    The ruffus decorator @originate takes only one argument, which it passes 
    to your pipeline function 'taskOne()'. In this case the argument is the
    name of the outfile to be created by taskOne.

    You are free to write any code within task one, so long as you output
    is a single file entitled 'Task01_output_file.txt'.

    In this case your 'taskOne' function simply takes the name it is passed
    as the positional argument 'outfile'  and writes 'Hello world one' to a
    file of this name. 
    '''

    # write a simple file based on the name passed to outfile
    outfile_handle = open(outfile, 'w')
    outfile_handle.write('Hello world ONE\n')
    outfile_handle.close()

@follows(taskOne)
@files('Task01_output_file.txt', 'Task02_output_file.txt')
def taskTwo(infile, outfile):
    '''@files: The second most basic ruffus function.
    The ruffus decorator @files takes two arguments, the first is the name of
    an input file that already exists, the second is the name of an outfile
    that to be created by taskTwo. These two arguments are passed to function 
    taskTwo's arguments 'infile' and 'outfile'.

    Again, you are free to write any code within task two, so long as you 
    output a file entitled 'Task02_output_file.txt',

    NOTE: that I pass a file name as the input file. I could instead pass
    the name of a previous ruffus task. Ruffus decorators are able to receive
    other ruffus tasks in place of input, they implicitly take the output
    of the preceeding task as input.

    Because the infile is created by taskOne(), I have to specify that 
    taskTwo can only be executed after task one has been completed. I do
    this by using the ruffus decorator @follows (explained below).

    Similar to make, ruffus dependency checking means that any task that
    recieves an input (either a preceeding ruffus task, or a filename), 
    will check the time at which the input file was created. If the
    output file doesn't exist, or if the output file was created before
    the input file, then this section of the pipeline is rerun. 

    In this case your 'taskTwo' function takes the contents of the infile
    and copies it to the outfile. It does this using a commandline script
    that is run as a job on the cluster. All the cluster job submission
    is handled in the cgatcore.pipeline module  imported above. 

    Note that Pipeline implicitly handles python string substitution
    (using the string .format() method), allowing names of the input and
    output file to be insterted into the commmandline script, rather than
    being hardcoded.
    '''

    # this is the simple commandline script to be submitted to the cluster
    # infile and outfile are names of variables in the local namespace. 
    statement = ("cat %(infile)s | sed 's/ONE/TWO/' > %(outfile)s")

    # this is the function that handles running the job on the cluster
    # you can run a job locally by setting the to_cluster argument to false
    P.run(statement, to_cluster=True)


@transform([taskOne, taskTwo],
           suffix('.txt'),
           '_modified.txt')
def taskThree(infile, outfile):
    '''@transform: a very useful ruffus function
    The ruffus decorator @transform can receive a single file, or a list
    of files as its first argument. (In this case I have passed it a list
    of ruffus tasks, each producing a single file). As its second argument
    it takes a ruffus function 'suffix', which specifies the suffix
    that the input file(s) are expected to end with (see also regex()).
    The third and final argument is a string, which specifies the new suffix
    that will be added to the outfile, inplace of the old suffix. 

    As an example, the first file passed to argument one, is the output of 
    taskOne 'Task01_output_file.txt', which ends with the suffix '.txt', 
    the output file created will therefore be 'Task01_output_file_modified.txt'

    You are free to write any code within taskThree, so long as it outputs 
    files entitled <input-prefix>_modified.txt.

    @transform, all input file(s) and will execute your code on each one 
    in parallel. e.g. if you pass 100 fastq files to a function that 
    converts them to fasta files, each one will be converted in parallel
    (...provided you have access to 100 cpus when running the pipeline)
    '''

    # In this example, I do something to the text in each infile
    # and then write it to the outfile. 
    # As this is pure python, it is executed locally, nothing is submitted
    # to the cluster, as P.run() is not called.  
    infile_text = open(infile).readline()
    infile_text = infile_text.split()
    
    outfile_handle = open(outfile, 'w')
    outfile_handle.write(' '.join([infile_text[0],
                                   "brave new",
                                   infile_text[1],
                                   infile_text[2]])
                                  + '\n')
    outfile_handle.close()


@follows(taskOne, taskTwo, taskThree)
def full():
    '''@follows: another useful ruffus function
    The ruffus decorator @follows creates no output. It simply checks the
    dependencies of all the files/ruffus tasks passed to it, and makes 
    sure they are all up to date. 

    In this example, I am using it to make a dummy function full. Which
    does nothing, but by calling 'pipeline_intro_1.py make full' 
    from the commandline, all the tasks passed to @follows will be run.
    '''


# Note: The actual running of the pipeline is taken care of from within the
# cgatcore.pipeline module.
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))

