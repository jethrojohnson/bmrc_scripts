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
=======================================
pipeline_intro_2.py

A second  pipeline to introduce ruffus. 
=======================================

Jethro Johnson

The purpose of this pipeline is to provide a general overview of running
ruffus pipelines. It is divided into three sections:

Section 1: Contains an example of i) how python functions can be executed
locally, ii) how commandline statements can be submitted to the cluster
using the ruffus drmaa_wrapper, and iii) how commandline statements can
be submitted to the cluster using the wrapper in cgatcore.pipeline
(see P.run()).

Section 2: Contains an example of how ruffus pipelines may be used in 
conjunction with SQL, pandas, R for specific analyses. Specifically,
i) loading pandas dataframes into an sqlite3 database, ii) retrieving 
pandas dataframes from an sqlite3 database, iii) executing R code from
within a ruffus task, and iv) executing R code within the python
namespace. 

Section 3: Contains an example of how ruffus pipelines may be used in
conjunction with YAML files to run external tools via the commandline.
Specifically, i) trim fastq files using the tool trimmomatic, and ii)
merge trimmed fastq files using the tool flash. 

INPUTS: There are no input files required to run Section 1 of this 
        pipeline. The files required for Sections 2/3 are located in 
        the source directory containing the YAML file template. Their 
        location does not need to be specified. 

OUTPUT: Section 1 creates a bash script, then creates two flatfiles that
        log the shell environment on the cluster when submitting jobs 
        using ruffus.drmaa_wrapper or cgatcore.pipeline.run. Note differences
        in PATH. Output for Section 2 is written to a subdirectory
        analyse_count_table.dir. Output for Section 3 is written to the 
        subdirectories trimmed_fastqs.dir (trimmomatic run) and 
        merged_fastqs.dir (flash run). 

TO RUN: Within your working directory type:
        python <path-to-pipeline_dir>/pipeline_intro_2.py -v6 -p4 <command> <target>
        where command is any ruffus run command (e.g. make, show, plot,
        dump), and target is the name of any ruffus task (e.g full, 
        plotNMDS, sectionTwo).
"""

####################### Define the pipeline environment #######################
# ruffus modules
import ruffus
from ruffus import *
# While import * isn't preferred practice for python. There are a lot of ruffus
# functions to import. imports among others: 
# active_if, add_inputs, formatter, inputs, regex, suffix,
# collate, combine, files, files_re, merge, originate, split, combinatorics
# subdivide
# follows, jobs_limit, mkdir, 
# graph, graph_colour_demo_printout, graphviz

# Custom toolkit to facilitate running of ruffus pipelines
import cgatcore.pipeline as P

import logging as L
import sys,os,re
import sqlite3
import drmaa

# Custom configuration of pipeline run parameters using INI files/configParser
# (see https://en.wikipedia.org/wiki/INI_file)
# Files must be called pipeline.ini, but can be located in the source
# code dir, or in the run dir. If both, the latter will take precedence.
# The result is PARAMS: a dictionary, that can be referenced to retrieve
# run parameters.
PARAMS = P.get_parameters(["{}/pipeline.yml".format(os.path.splitext(__file__)[0]),
                          "pipeline.yml"],)

###############################################################################
# Utility functions
###############################################################################
def connect():
    """Connect to default sqlite database, returning a database handle
    that can be used for database IO. See pandas.DataFrame.to_sql() and
    and pandas.read_sql_query()
    """
    dbh = sqlite3.connect(PARAMS['database']) # name of database is in yml
    
    return dbh

###############################################################################
# Pipeline Section 1: Demonstrating job submission to the cluster. 
###############################################################################
@originate('clusterEnvironment.sh')
def createShellScript(outfile): # outfile captures the only variable passed from @originate
    """Write shell script designed to output bash environment.

    Note: This task is executed locally, jobs will not be submitted to
    the cluster"""

    # write bash script
    outf = open(outfile, 'w')
    outf.write('#!/bin/bash\n\n'
               'env\n')
    outf.close()
    # change permissions
    os.chmod(outfile, 0o555)



@transform(createShellScript, suffix('.sh'), '_ruffus.txt')
def submitClusterJob_ruffus(infile, outfile): # infile captures createShellScript output
    """This task demonstrates the ruffus wrapper for submitting jobs to 
    the cluster (ruffus.drmaa_wrapper). 

    Note: The drmaa session only needs to be initialized once within the
    pipeline. It's initialised here for run_job.
    """
    # configure ruffus to submit cluster jobs
    from ruffus.drmaa_wrapper import run_job, error_drmaa_job    

    # create the commandline statement you wish to run.
    statement = './{0} > {1}'.format(infile, outfile)

    # run it
    try:
        # a wrapper for the drmaa.jobTemplate instance
        stdout_res, stderr_res = "",""
        stdout_res, stderr_res = run_job(cmd_str=statement,
                                         job_name='hello_cluster',
                                         logger=L.getLogger(), # default python logging 
                                         drmaa_session=drmaa.Session(), #P.GLOBAL_SESSION,
                                         run_locally=False, # option to run locally
                                         job_other_options='-M jethro.johnson@kennedy.ox.ac.uk -q short.qa') # option to pass other native specifications.
    except error_drmaa_job as err:
        # handle errors
        raise Exception("\n".join(map(str,
                            "Failed to run:",
                            statement,
                            err,
                            stdout_res,
                            stderr_res)))


@follows(submitClusterJob_ruffus)
@transform(createShellScript, suffix('.sh'), '_cgatcore.txt')
def submitClusterJob_cgatcore(infile, outfile):
    """This task demonstrates submitting jobs using cgatcore.pipeline.run()

    Note: Variables such as to_cluster etc. can be set in the YAML config
    file. They can also appear in the task, in which case the local namespace
    takes precedence. Finally, they can be passed as arguments to run()

    cgatcore.pipeline.run() assumes that  thebash environment for a pipeline
    is passed to nodes (see outfile contents). It also implicitly
    handles logging.
    """
    # option to configure ruffus job submission
    # (these don't have to be specified, they may also be placed in INI)
    copy_environment=True # option to replicate bash environment on nodes
    to_cluster=True # option to run job locally
    cluster_options="" # option to pass other native specifications. 

    # create the commandline statement you wish to run.
    # string substitution works implicitly, but ONLY with old style formatting
    statement = "./%(infile)s > %(outfile)s"

    # option to write comments to stdout/logfile
    L.debug("This commend probably won't appear in logfile")
    L.info('This comment may not appear in logfile')
    L.warn('This comment will almost definitely appear in logfile')

    # run commandline statement
    P.run(statement)


###############################################################################
# Pipeline Section 2: Using pipelines in conjunction with R, pandas & sqlite3
###############################################################################
@follows(mkdir('analyse_count_table.dir'))
@transform(os.path.join(os.path.splitext(__file__)[0], 'counts_table.tsv'), # infile
           regex('.+/(.+).tsv'),
           r'analyse_count_table.dir/\1.load') # a sentinel file
def loadSQLTable(infile, outfile):
    """This task demonstrates how data can be loaded into a SQL
    database within the run directory using pandas and sqlite3
    """
    import pandas as pd

    # first read the flatfile into pandas as a dataframe...
    df = pd.read_table(infile, sep='\t', index_col='family') 

    # ...derive a table name from outfile...
    table_name = os.path.basename(outfile)[:-len('.load')]

    # ...then load it into a database
    df.to_sql(name=table_name, # name of the db table created
              con=connect(), # see utility functions
              index=True, # index_col will be indexed in db
              if_exists='replace') # overwrite old version
    open(outfile, 'a').close()


@transform(loadSQLTable, suffix('.load'), '.tsv')
def retrieveSQLTable(infile, outfile):
    """This task demonstrates how to retreive a table from an SQL
    database within the run directory using pandas and sqlite3
    """
    import pandas as pd

    # identify table based on sentinel passed as infile
    table_name = os.path.basename(infile)[:-len('.load')]

    # define the SQL query to be executed
    query = 'SELECT * FROM {}'.format(table_name)

    # fetch dataframe 
    df = pd.read_sql_query(sql=query,
                           con=connect(),
                           index_col='family')

    # write it to an outfile
    df.to_csv(outfile, sep="\t")


# because of issues with R namespace encapsulation, it's a good 
# idea to use the @jobs_limit decorator to limit the number of parallel 
# jobs running R. 
@jobs_limit(1, 'RGlobal') 
@subdivide(loadSQLTable,
           regex('(.*).load'),
           [r'\1_normalised.tsv', r'\1_normalised.png'])
def plotNMDS(infile, outfiles):
    """This task demonstrates how to use ruffus one to many with @subdivide 
    
    It also demonstrates how to pass objects between python and R using rpy2
    and how to run objects directly in the background R session using 
    rpy2.robjects.r

    In this case we're using the R package vegan to create an ordination
    (NMDS) from a count table of bacterial taxonomic abundance, then plotting
    the ordination. 
    """

    import pandas as pd
    import rpy2.robjects as robjects
    from rpy2.robjects import r as R
    from rpy2.robjects import pandas2ri

    # outfiles is a list
    out_table, out_png = outfiles

    # fetch dataframe
    table_name = os.path.basename(infile)[:-len('.load')]
    query = 'SELECT * FROM {}'.format(table_name)
    df = pd.read_sql_query(sql=query,
                           con=connect(),
                           index_col='family')

    # manipulate pandas dataframe (transpose and normalise)
    df = df.apply(lambda x: x/x.sum() *100, axis=0)
    df.index.name = None
    df = df.transpose()
    df.index.name='SampleID'

    # write transformed dataframe to first outfile
    df.to_csv(out_table, sep='\t')

    # get sample details from dataframe index
    text = [x.split('-')[1] for x in df.index.tolist()]
    cols = [x.split('-')[2] for x in df.index.tolist()]

    # convert sample details and dataframe from python objects to r objects
    # this can be done implicitly with pandas2ri.activate(), but this creates
    # unexpected behaviour if used in imported modules.
    txt = robjects.StrVector(text)
    cols = robjects.StrVector(cols)
    # df = pandas2ri.py2ri(df) # is currently broken
    pandas2ri.activate()

    # assign the robjects to variables in the r namespace
    R.assign('df', df)
    R.assign('cols', cols)
    R.assign('txt', txt)

    # execute R code. 
    R('''
      require('vegan')

      # create NMDS
      fam.nmds <- metaMDS(df, autotransform=FALSE)

      # write NMDS plot to second outfile
      png('{out_png}', type='cairo')
      plot(fam.nmds, type='n')
      text(fam.nmds, display='sites', labels=txt)
      dev.off()

      # a safety measure, given that many things may occur in R namespace
      rm(list=ls())'''.format(**locals()))


@transform(plotNMDS, suffix('.tsv'), '_ANOSIM_R.tsv')
def calculateAnosimR(infile, outfile):
    """This task demonstrates how to sub-select output from a parent tasks
    using the suffix() as an argument to @transform.

    It also showcases an alternative way to execute R code within a 
    pipeline by bringing R packages into the python namespace using 
    rpy2.robjects.packages.importr


    The input dataframe is a table of bacterial taxon counts (where sampleIDs
    are rows, and taxa are columns). Before caculating taxon abundance, the
    raw sequence data for each sample was rarefied to a number of different 
    read depths (max 25K, min 1K reads). The question being asked is how does
    this rarefying alter the composition of the sample. To answer it each 
    rarefied read depth (1K, 5K, 10K, 15K, 20K) is compared to the max read
    depth (25K) using the anosim R statistic. The anosim test is being run 
    using the R package vegan. 


    Samples in the infile have the naming convention:
    <SampleID>-<Depth>-<Replicate>.
    """
    import numpy as np
    import pandas as pd

    # Using rpy2 to handle python R interface
    import rpy2.robjects as ro
    from rpy2.robjects import pandas2ri
    from rpy2.robjects.conversion import localconverter

    # bring required R tools into python namespace
    from rpy2.robjects.packages import importr
    vegan = importr('vegan')

    # fetch a pandas dataframe (python)
    df = pd.read_table(infile, sep='\t', index_col='SampleID')
    # discard empty rows in dataframe
    not_empty = df.apply(lambda x: np.sum(x) != 0).tolist()
    df = df.iloc[:,not_empty]

    # get the rarefaction depths (present in the sample IDs in dataframe)
    depths = [x.split('-')[1] for x in df.index.tolist()]
    df['depth'] = depths

    # fetch the max_depth against which all other rarefied depths are compared 
    depths = sorted(map(int, [x.strip('K') for x in set(depths)]))
    max_depth = str(depths.pop()) + 'K'

    # open outfile as a table to store the comparison of downsampled data vs. 
    # original data. 
    outf = open(outfile, 'w')
    outf.write('SubsampleDepth\tR_Statistic\n')

    # iterate over all of the rarefied depths and compare community composition
    # to the max depth using ANOSIM R statistic. 
    for depth in map(lambda x: str(x) + 'K', depths):

        #  subsample dataframe to contain one rarefied depth and max_depth
        df_subsampled = df.loc[df['depth'].isin([depth, max_depth])]

        # ANOSIM function takes two arguments: a count table (positional)
        # and group (kw)

        # Explicitly convert group into an R object (factor)
        group = df_subsampled['depth'].tolist()
        group_r = ro.FactorVector(group) 

        # Explitcily convert the count table into an R object (dataframe)
        df_subsampled.drop(['depth'], axis=1, inplace=True)
        with localconverter(ro.default_converter + pandas2ri.converter):
            df_r = ro.conversion.py2rpy(df_subsampled)

        # perform R analysis
        anosim = vegan.anosim(df_r, grouping=group_r)

        # retrieve anosim R statistic from the returned R object using rx
        anosim_r = anosim.rx2('statistic')[0]

        outf.write("\t".join([depth, '\t', str(anosim_r)]) +'\n')

    outf.close()
    

###############################################################################
# Pipeline Section 3: Using pipelines in conjunction with external tools
###############################################################################
@follows(mkdir("trimmed_fastqs.dir"))
@transform(['{}/fastqs.dir/AGCYW-25K-R1.fastq.1.gz'.format(os.path.splitext(__file__)[0]),
            '{}/fastqs.dir/AGCYW-25K-R2.fastq.1.gz'.format(os.path.splitext(__file__)[0])],
           regex(".+/(.+)-R([12]).fastq.1.gz"),
           r"trimmed_fastqs.dir/\1-R\2.fastq.1.gz")
def trimFastqReads(fastq_1, fastq_1_out):
    """This task demonstrates how to run commandline tools from within a 
    ruffus pipeline. 

    Note: Key tool parameters been made accessible via options in the config yml file,
    which is parsed into a dictionary using cgatcore.pipeline.get_parameters at the 
    start of this pipeline script. 

    Note: There are two paired fastq files in the specified directory, each input
    file will create a separate ruffus job that will be run in parallel.

    trim reads using trimmomatic:
        filter both reads so that they're above specified length
        chop n bases off read 1
        chop n bases off read 2
        """

    # specify outfiles and intermediary files
    fastq_2 = re.sub('fastq.1', 'fastq.2', fastq_1)    
    fastq_2_out = re.sub('fastq.1', 'fastq.2', fastq_1_out)    
    
    fastq_1_unpaired = re.sub('.fastq.1.gz', '_unpaired.fastq.1.gz', fastq_1)
    fastq_2_unpaired = re.sub('.fastq.2.gz', '_unpaired.fastq.2.gz', fastq_2)

    tmpf_1 = P.get_temp_filename('.')
    tmpf_2 = P.get_temp_filename('.')

    # filter based on min lenth
    min_length = PARAMS['trim_min_length']
    statement = ("java -Xmx5g -jar"
                 " %(trim_tool_location)s" # Location of the tool is given in config.yml
                 " PE -phred33"
                 " %(fastq_1)s" # input forward
                 " %(fastq_2)s" # input reverse
                 " %(tmpf_1)s" # output forward, paired
                 " %(fastq_1_unpaired)s" # output forward, unpaired
                 " %(tmpf_2)s" # output reverse, paired
                 " %(fastq_2_unpaired)s" # output reverse, unpaired
                 " MINLEN:%(trim_min_length)s") # minimum acceptable read length
    P.run(statement)

    # trim forward read
    r1_crop = PARAMS['trim_forward']
    r2_crop = PARAMS['trim_reverse']
    statement = (" java -Xmx5g -jar"
                 "  %(trim_tool_location)s"
                 "  SE -phred33"
                 "  %(tmpf_1)s" # input (output forward, paired)
                 "  %(fastq_1_out)s" # outfile
                 "  HEADCROP:%(r1_crop)s &&"
                 " java -Xmx5g -jar"
                 "  %(trim_tool_location)s"
                 "  SE -phred33"
                 "  %(tmpf_2)s" # input (output reverse, paired)
                 "  %(fastq_2_out)s" # outfile
                 "  HEADCROP:%(r2_crop)s")
    P.run(statement)
 
    # remove temporary files
    os.unlink(tmpf_1)
    os.unlink(tmpf_2)


@follows(mkdir("merged_fastqs.dir"))
@transform(trimFastqReads,
           regex(".+/(.+).fastq.1.gz"),
           r"merged_fastqs.dir/\1.extendedFrags.fastq.gz")
def joinPEReads(infile, outfile):
    """This task provides a second demonstration of running commandline
    tools from within a ruffus pipeline.

    join paired end reads using flash
    """
    import shutil
    
    # infiles
    fastq_1 = infile
    fastq_2 = re.sub('fastq\.1', 'fastq\.2', fastq_1)

    # specify flash outfiles
    out_dir = os.path.dirname(outfile)
    out_stub = os.path.basename(outfile)[:-len('.extendedFrags.fastq.gz')]
    nc_1 = out_stub + '.notCombined_1.fastq.gz'
    nc_2 = out_stub + '.notCombined_2.fastq.gz'
    hist = out_stub + '.hist'
    histogram = out_stub + '.histogram'

    statement = ("%(assembly_tool_location)s" # location of flash
                 " --min-overlap=%(assembly_min_overlap)s"
                 " --max-overlap=%(assembly_max_overlap)s"
                 " --max-mismatch-density=%(assembly_mismatch)s"
                 " --threads=%(assembly_num_threads)s"
                 " --output-prefix=%(out_stub)s"
                 " --compress"
                 " %(fastq_1)s"
                 " %(fastq_2)s"
                 " 2>&1 | tee %(outfile)s.log")
    P.run(statement)

    # move flash outfiles out from working directory
    # this is not essential, but its nice to keep separate analyses in
    # discrete subdirectories. (Flash doesn't allow redirecting
    # of output).
    nc_1_out = os.path.join('merged_fastqs.dir', os.path.basename(nc_1))
    nc_2_out = os.path.join('merged_fastqs.dir', os.path.basename(nc_2))
    hist_out = os.path.join('merged_fastqs.dir', os.path.basename(hist))
    histogram_out = os.path.join('merged_fastqs.dir', 
                            os.path.basename(histogram))

    shutil.move(os.path.basename(outfile), outfile)
    shutil.move(nc_1, nc_1_out)
    shutil.move(nc_2, nc_2_out)
    shutil.move(hist, hist_out)
    shutil.move(histogram, histogram_out)


###############################################################################
# Generic pipeline tasks
###############################################################################
@follows(submitClusterJob_ruffus, 
         submitClusterJob_cgatcore)
def sectionOne():
    """Run only pipeline Section 1"""
    pass

@follows(retrieveSQLTable, calculateAnosimR)
def sectionTwo():
    """Run only pipeline Section 1"""
    pass

@follows(joinPEReads)
def sectionThree():
    """Run only pipeline Section 1"""
    pass

@follows(sectionOne, sectionTwo, sectionThree)
def full():
    """Run the complete pipeline (sections one, two, and three)"""
    pass

# The actual running of the pipeline is taken care of from within
# cgatcore.pipeline
if __name__ == "__main__":
    sys.exit(P.main(sys.argv))
