# Jethro
# 2021.09.23

# Requires python 3 with drmaa package installed
# Requires drmaa shared object files in the submit environment:
#   export DRMAA_LIBRARY_PATH=/mgmt/uge/8.6.8/lib/lx-amd64/libdrmaa.so.1.0

# To run:
# python submit.py

import drmaa
import os
import stat
import tempfile

def main():
    """Open and drmaa session, submit a bash script to be run on the cluster
    close the drmaa session
    """

    shell_script = tempfile.NamedTemporaryFile(dir='.', mode='w', delete=False)
    outfile = os.path.join(os.getcwd(), 'ClusterEnvironment.txt')
    shell_script.write("#!/bin/bash\n\n"
                       "printf 'python version is:\\n' > {outf}\n"
                       "which python >> {outf}\n"
                       "printf 'bash environment is:\\n': >> {outf}\n"
                       # "env >> {outf}".format(outf=outfile)) # py2
                        "env >> {outf}".format(outf=outfile))
    shell_script.close()
    # os.chmod(shell_script.name, 0555) # python 2
    os.chmod(shell_script.name, 0o555)

    # initialize session
    s = drmaa.Session()
    s.initialize()
    print('A session was started successfully')
    print('Session returns contact information: {}'.format(s.contact))

    # create a job template 
    jt = s.createJobTemplate()

    # SET for UGE on BMRC cluster
    # FAILS with '-V' but '-v', works
    # jt.nativeSpecification = "-V"
    jt.nativeSpecification = "-cwd -q short.qc -pe shmem 2 -v PATH={}".format(os.environ['PATH'])

    jt.remoteCommand = os.path.join(os.getcwd(), shell_script.name)
    jt.jobName = 'HelloCluster'
    # jt.errorPath = shell_script + '.error'
    # jt.jobEnvironment = os.environ
    jt.joinFiles=False


    # run join
    jobid = s.runJob(jt)
    print('Job submitted with ID: {}'.format(jobid))


    # clean up
    s.deleteJobTemplate(jt)
    s.exit()
    os.unlink(shell_script.name)

if __name__=='__main__':
    main()

