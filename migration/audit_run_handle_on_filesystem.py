#################################################################
#### This program reads all of the files for the run_handle, ####
#### calculates a checksum for each, and writes the file     ####
#### and checksum to stdout.                                 ####
#################################################################

import sys, os
import hashlib

phengs_path_prefix = '/phengs'
BUF_SIZE = 65536  # lets read stuff in 64kb chunks!

def recursively_checksum(os_path):

    for subdir, dirs, files in os.walk(os_path):
        for file in files:

            filepath = os.path.join(subdir, file)

            md5 = hashlib.md5()
            
            with open(filepath, 'rb') as f:
                while True:
                    data = f.read(BUF_SIZE)
                    if not data:
                        break
                    md5.update(data)
            
            print("{0} {1}".format(filepath, md5.hexdigest()))


def do_audit(run_handle):

    run_data_dir_filesystem = "%s/hpc_storage/run_data/%s" % (phengs_path_prefix, run_handle)
    machine_fastqs_dir_filesystem = "%s/hpc_storage/machine_fastqs/%s" % (phengs_path_prefix, run_handle)


    recursively_checksum(run_data_dir_filesystem)
    recursively_checksum(machine_fastqs_dir_filesystem)
    
    # open results_ngssample_dirs and register directories in it
    results_file = "%s/results_ngssample_dirs" % run_data_dir_filesystem
    
    with open(results_file) as f:
        for line in f:
            os_path = line.strip()
            recursively_checksum(os_path)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError("Use: python audit_run_handle_on_filesystem.py <run handle>")

    do_audit(sys.argv[1])

