import sys, os
from irods.session import iRODSSession

phengs_path_prefix = '/phengs'
#phengs_path_prefix = '/var/lib/irods/phengs'
irods_path_prefix = '/PHE/home/ngsservicearchive/archived_files'
#irods_path_prefix = '/tempZone/home/rods/archived_files'

def build_irods_path(os_path):
    irods_sub_path = ''
    if os_path.startswith(phengs_path_prefix):
        irods_sub_path = os_path[len(phengs_path_prefix):]

    irods_path = "%s%s" % (irods_path_prefix, irods_sub_path)
    return irods_path


def recursively_register(os_path):

    irods_path = build_irods_path(os_path)
    
    # run automated ingest
    print("python -m irods_capability_automated_ingest.irods_sync start %s %s --synchronous --progress --event_handler stat_eventhandler" % (os_path, irods_path))
    os.system("python -m irods_capability_automated_ingest.irods_sync start %s %s --synchronous --progress --event_handler stat_eventhandler" % (os_path, irods_path))
    #print("ireg -fCV %s %s" % (os_path, irods_path))
    #os.system("ireg -fCV %s %s" % (os_path, irods_path))


def recursively_replicate_and_trim(os_path, run_handle):

    for root, dirs, files in os.walk(os_path):

        for name in files:

            full_os_path = os.path.join(root, name)
            irods_path = build_irods_path(full_os_path)

            print("""irule -F async_replicate_and_trim.r "*irods_path='%s'" "*run_handle='%s'" """ % (irods_path, run_handle))
            os.system("""irule -F async_replicate_and_trim.r "*irods_path='%s'" "*run_handle='%s'" """ % (irods_path, run_handle))

def do_register(run_handle):

    run_data_dir_filesystem = "%s/hpc_storage/run_data/%s" % (phengs_path_prefix, run_handle)
    machine_fastqs_dir_filesystem = "%s/hpc_storage/machine_fastqs/%s" % (phengs_path_prefix, run_handle)

    recursively_register(run_data_dir_filesystem)
    recursively_register(machine_fastqs_dir_filesystem)

    # open results_ngssample_dirs and register directories in it
    results_file = "%s/results_ngssample_dirs" % run_data_dir_filesystem

    with open(results_file) as f:
        for line in f:
            os_path = line.strip()
            recursively_register(os_path)

            # replicate and trim, first make sure directory has g+rw so trim will work
            print("find %s -type d -print0 | xargs -0 chmod g+rw" % os_path)
            #os.system("find %s -type d -print0 | xargs -0 chmod g+rw" % os_path)

            recursively_replicate_and_trim(os_path, run_handle)

    # replicate and trim run_data, first make sure directory has g+rw so trim will work
    print("find %s -type d -print0 | xargs -0 chmod g+rw" % run_data_dir_filesystem)
    os.system("find %s -type d -print0 | xargs -0 chmod g+rw" % run_data_dir_filesystem)
    recursively_replicate_and_trim(run_data_dir_filesystem, run_handle)

    # replicate and trim fastqs, first make sure directory has g+rw so trim will work
    print("find %s -type d -print0 | xargs -0 chmod g+rw" % machine_fastqs_dir_filesystem)
    os.system("find %s -type d -print0 | xargs -0 chmod g+rw" % machine_fastqs_dir_filesystem)
    recursively_replicate_and_trim(machine_fastqs_dir_filesystem, run_handle)
    
if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError("Use: python register.py <run handle>")

    do_register(sys.argv[1])


