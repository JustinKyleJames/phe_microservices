import sys, os, time, hashlib, subprocess, base64
from irods.session import iRODSSession
from irods.models import Collection, DataObject, DataObjectMeta, Resource
from irods.column import Criterion

phengs_path_prefix = '/phengs'
#phengs_path_prefix = '/var/lib/irods/phengs'
irods_path_prefix = '/PHE/home/ngsservicearchive/archived_files'
#irods_path_prefix = '/tempZone/home/rods/archived_files'

BUF_SIZE = 65536

def build_irods_path(os_path):
    irods_sub_path = ''
    if os_path.startswith(phengs_path_prefix):
        irods_sub_path = os_path[len(phengs_path_prefix):]

    irods_path = "%s%s" % (irods_path_prefix, irods_sub_path)
    return irods_path

def recursively_checksum(os_path, checksum_map):

    for subdir, dirs, files in os.walk(os_path):
        for file in files:

            filepath = os.path.join(subdir, file)

            sha256 = hashlib.sha256()
            
            with open(filepath, 'rb') as f:
                while True:
                    data = f.read(BUF_SIZE)
                    if not data:
                        break
                    sha256.update(data)

            checksum_map[filepath] = sha256.hexdigest()
            
def recursively_register(os_path, checksum_map):

    # first calculate and save a checksum for all files on filesystem 
    recursively_checksum(os_path, checksum_map)
    
    # run automated ingest
    irods_path = build_irods_path(os_path)
    print("python -m irods_capability_automated_ingest.irods_sync start %s %s --synchronous --progress --event_handler stat_eventhandler" % (os_path, irods_path))
    os.system("python -m irods_capability_automated_ingest.irods_sync start %s %s --synchronous --progress --event_handler stat_eventhandler" % (os_path, irods_path))

def recursively_replicate_and_trim(os_path, run_handle):

    for root, dirs, files in os.walk(os_path):

        for name in files:

            full_os_path = os.path.join(root, name)
            irods_path = build_irods_path(full_os_path)

            print("""irule -F async_replicate_and_trim.r "*irods_path='%s'" "*run_handle='%s'" """ % (irods_path, run_handle))
            os.system("""irule -F async_replicate_and_trim.r "*irods_path='%s'" "*run_handle='%s'" """ % (irods_path, run_handle))

def do_register(run_handle):

    checksum_map = {}

    run_data_dir_filesystem = "%s/hpc_storage/run_data/%s" % (phengs_path_prefix, run_handle)
    machine_fastqs_dir_filesystem = "%s/hpc_storage/machine_fastqs/%s" % (phengs_path_prefix, run_handle)

    recursively_register(run_data_dir_filesystem, checksum_map)
    recursively_register(machine_fastqs_dir_filesystem, checksum_map)

    # open results_ngssample_dirs and register directories in it
    results_file = "%s/results_ngssample_dirs" % run_data_dir_filesystem

    with open(results_file) as f:
        for line in f:
            os_path = line.strip()
            recursively_register(os_path, checksum_map)

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

    # do post verification

    # wait for all rules to complete
    active_rules = int(subprocess.check_output(['iquest', '%s', "select COUNT(RULE_EXEC_ID) where RULE_EXEC_USER_NAME = 'ngsservicearchive'"]).strip())
    while active_rules > 0:
        print("Waiting for replication jobs to complete.  Job count = %d" % active_rules)
        time.sleep(5)
        active_rules = int(subprocess.check_output(['iquest', '%s', "select COUNT(RULE_EXEC_ID) where RULE_EXEC_USER_NAME = 'ngsservicearchive'"]).strip())

    print("Replication jobs completed...")

    # now compare checksum with those in checksum_map
    print("-------------------")
    print("Validating files...")
    print("-------------------")
    env_file = os.path.expanduser('~/.irods/irods_environment.json')

    validation_status = True

    with iRODSSession(irods_env_file=env_file) as session:

        for file_path in checksum_map:

            # find object in iRODS and get checksum
            # select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'filesystem::path' and META_DATA_ATTR_VALUE = <file_path>

            found_file = False
            results = session.query(Collection.name, DataObject, Resource.name).filter( \
                    Criterion('=', DataObjectMeta.name, 'filesystem::path')).filter( \
                    Criterion('=', DataObjectMeta.value, file_path)).filter( \
                    Criterion('=', Resource.name, 's3_resc'))


            for result in results:

                found_file = True

                # Verify the checksum.  This is overkill as iRODS should have already done this.
                stored_checksum = base64.b64decode(result[DataObject.checksum][5:]).hex()

                if checksum_map[file_path] == stored_checksum:
                    print("Checksum validated for %s" % file_path)
                else:
                    validation_status = False
                    print("ERROR:  Checksum validation failed for %s: %s vs %s" % (file_path, checksum_map[file_path], stored_checksum))

            if found_file is False:
                validation_status = False
                print("ERROR: File %s was not found in archive...")
              
 
    if validation_status is False:
        print("ERROR: Post replication validation failed for at least one file.")           
        sys.exit(1)
    else:
        print("Post replication validation succeeded.")           

    
if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError("Use: python register.py <run handle>")

    do_register(sys.argv[1])


