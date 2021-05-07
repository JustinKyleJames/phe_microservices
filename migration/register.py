import sys, os, time, hashlib, subprocess, base64
from irods.session import iRODSSession
from irods.models import Collection, DataObject, DataObjectMeta, Resource
from irods.column import Criterion
from irods.meta import iRODSMeta
from irods.exception import CollectionDoesNotExist
from irods.exception import CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME 
import irods.keywords as kw

phengs_path_prefix = '/phengs'
#phengs_path_prefix = '/var/lib/irods/phengs'
irods_path_prefix = '/PHE/home/ngsservicearchive/archived_files'
#irods_path_prefix = '/tempZone/home/rods/archived_files'
staging_resource = 'lustre_staging_resc'

BUF_SIZE = 65536

def build_irods_path(os_path):
    irods_sub_path = ''
    if os_path.startswith(phengs_path_prefix):
        irods_sub_path = os_path[len(phengs_path_prefix):]

    irods_path = "%s%s" % (irods_path_prefix, irods_sub_path)
    return irods_path

            
def recursively_register_and_checksum(os_path, checksum_map, run_handle):

    env_file = os.path.expanduser('~/.irods/irods_environment.json')
    with iRODSSession(irods_env_file=env_file) as session:

        # create the collection if it does not exist
        irods_collection_path = build_irods_path(os_path)
        try: 
            collection_object = session.collections.get(irods_collection_path)
        except CollectionDoesNotExist:
            print('collection %s does not exist.  creating...' % irods_collection_path)
            session.collections.create(irods_collection_path)

        # recursively register the directory
        options = {kw.VERIFY_CHKSUM_KW: '', kw.DEST_RESC_NAME_KW: staging_resource, kw.COLLECTION_KW: ''}
        print('recursively registering %s as %s' % (os_path, irods_collection_path))
        session.data_objects.register(os_path, irods_collection_path, **options)

        for subdir, dirs, files in os.walk(os_path):
            for file in files:

                # calculate a checksum and store in dictionary with file as key
                filepath = os.path.join(subdir, file)
                irods_path = build_irods_path(filepath)

                sha256 = hashlib.sha256()
                
                with open(filepath, 'rb') as f:
                    while True:
                        data = f.read(BUF_SIZE)
                        if not data:
                            break
                        sha256.update(data)

                checksum_map[filepath] = sha256.hexdigest()

                # add metadata on the file for restoration
                args = ['stat', '--printf', '%04a,%U,%G,%x,%y', filepath]
                out, err = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
                s = str(out.decode('UTF-8')).split(',')
                
                # if owner or group are unknown, get numeric values
                if s[1] == 'UNKNOWN' or s[2] == 'UNKNOWN':
                    args = ['stat', '--printf', '%04a,%u,%g,%x,%y', filepath]
                    out, err = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
                    s = str(out.decode('UTF-8')).split(',')

                #print(s, file=open('/tmp/debug', 'a'))

                obj = session.data_objects.get(irods_path)
                try: 
                    obj.metadata.add("filesystem::run_handle", run_handle, '')
                except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
                    pass
                try: 
                    obj.metadata.add("filesystem::perms", s[0], '')
                except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
                    pass
                try: 
                    obj.metadata.add("filesystem::owner", s[1], '')
                except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
                    pass
                try: 
                    obj.metadata.add("filesystem::group", s[2], '')
                except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
                    pass
                try: 
                    obj.metadata.add("filesystem::atime", s[3], '')
                except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
                    pass
                try: 
                    obj.metadata.add("filesystem::mtime", s[4], '')
                except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
                    pass
                try: 
                    obj.metadata.add("filesystem::path", filepath, '')
                except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
                    pass


def recursively_replicate_and_trim(os_path):

    for root, dirs, files in os.walk(os_path):

        for name in files:

            full_os_path = os.path.join(root, name)
            irods_path = build_irods_path(full_os_path)

            print("""irule -F async_replicate_and_trim.r "*irods_path='%s'" """ % irods_path)
            os.system("""irule -F async_replicate_and_trim.r "*irods_path='%s'" """ % irods_path)

def do_register(run_handle):

    checksum_map = {}

    run_data_dir_filesystem = "%s/hpc_storage/run_data/%s" % (phengs_path_prefix, run_handle)
    machine_fastqs_dir_filesystem = "%s/hpc_storage/machine_fastqs/%s" % (phengs_path_prefix, run_handle)

    recursively_register_and_checksum(run_data_dir_filesystem, checksum_map, run_handle)
    recursively_register_and_checksum(machine_fastqs_dir_filesystem, checksum_map, run_handle)

    # open results_ngssample_dirs and register directories in it
    results_file = "%s/results_ngssample_dirs" % run_data_dir_filesystem

    with open(results_file) as f:
        for line in f:
            os_path = line.strip()
            recursively_register_and_checksum(os_path, checksum_map, run_handle)

            # replicate and trim, first make sure directory has g+rw so trim will work
            print("find %s -type d -print0 | xargs -0 chmod g+rw" % os_path)
            #os.system("find %s -type d -print0 | xargs -0 chmod g+rw" % os_path)

            recursively_replicate_and_trim(os_path)

    # replicate and trim run_data, first make sure directory has g+rw so trim will work
    print("find %s -type d -print0 | xargs -0 chmod g+rw" % run_data_dir_filesystem)
    os.system("find %s -type d -print0 | xargs -0 chmod g+rw" % run_data_dir_filesystem)
    recursively_replicate_and_trim(run_data_dir_filesystem)

    # replicate and trim fastqs, first make sure directory has g+rw so trim will work
    print("find %s -type d -print0 | xargs -0 chmod g+rw" % machine_fastqs_dir_filesystem)
    os.system("find %s -type d -print0 | xargs -0 chmod g+rw" % machine_fastqs_dir_filesystem)
    recursively_replicate_and_trim(machine_fastqs_dir_filesystem)

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

                stored_checksum = "".join("%02x" % b for b in bytearray(base64.b64decode(result[DataObject.checksum][5:])))

                if checksum_map[file_path] == stored_checksum:
                    print("Checksum validated for %s" % file_path)
                else:
                    validation_status = False
                    print("ERROR:  Checksum validation failed for %s: %s vs %s" % (file_path, checksum_map[file_path], stored_checksum))

            if found_file is False:
                validation_status = False
                print("ERROR: File %s was not found in archive..." % file_path)
              
 
    if validation_status is False:
        print("ERROR: Post replication validation failed for at least one file.")           
        sys.exit(1)
    else:
        print("Post replication validation succeeded.")           

    
if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError("Use: python register.py <run handle>")

    do_register(sys.argv[1])


