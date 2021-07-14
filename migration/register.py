import sys, os, time, hashlib, subprocess, base64, re, glob, argparse, textwrap
from irods.session import iRODSSession
from irods.models import Collection, DataObject, DataObjectMeta, Resource
from irods.column import Criterion
from irods.meta import iRODSMeta
from irods.exception import CollectionDoesNotExist
from irods.exception import CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME 
from enum import Enum
import irods.keywords as kw

phengs_path_prefix = '/phengs'
#phengs_path_prefix = '/var/lib/irods/phengs'
irods_path_prefix = '/PHE/home/ngsservicearchive/archived_files'
#irods_path_prefix = '/tempZone/home/rods/archived_files'
staging_resource = 'lustre_staging_resc'

class OperationalMode(Enum):
    LEGACY = 1
    FTP_ONLY = 2
    BOTH = 3


repl_trim_rule_text = """
    delay("<PLUSET>1s</PLUSET>") {
        # Run Handle: XXX 
        writeLine("stdout", "replicating *irods_path")
        # replicate and trim
        *err = errormsg(msiDataObjRepl("*irods_path", "rescName=lustre_staging_resc++++destRescName=s3_resc++++irodsAdmin=++++verifyChksum=",*out), *msg)
        if (*err != 0) {
            writeLine("serverLog", "repl failed for *irods_path")
            failmsg(*err, *msg)
        } else {
            *err = errormsg(msiDataObjTrim("*irods_path", "lustre_staging_resc", "null", "1", "ADMIN", *out), *msg)
            if (*err != 0) {
                writeLine("serverLog", "trim failed for *irods_path")
                failmsg(*err, *msg)
            }
        }
    }
"""
repl_only_rule_text = """
    delay("<PLUSET>1s</PLUSET>") {
        # Run Handle: XXX 
        writeLine("stdout", "replicating *irods_path")
        # replicate
        *err = errormsg(msiDataObjRepl("*irods_path", "rescName=lustre_staging_resc++++destRescName=s3_resc++++irodsAdmin=++++verifyChksum=",*out), *msg)
        if (*err != 0) {
            writeLine("serverLog", "repl failed for *irods_path")
            failmsg(*err, *msg)
        }
    }
"""

BUF_SIZE = 65536

def build_irods_path(os_path):
    irods_sub_path = ''
    if os_path.startswith(phengs_path_prefix):
        irods_sub_path = os_path[len(phengs_path_prefix):]

    irods_path = "%s%s" % (irods_path_prefix, irods_sub_path)
    return irods_path

def read_path_metadata_store_to_irods(os_path, session, run_handle, is_file, error_file, ftp_root_files):

        irods_path = build_irods_path(os_path)

        # add metadata on the directory for restoration
        args = ['stat', '--printf', '%04a,%U,%G,%x,%y', os_path]
        out, err = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        s = str(out.decode('UTF-8')).split(',')
        
        # if owner or group are unknown, get numeric values
        if s[1] == 'UNKNOWN' or s[2] == 'UNKNOWN':
            args = ['stat', '--printf', '%04a,%u,%g,%x,%y', os_path]
            out, err = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            s = str(out.decode('UTF-8')).split(',')

        try:
            if is_file:
                obj = session.data_objects.get(irods_path)
            else:
                obj = session.collections.get(irods_path)

            try: 
                if ftp_root_files:
                    obj.metadata.add("filesystem::ftp_root::run_handle", run_handle, '')
                else:
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
                obj.metadata.add("filesystem::path", os_path, '')
            except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
                pass

        except:
            print('WARNING: Path %s not registered in iRODS.  Skipping.' % irods_path)
            error_file.write('WARNING: Path %s not registered in iRODS.  Skipping.' % irods_path)
            
def recursively_register_and_checksum(os_path, checksum_map, run_handle, error_file, ftp_root_files = False):

    if not os.path.isdir(os_path):
        print('WARNING: Could not register path %s.  Path is missing from source.' % os_path)
        error_file.write('WARNING: Could not register path %s.  Path is missing from source.\n' % os_path)
        return

    # make sure directory has g+rw so register and trim will work
    os.system("find %s -type d -print0 | xargs -0 chmod g+rw" % os_path)
        
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

        try:
            session.data_objects.register(os_path, irods_collection_path, **options)
        except:
            print('WARNING: Error when registering path %s.  Path will be skipped.' % os_path)
            error_file.write('WARNING: Error when registering path %s.  Path will be skipped.\n' % os_path)
            return

        # read and store metadata for the root of the tree
        read_path_metadata_store_to_irods(os_path, session, run_handle, False, error_file, ftp_root_files)

        for subdir, dirs, files in os.walk(os_path):

            for fname in files:

                # calculate a checksum and store in dictionary with file as key
                filepath = os.path.join(subdir, fname)
                irods_path = build_irods_path(filepath)

                # unregister and skip core files
                if re.match(r'^core$|^core.[0-9]+$', fname):
                    try:
                        session.data_objects.unregister(irods_path)
                    except:
                        # if unregister fails just do nothing as it probably wasn't registered
                        pass
                    continue

                sha256 = hashlib.sha256()
                
                with open(filepath, 'rb') as f:
                    while True:
                        data = f.read(BUF_SIZE)
                        if not data:
                            break
                        sha256.update(data)

                checksum_map[filepath] = sha256.hexdigest()

                read_path_metadata_store_to_irods(filepath, session, run_handle, True, error_file, ftp_root_files)

            for dirname in dirs:

                dirpath = os.path.join(subdir, dirname)
                irods_path = build_irods_path(dirpath)
                read_path_metadata_store_to_irods(dirpath, session, run_handle, False, error_file, ftp_root_files)


def recursively_replicate_and_trim(os_path, run_handle, ftp_root_files = False):

    env_file = os.path.expanduser('~/.irods/irods_environment.json')
    with iRODSSession(irods_env_file=env_file) as session:

        for root, dirs, files in os.walk(os_path):

            for fname in files:

                full_os_path = os.path.join(root, fname)
                irods_path = build_irods_path(full_os_path)

                # skip core files
                if fname == 'core':
                    continue

                if ftp_root_files:
                    rule_text = repl_only_rule_text.replace('XXX', run_handle)
                else:
                    rule_text = repl_trim_rule_text.replace('XXX', run_handle)

                os.system("""irule '{rule_text}' "*irods_path={irods_path}" ruleExecOut""".format(**locals()))

def do_register(run_handle, operational_mode):

    error_file = '%s_errors.log' % run_handle
    error_file = error_file.replace('/', '_')
    with open(error_file, 'w') as error_file:

        checksum_map = {}

        if operational_mode is OperationalMode.FTP_ONLY or operational_mode is OperationalMode.BOTH:

            ftp_root_dirs = glob.glob('%s/hpc_storage/ftp_root/*/*/%s' % (phengs_path_prefix, run_handle))
            for ftp_root_dir in ftp_root_dirs:
                recursively_register_and_checksum(ftp_root_dir, checksum_map, run_handle, error_file, True)
                recursively_replicate_and_trim(ftp_root_dir, run_handle, True)                          # replicate without trim 

                # create the ftp_root_backed_up file
                os.system("touch %s/ftp_root_backed_up" % ftp_root_dir)


        if operational_mode is OperationalMode.LEGACY or operational_mode is OperationalMode.BOTH:

            run_data_dir_filesystem = "%s/hpc_storage/run_data/%s" % (phengs_path_prefix, run_handle)
            machine_fastqs_dir_filesystem = "%s/hpc_storage/machine_fastqs/%s" % (phengs_path_prefix, run_handle)

            # remove the restore_from_archive and written_to_archive files, ignore error if they do not exist
            os.system("rm %s/restore_from_archive 2>/dev/null" % run_data_dir_filesystem)
            os.system("rm %s/written_to_archive 2>/dev/null" % run_data_dir_filesystem)

            recursively_register_and_checksum(run_data_dir_filesystem, checksum_map, run_handle, error_file)
            recursively_register_and_checksum(machine_fastqs_dir_filesystem, checksum_map, run_handle, error_file)

            # open results_ngssample_dirs and register directories in it
            results_file = "%s/results_ngssample_dirs" % run_data_dir_filesystem

            try:
                with open(results_file) as f:
                    for line in f:
                        os_path = line.strip()

                        # register
                        recursively_register_and_checksum(os_path, checksum_map, run_handle, error_file)

                        # replicate and trim
                        recursively_replicate_and_trim(os_path, run_handle)
            except IOError as e:
                print('WARNING:  No results_ngssample_dirs file found.')
                error_file.write('WARNING:  No results_ngssample_dirs file found.\n')

            # replicate and trim run_data
            recursively_replicate_and_trim(run_data_dir_filesystem, run_handle)

            # replicate and trim fastqs
            recursively_replicate_and_trim(machine_fastqs_dir_filesystem, run_handle)

            # create the written_to_archive file
            os.system("touch %s/written_to_archive" % run_data_dir_filesystem)

        # do post verification

        # wait for all rules to complete
        active_rules = int(subprocess.check_output(['iquest', '%s', "select COUNT(RULE_EXEC_ID) where RULE_EXEC_USER_NAME = 'ngsservicearchive' and RULE_EXEC_NAME like '%{run_handle}%'".format(**locals())]).strip())
        while active_rules > 0:
            print("Waiting for replication jobs to complete.  Job count = %d" % active_rules)
            time.sleep(5)
            active_rules = int(subprocess.check_output(['iquest', '%s', "select COUNT(RULE_EXEC_ID) where RULE_EXEC_USER_NAME = 'ngsservicearchive' and RULE_EXEC_NAME like '%{run_handle}%'".format(**locals())]).strip())

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
                        pass
                        #print("Checksum validated for %s" % file_path)
                    else:
                        validation_status = False
                        print("ERROR:  Checksum validation failed for %s: %s vs %s" % (file_path, checksum_map[file_path], stored_checksum))
                        error_file.write("ERROR:  Checksum validation failed for %s: %s vs %s\n" % (file_path, checksum_map[file_path], stored_checksum))

                if found_file is False:
                    validation_status = False
                    print("ERROR: File %s was not found in archive..." % file_path)
                    error_file.write("ERROR: File %s was not found in archive...\n" % file_path)

        if validation_status is False:
            print("ERROR: Post replication validation failed for at least one file.")           
            error_file.write("ERROR: Post replication validation failed for at least one file.\n")           
            sys.exit(1)
        else:
            print("Post replication validation succeeded.")           

    
if __name__ == "__main__":


    parser = argparse.ArgumentParser(description=textwrap.dedent("""Register files into iRODS, replicate to archive, and
                                                             optionally trim original files.

                                                             The default behavior is to archive ftp_root;
                                                             archive and trim the run_data, machine_fastqs and 
                                                             results directories"""))
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--ftp-only', action='store_const', help='archive only the ftp_root directory', dest='option', const='ftp_only')
    group.add_argument('--exclude-ftp', action='store_const', help='archive and trim the run_data, machine_fastqs and result directories', dest='option', const='exclude_ftp')
    #group.add_argument('--both', action='store_true', help='archive ftp_root; archive and trim the run_data, machine_fastqs and results directories')
    parser.add_argument('run_handle', help='the run handle')
    
    args = parser.parse_args()

    operational_mode = OperationalMode.BOTH
    if args.option == 'ftp_only':
        operational_mode = OperationalMode.FTP_ONLY
    elif args.option == 'exclude_ftp':
        operational_mode = OperationalMode.LEGACY # run_data and machine_fastqs

    #print(args.option)
    #print(operational_mode)

    do_register(args.run_handle, operational_mode)
