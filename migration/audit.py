import sys, os, datetime
import os.path
from irods.session import iRODSSession
from irods.models import Collection, DataObject, DataObjectMeta
from irods.column import Criterion

phengs_path_prefix = '/phengs'
irods_path_prefix = '/PHE/home/ngsservicearchive/archived_files'

def build_irods_path(os_path):
    irods_sub_path = ''
    if os_path.startswith(phengs_path_prefix):
        irods_sub_path = os_path[len(phengs_path_prefix):]

    irods_path = "%s%s" % (irods_path_prefix, irods_sub_path)
    return irods_path

def get_metadata_value(session, coll_name, data_name, key):

    results = session.query(DataObject, DataObjectMeta).filter( \
            Criterion('=', Collection.name, coll_name)).filter( \
            Criterion('=', DataObject.name, data_name)).filter( \
            Criterion('=', DataObjectMeta.name, key))
    for r in results:
        return r[DataObjectMeta.value]
    return ''

def do_audit(run_handle):

    run_data_dir_filesystem = "%s/hpc_storage/run_data/%s" % (phengs_path_prefix, run_handle)
    machine_fastqs_dir_filesystem = "%s/hpc_storage/machine_fastqs/%s" % (phengs_path_prefix, run_handle)

    env_file = os.path.expanduser('~/.irods/irods_environment.json')
    with iRODSSession(irods_env_file=env_file) as session:

        # select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'filesystem::run_handle' and META_DATA_ATTR_VALUE = <run_handle>
        results = session.query(Collection.name, DataObject).filter( \
                Criterion('=', DataObjectMeta.name, 'filesystem::run_handle')).filter( \
                Criterion('=', DataObjectMeta.value, run_handle))

        for r in results:

            filesystem_path = get_metadata_value(session, r[Collection.name], r[DataObject.name], 'filesystem::path')
            if not os.path.exists(filesystem_path):
                print("%s does not exist" % filesystem_path)
            else:
                print("%s exists" % filesystem_path)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError("Use: python audit.py <run handle>")

    do_audit(sys.argv[1])
