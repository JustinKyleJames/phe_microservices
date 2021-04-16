import sys, os, datetime
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

def restore_to_lustre(session, coll_name, data_name, restore_location, atime, mtime, owner, perms, group):

    code = os.WEXITSTATUS(os.system("iget -fK %s/%s %s" % (coll_name, data_name, restore_location)))
    if code != 0:
        # try again
        code = os.WEXITSTATUS(os.system("iget -fK %s/%s %s" % (coll_name, data_name, restore_location)))
        if code != 0:
            print >> sys.stderr, "Failed twice to get %s/%s" % (coll_name, data_name)

    # restore atime
    os.system("touch -a -d '%s' %s" % (atime, restore_location))

    # restore mtime
    os.system("touch -m -d '%s' %s" % (mtime, restore_location))

    # change owner/group
    os.system("chown %s:%s %s" % (owner, group, restore_location))

    # change access
    os.system("chmod %s %s" % (perms, restore_location))


def do_restore(run_handle):

    run_data_dir_filesystem = "%s/hpc_storage/run_data/%s" % (phengs_path_prefix, run_handle)
    machine_fastqs_dir_filesystem = "%s/hpc_storage/machine_fastqs/%s" % (phengs_path_prefix, run_handle)

    env_file = os.path.expanduser('~/.irods/irods_environment.json')
    with iRODSSession(irods_env_file=env_file) as session:

        # select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'filesystem::run_handle' and META_DATA_ATTR_VALUE = <run_handle>
        results = session.query(Collection.name, DataObject).filter( \
                Criterion('=', DataObjectMeta.name, 'filesystem::run_handle')).filter( \
                Criterion('=', DataObjectMeta.value, run_handle))

        for r in results:

            # get filesystem attributes
            filesystem_path = get_metadata_value(session, r[Collection.name], r[DataObject.name], 'filesystem::path')
            atime = get_metadata_value(session, r[Collection.name], r[DataObject.name], 'filesystem::atime')
            mtime = get_metadata_value(session, r[Collection.name], r[DataObject.name], 'filesystem::mtime')
            owner = get_metadata_value(session, r[Collection.name], r[DataObject.name], 'filesystem::owner')
            perms = get_metadata_value(session, r[Collection.name], r[DataObject.name], 'filesystem::perms')
            group = get_metadata_value(session, r[Collection.name], r[DataObject.name], 'filesystem::group')

            print(r[Collection.name], r[DataObject.name], filesystem_path, atime, mtime, owner, perms, group)
            restore_to_lustre(session, r[Collection.name], r[DataObject.name], filesystem_path, atime, mtime, owner, perms, group)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError("Use: python restore.py <run handle>")

    do_restore(sys.argv[1])

