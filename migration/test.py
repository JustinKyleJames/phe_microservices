import sys, os, datetime
from irods.session import iRODSSession
from irods.models import Collection, DataObject, DataObjectMeta
from irods.column import Criterion

def get_metadata_value(session, coll_name, data_name, key):

    results = session.query(DataObject, DataObjectMeta).filter( \
            Criterion('=', Collection.name, coll_name)).filter( \
            Criterion('=', DataObject.name, data_name)).filter( \
            Criterion('=', DataObjectMeta.name, key))
    for r in results:
        return r[DataObjectMeta.value]
    return ''

def doit(run_handle):

    env_file = os.path.expanduser('~/.irods/irods_environment.json')
    with iRODSSession(irods_env_file=env_file) as session:

        # select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'filesystem::run_handle' and META_DATA_ATTR_VALUE = <run_handle>
        results = session.query(Collection.name, DataObject).filter( \
                Criterion('=', DataObjectMeta.name, 'filesystem::run_handle')).filter( \
                Criterion('=', DataObjectMeta.value, run_handle))

        for r in results:

            # get filesystem attributes
            filesystem_path = get_metadata_value(session, r[Collection.name], r[DataObject.name], 'filesystem::path')

            print(r[Collection.name] + '/' + r[DataObject.name], filesystem_path)

if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError("Use: python test.py <run handle>")

    doit(sys.argv[1])
