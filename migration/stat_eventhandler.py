# /var/lib/irods/stat_eventhandler.py
from irods_capability_automated_ingest.core import Core
from irods_capability_automated_ingest.utils import Operation
from irods.meta import iRODSMeta
import subprocess
import grp
import os

class event_handler(Core):
    @staticmethod
    def to_resource(session, meta, **options):
        return "lustre_staging_resc"
    #@staticmethod
    #def operation(session, meta, **options):
    #    return Operation.
    @staticmethod
    def post_data_obj_create(hdlr_mod, logger, session, meta, **options):
        args = ['stat', '--printf', '%04a,%U,%G,%x,%y', meta['path']]
        out, err = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
        s = str(out.decode('UTF-8')).split(',')

        # if owner or group are unknown, get numeric values
        if s[1] == 'UNKNOWN' or s[2] == 'UNKNOWN':
            args = ['stat', '--printf', '%04a,%u,%g,%x,%y', meta['path']]
            out, err = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            s = str(out.decode('UTF-8')).split(',')

        print(s, file=open('/tmp/debug', 'a'))

        obj = session.data_objects.get(meta['target'])
        obj.metadata.add("filesystem::perms", s[0], '')
        obj.metadata.add("filesystem::owner", s[1], '')
        obj.metadata.add("filesystem::group", s[2], '')
        obj.metadata.add("filesystem::atime", s[3], '')
        obj.metadata.add("filesystem::mtime", s[4], '')
        obj.metadata.add("filesystem::path", meta['path'], '')
        session.cleanup()

        ## if necessary add irods to group (s[2])
        #name, passwd, num, members = grp.getgrnam(s[2])
        #if "irods" not in members:
        #    command = "usermod -a -G %s irods" % s[2]
        #    os.system(command)
        #print (meta, file=open('/tmp/debug', 'w'))
        os.system("ichksum -M %s" % meta['target'])

