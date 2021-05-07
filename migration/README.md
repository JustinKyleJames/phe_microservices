# Migration and Restoration Instructions

These instructions were tested on irodscol01 using the root account.  This account is configured to connect to iRODS using the ngsservicearchive account.

## Migration

Run the migration process:

```
python register.py <run_handle>   # example run_handle: miseq/M01481/160321_M01481_0183_000000000-ANC58
```

## Restoration

Run the restoration process:

```
python restore.py <run_handle>   # example run_handle: miseq/M01481/160321_M01481_0183_000000000-ANC58
```

Audit the restoration results:

```
python audit.py <run_handle>   # example run_handle: miseq/M01481/160321_M01481_0183_000000000-ANC58
```

## Other Operations 

### Re-run migration without full restoration

If the migration needs to be re-run without full restoration, the results_ngssample_dirs file needs to be restored from iRODS.

Find the results_ngssample_dirs file in iRODS by querying for the filesystem::run_handle = <run_handle> metadata:

```
$ iquest "%s/%s" "select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'filesystem::run_handle' and META_DATA_ATTR_VALUE = '<run_handle>' and DATA_NAME = 'results_ngssample_dirs'"
/PHE/home/ngsservicearchive/archived_files/hpc_storage/run_data/<run_handle>/results_ngssample_dirs
```

Using the iRODS path returned above, find the restoration location for this file (value for attribute filesystem::path):

```
imeta ls -d /PHE/home/ngsservicearchive/archived_files/hpc_storage/run_data/<run_handle>/results_ngssample_dirs
AVUs defined for dataObj /PHE/home/ngsservicearchive/archived_files/hpc_storage/run_data/<run_handle>/results_ngssample_dirs:
attribute: filesystem::atime
value: 2021-05-07 14:21:07.000000000 +0100
units: 
----
attribute: filesystem::group
value: 1001
units: 
----
attribute: filesystem::mtime
value: 2016-03-01 17:57:57.000000000 +0000
units: 
----
attribute: filesystem::owner
value: 527
units: 
----
attribute: filesystem::path
value: /phengs/hpc_storage/run_data/<run_handle>/results_ngssample_dirs
units: 
----
attribute: filesystem::perms
value: 0664
units: 
----
attribute: filesystem::run_handle
value: <run_handle> 
units: 
----
attribute: irods::access_time
value: 1620393666
units: 

```

Use the iRODS path and restoration location to restore the file:

```
iget /PHE/home/ngsservicearchive/archived_files/hpc_storage/run_data/<run_handle>/results_ngssample_dirs /phengs/hpc_storage/run_data/<run_handle>/results_ngssample_dirs
```

Run the migration:

```
python register.py <run_handle>   # example run_handle: miseq/M01481/160321_M01481_0183_000000000-ANC58
```

### Query all objects in iRODS for the run_handle

To query for the objects for the run handle, do a metadata search:

```
iquest "%s/%s" "select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'filesystem::run_handle' and META_DATA_ATTR_VALUE = '<run_handle>'"
```
