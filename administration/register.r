register {

    writeLine("stdout", "*logical_path")

    #*src_resc_hier = 'wosResc1;wosArchiveResc1'
    #*dst_resc_hier = 'wosResc2;wosArchiveResc2'
    *src_resc_hier = 'replresc;resc2'
    *dst_resc_hier = 'replresc;resc1'
    *coll_name = trimr(*logical_path, '/')
    *dir_parts = split(*logical_path, '/')
    *data_name = elem(*dir_parts, size(*dir_parts)-1)

    # get the leaf resource name
    *resc_hier_parts = split(*src_resc_hier, ';')
    *src_resc_name = elem(*resc_hier_parts, size(*resc_hier_parts) - 1)

    *results = SELECT DATA_PATH WHERE COLL_NAME = '*coll_name' AND DATA_NAME = '*data_name' AND RESC_NAME = '*src_resc_name';
    foreach (*row in *results) {
        *phypath = *row.DATA_PATH
    }

    *err = errormsg(msiregister_replica(*src_resc_hier, *dst_resc_hier, *phypath, "*coll_name/*data_name"), *msg)
    if (*err == 0) {
        writeLine("serverLog", "PHE_REGISTER *logical_path SUCCESS [*err]")
    } else {
        writeLine("serverLog", "PHE_REGISTER *logical_path ERROR [*err] - *msg")
    }
}
INPUT *logical_path = '/tempZone/home/rods/foo'
OUTPUT ruleExecOut

