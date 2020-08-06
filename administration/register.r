register {

    *src_resc_hier = 'defresc;resc1'
    *dst_resc_hier = 'defresc;resc2'

    # get the src leaf resource name
    *resc_hier_parts = split(*src_resc_hier, ';')
    *src_resc_name = elem(*resc_hier_parts, size(*resc_hier_parts) - 1)

    # get the dst leaf resource name
    *resc_hier_parts = split(*dst_resc_hier, ';')
    *dst_resc_name = elem(*resc_hier_parts, size(*resc_hier_parts) - 1)

    # get all objects at src
    *results = SELECT COLL_NAME, DATA_NAME where RESC_NAME = '*src_resc_name'
    foreach (*row in *results) {

        *coll_name = *row.COLL_NAME
        *data_name = *row.DATA_NAME
       
        # determine if object from src is in dst 
        *results2 = select COUNT(DATA_NAME) where COLL_NAME = '*coll_name' and DATA_NAME = '*data_name' and RESC_NAME = '*dst_resc_name'
        foreach (*row2 in *results2) {
            *count = *row2.DATA_NAME
        }

        if (*count == "0") {
        
            *results3 = SELECT DATA_PATH WHERE COLL_NAME = '*coll_name' AND DATA_NAME = '*data_name' AND RESC_NAME = '*src_resc_name';
            foreach (*row3 in *results3) {
                *phypath = *row3.DATA_PATH
                writeLine("stdout",*phypath)
            }
       
            writeLine("stdout", "msiregister_replica(*src_resc_hier, *dst_resc_hier, *phypath, *coll_name/*data_name)")
            *err = errormsg(msiregister_replica(*src_resc_hier, *dst_resc_hier, *phypath, "*coll_name/*data_name"), *msg)

            if (*err == 0) {
                writeLine("serverLog", "DO_REGISTER *logical_path SUCCESS [*err]")
            } else {
                writeLine("serverLog", "DO_REGISTER *logical_path ERROR [*err] - *msg")
            }

        } else {
            writeLine("stdout", "*coll_name/*data_name already registered. skip it.")
        }
    }
}
OUTPUT ruleExecOut

