register {

    *src_resc_hier = 'defresc;resc1'
    *dst_resc_hier = 'defresc;resc2'

    *err = errormsg(msiregister_iterator(*src_resc_hier, *dst_resc_hier, 1), *msg)

    if (*err == 0) {
        writeLine("serverLog", "msiregister_iterator SUCCESS")
    } else {
        writeLine("serverLog", "msiregister_iterator ERROR [*err] - *msg")
    }

}
OUTPUT ruleExecOut

