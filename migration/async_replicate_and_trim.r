replicate {

    delay("<PLUSET>1s</PLUSET>") {
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
}
INPUT *irods_path = ""
OUTPUT ruleExecOut
