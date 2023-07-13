/**
 * @file  register_replica.cpp
 *
 */

// =-=-=-=-=-=-=-
#include "apiHeaderAll.h"
#include "msParam.h"
#include "irods_ms_plugin.hpp"
#include "irods_file_object.hpp"
#include "irods_hierarchy_parser.hpp"
#include "rsRegReplica.hpp"

#define IRODS_QUERY_ENABLE_SERVER_SIDE_API

#define RODS_SERVER
#include "irods_query.hpp"
#include "query_builder.hpp"
#undef RODS_SERVER


// =-=-=-=-=-=-=-
#include <string>
#include <iostream>
#include <vector>

#include <boost/lexical_cast.hpp>


int register_replica(
    const std::string&      _src_resource_hierarchy,
    const std::string&      _dst_resource_hierarchy,
    const std::string&      _physical_path,
    const std::string&      _logical_path,
    ruleExecInfo_t* _rei ) {

    using std::cout;
    using std::endl;
    using std::string;

    if( !_rei ) {
        cout << "register_replica: null _rei parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    // get child resources
    std::string dst_child_resc;
    irods::hierarchy_parser parser;
    parser.set_string(_dst_resource_hierarchy.c_str());
    parser.last_resc(dst_child_resc);
    std::string src_child_resc;
    parser.set_string(_src_resource_hierarchy.c_str());
    parser.last_resc(src_child_resc);
    //printf("%s:%d (%s) [dst_child_resc=%s]\n", __FILE__, __LINE__, __FUNCTION__, dst_child_resc.c_str());

    // get resc_id
    std::string query = "select RESC_ID where RESC_NAME = '" + dst_child_resc + "'";

    rodsLong_t resc_id = 0;
    irods::experimental::query_builder qb;

    for (const auto& row : qb.build(*_rei->rsComm, query)) {
        try {
            resc_id = boost::lexical_cast<rodsLong_t>(row[0]);
        } catch (boost::bad_lexical_cast & e) {
            cout << "could not lexical cast resc_id to rodsLong_t" << endl;
            return SYS_INVALID_INPUT_PARAM;
        }
    }
    //printf("%s:%d (%s) [resc_id=%llu]\n", __FILE__, __LINE__, __FUNCTION__, resc_id);

    // get data_id, data_size, and max_repl_num
    boost::filesystem::path p(_logical_path.c_str());
    std::string collection_name = p.parent_path().string();
    std::string data_name = p.filename().string();
    query = "select DATA_ID, DATA_SIZE, DATA_REPL_NUM, RESC_NAME where COLL_NAME = '" + collection_name +
        "' AND DATA_NAME = '" + data_name + "'";

    int max_repl_num = 0;
    rodsLong_t data_id = 0;
    rodsLong_t data_size = 0;
    for (const auto& row : qb.build(*_rei->rsComm, query)) {
        try {
            int repl_num = boost::lexical_cast<int>(row[2]);
            if (repl_num > max_repl_num) {
                max_repl_num = repl_num;
            }
            data_id = boost::lexical_cast<rodsLong_t>(row[0]);
            if (src_child_resc == row[3]) {
                data_size = boost::lexical_cast<rodsLong_t>(row[1]);
            }
        } catch (boost::bad_lexical_cast & e) {
            cout << e.what() << endl;
            return SYS_INVALID_INPUT_PARAM;
        }
    }

    //printf("%s:%d (%s) [data_id=%lld][data_size=%llu][max_repl_num=%d]\n",__FILE__, __LINE__, __FUNCTION__, data_id, data_size, max_repl_num);

    // set the destination information
    dataObjInfo_t dst_data_obj;
    bzero( &dst_data_obj, sizeof( dst_data_obj ) );

    strncpy( dst_data_obj.objPath, _logical_path.c_str(), MAX_NAME_LEN );
    strncpy( dst_data_obj.rescName, dst_child_resc.c_str(), NAME_LEN );
    strncpy( dst_data_obj.rescHier, _dst_resource_hierarchy.c_str(), MAX_NAME_LEN );
    strncpy( dst_data_obj.dataType, "generic", NAME_LEN );

    dst_data_obj.dataSize = data_size;
    strncpy( dst_data_obj.filePath, _physical_path.c_str(), MAX_NAME_LEN );
    dst_data_obj.replNum    = max_repl_num+1;
    dst_data_obj.rescId = resc_id;
    dst_data_obj.replStatus = 0;
    dst_data_obj.dataId = data_id;
    dst_data_obj.dataMapId = 0;
    dst_data_obj.flags     = 0;

    // manufacture a src data obj
    dataObjInfo_t src_data_obj;
    memcpy( &src_data_obj, &dst_data_obj, sizeof( dst_data_obj ) );
    src_data_obj.replNum = 0;
    strncpy( src_data_obj.filePath, _physical_path.c_str(), MAX_NAME_LEN );
    strncpy( src_data_obj.rescHier, _src_resource_hierarchy.c_str(),  MAX_NAME_LEN );

    // =-=-=-=-=-=-=-
    // repl to an existing copy
    regReplica_t reg_inp;
    bzero( &reg_inp, sizeof( reg_inp ) );
    reg_inp.srcDataObjInfo  = &src_data_obj;
    reg_inp.destDataObjInfo = &dst_data_obj;
    addKeyVal(&reg_inp.condInput, IN_PDMO_KW, _dst_resource_hierarchy.c_str());
    addKeyVal(&reg_inp.condInput, ADMIN_KW, "");

    try {
        int status = rsRegReplica( _rei->rsComm, &reg_inp );
        //printf("%s:%d (%s) rsRegReplica returned: %d", __FILE__, __LINE__, __FUNCTION__, status);
        return status;
    } catch (irods::exception& e) {
        //printf("%s:%d (%s) rsRegReplica returned an error: %s", __FILE__, __LINE__, __FUNCTION__, e.what());
        return e.code();
    }

    return 0;

}


