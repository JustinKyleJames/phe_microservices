/**
 * @file  libmsisync_to_archive.cpp
 *
 */

// =-=-=-=-=-=-=-
#include "apiHeaderAll.h"
#include "msParam.h"
#include "irods_ms_plugin.hpp"
#include "irods_file_object.hpp"
#include "irods_hierarchy_parser.hpp"
#include "rsRegReplica.hpp"

#define RODS_SERVER
#include "irods_query.hpp"
#include "query_builder.hpp"
#undef RODS_SERVER


// =-=-=-=-=-=-=-
#include <string>
#include <iostream>
#include <vector>

#include <boost/lexical_cast.hpp>


int rsGenQuery(rsComm_t*, genQueryInp_t*, genQueryOut_t**);


/**
 * \fn msiregister_replica(msParam_t* _resource_hierarchy, msParam_t* _physical_path, msParam_t* _logical_path, ruleExecInfo_t *rei)
 *
 * \brief   This microservice registers a replica
 *
 * \since 4.2.8
 *
 * \param[in] _src_resource_hierarchy - The semicolon delimited string designating the source replica's location
 * \param[in] _dst_resource_hierarchy - The semicolon delimited string designating the destination replica's location
 * \param[in] _physical_path - The physical path of the to-be-created replica
 * \param[in] _logical_path - The logical path of the dataObject to be replicated
 * \param[in,out] rei - The RuleExecInfo structure that is automatically
 *    handled by the rule engine. The user does not include rei as a
 *    parameter in the rule invocation.
 *
 * \DolVarDependence none
 * \DolVarModified none
 * \iCatAttrDependence none
 * \iCatAttrModified none
 * \sideeffect none
 *
 * \return integer
 * \retval 0
 * \pre none
 * \post none
 * \sa none
 **/
int msiregister_replica(
    msParam_t*      _src_resource_hierarchy,
    msParam_t*      _dst_resource_hierarchy,
    msParam_t*      _physical_path,
    msParam_t*      _logical_path,
    ruleExecInfo_t* _rei ) {

    using std::cout;
    using std::endl;
    using std::string;

    char *src_resource_hierarchy = parseMspForStr( _src_resource_hierarchy );
    if( !_src_resource_hierarchy ) {
        cout << "msisync_to_archive - null _src_resc_hier parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    char *dst_resource_hierarchy = parseMspForStr( _dst_resource_hierarchy );
    if( !_dst_resource_hierarchy ) {
        cout << "msisync_to_archive - null _dst_resc_hier parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    char *physical_path = parseMspForStr( _physical_path );
    if( !physical_path ) {
        cout << "msisync_to_archive - null _physical_path parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    char *logical_path = parseMspForStr( _logical_path );
    if( !logical_path ) {
        cout << "msisync_to_archive - null _logical_path parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    if( !_rei ) {
        cout << "msisync_to_archive - null _rei parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    printf("%s:%d (%s) [src_resource_hierarchy=%s][dst_resource_hierarchy=%s][physical_path=%s][logical_path=%s]\n", __FILE__, __LINE__, __FUNCTION__, src_resource_hierarchy, dst_resource_hierarchy, physical_path, logical_path);

    // get child resources
    std::string dst_child_resc;
    irods::hierarchy_parser parser;
    parser.set_string(dst_resource_hierarchy);
    parser.last_resc(dst_child_resc);
    std::string src_child_resc;
    parser.set_string(src_resource_hierarchy);
    parser.last_resc(src_child_resc);
    printf("%s:%d (%s) [dst_child_resc=%s]\n", __FILE__, __LINE__, __FUNCTION__, dst_child_resc.c_str());

    // get resc_id
	std::string query = "select RESC_ID where RESC_NAME = '" + dst_child_resc + "'";

    rodsLong_t resc_id = 0;
	irods::experimental::query_builder qb;

    //rsComm_t& rsComm = *(_rei->rsComm);
	for (const auto& row : qb.build(*_rei->rsComm, query)) {
        try {
            resc_id = boost::lexical_cast<rodsLong_t>(row[0]);
        } catch (boost::bad_lexical_cast & e) {
            cout << "could not lexical cast resc_id to rodsLong_t" << endl;
            return SYS_INVALID_INPUT_PARAM;
        }
	}
    printf("%s:%d (%s) [resc_id=%llu]\n", __FILE__, __LINE__, __FUNCTION__, resc_id);

    boost::filesystem::path p(logical_path);
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

    printf("%s:%d (%s) [data_id=%lld][data_size=%llu][max_repl_num=%d]\n", __FILE__, __LINE__, __FUNCTION__, data_id, data_size, max_repl_num);

    // set the destination information
    dataObjInfo_t dst_data_obj;
    bzero( &dst_data_obj, sizeof( dst_data_obj ) );

    strncpy( dst_data_obj.objPath, logical_path, MAX_NAME_LEN );
    strncpy( dst_data_obj.rescName, dst_child_resc.c_str(), NAME_LEN );
    strncpy( dst_data_obj.rescHier, dst_resource_hierarchy, MAX_NAME_LEN );
    strncpy( dst_data_obj.dataType, "generic", NAME_LEN );

    dst_data_obj.dataSize = data_size;
    strncpy( dst_data_obj.filePath, physical_path, MAX_NAME_LEN );
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
    strncpy( src_data_obj.filePath, physical_path, MAX_NAME_LEN );
    strncpy( src_data_obj.rescHier, src_resource_hierarchy,  MAX_NAME_LEN );

    // =-=-=-=-=-=-=-
    // repl to an existing copy
    regReplica_t reg_inp;
    bzero( &reg_inp, sizeof( reg_inp ) );
    reg_inp.srcDataObjInfo  = &src_data_obj;
    reg_inp.destDataObjInfo = &dst_data_obj;
    addKeyVal(&reg_inp.condInput, IN_PDMO_KW, dst_resource_hierarchy);

    int status = rsRegReplica( _rei->rsComm, &reg_inp );
    if( status < 0 ) {
        return status;
    }


    return 0;

}

extern "C"
irods::ms_table_entry* plugin_factory() {
    irods::ms_table_entry* msvc = new irods::ms_table_entry(4);
    msvc->add_operation<
        msParam_t*,
        msParam_t*,
        msParam_t*,
        msParam_t*,
        ruleExecInfo_t*>("msiregister_path",
                         std::function<int(
                             msParam_t*,
                             msParam_t*,
                             msParam_t*,
                             msParam_t*,
                             ruleExecInfo_t*)>(msiregister_replica));
    return msvc;
}
