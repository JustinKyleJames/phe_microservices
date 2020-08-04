/**
 * @file  libmsisync_to_archive.cpp
 *
 */


// =-=-=-=-=-=-=-
#include "apiHeaderAll.h"
#include "msParam.h"
#include "irods_ms_plugin.hpp"
#include "irods_file_object.hpp"
#include "irods_resource_redirect.hpp"
#include "irods_hierarchy_parser.hpp"
#include "genQuery.h"
#include "irods_plugin_context.hpp"
#include "irods_re_plugin.hpp"
#include "irods_re_serialization.hpp"
#include "irods_re_ruleexistshelper.hpp"
#include "irods_server_properties.hpp"
#include "irods_resource_constants.hpp"
#include "irods_collection_object.hpp"
#include "rsModAccessControl.hpp"
#include "objMetaOpr.hpp"
#include "rsGenQuery.hpp"
#include "rsRegReplica.hpp"


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
 * \module microservice
 *
 * \since 4.1.8
 *
 * \usage See clients/icommands/test/rules/
 *
 * \param[in] _resource_hierarchy - The semicolon delimited string designating the source replica's location
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

    // get the root resc of the hier
    std::string child_resc;
    irods::hierarchy_parser parser;
    parser.set_string(dst_resource_hierarchy);
    parser.last_resc( child_resc );
    printf("%s:%d (%s) [child_resc=%s]\n", __FILE__, __LINE__, __FUNCTION__, child_resc.c_str());

    // ##### select RESC_ID where RESC_NAME = '' #####
    genQueryOut_t* gen_out = nullptr;
    genQueryInp_t gen_inp;
    memset(&gen_inp, 0, sizeof(gen_inp));

    gen_inp.maxRows = 1; // need to check for one or more

    std::string query_string("='");
    query_string += child_resc;
    query_string += "'";

    addInxVal(&gen_inp.sqlCondInp, COL_R_RESC_NAME, query_string.c_str());
    addInxIval(&gen_inp.selectInp, COL_R_RESC_ID, 1);

    printf("%s:%d (%s) [query_string=%s]\n", __FILE__, __LINE__, __FUNCTION__, query_string.c_str());

    // execute the query
    int status = rsGenQuery(_rei->rsComm, &gen_inp, &gen_out);
    printf("%s:%d (%s) [rsGenQuery status =%d]\n", __FILE__, __LINE__, __FUNCTION__, status);
    if (status < 0 || nullptr == gen_out || CAT_NO_ROWS_FOUND == status) {
        cout << "could not get resource_id from resource_name" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }
    sqlResult_t* results = getSqlResultByInx(gen_out, COL_R_RESC_ID);
    rodsLong_t resc_id;
    try {
        std::string resc_id_str(&results->value[0],  results->len);
        printf("%s:%d (%s) [resc_id_str=%s]\n", __FILE__, __LINE__, __FUNCTION__, resc_id_str.c_str());
        resc_id = boost::lexical_cast<rodsLong_t>(resc_id_str.c_str());
    } catch (boost::bad_lexical_cast & e) {
        cout << "could not lexical cast resource_id to rodsLong_t" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    printf("%s:%d (%s) [resc_id=%lld]\n", __FILE__, __LINE__, __FUNCTION__, resc_id);

    // END ##### select RESC_ID where RESC_NAME = '' #####

    // ##### select DATA_ID, DATA_SIZE, max(REPL_NUMBER) where COLL_NAME = '' and DATA_NAME = '' #####
    boost::filesystem::path p(logical_path);
    std::string collection_name = p.parent_path().string();
    std::string data_name = p.filename().string();
    printf("%s:%d (%s) [collection_name=%s][data_name=%s]\n", __FILE__, __LINE__, __FUNCTION__, collection_name.c_str(), data_name.c_str());
    gen_out = nullptr;
    memset(&gen_inp, 0, sizeof(gen_inp));

    gen_inp.maxRows = 1; // need to check for one or more

    std::string query_string1("='");
    query_string1 += collection_name;
    query_string1 += "'";

    std::string query_string2("='");
    query_string2 += data_name;
    query_string2 += "'";
    printf("%s:%d (%s) [query_string1=%s][query_string2=%s]\n", __FILE__, __LINE__, __FUNCTION__, query_string1.c_str(), query_string2.c_str());

    addInxVal(&gen_inp.sqlCondInp, COL_COLL_NAME, query_string1.c_str());
    addInxVal(&gen_inp.sqlCondInp, COL_DATA_NAME, query_string2.c_str());
    addInxIval(&gen_inp.selectInp, COL_D_DATA_ID, 1);
    addInxIval(&gen_inp.selectInp, COL_DATA_SIZE, 1);
    addInxIval(&gen_inp.selectInp, COL_DATA_REPL_NUM, SELECT_MAX);

    // execute the query
    status = rsGenQuery(_rei->rsComm, &gen_inp, &gen_out);
    printf("%s:%d (%s) [rsGenQuery status =%d]\n", __FILE__, __LINE__, __FUNCTION__, status);
    if (status < 0 || nullptr == gen_out || CAT_NO_ROWS_FOUND == status) {
        cout << "could not get data_id, data_size, and max(data_repl_num) object" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }
    sqlResult_t* results_data_id = getSqlResultByInx(gen_out, COL_D_DATA_ID);
    sqlResult_t* results_data_size = getSqlResultByInx(gen_out, COL_DATA_SIZE);
    sqlResult_t* results_data_repl_num = getSqlResultByInx(gen_out, COL_DATA_REPL_NUM);
    printf("%s:%d (%s) RESULTS [%s][%s][%s]\n", __FILE__, __LINE__, __FUNCTION__, &(results_data_id->value[0]), &(results_data_size->value[0]), &(results_data_repl_num->value[0]));
    int data_id, max_repl_num;
    rodsLong_t data_size;
    try {
        std::string data_id_str(&results_data_id->value[0],  results_data_id->len);
        std::string data_size_str(&results_data_size->value[0],  results_data_size->len);
        std::string max_repl_num_str(&results_data_repl_num->value[0],  results_data_repl_num->len);
        data_id = boost::lexical_cast<int>(data_id_str.c_str());
        data_size = boost::lexical_cast<rodsLong_t>(data_size_str.c_str());
        max_repl_num = boost::lexical_cast<int>(max_repl_num_str.c_str());
    } catch (boost::bad_lexical_cast & e) {
        cout << "could not lexical cast data_id or max_repl_num to int" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }
    // END ##### select DATA_ID, DATA_SIZE, max(REPL_NUMBER) where COLL_NAME = '' and DATA_NAME = '' #####

    printf("%s:%d (%s) [data_id=%d][data_size=%llu][max_repl_num=%d]\n", __FILE__, __LINE__, __FUNCTION__, data_id, data_size, max_repl_num);

    // set the destination information
    dataObjInfo_t dst_data_obj;
    bzero( &dst_data_obj, sizeof( dst_data_obj ) );

    strncpy( dst_data_obj.objPath, logical_path, MAX_NAME_LEN );
    strncpy( dst_data_obj.rescName, child_resc.c_str(), NAME_LEN );
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

    status = rsRegReplica( _rei->rsComm, &reg_inp );
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
