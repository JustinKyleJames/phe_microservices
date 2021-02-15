/**
 * @file  libmsicheck_ip_in_network.cpp
 */

// iRODS includes
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

// std includes
#include <string>
#include <iostream>
#include <vector>
#include <sstream>

// boost includes
#include <boost/lexical_cast.hpp>
#include <boost/asio/ip/network_v4.hpp>
#include <boost/asio/ip/address_v4.hpp>


int rsGenQuery(rsComm_t*, genQueryInp_t*, genQueryOut_t**);


/**
 * \fn msicheck_ip_in_network(msParam_t* _client_ip_address, msParam_t* _network_list, msParam_t* _logical_path, ruleExecInfo_t *rei)
 *
 * \brief   This microservice checks if _src_resource_hierarchy is in one of the networks defined in the semicolon separated list in _dst_resource_hierarchy.
 *
 * \since 4.2.8
 *
 * \param[in] _client_ip_address - String with a V4 network address.
 * \param[in] _network_list - String with V4 network lists separated by semicolons.  Networks on format A.B.C.D/N.
 * \param[out] _result - Integer 0 - ip not in list, 1 - ip is in list
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
int msicheck_ip_in_network(
    msParam_t*      _client_ip_address_param,
    msParam_t*      _network_list_param,
    msParam_t*      _result_param,
    ruleExecInfo_t* _rei ) {

    using std::cout;
    using std::endl;
    using std::string;
    using namespace boost::asio::ip;

    int result = 0;

    const char *client_ip_address = parseMspForStr(_client_ip_address_param);
    if( !client_ip_address ) {
        cout << "msicheck_ip_in_network - null client_ip_address_param parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    const char *network_list = parseMspForStr(_network_list_param);
    if( !network_list ) {
        cout << "msicheck_ip_in_network - null network_list_param parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    if( !_rei ) {
        cout << "msicheck_ip_in_network - null _rei parameter" << endl;
        return SYS_INVALID_INPUT_PARAM;
    }

    std::string client_ip_address_str(client_ip_address);
    std::string network_list_str(network_list);

    try {
        network_v4 client_network_v4 = make_network_v4(client_ip_address_str + "/32");

        std::istringstream ss(network_list_str);
        std::string network_address_str;
        while (getline(ss, network_address_str, ';')) {
            rodsLog(LOG_NOTICE, "UPDATE checking if %s is in %s", client_ip_address_str.c_str(), network_address_str.c_str());
            try { 
                network_v4 network_network_v4 = make_network_v4(network_address_str);
                if (client_network_v4.is_subnet_of(network_network_v4)) {
                    result = 1;
                    break;
                }
            } catch (...) {
                rodsLog(LOG_ERROR, "Bad network address in: %s.  Skipping...", network_address_str.c_str());
            }   
        }
    } catch (...) {
        rodsLog(LOG_ERROR, "Bad IP address received: %s", client_ip_address_str.c_str());
        return SYS_INVALID_INPUT_PARAM;
    }

    fillIntInMsParam(_result_param, result);
    return 0;
}

extern "C"
irods::ms_table_entry* plugin_factory() {
    irods::ms_table_entry* msvc = new irods::ms_table_entry(3);
    msvc->add_operation<
        msParam_t*,
        msParam_t*,
        msParam_t*,
        ruleExecInfo_t*>("msicheck_ip_in_network",
                         std::function<int(
                             msParam_t*,
                             msParam_t*,
                             msParam_t*,
                             ruleExecInfo_t*)>(msicheck_ip_in_network));
    return msvc;
}
