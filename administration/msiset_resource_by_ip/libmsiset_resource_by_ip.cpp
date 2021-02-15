/**
 * @file  libmsiset_resource_by_ip.cpp
 */

// iRODS includes
#include "apiHeaderAll.h"
#include "msParam.h"
#include "irods_ms_plugin.hpp"
#include "irods_file_object.hpp"
#include "irods_hierarchy_parser.hpp"
#include "rsRegReplica.hpp"
#include "miscServerFunct.hpp"

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
 * \fn msiset_resource_by_ip(msParam_t* resource_to_ip_kvp_param, ruleExecInfo_t *rei)
 *
 * \brief   This microservice set the resource for client IP address based on lookup in resource_to_ip_kvp_param. If there is no match, the
 *    resource is set to default_resource_parm.  The resource is identified by the first match.  If rei, rei->rsComm, or rei->rsComm->clientAddr are null
 *    this microservice logs a message and returns without updating the resource.
 *
 * \since 4.2.8
 *
 * \param[in] resource_to_ip_kvp_param - KVP with key being the resource and value being a semicolon delimited list of networks
 *    of the form A.B.C.D/N.
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
int msiset_resource_by_ip(
    msParam_t*      resource_to_ip_kvp_param,
    msParam_t*      default_resource_param,
    ruleExecInfo_t* rei ) {

    using namespace boost::asio::ip;

    if (resource_to_ip_kvp_param == nullptr) {
        rodsLog( LOG_NOTICE, "%s: resource_to_ip_kvp_parm is NULL", __FUNCTION__);
        return 0;
    }

    if (default_resource_param == nullptr) {
        rodsLog( LOG_NOTICE, "%s: default_resource_param is NULL", __FUNCTION__);
        return 0;
    }

    char *default_resource_cstr = parseMspForStr( default_resource_param );
    if( !default_resource_cstr ) {
        rodsLog( LOG_NOTICE, "%s: default_resource_param is NULL", __FUNCTION__);
        return 0;
    }

    if ( rei == nullptr || rei->rsComm == nullptr) {
        rodsLog( LOG_NOTICE, "%s: input rei or rsComm is NULL.", __FUNCTION__);
        return 0;
    }

    keyValPair_t *kvp = ( keyValPair_t* )resource_to_ip_kvp_param->inOutStruct;

    // get the client ip address
    std::string client_ip_address_str{rei->rsComm->clientAddr};

    bool found = false;
    for (int i = 0; i < kvp->len; ++i) {

        std::string resource_str{kvp->keyWord[i]};
        std::string network_list_str{kvp->value[i]};

        try {
            network_v4 client_network_v4 = make_network_v4(client_ip_address_str + "/32");

            std::istringstream ss(network_list_str);
            std::string network_address_str;
            while (getline(ss, network_address_str, ';')) {
                //rodsLog(LOG_NOTICE, "checking if %s is in %s", client_ip_address_str.c_str(), network_address_str.c_str());
                try {
                    network_v4 network_network_v4 = make_network_v4(network_address_str);
                    if (client_network_v4.is_subnet_of(network_network_v4)) {
                        snprintf(rei->rescName, NAME_LEN, "%s", resource_str.c_str());
                        found = true;
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

        if (found) {
            break;
        }
    }

    if (!found) {
        snprintf(rei->rescName, NAME_LEN, "%s", default_resource_cstr);
    }

    return 0;
}

extern "C"
irods::ms_table_entry* plugin_factory() {
    irods::ms_table_entry* msvc = new irods::ms_table_entry(2);
    msvc->add_operation<
        msParam_t*,
        msParam_t*,
        ruleExecInfo_t*>("msiset_resource_by_ip",
                         std::function<int(
                             msParam_t*,
                             msParam_t*,
                             ruleExecInfo_t*)>(msiset_resource_by_ip));
    return msvc;
}
