/**
 * @file  register_replica.hpp
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


#ifndef PHE_REGISTER_REPLICA_HPP
#define PHE_REGISTER_REPLICA_HPP


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
    ruleExecInfo_t* _rei );

#endif // PHE_REGISTER_REPLICA_HPP
