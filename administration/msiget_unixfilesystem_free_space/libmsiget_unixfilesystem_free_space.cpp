#include "msParam.h"
#include "rsGeneralAdmin.hpp"
#include "irods_ms_plugin.hpp"
#include "irods_plugin_name_generator.hpp"
#include "generalAdmin.h"
#include "irods_log.hpp"
#include <sys/statvfs.h>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>

int
msiget_unixfilesystem_free_space(
        msParam_t *unix_path_msparam,
        msParam_t *percent_used_msparam,
        ruleExecInfo_t *rei) {

    if (rei == nullptr) {
        rodsLog(LOG_ERROR, "[%s]: input rei is NULL", __FUNCTION__);
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    if (unix_path_msparam == nullptr) {
        rodsLog(LOG_ERROR, "[%s]: unix_path_msparam is NULL", __FUNCTION__);
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }
    if (unix_path_msparam->type == nullptr) {
        rodsLog(LOG_ERROR, "[%s]: unix_path_msparam->type is NULL", __FUNCTION__);
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }
    if (strcmp(unix_path_msparam->type, STR_MS_T)) {
        rodsLog(LOG_ERROR, "[%s]: first argument should be STR_MS_T, was [%s]", __FUNCTION__, unix_path_msparam->type);
        return USER_PARAM_TYPE_ERR;
    }

    const char* unix_path_cstr = static_cast<char*>(unix_path_msparam->inOutStruct);
    if (unix_path_cstr == NULL) {
        rodsLog(LOG_ERROR, "[%s]: input unix_path_mspar->inOutStruct is NULL", __FUNCTION__);
        return SYS_INTERNAL_NULL_INPUT_ERR;
    }

    boost::filesystem::path path_to_stat{unix_path_cstr};
    while(!boost::filesystem::exists(path_to_stat)) {
        rodsLog(LOG_NOTICE, "[%s]: path to stat [%s] doesn't exist, moving to parent", __FUNCTION__, path_to_stat.string().c_str());
        path_to_stat = path_to_stat.parent_path();
        if (path_to_stat.empty()) {
            rodsLog(LOG_ERROR, "[%s]: could not find existing path from given path path [%s]", __FUNCTION__, unix_path_cstr);
            return SYS_INVALID_RESC_INPUT;
        }
    }

    struct statvfs statvfs_buf;
    const int statvfs_ret = statvfs(path_to_stat.string().c_str(), &statvfs_buf);
    if (statvfs_ret != 0) {
        rodsLog(LOG_ERROR, "[%s]: statvfs() of [%s] failed with return %d and errno %d", __FUNCTION__, path_to_stat.string().c_str(), statvfs_ret, errno);
        return SYS_INVALID_RESC_INPUT;
    }

    uint64_t free_space_blocks = static_cast<uint64_t>(statvfs_buf.f_bavail);
    uint64_t total_space_blocks = static_cast<uint64_t>(statvfs_buf.f_blocks);

    // can't seem to get a double out of a rule so using int for percent used
    double percent_used = 100.0 * (1.0 - static_cast<double>(free_space_blocks) / static_cast<double>(total_space_blocks));
    std::string percent_used_str = std::to_string(percent_used);
    fillStrInMsParam(percent_used_msparam, percent_used_str.c_str());

    rodsLog(LOG_NOTICE, "[%s]: free_blocks=%zu total_blocks=%zu percent_use is %f", __FUNCTION__, free_space_blocks, total_space_blocks, percent_used);

    return 0;
}

extern "C"
irods::ms_table_entry* plugin_factory() {
    irods::ms_table_entry* msvc = new irods::ms_table_entry(2);
    msvc->add_operation<
        msParam_t*,
        msParam_t*,
        ruleExecInfo_t*>("msiget_unixfilesystem_resource_free_space",
                         std::function<int(
                             msParam_t*,
                             msParam_t*,
                             ruleExecInfo_t*)>(msiget_unixfilesystem_free_space));
    return msvc;
}
