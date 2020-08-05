cmake_minimum_required(VERSION 3.5.0 FATAL_ERROR) #CPACK_DEBIAN_<COMPONENT>_PACKAGE_NAME

find_package(IRODS 4.2.8 EXACT REQUIRED)
set(IRODS_PLUGIN_REVISION "0")

set(CMAKE_C_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang)
set(CMAKE_CXX_COMPILER ${IRODS_EXTERNALS_FULLPATH_CLANG}/bin/clang++)
set(CMAKE_EXE_LINKER_FLAGS_INIT "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")

set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -stdlib=libc++ -Wl,-z,defs")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -stdlib=libc++ -Wl,-z,defs")
add_compile_options(-nostdinc++ -Wall -Wextra -Werror)
add_compile_options(-Wno-unused-parameter) # Until fix of irods_re_plugin.hpp
add_compile_options(-Wno-unneeded-internal-declaration) # Until fix of https://github.com/irods/irods/issues/3396


set(
  IRODS_MICROSERVICE_ADMINISTRATION_PLUGINS
  msiregister_replica
  )


message("IRODS_EXTERNALS_FULLPATH_BOOST is ${IRODS_EXTERNALS_FULLPATH_BOOST}")

foreach(PLUGIN ${IRODS_MICROSERVICE_ADMINISTRATION_PLUGINS})
  add_library(
    ${PLUGIN}
    MODULE
    ${CMAKE_SOURCE_DIR}/${PLUGIN}/lib${PLUGIN}.cpp
    )

  target_include_directories(
    ${PLUGIN}
    PRIVATE
    /usr/include/irods
    ${IRODS_EXTERNALS_FULLPATH_CLANG}/include/c++/v1
    ${CMAKE_BINARY_DIR}/lib/core/include
    ${CMAKE_SOURCE_DIR}/lib/core/include
    ${CMAKE_SOURCE_DIR}/lib/api/include
    ${CMAKE_SOURCE_DIR}/server/drivers/include
    ${CMAKE_SOURCE_DIR}/server/api/include
    ${CMAKE_SOURCE_DIR}/server/core/include
    ${CMAKE_SOURCE_DIR}/server/icat/include
    ${CMAKE_SOURCE_DIR}/server/re/include
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/include
    ${IRODS_EXTERNALS_FULLPATH_FMT}/include
    ${IRODS_EXTERNALS_FULLPATH_JSON}/include
    ${CURL_INCLUDE_DIRS}
    )

  target_link_libraries(
    ${PLUGIN}
    PRIVATE
    irods_server
    irods_common
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so
    ${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so
    ${IRODS_EXTERNALS_FULLPATH_FMT}/lib/libfmt.so
    ${CURL_LIBRARIES}
    )

  target_compile_definitions(${PLUGIN} PRIVATE ENABLE_RE ${IRODS_COMPILE_DEFINITIONS} BOOST_SYSTEM_NO_DEPRECATED)
  target_compile_options(${PLUGIN} PRIVATE -Wno-write-strings -std=c++17)
  #set_property(TARGET ${PLUGIN} PROPERTY CXX_STANDARD ${IRODS_CXX_STANDARD})
endforeach()

install(
  TARGETS
    ${IRODS_MICROSERVICE_ADMINISTRATION_PLUGINS}
  LIBRARY
    DESTINATION ${IRODS_PLUGINS_DIRECTORY}/microservices
    COMPONENT ${IRODS_PACKAGE_COMPONENT_SERVER_NAME}
  )
