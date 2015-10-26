#-------------------------------------------------------------------------------
# Rapic Data Server client connection API
#
# Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
# See COPYING for licensing and warranty details
#-------------------------------------------------------------------------------
cmake_minimum_required(VERSION 2.8)

# version number used for shared library
#  - major -> update when breaking API
#  - minor -> update when breaking ABI - users only need to re-link
#  - patch -> update when no-relink is required (ie: self-contained inside .so)
set(RAPIC_DS_VERSION_MAJOR 1)
set(RAPIC_DS_VERSION_MINOR 0)
set(RAPIC_DS_VERSION_PATCH 2)
set(RAPIC_DS_VERSION "${RAPIC_DS_VERSION_MAJOR}.${RAPIC_DS_VERSION_MINOR}.${RAPIC_DS_VERSION_PATCH}")

# default to a release build (must be before project())
if (DEFINED CMAKE_BUILD_TYPE)
  set(DEF_BUILD_TYPE ${CMAKE_BUILD_TYPE})
else()
  set(DEF_BUILD_TYPE Release)
endif()
set(CMAKE_BUILD_TYPE ${DEF_BUILD_TYPE} CACHE STRING "Build type: <none>|Debug|Release|RelWithDebInfo|MinSizeRel")

# initialize our project
project(rapic_ds)

# default locations based on system configuration
get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
if (LIB64)
  set(DEF_INSTALL_LIB_DIR lib64)
else()
  set(DEF_INSTALL_LIB_DIR lib)
endif()
if (WIN32 AND NOT CYGWIN)
  set(DEF_INSTALL_CMAKE_DIR cmake)
else()
  set(DEF_INSTALL_CMAKE_DIR ${DEF_INSTALL_LIB_DIR}/cmake/rapic_ds)
endif()

# customizable install locations
set(INSTALL_LIB_DIR ${DEF_INSTALL_LIB_DIR} CACHE PATH "Installation directory for libraries")
set(INSTALL_INCLUDE_DIR include CACHE PATH "Installation directory for header files")
set(INSTALL_CMAKE_DIR ${DEF_INSTALL_CMAKE_DIR} CACHE PATH "Installation directory for CMake files")

# convert relative paths to absolute
foreach(p LIB INCLUDE CMAKE)
  set(var INSTALL_${p}_DIR)
  if (NOT IS_ABSOLUTE "${${var}}")
    set(${var} "${CMAKE_INSTALL_PREFIX}/${${var}}")
  endif()
endforeach()

# extract sourcee tree version information from git
find_package(Git)
if(GIT_FOUND)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" describe --dirty
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    OUTPUT_VARIABLE RAPIC_DS_RELEASE_TAG
    OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
if(NOT DEFINED RAPIC_DS_RELEASE_TAG OR "${RAPIC_DS_RELEASE_TAG}" STREQUAL "")
  set(RAPIC_DS_RELEASE_TAG "unknown-version")
endif()
add_definitions("-DRAPIC_DS_RELEASE_TAG=\"${RAPIC_DS_RELEASE_TAG}\"")

# ensure C++11 support is turned on
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# set a high warning level
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -Wextra -Wno-unused-parameter")

# build our library
add_library(rapic_ds SHARED rapic_ds.cc rapic_ds.h)
set_target_properties(rapic_ds PROPERTIES VERSION "${RAPIC_DS_VERSION}")
set_target_properties(rapic_ds PROPERTIES PUBLIC_HEADER rapic_ds.h)
install(TARGETS rapic_ds
  EXPORT rapic_ds-targets
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}" COMPONENT shlib
  PUBLIC_HEADER DESTINATION "${INSTALL_INCLUDE_DIR}"
  COMPONENT dev)

# create pkg-config file
configure_file(rapic_ds.pc.in "${PROJECT_BINARY_DIR}/rapic_ds.pc" @ONLY)
install(FILES "${PROJECT_BINARY_DIR}/rapic_ds.pc" DESTINATION "${INSTALL_LIB_DIR}/pkgconfig")

# build our demo application (not in the 'all' target.  type 'make demo' to build)
add_executable(demo demo.cc)
target_link_libraries(demo rapic_ds)
set_target_properties(demo PROPERTIES EXCLUDE_FROM_ALL 1)

# boilerplate to generate our config and version cmake files

# create config files
file(RELATIVE_PATH REL_INCLUDE_DIR "${INSTALL_CMAKE_DIR}" "${INSTALL_INCLUDE_DIR}")
set(CONF_INCLUDE_DIRS "\${RAINUTIL_CMAKE_DIR}/${REL_INCLUDE_DIR}")
configure_file(rapic_ds-config.cmake.in "${PROJECT_BINARY_DIR}/rapic_ds-config.cmake" @ONLY)
configure_file(rapic_ds-config-version.cmake.in "${PROJECT_BINARY_DIR}/rapic_ds-config-version.cmake" @ONLY)

# install the config files
install(FILES
  "${PROJECT_BINARY_DIR}/rapic_ds-config.cmake"
  "${PROJECT_BINARY_DIR}/rapic_ds-config-version.cmake"
  DESTINATION "${INSTALL_CMAKE_DIR}" COMPONENT dev)

# install the export set for use with the install-tree
install(EXPORT rapic_ds-targets DESTINATION "${INSTALL_CMAKE_DIR}" COMPONENT dev)
