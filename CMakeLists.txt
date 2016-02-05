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
set(RAINRAPIC_VERSION_MAJOR 1)
set(RAINRAPIC_VERSION_MINOR 0)
set(RAINRAPIC_VERSION_PATCH 2)
set(RAINRAPIC_VERSION "${RAINRAPIC_VERSION_MAJOR}.${RAINRAPIC_VERSION_MINOR}.${RAINRAPIC_VERSION_PATCH}")

# default to a release build (must be before project())
if (DEFINED CMAKE_BUILD_TYPE)
  set(DEF_BUILD_TYPE ${CMAKE_BUILD_TYPE})
else()
  set(DEF_BUILD_TYPE Release)
endif()
set(CMAKE_BUILD_TYPE ${DEF_BUILD_TYPE} CACHE STRING "Build type: <none>|Debug|Release|RelWithDebInfo|MinSizeRel")

# initialize our project
project(rainrapic)

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
  set(DEF_INSTALL_CMAKE_DIR ${DEF_INSTALL_LIB_DIR}/cmake/rainrapic)
endif()

# customizable install locations
set(INSTALL_LIB_DIR ${DEF_INSTALL_LIB_DIR} CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR bin CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR include CACHE PATH "Installation directory for header files")
set(INSTALL_CMAKE_DIR ${DEF_INSTALL_CMAKE_DIR} CACHE PATH "Installation directory for CMake files")

# convert relative paths to absolute
foreach(p LIB INCLUDE CMAKE)
  set(var INSTALL_${p}_DIR)
  if (NOT IS_ABSOLUTE "${${var}}")
    set(${var} "${CMAKE_INSTALL_PREFIX}/${${var}}")
  endif()
endforeach()

# external dependencies
find_package(rainhdf REQUIRED)
include_directories(${RAINHDF_INCLUDE_DIRS})

# extract sourcee tree version information from git
find_package(Git)
if(GIT_FOUND)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" describe --dirty
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    OUTPUT_VARIABLE RAINRAPIC_RELEASE_TAG
    OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
if(NOT DEFINED RAINRAPIC_RELEASE_TAG OR "${RAINRAPIC_RELEASE_TAG}" STREQUAL "")
  set(RAINRAPIC_RELEASE_TAG "unknown-version")
endif()
add_definitions("-DRAINRAPIC_RELEASE_TAG=\"${RAINRAPIC_RELEASE_TAG}\"")

# ensure C++11 support is turned on
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# set a high warning level
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -pedantic -Wextra -Wno-unused-parameter")

# build our library
add_library(rainrapic SHARED rainrapic.cc rainrapic.h)
target_link_libraries(rainrapic ${RAINUTIL_LIBRARIES} ${RAINHDF_LIBRARIES})
set_target_properties(rainrapic PROPERTIES VERSION "${RAINRAPIC_VERSION}")
set_target_properties(rainrapic PROPERTIES PUBLIC_HEADER rainrapic.h)
install(TARGETS rainrapic
  EXPORT rainrapic-targets
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}" COMPONENT shlib
  PUBLIC_HEADER DESTINATION "${INSTALL_INCLUDE_DIR}"
  COMPONENT dev)

# create pkg-config file
configure_file(rainrapic.pc.in "${PROJECT_BINARY_DIR}/rainrapic.pc" @ONLY)
install(FILES "${PROJECT_BINARY_DIR}/rainrapic.pc" DESTINATION "${INSTALL_LIB_DIR}/pkgconfig")

# build our rapic to odim convertor
add_executable(rapic_to_odim rapic_to_odim.cc)
target_link_libraries(rapic_to_odim rainrapic)
install(TARGETS rapic_to_odim DESTINATION "${INSTALL_BIN_DIR}")

# build our demo application (not in the 'all' target.  type 'make demo' to build)
add_executable(demo demo.cc)
target_link_libraries(demo rainrapic)
set_target_properties(demo PROPERTIES EXCLUDE_FROM_ALL 1)

# boilerplate to generate our config and version cmake files

# create config files
file(RELATIVE_PATH REL_INCLUDE_DIR "${INSTALL_CMAKE_DIR}" "${INSTALL_INCLUDE_DIR}")
set(CONF_INCLUDE_DIRS "\${RAINUTIL_CMAKE_DIR}/${REL_INCLUDE_DIR}")
configure_file(rainrapic-config.cmake.in "${PROJECT_BINARY_DIR}/rainrapic-config.cmake" @ONLY)
configure_file(rainrapic-config-version.cmake.in "${PROJECT_BINARY_DIR}/rainrapic-config-version.cmake" @ONLY)

# install the config files
install(FILES
  "${PROJECT_BINARY_DIR}/rainrapic-config.cmake"
  "${PROJECT_BINARY_DIR}/rainrapic-config-version.cmake"
  DESTINATION "${INSTALL_CMAKE_DIR}" COMPONENT dev)

# install the export set for use with the install-tree
install(EXPORT rainrapic-targets DESTINATION "${INSTALL_CMAKE_DIR}" COMPONENT dev)
