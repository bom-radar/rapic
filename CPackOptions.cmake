#-------------------------------------------------------------------------------
# Rapic Protocol Support Library
#
# Copyright 2016 Commonwealth of Australia, Bureau of Meteorology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#-------------------------------------------------------------------------------
if (${CPACK_GENERATOR} MATCHES "RPM")
  # CMake 3.6 is required to properly conform to the Fedora RPM standards
  cmake_minimum_required(VERSION 3.6)

  set(CPACK_RPM_PACKAGE_RELEASE 1)
  set(CPACK_RPM_PACKAGE_RELEASE_DIST ON)
  set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
  set(CPACK_RPM_PACKAGE_LICENSE "ASL 2.0")
  set(CPACK_RPM_PACKAGE_URL "http://github.com/bom-radar/rapic")
  set(CPACK_RPM_COMPONENT_INSTALL ON)
  set(CPACK_RPM_FILE_NAME "RPM-DEFAULT")

  set(CPACK_RPM_RUNTIME_PACKAGE_NAME "${CPACK_PACKAGE_NAME}")
  set(CPACK_RPM_RUNTIME_PACKAGE_REQUIRES ${CPACK_API_DEPS})

  set(CPACK_RPM_DEVEL_PACKAGE_NAME "${CPACK_RPM_RUNTIME_PACKAGE_NAME}-devel")
  set(CPACK_RPM_DEVEL_PACKAGE_REQUIRES "${CPACK_RPM_RUNTIME_PACKAGE_NAME} = ${CPACK_PACKAGE_VERSION}")

  # explicitly set the default packaging path to same as in-built one so that it is available for use below
  if (DEFINED CPACK_PACKAGING_INSTALL_PREFIX)
    set(PREFIX ${CPACK_PACKAGING_INSTALL_PREFIX})
  else()
    set(PREFIX "/usr")
  endif()

  # prevent CPack from trying to own the top level cmake and pkgconfig directories
  set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
    "${PREFIX}/${CPACK_INSTALL_LIBDIR}/cmake"
    "${PREFIX}/${CPACK_INSTALL_LIBDIR}/pkgconfig")

endif()
