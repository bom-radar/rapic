# Rapic Protocol Support Library
This library implements the rapic client protocol and makes it easy for you to
integrate live radar data into your application.

## Dependencies
The library includes functionality for converting a set of rapic scans into
an `ODIM_H5` compliant file.  This includes a function exposed in the main
`rapic.h` header and a standalone `rapic_to_odim` utility.

The ODIM conversion code makes use of the `odim_h5` library which is available
from the same location as this library.

To enable support for ODIM conversions please ensure that the `odim_h5` library
has been installed _before_ attempting to compile `rapic`.  The CMake process
will automatically detect the presence of `odim_h5` and enable building the ODIM
conversion function and standalone utility.

## Installation
To build and install the library use CMake to generate Makefiles.  For an
install to the standard locations on a linux system run the following commands
from the root of the source distribution:

    mkdir build
    cd build
    cmake ..
    make
    sudo make install

To install to a non-standard prefix (eg: ${HOME}/local) modify the cmake line
as such:

    mkdir build
    cd build
    cmake .. -DCMAKE_INSTALL_PREFIX=${HOME}/local
    make
    sudo make install

## Integrating with your project
To use the library within your project it is necessary to tell your build
system how to locate the correct header and shared library files.  Support
for discovery by `CMake` and `pkg-config` (and therefore `autotools`) is
included.

### Via CMake
To use the library within your `CMake` based project, simply add the line

    find_package(rapic)

to your `CMakeLists.txt`.  This function call will set the variables
`RAPIC_INCLUDE_DIRS` and `RAPIC_LIBRARIES` as needed.  A typical usage
scenario is found below:

    find_package(rapic REQUIRED)
    include_directories(${RAPIC_INCLUDE_DIRS})
    add_executable(foo foo.cc)
    target_link_libraries(foo ${RAPIC_LIBRARIES})

### Via pkg-config
To discover compilation and link flags via `pkg-config` use the following
commands:

    pkg-config --cflags rapic
    pkg-config --libs rapic

A typical usage scenario is found below:

    g++ -o foo $(pkg-config --cflags --libs rapic) foo.cc

If pkg-config cannot find the `rapic` package, ensure that you have set your
`PKG_CONFIG_PATH` environment variable correctly for the install prefix which
installed the library to.  For example, in the ${HOME}/local example above
the following will set the correct `PKG_CONFIG_PATH`:

    export PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${HOME}/local/lib/pkgconfig

## Using the API
Please consult the rapic.h header and the source of demo.cc for examples on how
to use the API within your code.

## License
This library is open source and made freely available according to the below
text:

    Copyright 2016 Commonwealth of Australia, Bureau of Meteorology

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

A copy of the license is also provided in the LICENSE file included with the
source distribution of the library.
