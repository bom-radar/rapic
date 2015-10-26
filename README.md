# Rapic Data Server client connection API for C++11

This library implements the rapic client protocol and makes it easy for you to
integrate live radar data into your application.


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


## Building the demo project
A demo application is included which simply uses the API to connect to a ROWLF
server and print received scan messages to the console.

To build and run this project, from within the 'build' directory above type:

    make demo
    ./demo


## Integrating with your project
To use the library within your project use pkg-config to determine the correct
compile and link flags:

    pkg-config --cflags rapic_ds
    pkg-config --libs rapic_ds

The library is written in C++11.  It will not compile on compilers that lack
support for this version of C++ or later.  Depending on your compiler you may
need to explicitly enable C++11 support by adding the '-std=c++11' flag.

For example:

    g++ -std=c++11 $(pkg-config --cflags --libs rapic_ds) my_project.cc
  
If pkg-config cannot find the gpats package, ensure that you have set your
PKG_CONFIG_PATH environment variable correctly for the install prefix which
installed the library to.  For example, in the ${HOME}/local example above
the following will set the correct PKG_CONFIG_PATH:

    export PKG_CONFIG_PATH=${PKG_CONFIG_PATH}:${HOME}/local/lib/pkgconfig


## Using the API
-------------------------------------------------------------------------------
Please consult the rapic_ds.h header and the source of demo.cc for examples on
how to use the API within your code.

