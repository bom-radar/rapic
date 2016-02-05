/*------------------------------------------------------------------------------
 * Rainfields Rapic Support Library (rainrapic)
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rainrapic.h"

#include <rainutil/files.h>
#include <rainutil/string_utils.h>
#include <rainutil/trace.h>
#include <getopt.h>
#include <cstdio>
#include <iostream>
#include <system_error>

using namespace rainfields;

static const char* try_again = "try --help for usage instructions\n";

static const char* usage_string =
R"(Rapic to ODIM_H5 converter

usage:
  rapic_to_odim [options] input.rapic output.h5

note:
  This program is a simple converter from a single rapic file into an ODIM_h5 volume.
  It does not check that all scans within the rapic file belong to the same volume.  If
  scans from multiple volumes, sites or products are interleaved then the result is
  undefined.

available options:
  -h, --help
      Show this message and exit

  -t, --trace level
      Set tracing level as one of none|status|error|warning|log|debug (log)
)";

static const char* short_options = "ht:";
static struct option long_options[] =
{
    { "help",         no_argument,       0, 'h' }
  , { "trace",        required_argument, 0, 't' }
  , { 0, 0, 0, 0 }
};

int main(int argc, char* argv[])
{
  // process options
  while (true)
  {
    int option_index = 0;
    int c = getopt_long(argc, argv, short_options, long_options, &option_index);
    if (c == -1)
      break;
    switch (c)
    {
    case 'h':
      std::cout << usage_string;
      return EXIT_SUCCESS;
    case 't':
      trace::set_min_level(from_string<trace::level>(optarg));
      break;
    case '?':
      std::cerr << try_again;
      return EXIT_FAILURE;
    }
  }

  if (argc - optind < 2)
  {
    std::cerr << "missing required parameters\n" << try_again;
    return EXIT_FAILURE;
  }

  auto path_input = argv[optind + 0];
  auto path_output = argv[optind + 1];

  try
  {
    // read the entire input file into memory
    file_stream fin{fopen(path_input, "rb")};
    if (!fin)
      throw std::system_error{errno, std::system_category(), "failed to open input file"};

    if (fseek(fin, 0, SEEK_END) != 0)
      throw std::system_error{errno, std::system_category(), "failed to read input file"};

    auto l = ftell(fin);
    if (l == -1)
      throw std::system_error{errno, std::system_category(), "failed to read input file"};
    size_t len = l;

    std::unique_ptr<uint8_t[]> buf{new uint8_t[len]};

    if (fseek(fin, 0, SEEK_SET) != 0)
      throw std::system_error{errno, std::system_category(), "failed to read input file"};

    if (fread(buf.get(), 1, len, fin) != len)
      throw std::system_error{errno, std::system_category(), "failed to read input file"};

    fin.reset();
    
    // find scans and parse them into a list
    std::list<rapic::scan> scans;
    for (size_t i = 0; i < len; ++i)
    {
      // whitespace - skip
      if (buf[i] <= 20)
        continue;

      // image header - skip
      if (buf[i] == '/')
      {
        while (i < len && buf[i] != '\n')
          ++i;
        continue;
      }

      // scan - find end and decode
      scans.emplace_back();
      i += scans.back().decode(&buf[i], len - i);
    }

    // convert the list of scans into a volume
    rapic::write_odim_h5_volume(path_output, scans, [](char const* msg) { trace::warning() << msg; });
  }
  catch (std::exception& err)
  {
    trace::error() << "fatal exception: " << format_exception(err);
    return EXIT_FAILURE;
  }
  catch (...)
  {
    trace::error() << "fatal exception: unknown";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}

