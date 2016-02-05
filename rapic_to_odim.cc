/*------------------------------------------------------------------------------
 * Rapic Protocol Support Library
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rainrapic.h"

#include <getopt.h>
#include <cstdio>
#include <iostream>
#include <sstream>
#include <system_error>

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

  -q, --quiet
      Suppress output of warnings during conversion process
)";

static const char* short_options = "hq";
static struct option long_options[] =
{
    { "help",  no_argument, 0, 'h' }
  , { "quiet", no_argument, 0, 'q' }
  , { 0, 0, 0, 0 }
};

bool quiet = false;

auto format_nested_exception(std::ostream& ss, const std::exception& err) -> void
try
{
  std::rethrow_if_nested(err);
}
catch (const std::exception& sub)
{
  ss << "\n  -> " << sub.what();
  format_nested_exception(ss, sub);
}
catch (...)
{
  ss << "\n  -> <unknown exception>";
}

auto format_exception(const std::exception& err) -> std::string
{
  std::ostringstream ss;
  ss << err.what();
  format_nested_exception(ss, err);
  return ss.str();
}

auto log_function(char const* msg) -> void
{
  if (!quiet)
    std::cout << msg << std::endl;
}

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
    case 'q':
      quiet = true;
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

  FILE* fin = nullptr;
  try
  {
    // read the entire input file into memory
    fin = fopen(path_input, "rb");
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
    rapic::write_odim_h5_volume(path_output, scans, log_function);
  }
  catch (std::exception& err)
  {
    if (!quiet)
      std::cout << "fatal exception: " << format_exception(err) << std::endl;
    return EXIT_FAILURE;
  }
  catch (...)
  {
    if (!quiet)
      std::cout << "fatal exception: unknown" << std::endl;
    return EXIT_FAILURE;
  }

  // ensure the input file is closed nicely
  if (fin)
    fclose(fin);

  return EXIT_SUCCESS;
}
