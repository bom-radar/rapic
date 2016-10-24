/*------------------------------------------------------------------------------
 * Rapic Protocol Support Library
 *
 * Copyright 2016 Commonwealth of Australia, Bureau of Meteorology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *----------------------------------------------------------------------------*/
#include "rapic.h"

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

    if (fseek(fin, 0, SEEK_SET) != 0)
      throw std::system_error{errno, std::system_category(), "failed to read input file"};

    rapic::buffer buf(len);
    if (fread(buf.write_acquire(len).first, 1, len, fin) != len)
      throw std::system_error{errno, std::system_category(), "failed to read input file"};
    buf.write_advance(len);

    // find scans and parse them into a list
    rapic::message_type type;
    size_t msglen;
    std::list<rapic::scan> scans;
    for (auto ra = buf.read_acquire(); ra.second != 0; ra = buf.read_acquire())
    {
      // whitespace - skip
      if (*ra.first <= 0x20)
      {
        buf.read_advance(1);
      }
      // image header - skip
      else if (*ra.first == '/')
      {
        auto i = 1;
        while (i < ra.second && ra.first[i] != '\n')
          ++i;
        buf.read_advance(i);
      }
      // scan - find end and decode
      else if (buf.read_detect(type, msglen))
      {
        if (type == rapic::message_type::scan)
        {
          scans.emplace_back();
          scans.back().decode(buf);
        }
        else if (!quiet)
          std::cout << "unexpected rapic message type encountered" << std::endl;

        buf.read_advance(msglen);
      }
      else
        throw std::runtime_error{"extra unknown data in file"};
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
