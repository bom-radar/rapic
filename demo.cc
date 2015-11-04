/*------------------------------------------------------------------------------
 * Rapic Data Server client connection API for C++11
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rapic_ds.h"

#include <rainutil/trace.h>

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <ctime>

#include <fstream>

using namespace rainfields;

#if 1
auto handle_rapic_messages(rapic::client& con) -> void
{
  rapic::message_type type;
  while (con.dequeue(type))
  {
    switch (type)
    {
    case rapic::message_type::mssg:
      {
        rapic::mssg msg;
        con.decode(msg);
        std::cout << msg.content;
      }
      break;
    case rapic::message_type::scan:
      try
      {
        rapic::scan msg;
        con.decode(msg);
        std::cout << "SCAN: " << msg.product() << std::endl;
#if 0
        std::cout << "SCAN:"
          << " stn " << msg.stnid
          << " tilt " << msg.tilt << "/" << msg.tilt_count
          << " pass " << msg.pass << "/" << msg.pass_count
          << " " << msg.video
          << " ts " << msg.timestamp
          << " volumelabel " << msg.volumelabel
          << std::endl;
#endif
      }
      catch (std::exception& err)
      {
        std::cerr << "error decoding scan: " << format_exception(err) << std::endl;
      }
      break;
    }
  }
}
#endif

int main(int argc, char const* argv[])
{
  if (   argc == 2
      && (   strcmp(argv[1], "-v") == 0
          || strcmp(argv[1], "--version") == 0))
  {
    std::cout << "Rapic client library demo\nVersion: " << rapic::release_tag() << std::endl;
    return EXIT_SUCCESS;
  }

  trace::set_min_level(trace::level::debug);

  try
  {
#if 0
    // technically the traversal from char to uint8_t here is unsafe
    std::ifstream in{"tindal_bad"};
    std::string buf{std::istreambuf_iterator<char>(in.rdbuf()), std::istreambuf_iterator<char>()};

    rapic::scan scan;
    scan.decode(reinterpret_cast<uint8_t const*>(buf.data()), buf.size());

    std::cout << "read scan okay" << std::endl;

#else
    // connect to GPATS
    rapic::client con;

#if 1
    con.add_filter(-1, "ANY");
#else
    con.add_filter(2, "VOL");
    //con.add_filter(3, "VOL");
    //con.add_filter(70, "VOL");
#endif

    con.connect("cmssdev.bom.gov.au", "15555");

    // loop forever as long as the connection stays open
    while (con.connected())
    {
      // wait for messages to arrive
      con.poll();

      // process socket traffic and handle messages until socket runs dry
      while (con.process_traffic())
        handle_rapic_messages(con);

      // handle remaining messages and return to polling
      handle_rapic_messages(con);
    }
#endif
  }
  catch (std::exception& err)
  {
    std::cerr << "fatal error: " << format_exception(err) << std::endl;
    return EXIT_FAILURE;
  }
  catch (...)
  {
    std::cerr << "fatal error: unknown" << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
