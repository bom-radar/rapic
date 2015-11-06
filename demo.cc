/*------------------------------------------------------------------------------
 * Rainfields Rapic Support Library (rainrapic)
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rainrapic.h"

#include <rainutil/trace.h>

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <ctime>

#include <fstream>

using namespace rainfields;

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
        trace::log() << msg.content;
      }
      break;
    case rapic::message_type::scan:
      try
      {
        rapic::scan msg;
        con.decode(msg);
        trace::log() << "SCAN:"
          << " stn " << msg.station_id()
          << " pass " << msg.pass() << "/" << msg.pass_count()
          << " product " << msg.product();
      }
      catch (std::exception& err)
      {
        trace::error() << "error decoding scan: " << format_exception(err);
      }
      break;
    }
  }
}

int main(int argc, char const* argv[])
{
  if (   argc == 2
      && (   strcmp(argv[1], "-v") == 0
          || strcmp(argv[1], "--version") == 0))
  {
    std::cout << "Rainfields rapic support library demo\nVersion: " << rapic::release_tag() << std::endl;
    return EXIT_SUCCESS;
  }

  trace::set_min_level(trace::level::log);

  try
  {
#if 0 // demo fragment used to debug individual corrupted messages
    // technically the traversal from char to uint8_t here is unsafe
    std::ifstream in{"bad_sellick"};
    std::string buf{std::istreambuf_iterator<char>(in.rdbuf()), std::istreambuf_iterator<char>()};
    rapic::scan scan;
    scan.decode(reinterpret_cast<uint8_t const*>(buf.data()), buf.size());
    trace::log() << "read scan okay";
#endif

    // connect to a ROWLF server
    rapic::client con;

#if 1 // switch between all or just a few radars
    con.add_filter(-1, "ANY");
#else
    con.add_filter(2, "VOL");
    con.add_filter(3, "VOL");
    con.add_filter(70, "VOL");
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
  }
  catch (std::exception& err)
  {
    trace::error() << "fatal error: " << format_exception(err);
    return EXIT_FAILURE;
  }
  catch (...)
  {
    trace::error() << "fatal error: unknown";
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
