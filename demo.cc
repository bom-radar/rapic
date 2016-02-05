/*------------------------------------------------------------------------------
 * Rapic Protocol Support Library
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rainrapic.h"

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <ctime>

#include <fstream>

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
        std::cout << msg.content << std::endl;
      }
      break;
    case rapic::message_type::scan:
      try
      {
        rapic::scan msg;
        con.decode(msg);
        std::cout << "SCAN:"
          << " stn " << msg.station_id()
          << " pass " << msg.pass() << "/" << msg.pass_count()
          << " product " << msg.product()
          << std::endl;
      }
      catch (std::exception& err)
      {
        std::cout << "error decoding scan: " << err.what() << std::endl;
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
    std::cout << "Rapic radar protocol support library demo\nVersion: " << rapic::release_tag() << std::endl;
    return EXIT_SUCCESS;
  }

  try
  {
#if 0 // demo fragment used to debug individual corrupted messages
    // technically the traversal from char to uint8_t here is unsafe
    std::ifstream in{"bad_sellick"};
    std::string buf{std::istreambuf_iterator<char>(in.rdbuf()), std::istreambuf_iterator<char>()};
    rapic::scan scan;
    scan.decode(reinterpret_cast<uint8_t const*>(buf.data()), buf.size());
    std::cout << "read scan okay" << std::endl;
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
    std::cout << "fatal error: " << err.what() << std::endl;
    return EXIT_FAILURE;
  }
  catch (...)
  {
    std::cout << "fatal error: unknown" << std::endl;
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
