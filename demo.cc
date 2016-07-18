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

#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

auto format_exception(std::exception const& err) -> std::string;
auto demo_client(std::string server, std::string port) -> void;
auto demo_server(std::string port) -> void;
auto handle_rapic_messages(rapic::client& con) -> void;

static const char* try_again = "try --help for usage instructions\n";
static const char* usage_string =
R"(Rapic radar protocol support library demo application

usage:
  rapic_demo [options] server port
  rapic_demo [options] port

If both server and port are given on the command line the application will
open a client connection to the given server.  A default filter set will be
sent asking for all data.

If only the port is given the application will listen on the given port for
incoming connections.  All incoming connections on the port will be accepted
and monitored until the application is terminated.

In either mode, a summary of all received messages will be output.

available options:
  -h, --help
      Show this message and exit

  -v, --version
      Print version information and exit

eg: rapic_demo cmssdev.bom.gov.au 15555
)";

static const char* short_options = "ht:v:nsV:m";
static struct option long_options[] =
{
    { "help",         no_argument,       0, 'h' }
  , { "version",      no_argument,       0, 'v' }
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
    case 'v':
      std::cout << rapic::release_tag() << std::endl;
      return EXIT_SUCCESS;
    case '?':
      std::cerr << try_again;
      return EXIT_FAILURE;
    }
  }

  try
  {
    if (argc - optind == 1)
      demo_server(argv[optind + 0]);
    else if (argc - optind == 2)
      demo_client(argv[optind + 0], argv[optind + 1]);
    else
      throw std::runtime_error{"invalid command line arguments"};
  }
  catch (std::exception& err)
  {
    std::cout << "fatal error: " << format_exception(err) << std::endl;
    return EXIT_FAILURE;
  }
  catch (...)
  {
    std::cout << "fatal error: unknown" << std::endl;
    return EXIT_FAILURE;
  }
}

auto format_exception(std::exception const& err) -> std::string
{
  std::string ret = err.what();
  ret.append("\n");
  try
  {
    std::rethrow_if_nested(err);
  }
  catch (std::exception& e)
  {
    ret.append("-> ").append(format_exception(e));
  }
  return ret;
}

auto demo_client(std::string server, std::string port) -> void
{
#if 0 // demo fragment used to debug individual corrupted messages
  // technically the traversal from char to uint8_t here is unsafe
  std::ifstream in{"bad_scan.rapic"};
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

  con.connect(server, port);

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

auto demo_server(std::string port) -> void
{
  // master list of client connections
  std::list<rapic::client> clients;

  // listen (server) socket manager
  rapic::server srv;
  srv.listen(port);

  // TEMP TEMP - should use poll to know whether connections are waiting or need processing
  while (true)
  {
    auto new_clients = srv.accept_pending_connections();

    // tell the user about any new connections
    for (auto& cli : new_clients)
      std::cout << "NEW CLIENT CONNECTION: " << cli.address() << ":" << cli.service() << std::endl;

    // splice the new connections into our main list
    clients.splice(clients.end(), new_clients);

    // process traffic on all clients
    for (auto& cli : clients)
    {
      while (cli.process_traffic())
        handle_rapic_messages(cli);
      handle_rapic_messages(cli);
    }
  }
}

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
        std::cout << "MESSAGE: "
          << " number " << msg.number()
          << " text " << msg.text()
          << std::endl;
      }
      break;
    case rapic::message_type::status:
      {
        rapic::status msg;
        con.decode(msg);
        std::cout << "STATUS: " << std::endl;
      }
      break;
    case rapic::message_type::permcon:
      {
        rapic::permcon msg;
        con.decode(msg);
        std::cout << "PERMCON: "
          << " txcomplete " << (msg.tx_complete_scans() ? "true" : "false")
          << std::endl;
      }
      break;
    case rapic::message_type::query:
      {
        rapic::query msg;
        con.decode(msg);
        std::cout << "QUERY: " << std::endl;
      }
      break;
    case rapic::message_type::filter:
      {
        rapic::filter msg;
        con.decode(msg);
        std::cout << "FILTER: "
          << " station " << msg.station_id()
          << " stype " << static_cast<int>(msg.scan_type())
          << " volid " << msg.volume_id()
          << " vres " << msg.video_resolution()
          << " types";
        for (auto& type : msg.data_types())
          std::cout << " " << type;
        std::cout << std::endl;
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
