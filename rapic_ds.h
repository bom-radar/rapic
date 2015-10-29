/*------------------------------------------------------------------------------
 * Rapic Data Server client connection API for C++11
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#pragma once

#include <rainutil/array.h>
#include <rainutil/real.h>
#include <rainutil/timestamp.h>

#include <atomic>
#include <bitset>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <list>

namespace rainfields {
namespace rapic {

  /// Get the SCM release tag that the library was built from
  auto release_tag() -> char const*;

  /// Scan types
  enum class product_type
  {
      unknown
    , ppi
    , rhi
    , comp_ppi      // special - can have product index
    , image
    , volume        // special - can have product index
    , rhi_set
    , merge
    , scan_error
  };

  /// Data formats
  /** It is important that these have the value corresponding to their name - they are used as raw integers
   *  occasionally (look for static_cast's involving the 'scan::vidres' member. */
  enum class video_format
  {
      unknown
    , _6 = 6
    , _16 = 16
    , _32 = 32
    , _64 = 64
    , _160 = 160
    , _256 = 256
    //, raw_256 - specified as an enumeration in rapic and rowlf but unused by rapic
  };

  enum class dual_prf_strategy
  {
      unknown
    , high_evens
    , high_odds
  };

  enum class dual_prf_ratio
  {
      unknown
    , none
    , _2_3
    , _3_4
    , _4_5
  };

  enum class image_format
  {
      unknown
    , rhi
    , compppi
    , ppi
  };

  enum class polarisation_type
  {
      unknown
    , horizontal
    , vertical
    , alternating
  };

  /// Available message types
  enum class message_type
  {
      mssg      ///< MSSG administration message
    , scan      ///< rapic scan message
  };

  /// MSSG status message
  struct mssg
  {
    std::string         content;
  };

  /// Radar product message
  struct scan
  {
    double              angle1          = nan<double>();
    double              angle2          = nan<double>();
    bool                angleincreasing = true;
    double              anglerate       = nan<double>();
    double              angres          = nan<double>();
    double              antdiam         = nan<double>();
    double              azcorr          = nan<double>();
    double              azim            = nan<double>();
    long                compppiid       = -1;
    std::string         copyright;
    long                country         = -1;
    long                date            = -1;
    double              dbm2dbz         = nan<double>();
    std::vector<double> dbmlvl;
    std::vector<double> dbzlvl;
    double              elcorr          = nan<double>();
    double              elev            = nan<double>();
    double              endrng          = nan<double>();
    double              frequency       = nan<double>();
    std::string         fault;
    double              hbeamwidth      = nan<double>();
    double              height          = nan<double>();
    dual_prf_strategy   hiprf           = dual_prf_strategy::unknown;
    image_format        imgfmt          = image_format::unknown;
    double              latitude        = nan<double>();
    double              longitude       = nan<double>();
    std::string         name;
    double              nyquist         = nan<double>();
    long                pass            = -1;
    long                pass_count      = -1;
    double              peakpower       = nan<double>();
    double              peakpowerh      = nan<double>();
    double              peakpowerv      = nan<double>();
    polarisation_type   polarisation    = polarisation_type::unknown;
    double              prf             = nan<double>();
    product_type        product         = product_type::unknown;
    double              pulselength     = nan<double>();
    std::string         radartype;
    double              rngres          = nan<double>();
    double              rxgain_h        = nan<double>();
    double              rxgain_v        = nan<double>();
    double              rxnoise_h       = nan<double>();
    double              rxnoise_v       = nan<double>();
    double              startrng        = nan<double>();
    long                stn_num         = -1;
    long                stnid           = -1;
    long                tilt            = -1;
    long                tilt_count      = -1;
    std::pair<int, int> time            = {-1, -1};
    rainfields::timestamp timestamp     = rainfields::timestamp::min();
    dual_prf_ratio      unfolding       = dual_prf_ratio::unknown;
    double              vbeamwidth      = nan<double>();
    std::string         vers;
    std::string         video;
    double              videogain       = nan<double>();
    double              videooffset     = nan<double>();
    std::string         videounits;
    video_format        vidres          = video_format::unknown;
    long                volumeid        = -1;
    std::string         volumelabel;
    long                wmo_number      = -1;

    long                first_ray       = -1;
    array2<int>         data;

    scan() = default;
    scan(std::istream& in);

  private:
    auto parse_header(std::string const& key, std::string const& value) -> void;
  };

  /// Rapic Data Server client connection manager
  /** This class is implemented with the expectation that it may be used in an environment where asynchronous I/O
   *  is desired.  As such, the most basic use of this class requires calling separate functions for checking
   *  data availability on the connection, processing connection traffic, dequeuing and decoding messages.
   *  If synchronous I/O is desired then these calls may simply be chained together one after another.
   *
   *  The basic synchronous usage sequence is:
   *    // create a connection and connect to server
   *    client con;
   *    con.connect("myhost", "1234");
   *
   *    // wait for data to arrive
   *    while (con.connected()) {
   *      con.poll();
   *    
   *      // process messages from server
   *      bool again = true;
   *      while (again) {
   *        again = con.process_traffic();
   *    
   *        // dequeue each message
   *        message_type type;
   *        while (con.dequeue(type)) {
   *          // decode and handle interesting messages
   *          if (type == message_type::scan) {
   *            scan msg;
   *            con.decode(msg);
   *            ...
   *          }
   *        }
   *      }
   *    }
   *
   * For asynchronous usage, the user should use the pollable_fd(), poll_read() and poll_write() functions to
   * setup the appropriate multiplexed polling function for their application.
   *
   * It is also safe to use this class in a multi-threaded environment where one thread manages the communications
   * and another thread handles the incoming messages.  In such a setup thread safety is contingent on the following
   * conditions:
   *  - Communications is handled by a single thread which calls process_traffic()
   *  - Message processing is handled by a single thread which calls dequeue() and decode()
   *  - The connect() function must not be called at the same time as any other member function
   *
   * The const member functions may be called safely from any thread at any time.  It is suggested that the poll
   * functions be called from the communications thread, while the syncrhonized function be called from the 
   * message handler thread for maximum consistency.
   */
  class client
  {
  public:
    /// Construct a new connection
    client(size_t buffer_size = 10 * 1024 * 1024, time_t keepalive_period = 40);

    client(client const&) = delete;
    auto operator=(client const&) -> client& = delete;

    /// Move construct a client connection
    client(client&& rhs) noexcept;

    /// Move assign a client connection
    auto operator=(client&& rhs) noexcept -> client&;

    /// Destroy the client connection and automatically disconnect if needed
    ~client();

    /// Add a product to filter for radar products
    /** Filters added by this function will only take effect at the next call to connect(). */
    auto add_filter(
          int station
        , product_type type
        , int scan_id = -1
        , std::vector<std::string> const& moments = {}
        ) -> void;

    /// Connect to a server
    auto connect(std::string address, std::string service) -> void;

    /// Disconnect from the server
    auto disconnect() -> void;

    /// Return true if a connection to the server is currently active
    auto connected() const -> bool;

    /// Get the file descriptor of the socket which may be used for multiplexed polling
    /** This function along with poll_read and poll_write are useful in an asynchronous I/O environment and you
     *  would like to block on multiple I/O sources.  The file descriptor returned by this function may be passed
     *  to pselect or a similar function.  The poll_read and poll_write functions return true if you should wait
     *  for read and write availability respectively. */
    auto pollable_fd() const -> int;

    /// Get whether the socket file descriptor should be monitored for read availability
    auto poll_read() const -> bool;

    /// Get whether the socket file descriptor should be montored for write availability
    auto poll_write() const -> bool;

    /// Wait (block) on the socket until some traffic arrives for processing
    /** The optional timeout parameter may be supplied to force the function to return after a cerain number
     *  of milliseconds.  The default is 10 seconds. */
    auto poll(int timeout = 10000) const -> void;

    /// Process traffic on the socket (may cause new messages to be available for dequeue)
    /** This function will read from the server connection and queue any available messages in a buffer.  The
     *  messages may subsequently be retrieved by calling deqeue (and decode if desired) repeatedly until
     *  dequeue returns message_type::none.
     *
     *  If this function returns false then there is no more data currently available on the socket.  This
     *  behaviour can be used in an asynchronous I/O environment when deciding whether to continue processing
     *  traffic on this socket, or allow entry to a multiplexed wait (such as pselect). */
    auto process_traffic() -> bool;

    /// Get the hostname or address of the remote server
    auto address() const -> std::string const&;

    /// Get the service or port name for the connection
    auto service() const -> std::string const&;

    /// Dequeue the next available message and return its type
    /** If no message is available, the function returns false.
     *  Each time dequeue is called the stream position is advanced to the next message regardless of whether
     *  the decode function has been called for the current message.  This means that there is no need to decode
     *  messages about which you are not interested. */
    auto dequeue(message_type& type) -> bool;

    /// Decode the current message into the relevant message structure
    /** If the type of the message argument passed does not match the currently active message (as returned by the
     *  most recent call to dequeue) then a runtime exception will be thrown. */
    auto decode(mssg& msg) -> void;
    auto decode(scan& msg) -> void;

  private:
    using filter_store = std::vector<std::string>;
    using buffer = std::unique_ptr<uint8_t[]>;

  private:
    auto check_cur_type(message_type type) -> void;
    auto buffer_ignore_whitespace() -> void;
    auto buffer_starts_with(std::string const& str) const -> bool;
    auto buffer_find(std::string const& str, size_t& pos) const -> bool;

  private:
    std::string       address_;           // remote hostname or address
    std::string       service_;           // remote service or port number
    time_t            keepalive_period_;  // time between sending keepalives
    filter_store      filters_;           // filter strings
    int               socket_;            // socket handle
    bool              establish_wait_;    // are we waiting for socket connection to be established?
    time_t            last_keepalive_;    // time of last keepalive send

    buffer            buffer_;            // ring buffer to store packets off the wire
    size_t            capacity_;          // total usable buffer capacity
    std::atomic_uint  wcount_;            // total bytes that have been written (wraps)
    std::atomic_uint  rcount_;            // total bytes that have been read (wraps)

    message_type      cur_type_;          // type of currently dequeued message (awaiting decode)
    size_t            cur_size_;          // size of currently dequeued message
  };

  void write_odim_h5_volume(std::string const& path, std::list<scan> const& scan_set);
}}
