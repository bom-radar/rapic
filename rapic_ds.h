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

  /// Available message types
  enum class message_type
  {
      mssg      ///< MSSG administration message
    , scan      ///< rapic scan message
  };

  /// MSSG status message
  struct mssg
  {
    std::string content;
  };

  /// Header used by a scan message
  class header
  {
  public:
    header(std::string name, std::string value)
      : name_(std::move(name)), value_(std::move(value))
    { }

    /// Get the name of the header
    auto name() const -> std::string const&           { return name_; }
    /// Set the name of the header
    auto set_name(std::string const& name) -> void    { name_ = name; }

    /// Get the header value
    auto value() const -> std::string const&          { return value_; }
    /// Get the header value
    auto set_value(std::string const& value) -> void  { value_ = value; }

    /// Get the header value as a bool
    auto get_boolean() const -> bool;
    /// Get the header value as a long
    auto get_integer() const -> long;
    /// Get the header value as a double
    auto get_real() const -> double;
    /// Get the header value as a vector of longs
    auto get_integer_array() const -> std::vector<long>;
    /// Get the header value as a vector of doubles
    auto get_real_array() const -> std::vector<double>;

  private:
    std::string name_;
    std::string value_;
  };

  /// Information about a single ray
  class ray
  {
  public:
    ray()
      : azimuth_{nan<float>()}, elevation_{nan<float>()}, time_offset_{-1}
    { }

    ray(float azimuth)
      : azimuth_{azimuth}, elevation_{nan<float>()}, time_offset_{-1} 
    { }

    ray(float azimuth, float elevation, int time_offset)
      : azimuth_{azimuth}, elevation_{elevation}, time_offset_{time_offset}
    { }

    /// Get the azimuth at the center of this ray (degrees)
    auto azimuth() const -> float   { return azimuth_; }

    /// Get the elevation at the center of this ray (degrees)
    auto elevation() const -> float { return elevation_; }

    /// Get the time offset from the start of scan to this ray (seconds)
    auto time_offset() const -> int { return time_offset_; }

  private:
    float azimuth_;
    float elevation_;
    int   time_offset_;
  };

  /// Radar product message
  class scan
  {
  public:
    /// Construct an empty scan
    scan();

    /// Reset the scan to an uninitialized state
    auto reset() -> void;

    /// Decode a scan from the raw wire format
    auto decode(uint8_t const* in, size_t size) -> void;

    /// Get the station identifier
    auto station_id() const -> int                                    { return station_id_; }

    /// Get the volume identifier
    /** If there is no volume identifier this function returns -1 */
    auto volume_id() const -> int                                     { return volume_id_; }

    /// Get the product string
    /** This value is normally unique to each complete product which is built from multiple scan messages.  For
     *  example, a volume product contains many passes which each share this string. */
    auto product() const -> std::string const&                        { return product_; }

    /// Get the pass number
    /** If the pass number is unavailable this function returns -1 */
    auto pass() const -> int                                          { return pass_; }

    /// Get the number of passes in the containing product
    /** If the pass count is unavailable this function returns -1 */
    auto pass_count() const -> int                                    { return pass_count_; }

    /// Get the minimum angle for the scan
    /** This value is normally 0 (for a complete sweep), ANGLE1 from the product header (for ascending RHIs or
     *  CW sector sweeps) or ANGLE2 from the product header (for descending RHIs or CCW sector sweeps). */
    auto angle_min() const -> float                                   { return angle_min_; }

    /// Get the maximum angle for the scan
    /** This value is normally 360 (for a complete sweep), ANGLE2 from the product header (for ascending RHIs or
     *  CW sector sweeps) or ANGLE1 from the product header (for descending RHIs or CCW sector sweeps). */
    auto angle_max() const -> float                                   { return angle_max_; }

    /// Get the anglular resolution for the scan
    /** This value represents the angular sweep width of a single ray. */
    auto angle_resolution() const -> float                            { return angle_resolution_; }

    /// Access all the scan headers
    /** Note that all rapic headers are available via the returned container including those which are exposed
     *  explicitly via other functions. */
    auto headers() const -> std::vector<header> const&                { return headers_; }

    /// Find a specific header
    /** Returns nullptr if the header is not present. */
    auto find_header(std::string const& name) const -> header const*  { return find_header(name.c_str()); }
    auto find_header(char const* name) const -> header const*;

    /// Access the information about each ray
    auto rays() const -> std::vector<ray> const&                      { return rays_; }

    /// Access the scan data encoded as levels
    auto level_data() const -> array2<uint8_t> const&                 { return level_data_; }

    /// Access the scan data as native moment values
    /** If the correct conversion from level data to moment data cannot be determined this function will throw
     *  an exception. */
    auto scan_data() const -> array2<float>;

  private:
    auto get_header_string(char const* name) const -> std::string const&;
    auto get_header_integer(char const* name) const -> long;
    auto get_header_real(char const* name) const -> double;
    auto initialize_rays() -> void;

  private:
    std::vector<header> headers_;     // scan headers
    std::vector<ray>    rays_;        // ray headers
    array2<uint8_t>     level_data_;  // level encoded scan data

    // these are cached from the headers structure due to likelyhood of frequent access
    int         station_id_;
    int         volume_id_;
    std::string product_;
    int         pass_;
    int         pass_count_;
    bool        is_rhi_;
    float       angle_min_;
    float       angle_max_;
    float       angle_resolution_;
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
    auto add_filter(int station, std::string const& product, std::vector<std::string> const& moments = {}) -> void;

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

  /// Write a list of rapic scans as an ODIM_H5 polar volume file
  /**
   * This function assumes the following preconditions about the scan_set:
   * - All scans are of the VOLUMETRIC product type
   * - All scans belong to the same product instance
   * - The list is sorted by pass order such that all passess associated with a tilt are grouped together
   *
   * The tilts and passess will be written out in the order of the list.  That is, the first scan will be
   * written to the ODIM group dataset1/data1.
   */
  void write_odim_h5_volume(std::string const& path, std::list<scan> const& scan_set);
}}
