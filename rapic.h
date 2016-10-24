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
#ifndef RAPIC_H
#define RAPIC_H

#include <atomic>
#include <bitset>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <list>

namespace rapic
{
  /// Get the SCM release tag that the library was built from
  auto release_tag() -> char const*;

  /// Available message types
  enum class message_type
  {
      comment   ///< Comment line starting with '/' (used for IMAGE headers in volume files)
    , mssg      ///< Administration message (MSSG)
    , status    ///< Status message (RDRSTAT)
    , permcon   ///< Semipermanent connection message (RPQUERY: SEMIPERMANENT CONNECTION)
    , query     ///< Data request message (RPQUERY)
    , filter    ///< Filter specification message (RPFILTER)
    , scan      ///< Rapic scan message
  };

  /// Possible scan types for queries and filters
  /* note: the numbers here are chosen to match Rowlf (since Rowlf sends numeric values instead of strings) */
  enum class scan_type
  {
      any = -1
    , ppi = 0
    , rhi = 1
    , comp_ppi = 2
    , image = 3
    , volume = 4
    , rhi_set = 5
    , merge = 6
    , scan_error = 7
  };

  /// Possible query types, largely unused
  enum class query_type
  {
      latest
    , to_time
    , from_time
    , center_time
  };

  /// Error thrown upon failure to decode the rapic message stream
  class decode_error : public std::runtime_error
  {
  public:
    decode_error(char const* desc, uint8_t const* in, size_t size);
    decode_error(message_type type, uint8_t const* in, size_t size);
  };

  /// Buffer for raw message data
  class buffer
  {
  public:
    /// Construct a buffer of the given size
    buffer(size_t size, size_t max_size = std::numeric_limits<size_t>::max());

    /// Copy constructor
    buffer(buffer const& rhs);

    /// Move constructor
    buffer(buffer&& rhs) noexcept = default;

    /// Copy assignment operator
    auto operator=(buffer const& rhs) -> buffer&;

    /// Move assignment operator
    auto operator=(buffer&& rhs) noexcept -> buffer& = default;

    /// Get the total size of the buffer
    auto size() const -> size_t;

    /// Clear any unread contents in the buffer
    auto clear() -> void;

    /// Change the buffer capacity
    /** If the size changed then the underlying buffer memory will be reallocated.  Unlike std::vector, this
     *  reallocation will occur even if the size is reduced. */
    auto resize(size_t size) -> void;

    /// Shift unread data to the front of the buffer
    auto optimize() -> void;

    /// Get a pointer to the write position of the buffer and the amount of contiguous space available for writing
    auto write_acquire(size_t min_space) -> std::pair<uint8_t*, size_t>;

    /// Advance the write position after having written len bytes into the pointer returned by write_acquire()
    auto write_advance(size_t len) -> void;

    /// Get a pointer to the read position of the buffer and the amount of contiguous space available for reading
    /** This function is used to allow direct reading from the buffer.  This can be useful in applications where
     *  there is no requirement to actually decode the rapic data (such as data logging). */
    auto read_acquire() const -> std::pair<uint8_t const*, size_t>;

    /// Advance the read position by len bytes
    auto read_advance(size_t len) -> void;

    /// Determine whether there is a complete message that can be read from the buffer, its type and length
    /** This function should be used to detect a message ready for decoding in a buffer.  If a message is ready for
     *  reading then the function will return true.  At this point the user may choose to decode the message using
     *  the decode() function of the appropriate concrete messag type.  The user must call read_advance() on the
     *  buffer passing in the length which is output from this function to advance past the current message to the
     *  next message in the buffer.  If read_advance() is not called then read_detect() and decode() will repeatedly
     *  detect and decode the same message. */
    auto read_detect(message_type& type, size_t& len) const -> bool;

  private:
    size_t                      size_;
    std::unique_ptr<uint8_t[]>  data_;
    size_t                      wpos_;
    size_t                      rpos_;
    size_t                      max_size_;
  };

  /// Abstract base class for message types
  class message
  {
  public:
    virtual ~message();

    /// Get the type of this message
    virtual auto type() const -> message_type = 0;

    /// Reset the message to a default state
    virtual auto reset() -> void = 0;

    /// Encode the message into the wire format
    virtual auto encode(buffer& out) const -> void = 0;

    /// Decode the message from the wire format
    /** It is the user's responsibility to ensure that the concrete type of the message object matches the encoded
     *  message currently at the front of the buffer.  This would normally be ensured by first calling the
     *  read_detect() function of the input buffer. */
    virtual auto decode(buffer const& in) -> void = 0;
  };

  /// Comment message
  /** This message type is generally only found in rapic files where multiple rapic scans have been concatenated
   *  into a single volume file.  Comments start with a forward slash '/' and are used to implement meta-headers
   *  such as IMAGE, RXTIME, IMAGESCANS, IMAGESIZE, IMAGEHEADER etc.  These headers provide information to
   *  3D-Rapic which allow it to index directly into the file without parsing every scan.  Messages of this type
   *  are never sent by Bureau radar transmitters over the wire. */
  class comment : public message
  {
  public:
    /// Construct an empty comment message
    comment();

    auto type() const -> message_type override;
    auto reset() -> void override;
    auto encode(buffer& out) const -> void override;
    auto decode(buffer const& in) -> void override;

    /// Get the message string
    auto text() const -> std::string const&                           { return text_; }
    /// Set the message string
    auto set_text(std::string const& val) -> void                     { text_ = val; }

  private:
    std::string text_;
  };

  /// MSSG status message
  class mssg : public message
  {
  public:
    /// Construct an empty status message
    mssg();

    auto type() const -> message_type override;
    auto reset() -> void override;
    auto encode(buffer& out) const -> void override;
    auto decode(buffer const& in) -> void override;

    /// Get the message number
    auto number() const -> int                                        { return number_; }
    /// Set the message number
    auto set_number(int val) -> void                                  { number_ = val; }

    /// Get the message string
    auto text() const -> std::string const&                           { return text_; }
    /// Set the message string
    auto set_text(std::string const& val) -> void                     { text_ = val; }

  private:
    int         number_;
    std::string text_;
  };

  /// RDRSTAT status message
  /** This message is used as a keepalive for rapic protocol connections.  It contains no actual useful data. */
  class status : public message
  {
  public:
    /// Construct an empty status message
    status();

    auto type() const -> message_type override;
    auto reset() -> void override;
    auto encode(buffer& out) const -> void override;
    auto decode(buffer const& in) -> void override;

    /// Get the message string
    auto text() const -> std::string const&                           { return text_; }
    /// Set the message string
    auto set_text(std::string const& val) -> void                     { text_ = val; }

  private:
    std::string text_;
  };

  /// Semi-permanent connection message
  class permcon : public message
  {
  public:
    /// Construct an empty permanent connection message
    permcon();

    auto type() const -> message_type override;
    auto reset() -> void override;
    auto encode(buffer& out) const -> void override;
    auto decode(buffer const& in) -> void override;

    /// Get whether txcompletescans is set
    auto tx_complete_scans() const -> bool                            { return tx_complete_scans_; }

  private:
    bool tx_complete_scans_;
  };

  /// RPQUERY message
  class query : public message
  {
  public:
    /// Construct an empty query
    query();

    auto type() const -> message_type override;
    auto reset() -> void override;
    auto encode(buffer& out) const -> void override;
    auto decode(buffer const& in) -> void override;

    /// Get the station identifier (0 = any)
    auto station_id() const -> int                                    { return station_id_; }

    /// Get the scan type
    auto scan_type() const -> rapic::scan_type                        { return scan_type_; }

    /// Get the volume id (-1 = any or not volume)
    auto volume_id() const -> int                                     { return volume_id_; }

    /// Get the selected angle (-1 = default)
    auto angle() const -> float                                       { return angle_; }

    /// Get the repeat count (-1 = default)
    auto repeat_count() const -> int                                  { return repeat_count_; }

    /// Get the query type (latest by default)
    auto query_type() const -> rapic::query_type                      { return query_type_; }

    /// Get the image time (0 = latest image)
    auto time() const -> time_t                                       { return time_; }

    /// Get the data types
    auto data_types() const -> std::vector<std::string> const&        { return data_types_; }

    /// Get the video resolution
    auto video_resolution() const -> int                              { return video_res_; }

  private:
    int                       station_id_;
    rapic::scan_type          scan_type_;
    int                       volume_id_;
    float                     angle_;
    int                       repeat_count_;
    rapic::query_type         query_type_;
    time_t                    time_;
    std::vector<std::string>  data_types_;
    int                       video_res_;
  };

  /// RPFILTER message
  class filter : public message
  {
  public:
    /// Construct an empty query
    filter();

    auto type() const -> message_type override;
    auto reset() -> void override;
    auto encode(buffer& out) const -> void override;
    auto decode(buffer const& in) -> void override;

    /// Get the station identifier
    auto station_id() const -> int                                    { return station_id_; }

    /// Get the scan type
    auto scan_type() const -> rapic::scan_type                        { return scan_type_; }

    /// Get the volume id (-1 = any or not volume)
    auto volume_id() const -> int                                     { return volume_id_; }

    /// Get the video resolution
    auto video_resolution() const -> int                              { return video_res_; }

    /// Get the source identifier (-1 = default)
    auto source() const -> std::string const&                         { return source_; }

    /// Get the data types
    auto data_types() const -> std::vector<std::string> const&        { return data_types_; }

  private:
    int                       station_id_;
    rapic::scan_type          scan_type_;
    int                       volume_id_;
    int                       video_res_;
    std::string               source_;
    std::vector<std::string>  data_types_;
  };

  /// Radar product message
  class scan : public message
  {
  public:
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
    class ray_header
    {
    public:
      ray_header()
        : azimuth_{std::numeric_limits<float>::quiet_NaN()}
        , elevation_{std::numeric_limits<float>::quiet_NaN()}
        , time_offset_{-1}
      { }

      ray_header(float azimuth)
        : azimuth_{azimuth}, elevation_{std::numeric_limits<float>::quiet_NaN()}, time_offset_{-1}
      { }

      ray_header(float azimuth, float elevation, int time_offset)
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

  public:
    /// Construct an empty scan
    scan();

    auto type() const -> message_type override;
    auto reset() -> void override;
    auto encode(buffer& out) const -> void override;
    auto decode(buffer const& in) -> void override;

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
    auto ray_headers() const -> std::vector<ray_header> const&        { return ray_headers_; }

    /// Get the number of rays (ie: rows) in the level data array
    auto rays() const -> int                                          { return rays_; }

    /// Get the number of bins (ie: columns) in the level data array
    auto bins() const -> int                                          { return bins_; }

    /// Access the scan data encoded as levels
    auto level_data() const -> uint8_t const*                         { return level_data_.data(); }

  private:
    auto get_header_string(char const* name) const -> std::string const&;
    auto get_header_integer(char const* name) const -> long;
    auto get_header_real(char const* name) const -> double;
    auto initialize_rays() -> void;

  private:
    std::vector<header>     headers_;     // scan headers
    std::vector<ray_header> ray_headers_; // ray headers
    int                     rays_;
    int                     bins_;
    std::vector<uint8_t>    level_data_;  // level encoded scan data

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

  /// RAII wrapper for a socket file descriptor
  class socket_handle
  {
  public:
    explicit socket_handle(int fd = -1) : fd_(fd) { }
    socket_handle(const socket_handle& rhs) = delete;
    socket_handle(socket_handle&& rhs) noexcept : fd_(rhs.fd_) { rhs.fd_ = -1; }
    socket_handle& operator=(const socket_handle& rhs) = delete;
    socket_handle& operator=(socket_handle&& rhs) noexcept { std::swap(fd_, rhs.fd_); return *this; }
    ~socket_handle();

    operator int() const                      { return fd_; }
    operator bool() const                     { return fd_ != -1; }

    auto fd() const -> int                    { return fd_; }
    auto reset(int fd = -1) -> void;
    auto release() -> int;

  private:
    int fd_;
  };

  /// Rapic protocol connection manager
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
   *    // keep processing while the connection is open
   *    while (con.connected()) {
   *      // wait for data to arrive
   *      con.poll();
   *
   *      // process data received from remote host
   *      while (con.proces_traffic()) {
   *        // dequeue each completed message
   *        message_type type;
   *        while (con.dequeue(type)) {
   *          // decode and handle the message types we care about
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
   */
  class client
  {
  public:
    /// Construct a new connection
    client(size_t max_buffer_size = std::numeric_limits<size_t>::max(), time_t keepalive_period = 40);

    client(client const&) = delete;
    auto operator=(client const&) -> client& = delete;

    /// Move construct a client connection
    client(client&& rhs) noexcept = default;

    /// Move assign a client connection
    auto operator=(client&& rhs) noexcept -> client& = default;

    /// Destroy the client connection and automatically disconnect if needed
    ~client() = default;

    /// Add a product to filter for radar products
    /** Filters added by this function will only take effect at the next call to connect(). */
    auto add_filter(int station, std::string const& product, std::vector<std::string> const& moments = {}) -> void;

    /// Accept a pending connection from a listening socket
    auto accept(socket_handle sock, std::string address, std::string service) -> void;

    /// Connect to a remote server
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

    /// Encode and send a message to the remote server
    auto enqueue(message& msg) -> void;

    /// Dequeue the next available message and return its type
    /** If no message is available, the function returns false.
     *  Each time dequeue is called the stream position is advanced to the next message regardless of whether
     *  the decode function has been called for the current message.  This means that there is no need to decode
     *  messages about which you are not interested. */
    auto dequeue(message_type& type) -> bool;

    /// Decode the current message into the relevant message structure
    /** If the type of the message argument passed does not match the currently active message (as returned by the
     *  most recent call to dequeue) then a runtime exception will be thrown. */
    auto decode(message& msg) -> void;

  private:
    using filter_store = std::vector<std::string>;

  private:
    std::string       address_;           // remote hostname or address
    std::string       service_;           // remote service or port number
    time_t            keepalive_period_;  // time between sending keepalives
    filter_store      filters_;           // filter strings
    socket_handle     socket_;            // socket handle
    bool              establish_wait_;    // are we waiting for socket connection to be established?
    time_t            last_keepalive_;    // time of last keepalive send

    buffer            rbuf_;              // read buffer

    message_type      cur_type_;          // type of currently dequeued message (awaiting decode)
    size_t            cur_size_;          // size of currently dequeued message
  };

  /// Rapic protocol listen socket manager
  class server
  {
  public:
    server();

    /// Start listening for new clients on the passed service/port
    auto listen(std::string service, bool ipv6 = true) -> void;

    /// Cease listening for new clients and release the service/port
    auto release() -> void;

    /// Accept any pending connections and return connection managers for them
    auto accept_pending_connections(size_t buffer_size = 10 * 1024 * 1024, time_t keepalive_period = 40) -> std::list<client>;

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

  private:
    socket_handle     socket_;            // listen socket handle
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
   *
   * A custom function may be provided in the third argument to provide a mechanism for reporting warnings
   * during the conversion process.
   */
  auto write_odim_h5_volume(
        std::string const& path
      , std::list<scan> const& scan_set
      , std::function<void(char const*)> log_fn = [](char const*) { }
      ) -> time_t;
}
#endif
