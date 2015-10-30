/*------------------------------------------------------------------------------
 * Rapic Data Server client connection API for C++11
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rapic_ds.h"

#include <rainutil/array_utils.h>
#include <rainutil/trace.h>
#include <rainutil/string_utils.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <cmath>
#include <ctime>
#include <stdexcept>
#include <sstream>
#include <system_error>
#include <tuple>

using namespace rainfields;
using namespace rainfields::rapic;

static constexpr message_type no_message = static_cast<message_type>(-1);

static const std::string msg_connect{"RPQUERY: SEMIPERMANENT CONNECTION - SEND ALL DATA TXCOMPLETESCANS=0\n"};
static const std::string msg_keepalive{"RDRSTAT:\n"};
static const std::string msg_mssg_head{"MSSG:"};
static const std::string msg_mssg_term{"\n"};
static const std::string msg_mssg30_head{"MSSG: 30"};
static const std::string msg_mssg30_term{"END STATUS\n"};
static const std::string msg_scan_term{"END RADAR IMAGE"};

// this table translates the ASCII encoding absolute, RLE digits and delta lookups
enum enc_type : int
{
    value
  , digit
  , delta
  , error
  , terminate
};
struct lookup_value
{
  enc_type type;
  int val;
  int val2;
};
constexpr lookup_value lend() { return { enc_type::terminate, 0, 0 }; }
constexpr lookup_value lnul() { return { enc_type::error, 0, 0 }; }
constexpr lookup_value lval(int x) { return { enc_type::value, x, 0 }; }
constexpr lookup_value lrel(int x) { return { enc_type::digit, x, 0 }; }
constexpr lookup_value ldel(int x, int y) { return { enc_type::delta, x, y }; }
constexpr lookup_value lookup[] = 
{
  lend(),     lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      // 00-07
  lnul(),     lnul(),      lend(),      lnul(),      lnul(),      lend(),      lnul(),      lnul(),      // 08-0f
  lnul(),     lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      // 10-17
  lnul(),     lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      // 18-1f
  lnul(),     ldel(-3,-3), lval(16),    lnul(),      ldel(-3,3),  lnul(),      ldel(3, 3),  lval(17),    // 20-27
  ldel(-3,2), ldel(3,2),   lval(18),    ldel(1,0),   lval(19),    ldel(-1,0),  ldel(0,0),   ldel(-3,-2), // 28-2f
  lrel(0),    lrel(1),     lrel(2),     lrel(3),     lrel(4),     lrel(5),     lrel(6),     lrel(7),     // 30-37
  lrel(8),    lrel(9),     lval(20),    lval(21),    ldel(0,-1),  lval(22),    ldel(0,1),   lval(23),    // 38-3f
  ldel(3,-3), lval(0),     lval(1),     lval(2),     lval(3),     lval(4),     lval(5),     lval(6),     // 40-47
  lval(7),    lval(8),     lval(9),     lval(10),    lval(11),    lval(12),    lval(13),    lval(14),    // 48-4f
  lval(15),   lval(24),    lval(25),    ldel(-1,2),  ldel(0,2),   ldel(1,2),   ldel(2,2),   ldel(-1,3),  // 50-57
  ldel(0,3),  ldel(1,3),   lval(26),    ldel(-2,-3), ldel(3,-2),  ldel(2,-3),  lval(27),    lval(28),    // 58-5f
  lnul(),     ldel(-1,-3), ldel(0,-3),  ldel(1,-3),  ldel(-2,-2), ldel(-1,-2), ldel(0,-2),  ldel(1,-2),  // 60-67
  ldel(2,-2), ldel(-3,-1), ldel(-2,-1), ldel(-1,-1), ldel(1,-1),  ldel(2,-1),  ldel(3,-1),  ldel(-3,0),  // 68-6f
  ldel(-2,0), ldel(2,0),   ldel(3,0),   ldel(-3,1),  ldel(-2,1),  ldel(-1,1),  ldel(1,1),   ldel(2,1),   // 70-77
  ldel(3,1),  ldel(-2,2),  lval(29),    ldel(-2,3),  lval(30),    ldel(2,3),   lval(31),    lnul(),      // 78-7f
  lval(32),   lval(33),    lval(34),    lval(35),    lval(36),    lval(37),    lval(38),    lval(39),    // 80-87
  lval(40),   lval(41),    lval(42),    lval(43),    lval(44),    lval(45),    lval(46),    lval(47),    // 88-8f
  lval(48),   lval(49),    lval(50),    lval(51),    lval(52),    lval(53),    lval(54),    lval(55),    // 90-97
  lval(56),   lval(57),    lval(58),    lval(59),    lval(60),    lval(61),    lval(62),    lval(63),    // 98-9f
  lval(64),   lval(65),    lval(66),    lval(67),    lval(68),    lval(69),    lval(70),    lval(71),    // a0-a7
  lval(72),   lval(73),    lval(74),    lval(75),    lval(76),    lval(77),    lval(78),    lval(79),    // a8-af
  lval(80),   lval(81),    lval(82),    lval(83),    lval(84),    lval(85),    lval(86),    lval(87),    // b0-b7
  lval(88),   lval(89),    lval(90),    lval(91),    lval(92),    lval(93),    lval(94),    lval(95),    // b8-bf
  lval(96),   lval(97),    lval(98),    lval(99),    lval(100),   lval(101),   lval(102),   lval(103),   // c0-c7
  lval(104),  lval(105),   lval(106),   lval(107),   lval(108),   lval(109),   lval(110),   lval(111),   // c8-cf
  lval(112),  lval(113),   lval(114),   lval(115),   lval(116),   lval(117),   lval(118),   lval(119),   // d0-d7
  lval(120),  lval(121),   lval(122),   lval(123),   lval(124),   lval(125),   lval(126),   lval(127),   // d8-df
  lval(128),  lval(129),   lval(130),   lval(131),   lval(132),   lval(133),   lval(134),   lval(135),   // e0-e7
  lval(136),  lval(137),   lval(138),   lval(139),   lval(140),   lval(141),   lval(142),   lval(143),   // e8-ef
  lval(144),  lval(145),   lval(146),   lval(147),   lval(148),   lval(149),   lval(150),   lval(151),   // f0-f7
  lval(152),  lval(153),   lval(154),   lval(155),   lval(156),   lval(157),   lval(158),   lval(159)    // f8-ff
};

auto rainfields::rapic::release_tag() -> char const*
{
  return RAPIC_DS_RELEASE_TAG;
}

auto header::get_boolean() const -> bool
{
  if (   strcasecmp(value_.c_str(), "true") == 0
      || strcasecmp(value_.c_str(), "on") == 0
      || strcasecmp(value_.c_str(), "yes") == 0
      || strcasecmp(value_.c_str(), "1") == 0)
    return true;

  if (   strcasecmp(value_.c_str(), "false") == 0
      || strcasecmp(value_.c_str(), "off") == 0
      || strcasecmp(value_.c_str(), "no") == 0
      || strcasecmp(value_.c_str(), "0") == 0)
    return false;

  throw std::runtime_error{"bad boolean value"};
}

auto header::get_integer() const -> long
{
  return from_string<long>(value_, 10);
}

auto header::get_real() const -> double
{
  return from_string<double>(value_);
}

auto header::get_integer_array() const -> std::vector<long>
{
  return tokenize<long>(value_, " ", 10);
}

auto header::get_real_array() const -> std::vector<double>
{
  return tokenize<double>(value_, " ");
}

scan::scan()
{
  reset();
}

auto scan::reset() -> void
{
  headers_.clear();
  rays_.clear();
  level_data_.resize(0, 0);

  station_id_ = -1;
  product_idx_ = -1;
  pass_ = -1;
  pass_count_ = -1;
  is_rhi_ = false;
}

auto scan::decode(uint8_t const* in, size_t size) -> void
try
{
  reset();

  for (size_t pos = 0; pos < size; ++pos)
  {
    auto next = in[pos];

    // ascii encoded ray
    if (next == '%')
    {
      ++pos;

      // if this is our first ray, setup the data structures
      if (rays_.empty())
        initialize_rays();

      // sanity check that we don't have too many rays
      if (rays_.size() == level_data_.rows())
        throw std::runtime_error{"scan data overflow (too many rays)"};

      // sanity check that we have enough space for at least the header
      if (pos + 4 >= size)
        throw std::runtime_error{"corrupt scan detected (1)"};

      // determine the ray angle
      float angle;
      if (sscanf(reinterpret_cast<char const*>(&in[pos]), is_rhi_ ? "%4f" : "%3f", &angle) != 1)
        throw std::runtime_error{"invalid ascii ray header"};
      pos += is_rhi_ ? 4 : 3;

      // create the ray entry
      rays_.emplace_back(angle);
      
      // decode the data into levels
      auto out = level_data_[rays_.size() - 1];
      int prev = 0;
      size_t bin = 0;
      while (pos < size)
      {
        auto& cur = lookup[in[pos++]];

        //  absolute pixel value
        if (cur.type == enc_type::value)
        {
          if (bin < level_data_.cols())
            out[bin++] = prev = cur.val;
          else
            throw std::runtime_error{"scan data overflow (ascii abs)"};
        }
        // run length encoding of the previous value
        else if (cur.type == enc_type::digit)
        {
          auto count = cur.val;
          while (pos < size && lookup[in[pos]].type == enc_type::digit)
          {
            count *= 10;
            count += lookup[in[pos++]].val;
          }
          if (bin + count > level_data_.cols())
            throw std::runtime_error{"scan data overflow (ascii rle)"};
          for (int i = 0; i < count; ++i)
            out[bin++] = prev;
        }
        // delta encoding
        // silently ignore potential overflow caused by second half of a delta encoding at end of ray
        // we assume it is just an artefact of the encoding process
        else if (cur.type == enc_type::delta)
        {
          if (bin < level_data_.cols())
            out[bin++] = prev += cur.val;
          else
            throw std::runtime_error{"scan data overflow (ascii delta)"};

          if (bin < level_data_.cols())
            out[bin++] = prev += cur.val2;
          else if (pos < size && lookup[in[pos]].type != enc_type::terminate)
            throw std::runtime_error{"scan data overflow (ascii delta)"};
        }
        // null or end of line character - end of radial
        else if (cur.type == enc_type::terminate)
        {
          --pos;
          break;
        }
        else
          throw std::runtime_error{"invalid character encountered in ray encoding"};
      }
    }
    // binary encoding
    else if (next == '@')
    {
      ++pos;

      // if this is our first ray, setup the data structures
      if (rays_.empty())
        initialize_rays();

      // sanity check that we don't have too many rays
      if (rays_.size() == level_data_.rows())
        throw std::runtime_error{"scan data overflow (too many rays)"};

      // sanity check that we have enough space for at least the header
      if (pos + 18 >= size)
        throw std::runtime_error{"corrupt scan detected (2)"};

      // read the ray header
      float azi, el;
      int sec;
      if (sscanf(reinterpret_cast<char const*>(&in[pos]), "%f,%f,%d=", &azi, &el, &sec) != 3)
        throw std::runtime_error("invalid binary ray header");
      // note: we ignore the length for now
      //auto len = (((unsigned int) in[16]) << 8) + ((unsigned int) in[17]);
      pos += 18;

      // create the ray entry
      rays_.emplace_back(azi, el, sec);

      // decode the data into levels
      auto out = level_data_[rays_.size() - 1];
      size_t bin = 0;
      while (true)
      {
        int val = in[pos++];
        if (val == 0 || val == 1)
        {
          size_t count = in[pos++];
          if (count == 0)
            break;
          if (bin + count > level_data_.cols())
            throw std::runtime_error{"scan data overflow (binary rle)"};
          for (size_t i = 0; i < count; ++i)
            out[bin++] = val;
        }
        else if (bin < level_data_.cols())
          out[bin++] = val;
        else
          throw std::runtime_error{"scan data overflow (binary abs)"};
      }
    }
    // header field
    else if (next > ' ')
    {
      size_t pos2, pos3, pos4;

      // find the end of the header name
      for (pos2 = pos + 1; pos2 < size; ++pos2)
        if (in[pos2] < ' ' || in[pos2] == ':')
          break;
      
      // check for end of scan or corruption
      if (pos2 >= size || in[pos2] != ':')
      {
        // valid end of scan?
        if (   pos2 - pos == msg_scan_term.size()
            && strncmp(reinterpret_cast<char const*>(&in[pos]), msg_scan_term.c_str(), msg_scan_term.size()) == 0)
          return;
        throw std::runtime_error{"corrupt scan detected (3)"};
      }

      // find the start of the header value
      for (pos3 = pos2 + 1; pos3 < size; ++pos3)
        if (in[pos3] > ' ')
          break;

      // check for corruption
      if (pos3 == size)
        throw std::runtime_error{"corrupt scan detected (4)"};

      // find the end of the header value
      for (pos4 = pos3 + 1; pos4 < size; ++pos4)
        if (in[pos4] < ' ') // note: spaces are valid characters in the header value
          break;

      // store the header
      headers_.emplace_back(
            std::string(reinterpret_cast<char const*>(&in[pos]), pos2 - pos)
          , std::string(reinterpret_cast<char const*>(&in[pos3]), pos4 - pos3));

      // advance past the header line
      pos = pos4;
    }
  }

  throw std::runtime_error{"corrupt scan detected (5)"};
}
catch (std::exception& err)
{
  msg desc;
  desc << "failed to decode scan";
  if (auto p = find_header("STNID"))
    desc << " stnid: " << p->value();
  if (auto p = find_header("NAME"))
    desc << " name: " << p->value();
  if (auto p = find_header("PRODUCT"))
    desc << " product: " << p->value();
  if (auto p = find_header("TILT"))
    desc << " tilt: " << p->value();
  if (auto p = find_header("PASS"))
    desc << " pass: " << p->value();
  if (auto p = find_header("VIDEO"))
    desc << " video: " << p->value();
  std::throw_with_nested(std::runtime_error{desc});
}

auto scan::find_header(char const* name) const -> header const*
{
  for (auto& h : headers_)
    if (h.name() == name)
      return &h;
  return nullptr;
}

auto scan::get_header_string(char const* name) const -> std::string const&
{
  if (auto p = find_header(name))
    return p->value();
  throw std::runtime_error{msg{} << "missing mandatory header " << name};
}

auto scan::get_header_integer(char const* name) const -> long
{
  if (auto p = find_header(name))
    return p->get_integer();
  throw std::runtime_error{msg{} << "missing mandatory header " << name};
}

auto scan::get_header_real(char const* name) const -> double
{
  if (auto p = find_header(name))
    return p->get_real();
  throw std::runtime_error{msg{} << "missing mandatory header " << name};
}

auto scan::initialize_rays() -> void
{
  // if this is our first ray, setup the data array

  // store the header fields which we cache
  station_id_ = get_header_integer("STNID");
  // TODO - product
  // TODO pass
  // TODO pass_count
  is_rhi_ = get_header_string("IMGFMT") == "RHI";

  // get the mandatory characteristics needed to determine scan structure
  double angres = get_header_real("ANGRES");
  double rngres = get_header_real("RNGRES");
  double startrng = get_header_real("STARTRNG");
  double endrng = get_header_real("ENDRNG");

  // if start/end angles are provided, use them to limit our ray count
  double azi_min, azi_max;
  // TODO - these come from the product itself...
  double angle1 = nan<float>(), angle2 = nan<float>();
  bool angleincreasing = true;
  // END TODO
  if (is_nan(angle1) || is_nan(angle2))
  {
    azi_min = 0.0;
    azi_max = 360.0;
  }
  else
  {
    azi_min = angleincreasing ? angle1 : angle2;
    azi_max = angleincreasing ? angle2 : angle1;
#if 0 // might need to re-enable this, but only for PPIs (not RHIs which validly have -ve angles)
    while (azi_min < 0)
      azi_min += 360;
    while (azi_max < azi_min)
      azi_max += 360;
#endif
  }
  
  int rays = std::lround((azi_max - azi_min) / angres);
  if (remainder(azi_max - azi_min, angres) > 0.001)
    throw std::runtime_error{"ANGRES is not a factor of sweep length"};

  int bins = std::lround((endrng - startrng) / rngres);
  if (bins < 0 || remainder(endrng - startrng, rngres) > 0.001)
    throw std::runtime_error("RNGRES is not a factor of range span");

  rays_.reserve(rays);
  level_data_.resize(rays, bins);
  array_utils::fill(level_data_, 0);
}

client::client(size_t buffer_size, time_t keepalive_period)
  : keepalive_period_{keepalive_period}
  , socket_{-1}
  , establish_wait_{false}
  , last_keepalive_{0}
  , buffer_{new uint8_t[buffer_size]}
  , capacity_{buffer_size}
  , wcount_{0}
  , rcount_{0}
  , cur_type_{no_message}
  , cur_size_{0}
{ }

client::client(client&& rhs) noexcept
  : address_(std::move(rhs.address_))
  , service_(std::move(rhs.service_))
  , keepalive_period_(std::move(keepalive_period_))
  , filters_(std::move(rhs.filters_))
  , socket_{rhs.socket_}
  , establish_wait_{rhs.establish_wait_}
  , last_keepalive_{rhs.last_keepalive_}
  , buffer_(std::move(rhs.buffer_))
  , capacity_{rhs.capacity_}
  , wcount_{static_cast<unsigned int>(rhs.wcount_)}
  , rcount_{static_cast<unsigned int>(rhs.rcount_)}
  , cur_type_{std::move(rhs.cur_type_)}
  , cur_size_{std::move(rhs.cur_size_)}
{
  rhs.socket_ = -1;
}

auto client::operator=(client&& rhs) noexcept -> client&
{
  address_ = std::move(rhs.address_);
  service_ = std::move(rhs.service_);
  keepalive_period_ = std::move(rhs.keepalive_period_);
  filters_ = std::move(rhs.filters_);
  socket_ = rhs.socket_;
  establish_wait_ = rhs.establish_wait_;
  last_keepalive_ = rhs.last_keepalive_;
  buffer_ = std::move(rhs.buffer_);
  capacity_ = rhs.capacity_;
  wcount_ = static_cast<unsigned int>(rhs.wcount_);
  rcount_ = static_cast<unsigned int>(rhs.rcount_);
  cur_type_ = std::move(rhs.cur_type_);
  cur_size_ = std::move(rhs.cur_size_);

  rhs.socket_ = -1;

  return *this;
}

client::~client()
{
  disconnect();
}

auto client::add_filter(int station, std::string const& product, std::vector<std::string> const& moments) -> void
{
  if (socket_ != -1)
    throw std::runtime_error{"rapic_ds: add_filter called while connected"};

  // RPFILTER
  // :station number (-1 = all)
  // :product type (ANY, PPI, RHI, COMPPPI, IMAGE, VOLUME, RHI_SET, MERGE, SCAN_ERROR)
  // :video format (-1 = whatever is available)
  // :data source (unused, always -1)
  // :moments to retrieve (ommitted for all available)
  std::ostringstream oss;
  oss << "RPFILTER:" << station << ":" << product << ":-1:-1";
  for (size_t i = 0; i < moments.size(); ++i)
    oss << (i == 0 ? ':' : ',') << moments[i];
  oss << "\n";
  filters_.emplace_back(oss.str());
}

auto client::connect(std::string address, std::string service) -> void
{
  if (socket_ != -1)
    throw std::runtime_error{"rapic_ds: connect called while already connected"};

  // store connection details
  address_ = std::move(address);
  service_ = std::move(service);

  // reset connection state
  wcount_ = 0;
  rcount_ = 0;

  // lookupt the host
  addrinfo hints, *addr;
  memset(&hints, 0, sizeof(hints));
  hints.ai_flags = 0;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  int ret = getaddrinfo(address_.c_str(), service_.c_str(), &hints, &addr);
  if (ret != 0 || addr == nullptr)
    throw std::runtime_error{"rapic_ds: unable to resolve server address"};

  // TODO - loop through all addresses?
  if (addr->ai_next)
  {
    
  }

  // create the socket
  socket_ = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
  if (socket_ == -1)
  {
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic_ds: socket creation failed"};
  }

  // set non-blocking I/O
  int flags = fcntl(socket_, F_GETFL);
  if (flags == -1)
  {
    disconnect();
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic_ds: failed to read socket flags"};
  }
  if (fcntl(socket_, F_SETFL, flags | O_NONBLOCK) == -1)
  {
    disconnect();
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic_ds: failed to set socket flags"};
  }

  // connect to the remote host
  ret = ::connect(socket_, addr->ai_addr, addr->ai_addrlen);
  if (ret < 0)
  {
    if (errno != EINPROGRESS)
    {
      disconnect();
      freeaddrinfo(addr);
      throw std::system_error{errno, std::system_category(), "rapic_ds: failed to establish connection"};
    }
    establish_wait_ = true;
  }
  else
    establish_wait_ = false;

  // clean up the address list allocated by getaddrinfo
  freeaddrinfo(addr);
}

auto client::disconnect() -> void
{
  if (socket_ != -1)
  {
    close(socket_);
    socket_ = -1;
    last_keepalive_ = 0;
  }
}

auto client::connected() const -> bool
{
  return socket_ != -1;
}

auto client::pollable_fd() const -> int
{
  return socket_;
}

auto client::poll_read() const -> bool
{
  return socket_ != -1 && !establish_wait_;
}

auto client::poll_write() const -> bool
{
  return socket_ != -1 && establish_wait_;
}

auto client::poll(int timeout) const -> void
{
  if (socket_ == -1)
    throw std::runtime_error{"rapic_ds: attempt to poll while disconnected"};

  struct pollfd fds;
  fds.fd = socket_;
  fds.events = POLLRDHUP | (poll_read() ? POLLIN : 0) | (poll_write() ? POLLOUT : 0);
  ::poll(&fds, 1, timeout);
}

auto client::process_traffic() -> bool
{
  // sanity check
  if (socket_ == -1)
    return false;

  // get current time
  auto now = time(NULL);

  // need to check our connection attempt progress
  if (establish_wait_)
  {
    int res = 0; socklen_t len = sizeof(res);
    if (getsockopt(socket_, SOL_SOCKET, SO_ERROR, &res, &len) < 0)
    {
      disconnect();
      throw std::system_error{errno, std::system_category(), "rapic_ds: getsockopt failure"};
    }

    // not connected yet?
    if (res == EINPROGRESS)
      return false;

    // okay, connection attempt is complete.  did it succeed?
    if (res < 0)
    {
      disconnect();
      throw std::system_error{res, std::system_category(), "rapic_ds: failed to establish connection (async)"};
    }

    establish_wait_ = false;

    /* note: since the only things we ever send is the initial connection, the filters and occasional keepalive
     *       messages, we don't bother with buffering the writes.  if for some reason we manage to fill the
     *       write buffer here (extremely unlikely) then we will need to buffer writes like we do reads */

    // activate the semi-permanent connection
    write(socket_, msg_connect.c_str(), msg_connect.size());

    // activate each of our filters
    for (auto& filter : filters_)
      write(socket_, filter.c_str(), filter.size());
  }

  // do we need to send a keepalive? (ie: RDRSTAT)
  if (now - last_keepalive_ > keepalive_period_)
  {
    write(socket_, msg_keepalive.c_str(), msg_keepalive.size());
    last_keepalive_ = now;
  }

  // read everything we can
  while (true)
  {
    // if our buffer is full return and allow client to do some reading
    if (wcount_ - rcount_ == capacity_)
      return true;

    // determine current read and write positions
    auto rpos = rcount_ % capacity_;
    auto wpos = wcount_ % capacity_;

    // see how much _contiguous_ space is left in our buffer (may be less than total available write space)
    auto space = wpos < rpos ? rpos - wpos : capacity_ - wpos;

    // read some data off the wire
    auto bytes = recv(socket_, &buffer_[wpos], space, 0);
    if (bytes > 0)
    {
      // advance our write position
      wcount_ += bytes;

      // if we read as much as we asked for there may be more still waiting so return true
      return static_cast<size_t>(bytes) == space;
    }
    else if (bytes < 0)
    {
      // if we've run out of data to read stop trying
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        return false;

      // if we were interrupted by a signal handler just try again
      if (errno == EINTR)
        continue;

      // a real receive error - kill the connection
      auto err = errno;
      disconnect();
      throw std::system_error{err, std::system_category(), "rapic_ds: recv failure"};
    }
    else /* if (bytes == 0) */
    {
      // connection has been closed
      disconnect();
      return false;
    }
  }
}

auto client::address() const -> std::string const&
{
  return address_;
}

auto client::service() const -> std::string const&
{
  return service_;
}

auto client::dequeue(message_type& type) -> bool
{
  // move along to the next packet in the buffer if needed
  if (cur_type_ != no_message)
    rcount_ += cur_size_;

  // reset our current type
  cur_type_ = no_message;
  cur_size_ = 0;

  // ignore leading whitespace (and return if no data at all)
  while (true)
  {
    if (wcount_ == rcount_)
      return false;
    if (buffer_[rcount_ % capacity_] > 0x20)
      break;
    ++rcount_;
  }

  // cache write count to ensure consistent overflow check at the end of this function
  size_t wc = wcount_;

  // is it an MSSG style message?
  if (buffer_starts_with(msg_mssg_head))
  {
    // status 30 is multi-line terminated by "END STATUS"
    if (buffer_starts_with(msg_mssg30_head))
    {
      if (buffer_find(msg_mssg30_term, cur_size_))
      {
        cur_type_ = type = message_type::mssg;
        cur_size_ += msg_mssg30_term.size();
        return true;
      }
    }
    // otherwise assume it is a single line message and look for an end of line
    else
    {
      if (buffer_find(msg_mssg_term, cur_size_))
      {
        cur_type_ = type = message_type::mssg;
        cur_size_ += msg_mssg_term.size();
        return true;
      }
    }
  }
  // otherwise assume it is a scan message and look for "END RADAR IMAGE"
  else
  {
    if (buffer_find(msg_scan_term, cur_size_))
    {
      cur_type_ = type = message_type::scan;
      cur_size_ += msg_scan_term.size();
      return true;
    }
  }

  // if the buffer is full but we still cannot read a message then we are in overflow, fail hard
  if (wc - rcount_ == capacity_)
    throw std::runtime_error{"rapic_ds: buffer overflow (try increasing buffer size)"};
  
  return false;
}

auto client::decode(mssg& msg) -> void
{
  check_cur_type(message_type::mssg);

  msg.content.reserve(cur_size_);

  // if the message spans the buffer wrap around point, copy it into a temporary location to ease parsing
  auto pos = rcount_ % capacity_;
  if (pos + cur_size_ > capacity_)
  {
    msg.content.assign(reinterpret_cast<char const*>(&buffer_[pos]), capacity_ - pos);
    msg.content.append(reinterpret_cast<char const*>(&buffer_[0]), cur_size_ - (capacity_ - pos));
  }
  else
  {
    msg.content.assign(reinterpret_cast<char const*>(&buffer_[pos]), cur_size_);
  }
}

auto client::decode(scan& msg) -> void
{
  check_cur_type(message_type::scan);

  auto pos = rcount_ % capacity_;
  if (pos + cur_size_ > capacity_)
  {
    std::unique_ptr<uint8_t[]> buf{new uint8_t[cur_size_]};
    std::memcpy(buf.get(), &buffer_[pos], capacity_ - pos);
    std::memcpy(buf.get() + capacity_ - pos, &buffer_[0], cur_size_ - (capacity_ - pos));
    msg.decode(buf.get(), cur_size_);
  }
  else
    msg.decode(&buffer_[pos], cur_size_);
}

auto client::check_cur_type(message_type type) -> void
{
  if (cur_type_ != type)
  {
    if (cur_type_ == no_message)
      throw std::runtime_error{"gpats: no message dequeued for decoding"};
    else
      throw std::runtime_error{"gpats: incorrect type passed for decoding"};
  }
}

auto client::buffer_starts_with(std::string const& str) const -> bool
{
  // cache rcount_ to reduce performance drop of atomic reads
  // this function is only ever called from the read thread
  size_t rc = rcount_;

  // is there even enough data in the buffer?
  auto size = wcount_ - rc;
  if (size < str.size())
    return false;

  // check each character for a match
  for (size_t i = 0; i < str.size(); ++i)
    if (str[i] != buffer_[(rc + i) % capacity_])
      return false;

  return true;
}

auto client::buffer_find(std::string const& str, size_t& pos) const -> bool
{
  // cache rcount_ to reduce performance drop of atomic reads
  // this function is only ever called from the read thread
  size_t rc = rcount_;

  // is there even enough data in the buffer?
  auto size = wcount_ - rc;
  if (size < str.size())
    return false;

  // naive search through the buffer
  for (size_t i = 0; i < size - str.size(); ++i)
  {
    for (size_t j = 0; j < str.size(); ++j)
      if (str[j] != buffer_[(rc + i + j) % capacity_])
        goto next_i;
    pos = i;
    return true;
next_i:
    ;
  }

  // we return 0 to indicate failure - it's easy but
  return false;
}

#if 0
#include <rainhdf/rainhdf.h>

void rainfields::rapic::write_odim_h5_volume(std::string const& path, std::list<scan> const& scan_set)
{
  auto cur = scan_set.begin();

  hdf::polar_volume vol{path, hdf::file::io_mode::create};
  
  vol.set_date_time(cur->timestamp.as_time_t());
  {
    int ctyn;
    char const* ctys;
    if (cur->country == 36)
    {
      ctyn = 500;
      ctys = "AU";
    }
    else
    {
      trace::error() << "unknown country code, using 000 and XX as placeholders";
      ctyn = 0;
      ctys = "XX";
    }

    int stnid = cur->stnid;
    if (cur->stnid == -1)
    {
      trace::error() << "missing station id, using 00 as placeholder";
      stnid = 0;
    }

    char const* plc = cur->name.c_str();
    if (cur->name.empty())
    {
      trace::error() << "missing station name, using XXX as placeholder";
      plc = "XXX";
    }

    char buf[64];
    if (cur->stn_num == -1)
      snprintf(buf, 64, "RAD:%s%02d,PLC:%s,CTY:%03d", ctys, stnid, plc, ctyn);
    else
      snprintf(buf, 64, "RAD:%s%02d,PLC:%s,CTY:%03d,STN:%ld", ctys, stnid, plc, ctyn, cur->stn_num);
    
    vol.set_source(buf);
  }
  vol.set_latitude(cur->latitude);
  vol.set_longitude(cur->longitude);
  vol.set_height(cur->height);

  if (!is_nan(cur->antdiam))
    vol.attributes()["antenna_diameter"].set(cur->antdiam);
  if (!is_nan(cur->azcorr))
    vol.attributes()["azcorr"].set(cur->azcorr);
  // 'azim' for rhi only
  // 'compppiid' for compppis only
  if (!cur->copyright.empty())
    vol.attributes()["copyright"].set(cur->copyright);
  if (!is_nan(cur->elcorr))
    vol.attributes()["elcorr"].set(cur->elcorr);
  // calculate wavelength in m/s from frequency
  if (!is_nan(cur->frequency))
    vol.attributes()["wavelength"].set((299792458.0 / (cur->frequency * 1000000.0)) * 100.0);
  if (!is_nan(cur->hbeamwidth))
    vol.attributes()["beamwH"].set(cur->hbeamwidth);
  if (!cur->radartype.empty())
    vol.attributes()["system"].set(cur->radartype);
  if (!is_nan(cur->vbeamwidth))
    vol.attributes()["beamwV"].set(cur->vbeamwidth);
  if (!cur->vers.empty())
    vol.attributes()["sw_version"].set(cur->vers);
  if (cur->volumeid != -1)
    vol.attributes()["volume_id"].set(cur->volumeid);
  if (!cur->volumelabel.empty())
    vol.attributes()["volume_label"].set(cur->volumelabel);

  // write each tilt
  while (cur != scan_set.end())
  {
    auto scan = vol.scan_append();
    scan.set_elevation_angle(cur->elev);
    scan.set_bin_count(cur->data.cols());
    scan.set_range_start(cur->startrng / 1000.0);
    scan.set_range_scale(cur->rngres);
    scan.set_ray_count(cur->data.rows());
    scan.set_first_ray_radiated(cur->first_ray);
    scan.set_start_date_time(cur->timestamp.as_time_t());
    scan.set_end_date_time(/*TODO*/0);

    // write each moment
    for (auto tilt = cur->tilt; cur != scan_set.end() && cur->tilt == tilt; ++cur)
    {
      // these min/max values are copied from the rapic encoder (ConcEncodeClient.cpp)
      // the gains and offsets here are taken directly from 3d rapic's code
      double min = 0.0, max = static_cast<int>(cur->vidres);
      char const* quantity = nullptr;
      std::vector<double> const* table = nullptr;
      if (cur->video == "Refl")
      {
        quantity = cur->polarisation == polarisation_type::vertical ? "DBZV" : "DBZH";
        table = cur->dbzlvl.empty() ? nullptr : &cur->dbzlvl;
        min = -31.5;
        max = 96.0;
      }
      else if (cur->video == "UnCorRefl")
      {
        quantity = cur->polarisation == polarisation_type::vertical ? "TV" : "TH";
        table = cur->dbzlvl.empty() ? nullptr : &cur->dbzlvl;
        min = -31.5;
        max = 96.0;
      }
      else if (cur->video == "RawUnCorRefl")
      {
        quantity = cur->polarisation == polarisation_type::vertical ? "RAW_TV" : "RAW_TH";
        table = cur->dbzlvl.empty() ? nullptr : &cur->dbzlvl;
        min = -31.5;
        max = 96.0;
      }
      else if (cur->video == "Vel")
      {
        quantity = cur->polarisation == polarisation_type::vertical ? "VRADV" : "VRADH";
        min = -cur->nyquist;
        max = cur->nyquist;
      }
      else if (cur->video == "SpWdth")
      {
        quantity = cur->polarisation == polarisation_type::vertical ? "WRADV" : "WRADH";
        min = 0.0;
        max = cur->nyquist;
      }
      else if (cur->video == "ADCdata")
        ;
      else if (cur->video == "PEchnl")
        ;
      else if (cur->video == "Interference")
        ;
      else if (cur->video == "RawVel")
        ;
      else if (cur->video == "CCOR")
      {
        quantity = cur->polarisation == polarisation_type::vertical ? "CCORV" : "CCORH";
      }
      else if (cur->video == "SQI")
      {
        quantity = cur->polarisation == polarisation_type::vertical ? "SQIV" : "SQIH";
      }
      else if (cur->video == "QCFLAGS")
      {
        quantity = "QCFLAGS";
      }
      else if (cur->video == "QC_CCOR")
        ;
      else if (cur->video == "QC_DESPECKLE")
        ;
      else if (cur->video == "QC_SQI")
        ;
      else if (cur->video == "QC_2TSUPPR")
        ;
      else if (cur->video == "QC_CCOR_2db")
        ;
      else if (cur->video == "ZDR")
      {
        quantity = "ZDR";
        min = -9.0;
        max = 9.0;
      }
      else if (cur->video == "PHIDP")
      {
        quantity = "PHIDP";
        min = -90.0;
        max = 90.0;
      }
      else if (cur->video == "RHOHV")
      {
        quantity = "RHOHV";
        min = 0.0;
        max = 1.15;
      }
      else
      {
        trace::error() << "unknown VIDEO type '" << cur->video << "' encoding directly with levels";
        quantity = cur->video;
      }

      size_t dims[2] = { cur->data.rows(), cur->data.cols() };
      // TEMP auto moment = scan.data_append(hdf::data::data_type::u8, 2, dims);
      auto moment = scan.data_append(hdf::data::data_type::f32, 2, dims);


      // rapic has no concept of 'undetect' so we have to set it to the same as nodata
      // always use level 0 for nodata
      moment.set_nodata(0.0);
      moment.set_undetect(0.0);

#if 0
      // TEMP
      moment.set_gain((max - min) / (static_cast<int>(cur->vidres) - 2));
      moment.set_offset(min - ((max - min) / (static_cast<int>(cur->vidres) - 2)));
#endif

      // write the moment data
      if (table)
      {
        // convert the data to the real floating point moment values
        array1<float> rd{cur->data.size()};
        for (size_t i = 0; i < rd.size(); ++i)
        {
          auto level = cur->data.data()[i];
          if (level == 0)
            rd[i] = nan<float>();
          else if (level > (int) table->size())
            throw std::runtime_error{"level exceeds table size"};
          else
            rd[i] = (*table)[level - 1];
        }

        // determine the minimum 

        moment.set_gain(1.0);
        moment.set_offset(1.0);
        moment.write_pack(rd.data(), [](float v){return false;}, [](float v){return is_nan(v);});
      }
      else
      {
        moment.set_gain((max - min) / (static_cast<int>(cur->vidres) - 2));
        moment.set_offset(min - ((max - min) / (static_cast<int>(cur->vidres) - 2)));
        moment.write(cur->data.data());
      }
    }
  }
}

#endif
#if 0
scan::scan(std::istream& in)
{
  std::string buf1, buf2;

  double azi_min = 0.0, azi_max = 0.0;

  for (char next = in.get(); in.good(); next = in.get())
  {
    // ray of polar data
    if (next == '%' || next == '@')
    {
      // if this is our first ray, setup the data array
      if (data.size() == 0)
      {
        if (   is_nan(angres)
            || is_nan(rngres) || is_nan(startrng) || is_nan(endrng)
            || product == product_type::unknown
            || product == product_type::scan_error
            || imgfmt == image_format::unknown)
          throw std::runtime_error("missing or invalid scan headers");

        if (is_nan(angle1) || is_nan(angle2))
        {
          if (product == product_type::rhi_set)
            throw std::runtime_error("missing or invalid scan headers");
          azi_min = 0.0;
          azi_max = 360.0;
        }
        else
        {
          azi_min = angleincreasing ? angle1 : angle2;
          azi_max = angleincreasing ? angle2 : angle1;
#if 0 // might need to re-enable this, but only for PPIs (not RHIs which validly have -ve angles)
          while (azi_min < 0)
            azi_min += 360;
          while (azi_max < azi_min)
            azi_max += 360;
#endif
        }
        
        int rows = std::lround((azi_max - azi_min) / angres);
        if (remainder(azi_max - azi_min, angres) > 0.001)
          throw std::runtime_error("invalid angres for 360 degree sweep");

        int cols = std::lround((endrng - startrng) / rngres);
        if (cols < 0 || remainder(endrng - startrng, rngres) > 0.001)
          throw std::runtime_error("invalid rngres given end and start ranges");

        level_data_.resize(rows, cols);
        array_utils::fill(level_data_, 0);
      }

      buf2.clear();

      // ascii encoding
      if (next == '%')
      {
        // read the line of data
        getline(in, buf1);

        // determine the ray index
        float ang;
        if (sscanf(buf1.c_str(), imgfmt == image_format::rhi ? "%4f" : "%3f", &ang) != 1)
          throw std::runtime_error("invalid azimuth angle in ray encoding");
        while (ang >= azi_max)
          ang -= 360;
        while (ang < azi_min)
          ang += 360;
        size_t ray = std::lround((ang - azi_min) / angres);
        if (ray >= level_data_.rows() || std::abs(remainder(ang - azi_min, angres)) > 0.001)
          throw std::runtime_error("invalid azimuth angle in ray encoding");
        auto out = level_data_[ray];

        if (first_ray == -1)
          first_ray = ray;

        // decode the data into levels
        // NOTE: corrupt encodings can cause buffer overruns here, in the future check bin < level_data_.cols() at each
        //       write of a bin
        int prev = 0;
        size_t bin = 0;
        for (auto i = buf1.begin() + 3; i != buf1.end(); ++i)
        {
          auto& cur = lookup[(unsigned char)*i];

          //  absolute pixel value
          if (cur.type == enc_type::value)
          {
            if (bin < level_data_.cols())
              out[bin++] = prev = cur.val;
            else
              throw std::runtime_error{"scan data overflow (ascii abs)"};
          }
          // run length encoding of the previous value
          else if (cur.type == enc_type::digit)
          {
            auto count = cur.val;
            while (i + 1 != buf1.end() && lookup[(unsigned char)*(i + 1)].type == enc_type::digit)
            {
              count *= 10;
              count += lookup[(unsigned char)*++i].val;
            }
            if (bin + count > level_data_.cols())
              throw std::runtime_error{"scan data overflow (ascii rle)"};
            for (int j = 0; j < count; ++j)
              out[bin++] = prev;
          }
          // delta encoding
          // silently ignore potential overflow caused second half of a delta encoding at end of ray assuming it
          // is just an artefact of the encoding process
          else if (cur.type == enc_type::delta)
          {
            if (bin < level_data_.cols())
              out[bin++] = prev += cur.val;
            else
              throw std::runtime_error{"scan data overflow (ascii delta)"};

            if (bin < level_data_.cols())
              out[bin++] = prev += cur.val2;
            else if (i + 1 != buf1.end())
              throw std::runtime_error{"scan data overflow (ascii delta)"};
          }
          else
            throw std::runtime_error("invalid character encountered in ray encoding");
        }
      }
      // binary encoding
      else
      {
        // read the header bytes
        char buf2[18];
        in.read(buf2, 18);
        if (!in.good())
          throw std::runtime_error("invalid binary ray header");

        float azi, el;
        int sec;
        if (sscanf(buf2, "%f,%f,%d=", &azi, &el, &sec) != 3)
          throw std::runtime_error("invalid binary ray header");

        // determine the ray index
        float ang = imgfmt == image_format::rhi ? el : azi;
        while (ang >= azi_max)
          ang -= 360;
        while (ang < azi_min)
          ang += 360;
        size_t ray = std::lround((ang - azi_min) / angres);
        if (ray >= level_data_.rows() || std::abs(remainder(ang - azi_min, angres)) > 0.001)
          throw std::runtime_error("invalid azimuth angle in ray encoding");
        auto out = level_data_[ray];

        if (first_ray == -1)
          first_ray = ray;

        // we ignore the length for now
        //auto len = (((unsigned int) ((unsigned char) buf2[16])) << 8) + (unsigned int) ((unsigned char) buf2[17]);

        // decode the data into levels
        size_t bin = 0;
        while (true)
        {
          int val = (unsigned char) in.get();
          if (val == 0 || val == 1)
          {
            size_t count = (unsigned char) in.get();
            if (count == 0)
              break;
            if (bin + count > level_data_.cols())
              throw std::runtime_error{"scan data overflow (binary rle)"};
            for (size_t i = 0; i < count; ++i)
              out[bin++] = val;
          }
          else if (bin < level_data_.cols())
            out[bin++] = val;
          else
            throw std::runtime_error{"scan data overflow (binary abs)"};
        }
      }
    }
    // header field
    else
    {
      // retrieve entire header string into buf1
      in.putback(next);
      getline(in, buf1);

      // skip empty header lines
      if (buf1.empty())
        continue;

      // end of scan?
      // 3d rapic strips the _mandatory_ "^Z " (0x1a, 0x20) from the start of this field so use find here
      if (   buf1.size() >= 15
          && buf1.compare(buf1.size() - 15, std::string::npos, "END RADAR IMAGE") == 0)
        return;

      // split into header name and value (name in buf1, value in buf2)
      auto pos1 = buf1.find(':');
      if (pos1 == std::string::npos)
      {
#if 1
        trace::error() << "Invalid header detected.  station " << stnid 
          << " product " << (int) product
          << " tilt " << tilt
          << " pass " << pass;
    //      << " rays_so_far " << rays_so_far
    //      << " data " << buf1 << std::endl;
#endif
        throw std::runtime_error("invalid header");
      }
      auto pos2 = buf1.find_first_not_of(' ', pos1 + 1);
      buf2.assign(buf1, pos2, std::string::npos);
      buf1.resize(pos1);
      
      // apply the header to our volume
      parse_header(buf1, buf2);
    }
  }

  throw std::runtime_error("unterminated scan");
}

void scan::parse_header(std::string const& key, std::string const& value)
{
  trace::debug() << "header: " << key << " value: " << value;
  if (key == "ANGLERATE")
    anglerate = from_string<double>(value);
  else if (key == "ANGRES")
    angres = from_string<double>(value);
  else if (key == "ANTDIAM")
    antdiam = from_string<double>(value);
  else if (key == "AZCORR")
    azcorr = from_string<double>(value);
  else if (key == "AZIM")
    azim = from_string<double>(value);
  else if (key == "BEAMWIDTH")
    hbeamwidth = vbeamwidth = from_string<double>(value);
  else if (key == "COMPPPIID")
    compppiid = from_string<long>(value, 10);
  else if (key == "COPYRIGHT")
    copyright = value;
  else if (key == "COUNTRY")
    country = from_string<long>(value, 10);
  else if (key == "DATE")
    date = from_string<long>(value, 10);
  else if (key == "DBM2DBZ")
    dbm2dbz = from_string<double>(value);
  else if (key == "DBMLVL")
    dbmlvl = tokenize<double>(value, " ");
  else if (key == "DBZLVL")
    dbzlvl = tokenize<double>(value, " ");
  else if (key == "ELCORR")
    elcorr = from_string<double>(value);
  else if (key == "ELEV")
    elev = from_string<double>(value);
  else if (key == "ENDRNG")
    endrng = from_string<double>(value);
  else if (key == "FREQUENCY" || key == "TXFREQUENCY")
    frequency = from_string<double>(value);
  else if (key == "FAULT")
    fault = value;
  else if (key == "HBEAMWIDTH")
    hbeamwidth = from_string<double>(value);
  else if (key == "HEIGHT")
    height = from_string<double>(value);
  else if (key == "HIPRF")
  {
    if (value == "EVENS")
      hiprf = dual_prf_strategy::high_evens;
    else if (value == "ODDS")
      hiprf = dual_prf_strategy::high_odds;
    else if (value == "UNKNOWN")
      hiprf = dual_prf_strategy::unknown;
    else
      trace::error() << "unknown HIPRF value (" << value << ") ignored";
  }
  else if (key == "IMGFMT")
  {
    if (value == "RHI")
      imgfmt = image_format::rhi;
    else if (value == "CompPPI")
      imgfmt = image_format::compppi;
    else if (value == "PPI")
      imgfmt = image_format::ppi;
    else
      trace::error() << "unknown IMGFMT value (" << value << ") ignored";
  }
  else if (key == "LATITUDE")
    latitude = from_string<double>(value);
  else if (key == "LONGITUDE")
    longitude = from_string<double>(value);
  else if (key == "NAME")
    name = value;
  else if (key == "NYQUIST")
    nyquist = from_string<double>(value);
  else if (key == "PASS")
  {
    if (sscanf(value.c_str(), "%ld of %ld", &pass, &pass_count) != 2)
      throw std::runtime_error{"invalid PASS header"};
  }
  else if (key == "PEAKPOWER")
    peakpower = from_string<double>(value);
  else if (key == "PEAKPOWERH")
    peakpowerh = from_string<double>(value);
  else if (key == "PEAKPOWERV")
    peakpowerv = from_string<double>(value);
  else if (key == "POLARISATION")
  {
    if (value == "H")
      polarisation = polarisation_type::horizontal;
    else if (value == "V")
      polarisation = polarisation_type::vertical;
    else if (value == "ALT_HV")
      polarisation = polarisation_type::alternating;
    else
      trace::error() << "unknown POLARISATION value (" << value << ") ignored";
  }
  else if (key == "PRF")
    prf = from_string<double>(value);
  else if (key == "PRODUCT")
  {
    char pt[16], label[64];
    int increasing;
    int ret = sscanf(
          value.c_str()
        , "%16s [%64s SECTOR ANGLE1=%lf ANGLE2=%lf ANGLEINCREASING=%d"
        , pt
        , label
        , &angle1
        , &angle2
        , &increasing);
    if (ret < 1)
      throw std::runtime_error{"invalid PRODUCT header"};
    if (strcmp(pt, "ERROR") == 0)
      product = product_type::scan_error;
    else if (strcmp(pt, "NORMAL") == 0)
      product = product_type::comp_ppi;
    else if (strcmp(pt, "RHISet") == 0)
    {
      if (ret != 5)
        throw std::runtime_error{"invalid PRODUCT header"};
      product = product_type::rhi_set;
      volumelabel.assign(label);
      if (!volumelabel.empty() && volumelabel.back() == ']')
        volumelabel.pop_back();
      angleincreasing = increasing != 0;
    }
    else if (strcmp(pt, "VOLUMETRIC") == 0)
    {
      if (ret != 2 && ret != 5)
        throw std::runtime_error{"invalid PRODUCT header"};
      product = product_type::volume;
      volumelabel.assign(label);
      if (!volumelabel.empty() && volumelabel.back() == ']')
        volumelabel.pop_back();
      if (ret == 5)
        angleincreasing = increasing != 0;
    }
    else
      trace::error() << "unknown PRODUCT value (" << value << ") ignored";
  }
  else if (key == "PULSELENGTH")
    pulselength = from_string<double>(value);
  else if (key == "RADARTYPE")
    radartype = value;
  else if (key == "RNGRES")
    rngres = from_string<double>(value);
  else if (key == "RXGAIN_H")
    rxgain_h = from_string<double>(value);
  else if (key == "RXGAIN_V")
    rxgain_v = from_string<double>(value);
  else if (key == "RXNOISE_H")
    rxnoise_h = from_string<double>(value);
  else if (key == "RXNOISE_V")
    rxnoise_v = from_string<double>(value);
  else if (key == "STARTRNG")
    startrng = from_string<double>(value);
  else if (key == "STN_NUM")
    stn_num = from_string<long>(value, 10);
  else if (key == "STNID")
    stnid = from_string<long>(value, 10);
  else if (key == "TILT")
  {
    if (sscanf(value.c_str(), "%ld of %ld", &tilt, &tilt_count) != 2)
      throw std::runtime_error{"invalid TILT header"};
  }
  else if (key == "TIME")
  {
    if (sscanf(value.c_str(), "%d.%d", &time.first, &time.second) != 2)
      throw std::runtime_error{"invalid TIME header"};
  }
  else if (key == "TIMESTAMP")
  {
    int y,m,d,h,mi,s;
    if (sscanf(value.c_str(), "%04d%02d%02d%02d%02d%02d", &y, &m, &d, &h, &mi, &s) != 6)
      throw std::runtime_error{"invalid TIMESTAMP header"};
    timestamp = rainfields::timestamp{y, m, d, h, mi, s};
  }
  else if (key == "UNFOLDING")
  {
    if (value == "None")
      unfolding = dual_prf_ratio::none;
    else if (value == "2:3")
      unfolding = dual_prf_ratio::_2_3;
    else if (value == "3:4")
      unfolding = dual_prf_ratio::_3_4;
    else if (value == "4:5")
      unfolding = dual_prf_ratio::_4_5;
    else
      trace::error() << "unknown UNFOLDING value (" << value << ") ignored";
  }
  else if (key == "VBEAMWIDTH")
    vbeamwidth = from_string<double>(value);
  else if (key == "VERS")
    vers = value;
  else if (key == "VIDEO")
    video = value;
  else if (key == "VIDEOGAIN")
  {
    if (value == "THRESH")
      videogain = -999.0;
    else
      videogain = from_string<double>(value);
  }
  else if (key == "VIDEOOFFSET")
  {
    if (value == "THRESH")
      videooffset = -999.0;
    else
      videooffset = from_string<double>(value);
  }
  else if (key == "VIDEOUNITS")
    videounits = value;
  else if (key == "VIDRES")
  {
    auto vr = from_string<long>(value, 10);
    if (   vr != 6
        && vr != 16
        && vr != 32
        && vr != 32
        && vr != 64
        && vr != 160
        && vr != 256)
    {
      trace::error() << "unknown VIDRES value (" << value << ") ignored";
    }
    if (vr == 6)
      throw std::runtime_error("six level ASCII encoding not supported");

    vidres = static_cast<video_format>(vr);
  }
  else if (key == "VOLUMEID")
    volumeid = from_string<long>(value, 10);
  else if (key == "WMONUMBER")
    wmo_number = from_string<long>(value, 10);
  else
    trace::warning() << "unknown header encountered: " << key << " = " << value;
}
#endif

