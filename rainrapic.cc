/*------------------------------------------------------------------------------
 * Rainfields Rapic Support Library (rainrapic)
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rainrapic.h"

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
#include <map>
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
  return RAINRAPIC_RELEASE_TAG;
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
  volume_id_ = -1;
  product_.clear();
  pass_ = -1;
  pass_count_ = -1;
  is_rhi_ = false;
  angle_min_ = nan<float>();
  angle_max_ = nan<float>();
  angle_resolution_ = nan<float>();
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
  if (auto p = find_header("VOLUMEID"))
    volume_id_ = p->get_integer();
  product_ = get_header_string("PRODUCT");
  if (auto p = find_header("PASS"))
  {
    if (sscanf(p->value().c_str(), "%d of %d", &pass_, &pass_count_) != 2)
      throw std::runtime_error{"invalid PASS header"};
  }
  is_rhi_ = get_header_string("IMGFMT") == "RHI";

  // get the mandatory characteristics needed to determine scan structure
  angle_resolution_ = get_header_real("ANGRES");
  double rngres = get_header_real("RNGRES");
  double startrng = get_header_real("STARTRNG");
  double endrng = get_header_real("ENDRNG");

  // if start/end angles are provided, use them to limit our ray count
  int inc = 1;
  if (sscanf(product_.c_str(), "%*s %*s SECTOR ANGLE1=%f ANGLE2=%f ANGLEINCREASING=%d", &angle_min_, &angle_max_, &inc) == 3)
  {
    if (inc == 0)
      std::swap(angle_min_, angle_max_);
    while (angle_max_ <= angle_min_)
      angle_max_ += 360.0;
  }
  else
  {
    angle_min_ = 0.0f;
    angle_max_ = 360.0f;
  }

  int rays = std::lround((angle_max_ - angle_min_) / angle_resolution_);
  if (remainder(angle_max_ - angle_min_, angle_resolution_) > 0.001)
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
    throw std::runtime_error{"rainrapic: add_filter called while connected"};

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
    throw std::runtime_error{"rainrapic: connect called while already connected"};

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
    throw std::runtime_error{"rainrapic: unable to resolve server address"};

  // TODO - loop through all addresses?
  if (addr->ai_next)
  {
    
  }

  // create the socket
  socket_ = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
  if (socket_ == -1)
  {
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rainrapic: socket creation failed"};
  }

  // set non-blocking I/O
  int flags = fcntl(socket_, F_GETFL);
  if (flags == -1)
  {
    disconnect();
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rainrapic: failed to read socket flags"};
  }
  if (fcntl(socket_, F_SETFL, flags | O_NONBLOCK) == -1)
  {
    disconnect();
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rainrapic: failed to set socket flags"};
  }

  // connect to the remote host
  ret = ::connect(socket_, addr->ai_addr, addr->ai_addrlen);
  if (ret < 0)
  {
    if (errno != EINPROGRESS)
    {
      disconnect();
      freeaddrinfo(addr);
      throw std::system_error{errno, std::system_category(), "rainrapic: failed to establish connection"};
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
    throw std::runtime_error{"rainrapic: attempt to poll while disconnected"};

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
      throw std::system_error{errno, std::system_category(), "rainrapic: getsockopt failure"};
    }

    // not connected yet?
    if (res == EINPROGRESS)
      return false;

    // okay, connection attempt is complete.  did it succeed?
    if (res < 0)
    {
      disconnect();
      throw std::system_error{res, std::system_category(), "rainrapic: failed to establish connection (async)"};
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
      throw std::system_error{err, std::system_category(), "rainrapic: recv failure"};
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
    throw std::runtime_error{"rainrapic: buffer overflow (try increasing buffer size)"};
  
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
      throw std::runtime_error{"rainrapic: no message dequeued for decoding"};
    else
      throw std::runtime_error{"rainrapic: incorrect type passed for decoding"};
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

#include <rainhdf/rainhdf.h>

static auto rapic_timestamp_to_time_t(char const* str) -> time_t
{
  struct tm t;
  if (sscanf(str, "%04d%02d%02d%02d%02d%02d", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec) != 6)
    throw std::runtime_error{"invalid rapic timestamp"};
  t.tm_year -= 1900;
  t.tm_mon -= 1;
  return timegm(&t);
}

namespace {
struct quantity
{
  quantity(char const* hname, char const* vname, float gain = nan<float>(), float offset = nan<float>())
    : hname{hname}, vname{vname}, odim_gain{gain}, odim_offset{offset}
  { }

  char const* hname;  // odim name for unknown or horizontal polarized
  char const* vname;  // odim name for vertical polarized
  float       odim_gain;    // gain to use when encoding to 8-bit for odim output
  float       odim_offset;  // offset to use when encoding to 8-bit for odim output
};
static std::map<std::string, quantity> const video_map = 
{
    { "Refl",         { "DBZH",     "DBZV",     0.5f, -32.0f }}
  , { "UnCorRefl",    { "TH",       "TV",       0.5f, -32.0f }}
  , { "RawUnCorRefl", { "RAW_TH",   "RAW_TV",   0.5f, -32.0f }}
  , { "Vel",          { "VRADH",    "VRADV" }}
  , { "SpWdth",       { "WRADH",    "WRADV" }}
  , { "QCFLAGS",      { "QCFLAGS",  "QCFLAGS" }}
  , { "ZDR",          { "ZDR",      "ZDR" }}
  , { "PHIDP",        { "PHIDP",    "PHIDP" }}
  , { "RHOHV",        { "RHOHV",    "RHOHV" }}
};
struct meta_extra
{
  meta_extra(scan const& s, hdf::polar_volume& v, hdf::scan& t, hdf::data& d)
    : s(s), v(v), t(t), d(d)
  { }

  scan const& s;
  hdf::polar_volume& v;
  hdf::scan& t;
  hdf::data& d;

  bool        vpol = false;
  std::string video;
  std::string vidgain;
  std::string vidoffset;
  std::vector<double>  thresholds;
  double      maxvel = nan<double>();
  long        vidres = 0;
};
using odim_meta_fn = void (*)(header const&, meta_extra&);
#define METAFN [](header const& h, meta_extra& m)
static std::map<std::string, odim_meta_fn> const header_map = 
{
  // volume persistent metadata
    { "STNID",        METAFN { }} // ignored - special processing
  , { "NAME",         METAFN { }} // ignored - special processing
  , { "STN_NUM",      METAFN { }} // ignored - special processing
  , { "WMONUMBER",    METAFN { }} // ignored - special processing
  , { "COUNTRY",      METAFN { }} // ignored - special processing
  , { "IMGFMT",       METAFN { }} // ignored - implicit in ODIM_H5 product type
  , { "LATITUDE",     METAFN { m.v.set_latitude(h.get_real() * -1.0); }}
  , { "LONGITUDE",    METAFN { m.v.set_longitude(h.get_real()); }}
  , { "HEIGHT",       METAFN { m.v.set_height(h.get_real()); }}
  , { "RADARTYPE",    METAFN { m.v.attributes()["system"].set(h.value()); }}
  , { "PRODUCT",      METAFN { m.v.attributes()["rapic_PRODUCT"].set(h.value()); }}
  , { "VOLUMEID",     METAFN { m.v.attributes()["rapic_VOLUMEID"].set(h.get_integer()); }}
  , { "BEAMWIDTH",    METAFN { m.v.attributes()["beamwidth"].set(h.get_real()); }}
  , { "HBEAMWIDTH",   METAFN { m.v.attributes()["beamwH"].set(h.get_real()); }}
  , { "VBEAMWIDTH",   METAFN { m.v.attributes()["beamwV"].set(h.get_real()); }}
  , { "FREQUENCY",    METAFN
      { 
        auto freq = h.get_real();
        m.v.attributes()["rapic_FREQUENCY"].set(freq);
        m.v.attributes()["wavelength"].set((299792458.0 / (freq * 1000000.0)) * 100.0);
      }
    }
  , { "TXFREQUENCY",  METAFN
      { 
        auto freq = h.get_real();
        m.v.attributes()["rapic_FREQUENCY"].set(freq);
        m.v.attributes()["wavelength"].set((299792458.0 / (freq * 1000000.0)) * 100.0);
      }
    }
  , { "VERS",         METAFN { m.v.attributes()["sw_version"].set(h.value()); }}
  , { "COPYRIGHT",    METAFN { m.v.attributes()["copyright"].set(h.value()); }} // non-standard
  , { "ANGLERATE",    METAFN { m.v.attributes()["rpm"].set(h.get_real() * 60.0 / 360.0); }}
  , { "ANTDIAM",      METAFN { m.v.attributes()["rapic_ANTDIAM"].set(h.get_real()); }}
  , { "ANTGAIN",      METAFN { m.v.attributes()["antgainH"].set(h.get_real()); }}
  , { "AZCORR",       METAFN { m.v.attributes()["rapic_AZCORR"].set(h.get_real()); }}
  , { "ELCORR",       METAFN { m.v.attributes()["rapic_ELCORR"].set(h.get_real()); }}
  , { "RXNOISE_H",    METAFN { m.v.attributes()["nsampleH"].set(h.get_real()); }}
  , { "RXNOISE_V",    METAFN { m.v.attributes()["nsampleV"].set(h.get_real()); }}
  , { "RXGAIN_H",     METAFN { m.v.attributes()["rapic_RXGAIN_H"].set(h.get_real()); }}
  , { "RXGAIN_V",     METAFN { m.v.attributes()["rapic_RXGAIN_V"].set(h.get_real()); }}

  // tilt persistent metadata
  , { "TIME",         METAFN { }} // ignored - rendundant due to TIMESTAMP
  , { "DATE",         METAFN { }} // ignored - rendundant due to TIMESTAMP
  , { "ENDRNG",       METAFN { }} // ignored - implicit in scan dimensions
  , { "ANGRES",       METAFN { }} // ingored - implicit in scan dimensions
  , { "TIMESTAMP",    METAFN { m.t.set_start_date_time(rapic_timestamp_to_time_t(h.value().c_str())); }}
  , { "TILT",         METAFN
      {
        long a, b;
        sscanf(h.value().c_str(), "%ld of %ld", &a, &b);
        m.t.attributes()["scan_index"].set(a);
        m.t.attributes()["scan_count"].set(b);
      }
    }
  , { "ELEV",         METAFN { m.t.set_elevation_angle(h.get_real()); }}
  , { "RNGRES",       METAFN { m.t.set_range_scale(h.get_real()); }}
  , { "STARTRNG",     METAFN { m.t.set_range_start(h.get_real() / 1000.0); }}
  , { "NYQUIST",      METAFN
      {
        auto val = h.get_real();
        if (is_nan(m.maxvel))
          m.maxvel = val;
        m.t.attributes()["NI"].set(val);
      }
    }
  , { "PRF",          METAFN { m.t.attributes()["highprf"].set(h.get_real()); }}
  , { "HIPRF",        METAFN { m.t.attributes()["rapic_HIPRF"].set(h.value()); }}
  , { "UNFOLDING",    METAFN
      {
        m.t.attributes()["rapic_UNFOLDING"].set(h.value());
        if (h.value() != "None")
        {
          if (auto p = m.s.find_header("PRF"))
          {
            int a, b;
            if (sscanf(h.value().c_str(), "%d:%d", &a, &b) != 2)
              throw std::runtime_error{"invalid UNFOLDING value"};
            if (b < a)
              std::swap(a, b);
            m.t.attributes()["lowprf"].set(p->get_real() * a / b);
          }
        }
      }
    }
  , { "POLARISATION", METAFN
      {
        if (h.value() == "H")
          m.t.attributes()["polmode"].set("single-H");
        else if (h.value() == "V")
          m.vpol = true, m.t.attributes()["polmode"].set("single-V");
        else if (h.value() == "ALT_HV")
          m.t.attributes()["polmode"].set("switched-dual");
        else
          m.t.attributes()["polmode"].set(h.value());
      }
    }
  , { "TXPEAKPWR",    METAFN { m.t.attributes()["peakpwr"].set(h.get_real()); }}
  , { "PEAKPOWER",    METAFN { m.t.attributes()["peakpwr"].set(h.get_real()); }}
  , { "PEAKPOWERH",   METAFN { m.t.attributes()["peakpwrH"].set(h.get_real()); }} // non-standard
  , { "PEAKPOWERV",   METAFN { m.t.attributes()["peakpwrV"].set(h.get_real()); }} // non-standard
  , { "PULSELENGTH",  METAFN { m.t.attributes()["pulsewidth"].set(h.get_real()); }}
  , { "STCRANGE",     METAFN { m.t.attributes()["rapic_STCRANGE"].set(h.get_real()); }}

  // per moment metadata
  , { "VIDEOGAIN",    METAFN { m.vidgain = h.value(); m.d.attributes()["rapic_VIDEOGAIN"].set(m.vidgain); }} // special processing
  , { "VIDEOOFFSET",  METAFN { m.vidoffset = h.value(); m.d.attributes()["rapic_VIDEOOFFSET"].set(m.vidoffset); }} // special processing
  , { "VIDEO",        METAFN { m.video = h.value(); }} // special processing
  , { "FAULT",        METAFN { m.d.attributes()["malfunc"].set(true); m.d.attributes()["radar_msg"].set(h.value()); }}
  , { "CLEARAIR",     METAFN { m.d.attributes()["rapic_CLEARAIR"].set(h.value() == "ON"); }}
  , { "PASS",         METAFN { }} // ignored - implicit
  , { "VIDEOUNITS",   METAFN { m.d.attributes()["rapic_VIDEOUNITS"].set(h.value()); }} // mostly redundant, keep in case of unknown VIDEO
  , { "VIDRES",       METAFN { m.vidres = h.get_integer(); m.d.attributes()["rapic_VIDRES"].set(m.vidres); }}
  , { "DBZLVL",       METAFN { m.thresholds = h.get_real_array(); m.d.attributes()["rapic_DBZLVL"].set(m.thresholds); }}
  , { "DBZCALDLVL",   METAFN { m.d.attributes()["rapic_DBZCALDLVL"].set(h.get_real_array()); }}
  , { "DIGCALDLVL",   METAFN { m.d.attributes()["rapic_DIGCALDLVL"].set(h.get_real_array()); }}
  , { "VELLVL",       METAFN { m.maxvel = h.get_real(); m.d.attributes()["rapic_VELLVL"].set(m.maxvel); }}
  , { "NOISETHRESH",  METAFN { m.d.attributes()["rapic_NOISETHRESH"].set(h.get_real()); }}
  , { "QC0",          METAFN { m.d.attributes()["rapic_QC0"].set(h.value()); }}
  , { "QC1",          METAFN { m.d.attributes()["rapic_QC1"].set(h.value()); }}
  , { "QC2",          METAFN { m.d.attributes()["rapic_QC2"].set(h.value()); }}
  , { "QC3",          METAFN { m.d.attributes()["rapic_QC3"].set(h.value()); }}
  , { "QC4",          METAFN { m.d.attributes()["rapic_QC4"].set(h.value()); }}
  , { "QC5",          METAFN { m.d.attributes()["rapic_QC5"].set(h.value()); }}
  , { "QC6",          METAFN { m.d.attributes()["rapic_QC6"].set(h.value()); }}
  , { "QC7",          METAFN { m.d.attributes()["rapic_QC7"].set(h.value()); }}
};
}

static auto angle_to_index(scan const& s, float angle) -> size_t
{
  while (angle >= s.angle_max())
    angle -= 360.0f;
  while (angle < s.angle_min())
    angle += 360.0f;
  size_t ray = std::lround((angle - s.angle_min()) / s.angle_resolution());
  if (ray >= s.level_data().rows() || std::abs(remainder(angle - s.angle_min(), s.angle_resolution())) > 0.001)
    throw std::runtime_error{"invalid azimuth angle specified by ray"};
  return ray;
}

auto rainfields::rapic::write_odim_h5_volume(std::string const& path, std::list<scan> const& scan_set) -> time_t
{
  time_t vol_time = 0;
  std::decay<decltype(std::declval<scan>().level_data())>::type ibuf;
  array2<float> rbuf;
  array1<int> level_convert;

  // sanity check
  if (scan_set.empty())
    throw std::runtime_error{"empty scan set"};

  // initialize the volume file
  auto hvol = hdf::polar_volume{path, hdf::file::io_mode::create};

  // initialize the first scan
  auto hscan = hvol.scan_append();

  // write the special volume level headers
  {
    // use the PRODUCT [xxx] timestamp for the overall product time
    // use out-of-bounts mday to convert day of year into correct day
    struct tm t;
    if (sscanf(scan_set.front().product().c_str(), "VOLUMETRIC [%02d%02d%03d%02d]", &t.tm_hour, &t.tm_min, &t.tm_mday, &t.tm_year) != 4)
      throw std::runtime_error{"invalid PRODUCT header"};
    t.tm_sec = 0;
    t.tm_mon = 0; // january
    if (t.tm_year < 70) // cope with two digit year, convert to years since 1900
      t.tm_year += 100;
    t.tm_isdst = -1;
    vol_time = timegm(&t);
    hvol.set_date_time(vol_time);
  }
  {
    int pos = 0;
    char buf[128];

    int ctyn = -1;
    char const* ctys = "AU";
    if (auto p = scan_set.front().find_header("COUNTRY"))
    {
      if (p->get_integer() == 36)
      {
        ctyn = 500;
        ctys = "AU";
      }
      else
      {
        trace::warning() << "unknown country code, using 000 and XX as placeholders";
        ctyn = 0;
        ctys = "XX";
      }
    }

    pos += snprintf(buf + pos, 128 - pos, "RAD:%s%02d", ctys, scan_set.front().station_id());

    if (auto p = scan_set.front().find_header("NAME"))
      pos += snprintf(buf + pos, 128 - pos, ",PLC:%s", p->value().c_str());

    if (ctyn != -1)
      pos += snprintf(buf + pos, 128 - pos, ",CTY:%03d", ctyn);

    if (auto p = scan_set.front().find_header("WMONUMBER"))
      pos += snprintf(buf + pos, 128 - pos, ",WMO:%s", p->value().c_str());

    if (auto p = scan_set.front().find_header("STN_NUM"))
      pos += snprintf(buf + pos, 128 - pos, ",STN:%ld", p->get_integer());

    buf[127] = '\0';
    hvol.set_source(buf);
  }

  // add each scan to the volume
  auto end_tilt = scan_set.begin();
  size_t bins = 0;
  for (auto s = scan_set.begin(); s != scan_set.end(); ++s)
  {
    // detect the start of a new tilt
    bool new_tilt = s == end_tilt;
    if (new_tilt)
    {
      auto htilt = s->find_header("TILT");
      auto helev = s->find_header("ELEV");

      // look ahead and find the end of this tilt, also noting the maximum number of bins
      header const* h = nullptr;
      end_tilt = s;
      bins = s->level_data().cols();
      while (end_tilt != scan_set.end())
      {
        bins = std::max(bins, end_tilt->level_data().cols());
        if (htilt && (h = end_tilt->find_header("TILT")))
        {
          if (htilt->value() != h->value())
            break;
        }
        else if (helev && (h = end_tilt->find_header("ELEV")))
        {
          if (helev->value() != h->value())
            break;
        }
        else
          break;
        ++end_tilt;
      }

      // create the new scan (except for the first time - it's already done)
      if (s != scan_set.begin())
        hscan = hvol.scan_append();

      // resize our temporary buffers
      ibuf.resize(s->level_data().rows(), bins);
      rbuf.resize(s->level_data().rows(), bins);
    }

    // determine the appropriate data type and size
    size_t dims[2] = { s->level_data().rows(), bins };
    auto hdata = hscan.data_append(hdf::data::data_type::u8, 2, dims);

    // process each header
    meta_extra m{*s, hvol, hscan, hdata};
    for (auto& h : s->headers())
    {
      auto i = header_map.find(h.name());
      if (i == header_map.end())
      {
        trace::warning() << "unknown rapic header encountered: " << h.name() << " = " << h.value();
        hdata.attributes()["rapic_" + h.name()].set(h.value());
      }
      else
        i->second(h, m);
    }

    // write the special tilt level headers
    // done after main header loop since we read back some of the HDF headers below...
    if (new_tilt)
    {
      hscan.attributes()["product"].set("SCAN");
      hscan.set_bin_count(bins);
      hscan.set_ray_count(s->level_data().rows());
      hscan.set_ray_start(-0.5);
      hscan.set_first_ray_radiated(s->rays().empty() ? 0 : angle_to_index(*s, s->rays().front().azimuth()));

      // automatically determine scan end time
      if (!s->rays().empty() && s->rays().back().time_offset() != -1)
      {
        // use time of last ray if available
        hscan.set_end_date_time(hscan.start_date_time() + s->rays().back().time_offset());
      }
      else
      {
        // use rpm to determine end time if available
        auto i = hscan.attributes().find("rpm");
        if (i != hscan.attributes().end())
          hscan.set_end_date_time(hscan.start_date_time() + 60.0 / i->get_real());
        else
        {
          // use start time from next scan if available
          auto n = s; ++n;
          header const* h;
          if (n != scan_set.end() && (h = n->find_header("TIMESTAMP")))
            hscan.set_end_date_time(rapic_timestamp_to_time_t(h->value().c_str()));
          else
            // last resort.  just add 30 seconds to start time to prevent violation of ODIM spec
            hscan.set_end_date_time(hscan.start_date_time() + 30);
        }
      }
    }

    // cope with really old transmitters that don't send the VIDEO header
    // in this case it is always a corrected reflectivity moment
    if (m.video.empty())
    {
      // it's a known issue on V8.22
      auto vers = s->find_header("VERS");
      if (!(vers && (vers->value() == "8.21" || vers->value() == "8.22")))
        trace::warning() 
          << "no VIDEO header supplied, assuming reflectivity (VERS: " 
          << s->find_header("VERS")->value() << ")";
      m.video = "Refl";
    }

    // determine quantity value
    auto vm = video_map.find(m.video);
    if (vm == video_map.end())
      hdata.set_quantity(m.video);
    else
      hdata.set_quantity(m.vpol ? vm->second.vname : vm->second.hname);

    // write the moment data
    hdata.set_nodata(0.0);
    hdata.set_undetect(0.0);

    // convert rays from received order and possibly range truncated, to CW from north order full range
    array_utils::fill(ibuf, 0);
    for (size_t r = 0; r < s->rays().size(); ++r)
    {
      std::memcpy(
            ibuf[angle_to_index(*s, s->rays()[r].azimuth())]
          , s->level_data()[r]
          , s->level_data().cols() * sizeof(uint8_t));
    }

    // determine the conversion (if any) to real moment values
    // thresholded data?
    if (!m.thresholds.empty())
    {
      // check that we know how to repack this moment
      if (vm == video_map.end() || is_nan(vm->second.odim_gain))
        throw std::runtime_error{msg{} << "thresholded encoding used for unexpected video type '" << m.video << "'"};

      // determine the matching output level for each threshold and
      // check that each threshold level can be exactly represented by the 8-bit encoding
      /* note that we currently output the threshold values themselves as the ODIM value.  ODIM doesn't have a
       * concept of thresholded moments so it may be better to convert these to bin centers like we do for the
       * gain/offset style moments.  this isn't necessarily easy though due to the top bin (what width to use)
       * and the non-linear nature of dBZs... */
      level_convert.resize(m.thresholds.size() + 1);
      level_convert[0] = 0;
      for (size_t i = 0; i < m.thresholds.size(); ++i)
      {
        auto o = (m.thresholds[i] - vm->second.odim_offset) / vm->second.odim_gain;
        level_convert[i + 1] = o;
        if (std::abs(o - level_convert[i + 1]) > 0.001)
        {
          trace::warning()
            << "threshold value '" << m.thresholds[i] << "' cannot be represented exactly by 8bit encoding with gain "
            << vm->second.odim_gain << " offset " << vm->second.odim_offset
            << " will be encoded as " << level_convert[i + 1] << " -> " << (level_convert[i + 1] * vm->second.odim_gain + vm->second.odim_offset);
        }
      }

      // convert between the rapic and odim levels
      for (size_t i = 0; i < ibuf.size(); ++i)
      {
        auto lvl = ibuf.data()[i];
        if (lvl >= (int) level_convert.size())
        {
          trace::error() << "level exceeding threshold table size encountered: " << (int) lvl << " / " << level_convert.size();
          //throw std::runtime_error{"level exceeding threshold table size encountered"};
        }
        ibuf.data()[i] = level_convert[lvl];
      }

      // write it out
      hdata.set_gain(vm->second.odim_gain);
      hdata.set_offset(vm->second.odim_offset);
      hdata.write(ibuf.data());
    }
    // explicitly supplied gain and offset in rapic headers?
    else if (   !m.vidgain.empty() && m.vidgain != "THRESH"
             && !m.vidoffset.empty() && m.vidoffset != "THRESH")
    {
      // gain and offset specified directly by rapic - copy data straight to ODIM
      // we add half of the gain to the rapic offset to convert from the threshold bin lower edge into the bin
      // center value which is the best estimate for the real value we can get
      auto gain = from_string<double>(m.vidgain);
      hdata.set_gain(gain);
      hdata.set_offset(from_string<double>(m.vidoffset) + 0.5 * gain);
      hdata.write(ibuf.data());
    }
    // velocity moment with nyquist or VELLVL supplied?
    else if (m.video == "Vel")
    {
      if (is_nan(m.maxvel))
        throw std::runtime_error{"no VELLVL or NYQUIST supplied for default Vel encoded scan"};

      // this logic is copied from ConcEncodeClient.cpp (via Ray)
      auto gain = (2 * m.maxvel) / (m.vidres - 1);
      auto offset = -m.maxvel - gain;

      // as above, we add half the gain to the rapic offset to get bin centers instead of minimums
      hdata.set_gain(gain);
      hdata.set_offset(offset + 0.5 * gain);
      hdata.write(ibuf.data());
    }
    // otherwise we don't know what to do - just encode the levels directly
    else
    {
      trace::warning() << "unable to determine encoding for VIDEO '" << m.video << "', writing levels directly";

      hdata.set_gain(1.0);
      hdata.set_offset(0.0);
      hdata.write(ibuf.data());
    }
  }

  return vol_time;
}
