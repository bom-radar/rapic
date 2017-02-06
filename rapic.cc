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

#include <strings.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <sstream>
#include <system_error>
#include <tuple>

using namespace rapic;

static constexpr auto fnan = std::numeric_limits<float>::quiet_NaN();

static constexpr message_type no_message = static_cast<message_type>(-1);

static const std::string msg_connect{"RPQUERY: SEMIPERMANENT CONNECTION - SEND ALL DATA TXCOMPLETESCANS=0\n"};
static const std::string msg_keepalive{"RDRSTAT:\n"};
static const std::string msg_mssg_head{"MSSG:"};
static const std::string msg_mssg_term{"\n"};
static const std::string msg_mssg30_head{"MSSG: 30"};
static const std::string msg_mssg30_term{"END STATUS\n"};
static const std::string msg_scan_term{"END RADAR IMAGE"};

// this table translates the ASCII encoding absolute, RLE digits and delta lookups
namespace
{
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
}
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

auto rapic::release_tag() -> char const*
{
  return RAPIC_RELEASE_TAG;
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
  return std::stol(value_, nullptr, 10);
}

auto header::get_real() const -> double
{
  return std::stod(value_);
}

auto header::get_integer_array() const -> std::vector<long>
{
  std::vector<long> ret;
  auto pos = value_.c_str();
  while (*pos != '\0')
  {
    char* end;
    auto val = strtol(pos, &end, 10);

    // did the conversion fail?
    if (val == 0 && end == pos)
    {
      // check if it is just trailing spaces
      while (*pos == ' ')
        ++pos;
      if (*pos != '\0')
        throw std::runtime_error{"bad integer value"};
    }
    else
    {
      ret.push_back(val);
      pos = end;
    }
  }
  return ret;
}

auto header::get_real_array() const -> std::vector<double>
{
  std::vector<double> ret;
  auto pos = value_.c_str();
  while (*pos != '\0')
  {
    char* end;
    auto val = strtod(pos, &end);

    // did the conversion fail?
    if (val == 0 && end == pos)
    {
      // check if it is just trailing spaces
      while (*pos == ' ')
        ++pos;
      if (*pos != '\0')
        throw std::runtime_error{"bad double value"};
    }
    else
    {
      ret.push_back(val);
      pos = end;
    }
  }
  return ret;
}

scan::scan()
{
  reset();
}

auto scan::reset() -> void
{
  headers_.clear();
  ray_headers_.clear();
  rays_ = 0;
  bins_ = 0;
  level_data_.clear();

  station_id_ = -1;
  volume_id_ = -1;
  product_.clear();
  pass_ = -1;
  pass_count_ = -1;
  is_rhi_ = false;
  angle_min_ = fnan;
  angle_max_ = fnan;
  angle_resolution_ = fnan;
}

auto scan::decode(uint8_t const* in, size_t size) -> size_t
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
      if (ray_headers_.empty())
        initialize_rays();

      // sanity check that we don't have too many rays
      if (static_cast<int>(ray_headers_.size()) == rays_)
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
      ray_headers_.emplace_back(angle);

      // decode the data into levels
      auto out = &level_data_[bins_ * (ray_headers_.size() - 1)];
      int prev = 0;
      int bin = 0;
      while (pos < size)
      {
        auto& cur = lookup[in[pos++]];

        //  absolute pixel value
        if (cur.type == enc_type::value)
        {
          if (bin < bins_)
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
          if (bin + count > bins_)
            throw std::runtime_error{"scan data overflow (ascii rle)"};
          for (int i = 0; i < count; ++i)
            out[bin++] = prev;
        }
        // delta encoding
        // silently ignore potential overflow caused by second half of a delta encoding at end of ray
        // we assume it is just an artefact of the encoding process
        else if (cur.type == enc_type::delta)
        {
          if (bin < bins_)
            out[bin++] = prev += cur.val;
          else
            throw std::runtime_error{"scan data overflow (ascii delta)"};

          if (bin < bins_)
            out[bin++] = prev += cur.val2;
          else if (pos < size && lookup[in[pos]].type != enc_type::terminate)
            throw std::runtime_error{"scan data overflow (ascii delta)"};
        }
        // null or end of line character - end of radial
        else if (cur.type == enc_type::terminate)
        {
          /* hack to work around extra newline characters that corrupt the data stream of some radars
           * (looking at you Dampier).  if we ever have headers appear in the file after rays then this
           * will break. */
          {
            auto i = pos;
            while (i < size && in[i] <= ' ')
              ++i;
            if (   i < size
                && in[i] != '%'
                && size - i >= msg_scan_term.size()
                && strncmp(reinterpret_cast<char const*>(&in[i]), msg_scan_term.c_str(), msg_scan_term.size()) != 0)
              continue;
          }

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
      if (ray_headers_.empty())
        initialize_rays();

      // sanity check that we don't have too many rays
      if (static_cast<int>(ray_headers_.size()) == rays_)
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
      ray_headers_.emplace_back(azi, el, sec);

      // decode the data into levels
      auto out = &level_data_[bins_ * (ray_headers_.size() - 1)];
      int bin = 0;
      while (true)
      {
        int val = in[pos++];
        if (val == 0 || val == 1)
        {
          int count = in[pos++];
          if (count == 0)
            break;
          if (bin + count > bins_)
            throw std::runtime_error{"scan data overflow (binary rle)"};
          for (int i = 0; i < count; ++i)
            out[bin++] = val;
        }
        else if (bin < bins_)
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
          return pos + msg_scan_term.size();
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
  std::ostringstream desc;
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
  std::throw_with_nested(std::runtime_error{desc.str()});
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
  throw std::runtime_error{std::string("missing mandatory header ") + name};
}

auto scan::get_header_integer(char const* name) const -> long
{
  if (auto p = find_header(name))
    return p->get_integer();
  throw std::runtime_error{std::string("missing mandatory header ") + name};
}

auto scan::get_header_real(char const* name) const -> double
{
  if (auto p = find_header(name))
    return p->get_real();
  throw std::runtime_error{std::string("missing mandatory header ") + name};
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

  rays_ = std::lround((angle_max_ - angle_min_) / angle_resolution_);
  if (remainder(angle_max_ - angle_min_, angle_resolution_) > 0.001)
    throw std::runtime_error{"ANGRES is not a factor of sweep length"};

  bins_ = std::lround((endrng - startrng) / rngres);
  if (bins_ < 0 || remainder(endrng - startrng, rngres) > 0.001)
    throw std::runtime_error("RNGRES is not a factor of range span");

  ray_headers_.reserve(rays_);
  level_data_.resize(rays_ * bins_);
}

client::client(size_t buffer_size, time_t keepalive_period, time_t inactivity_timeout)
  : keepalive_period_{keepalive_period}
  , inactivity_timeout_{inactivity_timeout}
  , socket_{-1}
  , establish_wait_{false}
  , last_keepalive_{0}
  , last_activity_{0}
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
  , inactivity_timeout_(std::move(inactivity_timeout_))
  , filters_(std::move(rhs.filters_))
  , socket_{rhs.socket_}
  , establish_wait_{rhs.establish_wait_}
  , last_keepalive_{rhs.last_keepalive_}
  , last_activity_{rhs.last_activity_}
  , buffer_(std::move(rhs.buffer_))
  , capacity_{rhs.capacity_}
  , wcount_{rhs.wcount_.load()}
  , rcount_{rhs.rcount_.load()}
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
  inactivity_timeout_ = std::move(rhs.inactivity_timeout_);
  filters_ = std::move(rhs.filters_);
  socket_ = rhs.socket_;
  establish_wait_ = rhs.establish_wait_;
  last_keepalive_ = rhs.last_keepalive_;
  last_activity_ = rhs.last_activity_;
  buffer_ = std::move(rhs.buffer_);
  capacity_ = rhs.capacity_;
  wcount_ = rhs.wcount_.load();
  rcount_ = rhs.rcount_.load();
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
    throw std::runtime_error{"rapic: add_filter called while connected"};

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
    throw std::runtime_error{"rapic: connect called while already connected"};

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
    throw std::runtime_error{"rapic: unable to resolve server address"};

  // TODO - loop through all addresses?
  if (addr->ai_next)
  {

  }

  // create the socket
  socket_ = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
  if (socket_ == -1)
  {
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic: socket creation failed"};
  }

  // set non-blocking I/O
  int flags = fcntl(socket_, F_GETFL);
  if (flags == -1)
  {
    disconnect();
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic: failed to read socket flags"};
  }
  if (fcntl(socket_, F_SETFL, flags | O_NONBLOCK) == -1)
  {
    disconnect();
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic: failed to set socket flags"};
  }

  // connect to the remote host
  ret = ::connect(socket_, addr->ai_addr, addr->ai_addrlen);
  if (ret < 0)
  {
    if (errno != EINPROGRESS)
    {
      disconnect();
      freeaddrinfo(addr);
      throw std::system_error{errno, std::system_category(), "rapic: failed to establish connection"};
    }
    establish_wait_ = true;
  }
  else
    establish_wait_ = false;

  // clean up the address list allocated by getaddrinfo
  freeaddrinfo(addr);

  // set the last activity to now so we don't immediately timeout
  last_activity_ = time(NULL);

  // queue up our permanent connection and filter messages for output as soon as we connect
  /* note: since the only things we ever send is the initial connection, the filters and occasional keepalive
   *       messages, we don't bother with complex write buffering.  the buffer is a simple string which will
   *       be empty except for keepalives after the initial connection negotiation. */
  wbuffer_.assign(msg_connect);
  for (auto& filter : filters_)
    wbuffer_.append(filter);
}

auto client::disconnect() -> void
{
  if (socket_ != -1)
  {
    close(socket_);
    socket_ = -1;
    last_keepalive_ = 0;
    wbuffer_.clear();
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
  return socket_ != -1 && (establish_wait_ || !wbuffer_.empty());
}

auto client::poll(int timeout) const -> void
{
  if (socket_ == -1)
    throw std::runtime_error{"rapic: attempt to poll while disconnected"};

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
      throw std::system_error{errno, std::system_category(), "rapic: getsockopt failure"};
    }

    // not connected yet?
    if (res == EINPROGRESS)
      return false;

    // okay, connection attempt is complete.  did it succeed?
    if (res < 0)
    {
      disconnect();
      throw std::system_error{res, std::system_category(), "rapic: failed to establish connection (async)"};
    }

    establish_wait_ = false;
  }

  // do we need to send a keepalive? (ie: RDRSTAT)
  if (now - last_keepalive_ > keepalive_period_)
  {
    wbuffer_.append(msg_keepalive);
    last_keepalive_ = now;
  }

  // write everything we can
  if (!wbuffer_.empty())
  {
    // write as much as we can
    ssize_t ret;
    while ((ret = write(socket_, wbuffer_.c_str(), wbuffer_.size())) == -1)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
      {
        ret = 0;
        break;
      }
      else if (errno != EINTR)
      {
        auto err = errno;
        disconnect();
        throw std::system_error{err, std::system_category(), "rapic: write failure"};
      }
    }

    // remove the written data from the buffer
    wbuffer_.erase(0, ret);
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
      // reset our inactivity timeout
      last_activity_ = now;

      // advance our write position
      wcount_ += bytes;

      // if we read as much as we asked for there may be more still waiting so return true
      return static_cast<size_t>(bytes) == space;
    }
    else if (bytes < 0)
    {
      // if we've run out of data to read stop trying
      if (errno == EAGAIN || errno == EWOULDBLOCK)
      {
        // check our inactivity timeout
        if (now - last_activity_ > inactivity_timeout_)
        {
          disconnect();
          throw std::runtime_error{"rapic: inactivity timeout"};
        }

        return false;
      }

      // if we were interrupted by a signal handler just try again
      if (errno == EINTR)
        continue;

      // a real receive error - kill the connection
      auto err = errno;
      disconnect();
      throw std::system_error{err, std::system_category(), "rapic: recv failure"};
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
  auto wc = wcount_.load();

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
    throw std::runtime_error{"rapic: buffer overflow (try increasing buffer size)"};

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
      throw std::runtime_error{"rapic: no message dequeued for decoding"};
    else
      throw std::runtime_error{"rapic: incorrect type passed for decoding"};
  }
}

auto client::buffer_starts_with(std::string const& str) const -> bool
{
  // cache rcount_ to reduce performance drop of atomic reads
  // this function is only ever called from the read thread
  auto rc = rcount_.load();

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
  auto rc = rcount_.load();

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
