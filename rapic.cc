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

static const std::string msg_comment_head{"/"};
static const std::string msg_mssg_head{"MSSG:"};
static const std::string msg_mssg30_head{"MSSG: 30"};
static const std::string msg_mssg30_term{"END STATUS"};
static const std::string msg_status_head{"RDRSTAT:"};
static const std::string msg_permcon_head{"RPQUERY: SEMIPERMANENT CONNECTION"};
static const std::string msg_query_head{"RPQUERY:"};
static const std::string msg_filter_head{"RPFILTER:"};
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

static auto find_non_whitespace(uint8_t const* begin, uint8_t const* end) -> uint8_t const*
{
  while (begin != end && *begin <= 0x20)
    ++begin;
  return begin;
}

static auto find_non_whitespace_or_eol(uint8_t const* begin, uint8_t const* end) -> uint8_t const*
{
  while (begin != end && *begin <= 0x20 && *begin != '\n' && *begin != '\r' && *begin != '\0')
    ++begin;
  return begin;
}

static auto starts_with(uint8_t const* begin, uint8_t const* end, std::string const& str) -> bool
{
  auto b2 = str.c_str(), e2 = str.c_str() + str.size();
  while (begin != end && b2 != e2 && *begin == *b2)
  {
    ++begin;
    ++b2;
  }
  return b2 == e2;
}

#if 0
static auto find_string(uint8_t const* begin, uint8_t const* end, std::string const& str) -> uint8_t const*
{
  while (begin != end && !starts_with(begin, end, str))
    ++begin;
  return begin;
}
#endif

static auto find_eol(uint8_t const* begin, uint8_t const* end) -> uint8_t const*
{
  while (begin != end && *begin != '\n' && *begin != '\r' && *begin != '\0')
    ++begin;
  return begin;
}

auto rapic::release_tag() -> char const*
{
  return RAPIC_RELEASE_TAG;
}

auto rapic::parse_station_id(char const* in) -> int
{
  int ret = strcasecmp(in, "ANY") == 0 ? 0 : atoi(in);
  if (ret == 0 && in[0] != '0')
    throw std::runtime_error{"invalid station id"};
  return ret;
}

auto rapic::parse_scan_type(char const* in) -> std::pair<scan_type, int>
{
  // is it a numeric equivalent (must match ROWLF definitions - see scantype.cc)
  if (std::isdigit(in[0]))
  {
    int val = std::atoi(in);
    if (val < -1 || val > 7 || (val == 0 && in[0] != '0'))
      throw std::runtime_error{"invalid scan type"};
    return { static_cast<scan_type>(val), -1 };
  }

  // is it a plain type identifier string?
  if (strcasecmp(in, "ANY") == 0)
    return { scan_type::any, -1 };
  if (strcasecmp(in, "PPI") == 0)
    return { scan_type::ppi, -1 };
  if (strcasecmp(in, "RHI") == 0)
    return { scan_type::rhi, -1 };
  if (strcasecmp(in, "CompPPI") == 0)
    return { scan_type::comp_ppi, -1 };
  if (strcasecmp(in, "IMAGE") == 0)
    return { scan_type::image, -1 };
  if (strcasecmp(in, "VOL") == 0)
    return { scan_type::volume, -1 };
  if (strcasecmp(in, "VOLUME") == 0)
    return { scan_type::volume, -1 };
  if (strcasecmp(in, "RHI_SET") == 0)
    return { scan_type::rhi_set, -1 };
  if (strcasecmp(in, "MERGE") == 0)
    return { scan_type::merge, -1 };
  if (strcasecmp(in, "SCAN_ERROR") == 0)
    return { scan_type::scan_error, -1 };

  // is it a VOLUMEXX identifier?
  int volid;
  if (sscanf(in, "VOLUME%d", &volid) == 1)
    return { scan_type::volume, volid };
  if (sscanf(in, "COMPPPI%d", &volid) == 1)
    return { scan_type::comp_ppi, volid };

  throw std::runtime_error{"invalid scan type id"};
}

auto rapic::parse_query_type(char const* in) -> query_type
{
  if (strcasecmp(in, "LATEST") == 0)
    return query_type::latest;
  if (strcasecmp(in, "TOTIME") == 0)
    return query_type::to_time;
  if (strcasecmp(in, "FROMTIME") == 0)
    return query_type::from_time;
  if (strcasecmp(in, "CENTRETIME") == 0)
    return query_type::center_time;

  throw std::runtime_error{"invalid query type"};
}

auto rapic::parse_data_types(char const* in) -> std::vector<std::string>
{
  std::vector<std::string> ret;
  size_t pos = 0, end = 0;
  while (in[end] != '\0')
  {
    if (in[end] == ',' || in[end] == '\0')
    {
      if (end - pos > 0)
        ret.emplace_back(in + pos, end - pos);

      if (in[end] == '\0')
        break;

      pos = end + 1;
    }
    ++end;
  }

  return ret;
}

socket_handle::~socket_handle()
{
  if (fd_ != -1)
    close(fd_);
}

auto socket_handle::reset(int fd) -> void
{
  if (fd_ != -1)
    close(fd_);
  fd_ = fd;
}

auto socket_handle::release() -> int
{
  int ret = fd_;
  fd_ = -1;
  return ret;
}

decode_error::decode_error(message_type type, uint8_t const* in, size_t size)
  : std::runtime_error{"TODO"}
{ }

buffer::buffer(size_t size, size_t max_size)
  : size_{size}
  , data_{new uint8_t[size_]}
  , wpos_{0}
  , rpos_{0}
  , max_size_{max_size}
{ }

buffer::buffer(buffer const& rhs)
  : size_{rhs.size_}
  , data_{new uint8_t[size_]}
  , wpos_{rhs.wpos_ - rhs.rpos_}
  , rpos_{0}
  , max_size_{rhs.max_size_}
{
  for (size_t i = 0; i < wpos_; ++i)
    data_[i] = rhs.data_[rhs.rpos_ + i];
}

auto buffer::operator=(buffer const& rhs) -> buffer&
{
  if (size_ != rhs.size_)
    data_.reset(new uint8_t[rhs.size_]);

  // no exceptions beyond here
  size_ = rhs.size_;
  wpos_ = rhs.wpos_ - rhs.rpos_;
  rpos_ = 0;
  max_size_ = rhs.max_size_;
  for (size_t i = 0; i < wpos_; ++i)
    data_[i] = rhs.data_[rhs.rpos_ + i];

  return *this;
}

auto buffer::resize(size_t size) -> void
{
  if (size < wpos_ - rpos_)
    throw std::logic_error{"rapic buffer resize would corrupt data stream"};

  if (size == size_)
    return;

  auto tmp = std::unique_ptr<uint8_t[]>{new uint8_t[size]};
  for (size_t i = 0; i < wpos_ - rpos_; ++i)
    tmp[i] = data_[rpos_ + i];

  size_ = size;
  data_ = std::move(tmp);
  wpos_ -= rpos_;
  rpos_ = 0;
}

auto buffer::optimize() -> void
{
  if (rpos_ != 0)
  {
    for (size_t i = 0; i < wpos_ - rpos_; ++i)
      data_[i] = data_[rpos_ + i];
    wpos_ -= rpos_;
    rpos_ = 0;
  }
}

auto buffer::clear() -> void
{
  wpos_ = 0;
  rpos_ = 0;
}

auto buffer::write_acquire(size_t min_space) -> std::pair<uint8_t*, size_t>
{
  // expand or shuffle to ensure min_space requirement is met
  auto space = size_ - wpos_;
  if (space < min_space)
  {
    auto min_size = wpos_ - rpos_ + min_space;
    if (min_size > max_size_)
      throw std::runtime_error{"rapic: allocating requested write space would exceed maximum buffer size"};
    if (space + rpos_ < min_space)
      resize(std::max(size_ * 2, min_size));
    else
      optimize();
    space = size_ - wpos_;
  }
  /* if min_space is 0 and wpos_ hits the end then force a shuffle.  without this fixed size buffers (i.e. those
   * for which the user always specifies min_space == 0) which hit the fill point part way through a message will
   * never have a chance to clear themselves because they will never perform a read_advance(). */
  else if (space == 0)
    optimize();
  return {&data_[wpos_], space};
}

auto buffer::write_advance(size_t len) -> void
{
  wpos_ += len;
  if (wpos_ > size_)
    throw std::out_of_range{"rapic buffer overflow detected on write operation"};
}

auto buffer::read_acquire() const -> std::pair<uint8_t const*, size_t>
{
  return {&data_[rpos_], wpos_ - rpos_};
}

auto buffer::read_advance(size_t len) -> void
{
  rpos_ += len;
  if (wpos_ > size_)
    throw std::out_of_range{"rapic buffer overflow detected on read operation"};
  if (rpos_ == wpos_)
    rpos_ = wpos_ = 0;
}

auto buffer::read_detect(message_type& type, size_t& len) const -> bool
{
  uint8_t const* pos = &data_[rpos_];
  uint8_t const* end = &data_[wpos_];
  uint8_t const* nxt;
  message_type msg = no_message;

  // ignore leading whitespace (and return if no data at all)
  if ((pos = find_non_whitespace(pos, end)) == end)
    return false;

  // is it a comment (i.e. IMAGE header)?
  if (starts_with(pos, end, msg_comment_head))
  {
    if ((nxt = find_eol(pos, end)) != end)
      msg = message_type::comment;
  }
  // is it an MSSG 30 style message? (must check mssg30 before mssg as mssg header is a subset of mssg30 header)
  else if (starts_with(pos, end, msg_mssg30_head))
  {
    // status 30 is multi-line terminated by "END STATUS"
    pos += msg_mssg30_head.size();
    while ((nxt = find_eol(pos, end)) != end)
    {
      if (   size_t(nxt - pos) == msg_mssg30_term.size()
          && msg_mssg30_term.compare(0, msg_mssg30_term.size(), reinterpret_cast<char const*>(pos), nxt - pos) == 0)
      {
        msg = message_type::mssg;
        break;
      }
      pos = nxt + 1;
    }
  }
  // is it an MSSG style message?
  else if (starts_with(pos, end, msg_mssg_head))
  {
    if ((nxt = find_eol(pos, end)) != end)
      msg = message_type::mssg;
  }
  // is it an RDRSTAT message?
  else if (starts_with(pos, end, msg_status_head))
  {
    if ((nxt = find_eol(pos, end)) != end)
      msg = message_type::status;
  }
  // is it a SEMIPERMANENT CONNECTION message? (must check this before RPQUERY due to header similarity)
  else if (starts_with(pos, end, msg_permcon_head))
  {
    if ((nxt = find_eol(pos, end)) != end)
      msg = message_type::permcon;
  }
  // is it a RPQUERY style message?
  else if (starts_with(pos, end, msg_query_head))
  {
    if ((nxt = find_eol(pos, end)) != end)
      msg = message_type::query;
  }
  // is it an RPFILTER styles message?
  else if (starts_with(pos, end, msg_filter_head))
  {
    if ((nxt = find_eol(pos, end)) != end)
      msg = message_type::filter;
  }
  // otherwise assume it is a scan message and look for "END RADAR IMAGE"
  else
  {
    while ((nxt = find_eol(pos, end)) != end)
    {
      // the "END RADAR IMAGE" token is _sometimes_ prefixed with a ^Z (0x28), just detect and skip whitespace
      if ((pos = find_non_whitespace(pos, nxt)) == nxt)
        continue;

      if (   size_t(nxt - pos) == msg_scan_term.size()
          && msg_scan_term.compare(0, msg_scan_term.size(), reinterpret_cast<char const*>(pos), nxt - pos) == 0)
      {
        msg = message_type::scan;
        break;
      }
      pos = nxt + 1;
    }
  }

  // did we manage to locate a message?
  if (msg != no_message)
  {
    type = msg;
    len = nxt + 1 - &data_[rpos_];
    return true;
  }

  return false;
}

message::~message()
{ }

comment::comment()
{
  reset();
}

comment::comment(buffer const& in)
{
  reset();
  decode(in);
}

auto comment::type() const -> message_type
{
  return message_type::comment;
}

auto comment::reset() -> void
{
  text_.clear();
}

auto comment::encode(buffer& out) const -> void
{
  auto wa = out.write_acquire(text_.size() + 8);
  auto ret = snprintf(reinterpret_cast<char*>(wa.first), wa.second, "/%s\n", text_.c_str());
  if (ret < 0 || size_t(ret) >= wa.second)
    throw std::runtime_error{"rapic: failed to encode message"};
  out.write_advance(ret);
}

auto comment::decode(buffer const& in) -> void
{
  auto ra = in.read_acquire();
  auto pos = ra.first, end = ra.first + ra.second;

  // skip leading whitespace
  if ((pos = find_non_whitespace(pos, end)) == end)
    throw std::runtime_error{"failed to parse message header"};

  // read the message identifier and number
  ssize_t len = 0;
  if (sscanf(reinterpret_cast<char const*>(pos), "/%zn", &len) != 0 || len == 0)
    throw std::runtime_error{"failed to parse message header"};
  pos += len;
  if (pos >= end)
    throw std::runtime_error{"read buffer overflow"};

  // remainder of line is the message text
  decltype(pos) eol;
  if ((eol = find_eol(pos, end)) == end)
    throw std::runtime_error{"read buffer overflow"};
  text_.assign(reinterpret_cast<char const*>(pos), eol - pos);
  pos = eol + 1;
}

mssg::mssg()
{
  reset();
}

mssg::mssg(buffer const& in)
{
  reset();
  decode(in);
}

auto mssg::type() const -> message_type
{
  return message_type::mssg;
}

auto mssg::reset() -> void
{
  number_ = -1;
  text_.clear();
}

auto mssg::encode(buffer& out) const -> void
{
  auto wa = out.write_acquire(text_.size() + 32);
  auto ret = snprintf(
        reinterpret_cast<char*>(wa.first)
      , wa.second
      , number_ == 30 ? "MSSG: %d %s\nEND STATUS\n" : "MSSG: %d %s\n"
      , number_
      , text_.c_str());
  if (ret < 0 || size_t(ret) >= wa.second)
    throw std::runtime_error{"rapic: failed to encode message"};
  out.write_advance(ret);
}

auto mssg::decode(buffer const& in) -> void
{
  auto ra = in.read_acquire();
  auto pos = ra.first, end = ra.first + ra.second;

  // skip leading whitespace
  if ((pos = find_non_whitespace(pos, end)) == end)
    throw std::runtime_error{"failed to parse message header"};

  // read the message identifier and number
  ssize_t len = 0;
  if (sscanf(reinterpret_cast<char const*>(pos), "MSSG: %d%zn", &number_, &len) != 1 || len == 0)
    throw std::runtime_error{"failed to parse message header"};
  pos += len;
  if (pos >= end)
    throw std::runtime_error{"read buffer overflow"};

  // skip whitepsace between the number and text
  if ((pos = find_non_whitespace_or_eol(pos, end)) == end)
    throw std::runtime_error{"read buffer overflow"};

  // remainder of line is the message text
  decltype(pos) eol;
  if ((eol = find_eol(pos, end)) == end)
    throw std::runtime_error{"read buffer overflow"};
  text_.assign(reinterpret_cast<char const*>(pos), eol - pos);
  pos = eol + 1;

  // handle multi-line messages (only #30)
  if (number_ == 30)
  {
    while ((eol = find_eol(pos, end)) != end)
    {
      if (   size_t(eol - pos) == msg_mssg30_term.size()
          && msg_mssg30_term.compare(0, msg_mssg30_term.size(), reinterpret_cast<char const*>(pos), eol - pos) == 0)
        break;
      text_.push_back('\n');
      text_.append(reinterpret_cast<char const*>(pos), eol - pos);
      pos = eol + 1;
    }
    if (eol == end)
      throw std::runtime_error{"read buffer overflow"};
  }
}

status::status()
{
  reset();
}

status::status(buffer const& in)
{
  reset();
  decode(in);
}

auto status::type() const -> message_type
{
  return message_type::status;
}

auto status::reset() -> void
{
  text_.clear();
}

auto status::encode(buffer& out) const -> void
{
  auto wa = out.write_acquire(text_.size() + 16);
  auto ret = snprintf(reinterpret_cast<char*>(wa.first), wa.second, "RDRSTAT: %s\n", text_.c_str());
  if (ret < 0 || size_t(ret) >= wa.second)
    throw std::runtime_error{"rapic: failed to encode message"};
  out.write_advance(ret);
}

auto status::decode(buffer const& in) -> void
{
  auto ra = in.read_acquire();
  auto pos = ra.first, end = ra.first + ra.second;

  // skip leading whitespace
  if ((pos = find_non_whitespace(pos, end)) == end)
    throw std::runtime_error{"failed to parse message header"};

  // read the message identifier and number
  ssize_t len = 0;
  if (sscanf(reinterpret_cast<char const*>(pos), "RDRSTAT:%zn", &len) != 0 || len == 0)
    throw std::runtime_error{"failed to parse message header"};
  pos += len;
  if (pos >= end)
    throw std::runtime_error{"read buffer overflow"};

  // skip whitepsace between the number and text
  if ((pos = find_non_whitespace_or_eol(pos, end)) == end)
    throw std::runtime_error{"read buffer overflow"};

  // remainder of line is the message text
  decltype(pos) eol;
  if ((eol = find_eol(pos, end)) == end)
    throw std::runtime_error{"read buffer overflow"};
  text_.assign(reinterpret_cast<char const*>(pos), eol - pos);
  pos = eol + 1;
}

permcon::permcon()
{
  reset();
}

permcon::permcon(buffer const& in)
{
  reset();
  decode(in);
}

auto permcon::type() const -> message_type
{
  return message_type::permcon;
}

auto permcon::reset() -> void
{
  tx_complete_scans_ = false;
}

auto permcon::encode(buffer& out) const -> void
{
  auto wa = out.write_acquire(96);
  auto ret = snprintf(
        reinterpret_cast<char*>(wa.first)
      , wa.second
      , "RPQUERY: SEMIPERMANENT CONNECTION - SEND ALL DATA TXCOMPLETESCANS=%d\n"
      , tx_complete_scans_);
  if (ret < 0 || size_t(ret) >= wa.second)
    throw std::runtime_error{"rapic: failed to encode message"};
  out.write_advance(ret);
}

auto permcon::decode(buffer const& in) -> void
{
  auto ra = in.read_acquire();
  auto pos = ra.first, end = ra.first + ra.second;

  // skip leading whitespace
  if ((pos = find_non_whitespace(pos, end)) == end)
    throw std::runtime_error{"failed to parse message header"};

  // read the message identifier and number
  int ival;
  ssize_t len = 0;
  auto ret = sscanf(
        reinterpret_cast<char const*>(pos)
      , "RPQUERY: SEMIPERMANENT CONNECTION - SEND ALL DATA TXCOMPLETESCANS=%d%zn"
      , &ival
      , &len);
  if (ret != 1 || len == 0)
    throw std::runtime_error{"failed to parse message header"};
  tx_complete_scans_ = ival != 0;
  pos += len;
  if (pos >= end)
    throw std::runtime_error{"read buffer overflow"};

  // find the end of the line (discard any extra text)
  if ((pos = find_eol(pos, end)) == end)
    throw std::runtime_error{"read buffer overflow"};
  ++pos;
}

query::query()
{
  reset();
}

query::query(buffer const& in)
{
  reset();
  decode(in);
}

auto query::type() const -> message_type
{
  return message_type::query;
}

auto query::reset() -> void
{
  station_id_ = 0;
  scan_type_ = rapic::scan_type::any;
  volume_id_ = -1;
  angle_ = -1.0f;
  repeat_count_ = -1;
  query_type_ = rapic::query_type::latest;
  time_ = 0;
  data_types_.clear();
  video_res_ = -1;
}

auto query::encode(buffer& out) const -> void
{
  // TODO
}

auto query::decode(buffer const& in) -> void
{
  auto ra = in.read_acquire();
  auto pos = ra.first, end = ra.first + ra.second;

  // skip leading whitespace
  if ((pos = find_non_whitespace(pos, end)) == end)
    throw std::runtime_error{"failed to parse message header"};

  // read the header
  long time;
  char str_stn[21], str_stype[21], str_qtype[21], str_dtype[128];
  ssize_t len = 0;
  auto ret = sscanf(
        reinterpret_cast<char const*>(pos)
      , "RPQUERY: %20s %20s %f %d %20s %ld %127s %d%zn"
      , str_stn
      , str_stype
      , &angle_
      , &repeat_count_
      , str_qtype
      , &time
      , str_dtype
      , &video_res_
      , &len);
  if (ret != 7 || len == 0)
    throw std::runtime_error{"failed to parse message header"};
  pos += len;
  if (pos >= end)
    throw std::runtime_error{"read buffer overflow"};

  // check/parse the individual tokens
  station_id_ = parse_station_id(str_stn);
  std::tie(scan_type_, volume_id_) = parse_scan_type(str_stype);
  query_type_ = parse_query_type(str_qtype);
  time_ = time;
  data_types_ = parse_data_types(str_dtype);

  // find the end of the line (discard any extra text)
  if ((pos = find_eol(pos, end)) == end)
    throw std::runtime_error{"read buffer overflow"};
  ++pos;
}

filter::filter()
{
  reset();
}

filter::filter(buffer const& in)
{
  reset();
  decode(in);
}

auto filter::type() const -> message_type
{
  return message_type::filter;
}

auto filter::reset() -> void
{
  station_id_ = 0;
  scan_type_ = rapic::scan_type::any;
  volume_id_ = -1;
  video_res_ = -1;
  source_.clear();
  data_types_.clear();
}

auto filter::encode(buffer& out) const -> void
{
  // determine maximum needed buffer space
  size_t limit = 13 + 3 + 8 + 3 + 8;
  for (auto& type : data_types_)
    limit += type.size() + 1;

  // build the scan type string
  char const* str_stype = nullptr;
  char stypebuf[32];
  switch (scan_type_)
  {
  case rapic::scan_type::any:
    str_stype = "ANY";
    break;
  case rapic::scan_type::ppi:
    str_stype = "PPI";
    break;
  case rapic::scan_type::rhi:
    str_stype = "RHI";
    break;
  case rapic::scan_type::comp_ppi:
    if (volume_id_ != -1)
    {
      sprintf(stypebuf, "COMPPPI%d", volume_id_);
      str_stype = stypebuf;
    }
    else
      str_stype = "CompPPI";
    break;
  case rapic::scan_type::image:
    str_stype = "IMAGE";
    break;
  case rapic::scan_type::volume:
    if (volume_id_ != -1)
    {
      sprintf(stypebuf, "VOLUME%d", volume_id_);
      str_stype = stypebuf;
    }
    else
      str_stype = "VOLUME";
    break;
  case rapic::scan_type::rhi_set:
    str_stype = "RHI_SET";
    break;
  case rapic::scan_type::merge:
    str_stype = "MERGE";
    break;
  case rapic::scan_type::scan_error:
    str_stype = "SCAN_ERROR";
    break;
  }

  // build the datatypes string list
  char str_dtype[256];
  {
    auto ptr = &str_dtype[0];
    for (auto& type : data_types_)
    {
      strcpy(ptr, type.c_str());
      ptr += type.size();
      *ptr++ = ',';
    }
    *(ptr - 1) = '\0';
  }

  // write the message
  auto wa = out.write_acquire(limit);
  auto ret = snprintf(
        reinterpret_cast<char*>(wa.first)
      , wa.second
      , "RPFILTER:%d:%s:%d:%s:%s\n"
      , station_id_
      , str_stype
      , video_res_
      , source_.c_str()
      , str_dtype);
  if (ret < 0 || size_t(ret) >= wa.second)
    throw std::runtime_error{"rapic: failed to encode message"};
  out.write_advance(ret);
}

auto filter::decode(buffer const& in) -> void
{
  auto ra = in.read_acquire();
  auto pos = ra.first, end = ra.first + ra.second;

  // skip leading whitespace
  if ((pos = find_non_whitespace(pos, end)) == end)
    throw std::runtime_error{"failed to parse message header"};

  // read the header
  char str_stn[21], str_stype[21], str_src[21], str_dtype[256];
  str_src[0] = '\0';
  ssize_t len = 0;
  auto ret = sscanf(
        reinterpret_cast<char const*>(pos)
      , "RPFILTER:%20[^:]:%20[^:]:%d:%20[^:]:%255s%zn"
      , str_stn
      , str_stype
      , &video_res_
      , str_src
      , str_dtype
      , &len);
  if (ret != 5 || len == 0)
    throw std::runtime_error{"failed to parse message header"};
  pos += len;
  if (pos >= end)
    throw std::runtime_error{"read buffer overflow"};

  // check/parse the individual tokens
  station_id_ = parse_station_id(str_stn);
  std::tie(scan_type_, volume_id_) = parse_scan_type(str_stype);
  source_.assign(str_src);
  data_types_ = parse_data_types(str_dtype);

  // find the end of the line (discard any extra text)
  if ((pos = find_eol(pos, end)) == end)
    throw std::runtime_error{"read buffer overflow"};
  ++pos;
}

auto scan::header::get_boolean() const -> bool
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

auto scan::header::get_integer() const -> long
{
  return std::stol(value_, nullptr, 10);
}

auto scan::header::get_real() const -> double
{
  return std::stod(value_);
}

auto scan::header::get_integer_array() const -> std::vector<long>
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

auto scan::header::get_real_array() const -> std::vector<double>
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

scan::scan(buffer const& in)
{
  reset();
  decode(in);
}

auto scan::type() const -> message_type
{
  return message_type::scan;
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

auto scan::encode(buffer& out) const -> void
{
  // determine the maximum amount of memory that could be required to encode the scan
  // assumes rays are encoded as 256 level alternating level 0 and 1 which is worst case
  size_t limit = 0;
  for (auto& header : headers_)
    limit += header.name().size() + header.value().size() + 2;  // 2 for ':' + '\n'
  limit += rays_ * (bins_ * 2 + 18);                            // 2 for worst case rle, 18 for ray header
  limit += msg_scan_term.size() + 3;                            // 3 for '\n', '^Z', '\n'

  // acquire the worst case memory block from our buffer so we don't have to check buffer capacity after every write
  auto wa = out.write_acquire(limit);
  auto pos = wa.first, end = wa.first + wa.second;

  // write the headers
  for (auto& header : headers_)
  {
    strcpy(reinterpret_cast<char*>(pos), header.name().c_str());
    pos += header.name().size();
    *pos++ = ':';
    strcpy(reinterpret_cast<char*>(pos), header.value().c_str());
    pos += header.value().size();
    *pos++ = '\n';
  }

  // determine the video resolution
  auto vidres = 160;
  for (auto& header : headers_)
    if (header.name() == "VIDRES")
      vidres = header.get_integer();

  // write the rays
  if (vidres == 16 || vidres == 32 || vidres == 64 || vidres == 160)
  {
    for (int ray = 0; ray < rays_; ++ray)
    {
      // ascii ray header
      pos += sprintf(reinterpret_cast<char*>(pos), is_rhi_ ? "%%%4f" : "%%%3f", ray_headers_[ray].azimuth());

      // encode the bins
      int bin = 0;
      while (bin < bins_)
      {
        // TODO
      }

      // terminating new line
      *pos++ = '\n';
    }
  }
  else if (vidres == 256)
  {
    for (int ray = 0; ray < rays_; ++ray)
    {
      // binary ray header
      pos += sprintf(
            reinterpret_cast<char*>(pos)
          , "@%3.1f%3.1f,%03d="
          , ray_headers_[ray].azimuth()
          , ray_headers_[ray].elevation()
          , ray_headers_[ray].time_offset());

      // leave space for the count
      auto len_pos = pos;
      pos += 2;

      // encode the bins
      int bin = 0;
      auto ray_data = &level_data_[bins_ * ray];
      while (bin < bins_)
      {
        auto val = ray_data[bin];
        if (val == 0 || val == 1)
        {
          *pos++ = val;
          int count = 1;
          while (count < 256 && ray_data[bin + count] == val)
            ++count;
          *pos++ = count;
          bin += count;
        }
        else
        {
          *pos++ = val;
          ++bin;
        }
      }
      *pos++ = 0;
      *pos++ = 0;

      // fill the count now that we know what it is
      auto ray_len = pos - len_pos - 2;
      len_pos[0] = (ray_len >> 8) & 0x0f;
      len_pos[1] = (ray_len >> 0) & 0x0f;
    }
  }
  else
    throw std::runtime_error{"unsupported video resolution"};

  // write the terminator
  strcpy(reinterpret_cast<char*>(pos), msg_scan_term.c_str());
  pos += msg_scan_term.size();
  *pos++ = '\n';

  // commit the encoded message to the buffer (will also sanity check for overflow)
  out.write_advance(pos - wa.first);
}

auto scan::decode(buffer const& in) -> void
try
{
  auto ra = in.read_acquire();
  auto dat = ra.first;
  auto size = ra.second;

  reset();

  for (size_t pos = 0; pos < size; ++pos)
  {
    auto next = dat[pos];

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
      if (sscanf(reinterpret_cast<char const*>(&dat[pos]), is_rhi_ ? "%4f" : "%3f", &angle) != 1)
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
        auto& cur = lookup[dat[pos++]];

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
          while (pos < size && lookup[dat[pos]].type == enc_type::digit)
          {
            count *= 10;
            count += lookup[dat[pos++]].val;
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
          else if (pos < size && lookup[dat[pos]].type != enc_type::terminate)
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
            while (i < size && dat[i] <= ' ')
              ++i;
            if (   i < size
                && dat[i] != '%'
                && size - i >= msg_scan_term.size()
                && strncmp(reinterpret_cast<char const*>(&dat[i]), msg_scan_term.c_str(), msg_scan_term.size()) != 0)
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
      if (sscanf(reinterpret_cast<char const*>(&dat[pos]), "%f,%f,%d=", &azi, &el, &sec) != 3)
        throw std::runtime_error("invalid binary ray header");
      // note: we ignore the length for now
      //auto len = (((unsigned int) dat[16]) << 8) + ((unsigned int) dat[17]);
      pos += 18;

      // create the ray entry
      ray_headers_.emplace_back(azi, el, sec);

      // decode the data into levels
      auto out = &level_data_[bins_ * (ray_headers_.size() - 1)];
      int bin = 0;
      while (true)
      {
        int val = dat[pos++];
        if (val == 0 || val == 1)
        {
          int count = dat[pos++];
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
        if (dat[pos2] < ' ' || dat[pos2] == ':')
          break;

      // check for end of scan or corruption
      if (pos2 >= size || dat[pos2] != ':')
      {
        // valid end of scan?
        if (   pos2 - pos == msg_scan_term.size()
            && strncmp(reinterpret_cast<char const*>(&dat[pos]), msg_scan_term.c_str(), msg_scan_term.size()) == 0)
          return;
        throw std::runtime_error{"corrupt scan detected (3)"};
      }

      // find the start of the header value
      for (pos3 = pos2 + 1; pos3 < size; ++pos3)
        if (dat[pos3] > ' ')
          break;

      // check for corruption
      if (pos3 == size)
        throw std::runtime_error{"corrupt scan detected (4)"};

      // find the end of the header value
      for (pos4 = pos3 + 1; pos4 < size; ++pos4)
        if (dat[pos4] < ' ') // note: spaces are valid characters in the header value
          break;

      // store the header
      headers_.emplace_back(
            std::string(reinterpret_cast<char const*>(&dat[pos]), pos2 - pos)
          , std::string(reinterpret_cast<char const*>(&dat[pos3]), pos4 - pos3));

      // advance past the header line
      pos = pos4;
    }
    // else whitespace - skip
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

client::client(size_t max_buffer_size, time_t keepalive_period)
  : keepalive_period_{keepalive_period}
  , establish_wait_{false}
  , last_keepalive_{0}
  , rbuf_{1024, max_buffer_size}
  , cur_type_{no_message}
  , cur_size_{0}
{ }

auto client::add_filter(int station, std::string const& product, std::vector<std::string> const& moments) -> void
{
  if (socket_)
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

auto client::accept(socket_handle socket, std::string address, std::string service) -> void
{
  if (socket_)
    throw std::runtime_error{"rapic: accept called while already connected"};

  // set non-blocking I/O
  int flags = fcntl(socket, F_GETFL);
  if (flags == -1)
    throw std::system_error{errno, std::system_category(), "rapic: failed to read socket flags"};
  if (fcntl(socket, F_SETFL, flags | O_NONBLOCK) == -1)
    throw std::system_error{errno, std::system_category(), "rapic: failed to set socket flags"};

  // everything succeeded - commit the changes and take ownership of the socket
  address_ = std::move(address);
  service_ = std::move(service);
  socket_ = std::move(socket);
  last_keepalive_ = 0;
  rbuf_.clear();
  cur_type_ = no_message;
  cur_size_ = 0;
}

auto client::connect(std::string address, std::string service) -> void
{
  if (socket_)
    throw std::runtime_error{"rapic: connect called while already connected"};

  // lookupt the host
  addrinfo hints, *addr;
  memset(&hints, 0, sizeof(hints));
  hints.ai_flags = 0;
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  int ret = getaddrinfo(address.c_str(), service.c_str(), &hints, &addr);
  if (ret != 0 || addr == nullptr)
    throw std::runtime_error{"rapic: unable to resolve server address"};

  // TODO - loop through all addresses?
  if (addr->ai_next)
  {

  }

  // create the socket
  socket_handle socket{::socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol)};
  if (!socket)
  {
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic: socket creation failed"};
  }

  // set non-blocking I/O
  int flags = fcntl(socket, F_GETFL);
  if (flags == -1)
  {
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic: failed to read socket flags"};
  }
  if (fcntl(socket, F_SETFL, flags | O_NONBLOCK) == -1)
  {
    freeaddrinfo(addr);
    throw std::system_error{errno, std::system_category(), "rapic: failed to set socket flags"};
  }

  // connect to the remote host
  bool establish_wait = false;
  ret = ::connect(socket, addr->ai_addr, addr->ai_addrlen);
  if (ret < 0)
  {
    if (errno != EINPROGRESS)
    {
      freeaddrinfo(addr);
      throw std::system_error{errno, std::system_category(), "rapic: failed to establish connection"};
    }
    establish_wait = true;
  }

  // clean up the address list allocated by getaddrinfo
  freeaddrinfo(addr);

  // everything succeeded - commit the changes and take ownership of the socket
  address_ = std::move(address);
  service_ = std::move(service);
  socket_ = std::move(socket);
  establish_wait_ = establish_wait;
  last_keepalive_ = 0;
  rbuf_.clear();
  cur_type_ = no_message;
  cur_size_ = 0;
}

auto client::disconnect() -> void
{
  socket_.reset();
}

auto client::connected() const -> bool
{
  return socket_;
}

auto client::pollable_fd() const -> int
{
  return socket_;
}

auto client::poll_read() const -> bool
{
  return socket_ && !establish_wait_;
}

auto client::poll_write() const -> bool
{
  return socket_ && establish_wait_;
}

auto client::poll(int timeout) const -> void
{
  if (!socket_)
    throw std::runtime_error{"rapic: attempt to poll while disconnected"};

  struct pollfd fds;
  fds.fd = socket_;
  fds.events = POLLRDHUP | (poll_read() ? POLLIN : 0) | (poll_write() ? POLLOUT : 0);
  ::poll(&fds, 1, timeout);
}

auto client::process_traffic() -> bool
{
  // sanity check
  if (!socket_)
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

    /* note: since the only things we ever send is the initial connection, the filters and occasional keepalive
     *       messages, we don't bother with buffering the writes.  if for some reason we manage to fill the
     *       write buffer here (extremely unlikely) then we will need to buffer writes like we do reads */

    // activate the semi-permanent connection
    while (write(socket_, msg_connect.c_str(), msg_connect.size()) == -1)
      if (errno != EINTR)
        throw std::system_error{errno, std::system_category(), "rapic: failed to write to socket"};

    // activate each of our filters
    for (auto& filter : filters_)
      while (write(socket_, filter.c_str(), filter.size()) == -1)
        if (errno != EINTR)
          throw std::system_error{errno, std::system_category(), "rapic: failed to write to socket"};
  }

  // do we need to send a keepalive? (ie: RDRSTAT)
  if (now - last_keepalive_ > keepalive_period_)
  {
    while (write(socket_, msg_keepalive.c_str(), msg_keepalive.size()) == -1)
      if (errno != EINTR)
        throw std::system_error{errno, std::system_category(), "rapic: failed to write to socket"};
    last_keepalive_ = now;
  }

  // read everything we can
  while (true)
  {
    /* request minimum of 256 bytes buffer space to read into.  in practice we will normally be returned far more
     * than this.  if we cannot acquire 256 bytes then an exception will be thrown by the buffer. */
    auto wa = rbuf_.write_acquire(256);

    // read some data off the wire
    auto bytes = recv(socket_, wa.first, wa.second, 0);
    if (bytes > 0)
    {
      // commit the read bytes to the buffer
      rbuf_.write_advance(bytes);

      // if we read as much as we asked for there may be more still waiting so return true
      return static_cast<size_t>(bytes) == wa.second;
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

auto client::enqueue(message& msg) -> void
{
}

auto client::dequeue(message_type& type) -> bool
{
  // move along to the next packet in the buffer if needed
  if (cur_type_ != no_message)
  {
    rbuf_.read_advance(cur_size_);
    cur_type_ = no_message;
    cur_size_ = 0;
  }

  // detect the next message in the stream
  if (rbuf_.read_detect(cur_type_, cur_size_))
  {
    type = cur_type_;
    return true;
  }

  return false;
}

auto client::decode(message& msg) -> void
{
  // sanity check the passed message type
  if (cur_type_ != msg.type())
  {
    if (cur_type_ == no_message)
      throw std::runtime_error{"rapic: no message dequeued for decoding"};
    else
      throw std::runtime_error{"rapic: incorrect type passed for decoding"};
  }

  // we advance the buffer even if an exception is thrown during decoding
  // this ensures that corrupt messages do not stall the stream
  try
  {
    msg.decode(rbuf_);
  }
  catch (...)
  {
    rbuf_.read_advance(cur_size_);
    cur_type_ = no_message;
    cur_size_ = 0;
    throw;
  }

  rbuf_.read_advance(cur_size_);
  cur_type_ = no_message;
  cur_size_ = 0;
}

server::server()
{ }

auto server::listen(std::string service, bool ipv6) -> void
{
  if (socket_)
    throw std::runtime_error{"rapic: attempt to listen while already listening"};

  // lookup the port for the desired service
  uint16_t port = 0;
  if (struct servent* ent = getservbyname(service.c_str(), "tcp"))
    port = ntohs(ent->s_port);
  endservent();

  // if we couldn't find a service, try to parse as a port number directly
  if (port == 0)
    port = std::atoi(service.c_str());
  if (port == 0)
    throw std::runtime_error{"rapic: unknown or invalid service or port '" + service + "'"};

  // create the listen socket
  socket_.reset(::socket(ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM, 0));
  if (socket_ < 0)
    throw std::system_error{errno, std::generic_category(), "rapic: socket creation failed"};

  // allow immediate reuse of server socket after failure
  int on = 1;
  if (setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0)
    throw std::system_error{errno, std::generic_category(), "rapic: socket reuse mode set failed"};

  if (ipv6)
  {
    // allow connections from ipv4 clients when using an ipv6 socket
    on = 0;
    if (setsockopt(socket_, IPPROTO_IPV6, IPV6_V6ONLY, &on, sizeof(on)) < 0)
      throw std::system_error{errno, std::generic_category(), "rapic: socket failed to disable ipv6 only"};

    // build a local address
    struct sockaddr_in6 addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(port);
    addr.sin6_addr = in6addr_any;

    // bind the address
    if (bind(socket_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0)
      throw std::system_error{errno, std::generic_category(), "rapic: socket bind failed"};
  }
  else
  {
    // build a local address
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    // bind the address
    if (bind(socket_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0)
      throw std::system_error{errno, std::generic_category(), "rapic: socket bind failed"};
  }

  // mark as a passive socket
  if (::listen(socket_, SOMAXCONN) < 0)
    throw std::system_error{errno, std::generic_category(), "rapic: socket listen failed"};

  // set as a non-blocking socket
  int flags = fcntl(socket_, F_GETFL);
  if (flags == -1)
    throw std::system_error{errno, std::generic_category(), "rapic: failed to read socket flags"};
  if (fcntl(socket_, F_SETFL, flags | O_NONBLOCK) == -1)
    throw std::system_error{errno, std::generic_category(), "rapic: failed to set socket flags"};
}

auto server::release() -> void
{
  socket_.reset();
}

auto server::accept_pending_connections(size_t buffer_size, time_t keepalive_period) -> std::list<client>
{
  sockaddr_storage sa;
  socklen_t salen = sizeof(sa);

  std::list<client> clients;
  while (true)
  {
    // try to accept a pending connection
    socket_handle socket{accept(socket_, (sockaddr*) &sa, &salen)};
    if (!socket)
    {
      if (errno == EINTR)
        continue;
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        break;
      throw std::system_error{errno, std::generic_category(), "rapic: failed to accept socket"};
    }

    // convert the address into something readable
    char hostbuf[128]; char servbuf[32];
    if (getnameinfo(
              (struct sockaddr*) &sa, salen
            , hostbuf, sizeof(hostbuf)
            , servbuf, sizeof(servbuf)
            , NI_NUMERICHOST | NI_NUMERICSERV) != 0)
      throw std::system_error{errno, std::generic_category(), "rapic: getnameinfo failure"};

    // initialize a connection manager to own the connection
    client cli{buffer_size, keepalive_period};
    cli.accept(std::move(socket), hostbuf, servbuf);
    clients.push_back(std::move(cli));
  }
  return clients;
}

auto server::pollable_fd() const -> int
{
  return socket_;
}

auto server::poll_read() const -> bool
{
  return socket_;
}

auto server::poll_write() const -> bool
{
  return false;
}
