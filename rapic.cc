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
static const std::string msg_mssg30_term{"\nEND STATUS\n"};

static const std::string msg_status_head{"RDRSTAT:"};
static const std::string msg_status_term{"\n"};

static const std::string msg_permcon_head{"RPQUERY: SEMIPERMANENT CONNECTION"};
static const std::string msg_permcon_term{"\n"};

static const std::string msg_query_head{"RPQUERY:"};
static const std::string msg_query_term{"\n"};

static const std::string msg_filter_head{"RPFILTER:"};
static const std::string msg_filter_term{"\n"};

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

// find the next text within a line, or the end of line
static auto find_text(uint8_t const* in, size_t size, size_t& pos) -> size_t
{
  for (size_t i = pos; i < size; ++i)
  {
    if (in[i] == '\0' || in[i] == '\n')
      return i;
    if (!std::isspace(in[i]))
      return i;
  }
  throw std::runtime_error{"unterminated message"};
}

// find the next whitespace within a line, or the end of line
static auto find_white(uint8_t const* in, size_t size, size_t& pos) -> size_t
{
  for (size_t i = pos; i < size; ++i)
  {
    if (in[i] == '\0' || in[i] == '\n')
      return i;
    if (std::isspace(in[i]))
      return i;
  }
  throw std::runtime_error{"unterminated message"};
}

// find the end of the line
static auto find_eol(uint8_t const* in, size_t size, size_t pos) -> size_t
{
  for (size_t i = pos; i < size; ++i)
    if (in[i] == '\0' || in[i] == '\n')
      return i;
  throw std::runtime_error{"unterminated message"};
}

static auto parse_station_id(char const* in) -> int
{
  int ret = strcasecmp(in, "ANY") == 0 ? 0 : atoi(in);
  if (ret == 0 && in[0] != '0')
    throw std::runtime_error{"invalid station id"};
  return ret;
}

static auto parse_scan_type(char const* in) -> std::pair<scan_type, int>
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

auto parse_query_type(char const* in) -> query_type
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

auto parse_data_types(char const* in) -> std::vector<std::string>
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

auto rapic::release_tag() -> char const*
{
  return RAPIC_RELEASE_TAG;
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
{
}

message::~message()
{ }

mssg::mssg()
{
  reset();
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

auto mssg::encode(uint8_t* out, size_t size) const -> size_t
{
  if (number_ == 30)
    return snprintf(reinterpret_cast<char*>(out), size, "MSSG: 30 %s\nEND STATUS\n", text_.c_str());
  else
    return snprintf(reinterpret_cast<char*>(out), size, "MSSG: %d %s\n", number_, text_.c_str());
}

auto mssg::decode(uint8_t const* in, size_t size) -> size_t
try
{
  size_t pos = 0, end = 0;

  // read the message identifier and number
  if (sscanf(reinterpret_cast<char const*>(in), "MSSG: %d%zn", &number_, &pos) != 1 || pos == 0)
    throw std::runtime_error{"failed to parse message header"};

  // remainder of line is the message text
  pos = find_text(in, size, pos);
  end = find_eol(in, size, pos);
  text_.assign(reinterpret_cast<char const*>(in + pos), end - pos);

  // handle multi-line messages (only #30)
  if (number_ == 30)
  {
    while (true)
    {
      pos = end + 1;
      end = find_eol(in, size, pos);
      if (strncmp("END STATUS", reinterpret_cast<char const*>(in + pos), end - pos) == 0)
        break;
      text_.push_back('\n');
      text_.append(reinterpret_cast<char const*>(in + pos), end - pos);
    }
  }

  return end + 1;
}
catch (...)
{
  std::throw_with_nested(decode_error{message_type::mssg, in, size});
}

status::status()
{
  reset();
}

auto status::type() const -> message_type
{
  return message_type::status;
}

auto status::reset() -> void
{
  text_.clear();
}

auto status::encode(uint8_t* out, size_t size) const -> size_t
{
  return snprintf(reinterpret_cast<char*>(out), size, "RDRSTAT: %s\n", text_.c_str());
}

auto status::decode(uint8_t const* in, size_t size) -> size_t
try
{
  size_t pos = 0, end = 0;

  // read the header
  if (sscanf(reinterpret_cast<char const*>(in), "RDRSTAT:%zn", &pos) != 0 || pos == 0)
    throw std::runtime_error{"failed to parse message header"};

  // remainder of line is the message text
  pos = find_text(in, size, pos);
  end = find_eol(in, size, pos);
  text_.assign(reinterpret_cast<char const*>(in + pos), end - pos);

  return end + 1;
}
catch (...)
{
  std::throw_with_nested(decode_error{message_type::status, in, size});
}

permcon::permcon()
{
  reset();
}

auto permcon::type() const -> message_type
{
  return message_type::permcon;
}

auto permcon::reset() -> void
{
  tx_complete_scans_ = false;
}

auto permcon::encode(uint8_t* out, size_t size) const -> size_t
{
  return snprintf(
        reinterpret_cast<char*>(out)
      , size
      , "RPQUERY: SEMIPERMANENT CONNECTION - SEND ALL DATA TXCOMPLETESCANS=%d\n"
      , tx_complete_scans_);
}

auto permcon::decode(uint8_t const* in, size_t size) -> size_t
try
{
  int ival;
  size_t pos = 0, end = 0;

  // read the header
  auto ret = sscanf(
        reinterpret_cast<char const*>(in)
      , "RPQUERY: SEMIPERMANENT CONNECTION - SEND ALL DATA TXCOMPLETESCANS=%d%zn"
      , &ival
      , &pos);
  if (ret != 1)
    throw std::runtime_error{"failed to parse message header"};

  // find the end of the line
  end = find_eol(in, size, pos);

  return end + 1;
}
catch (...)
{
  std::throw_with_nested(decode_error{message_type::status, in, size});
}

query::query()
{
  reset();
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

auto query::encode(uint8_t* out, size_t size) const -> size_t
{
  // TODO
  return 0;
}

auto query::decode(uint8_t const* in, size_t size) -> size_t
try
{
  long time;
  char str_stn[21], str_stype[21], str_qtype[21], str_dtype[128];
  size_t pos = 0, end = 0;

  // read the header
  auto ret = sscanf(
        reinterpret_cast<char const*>(in)
      , "RPQUERY: %20s %20s %f %d %20s %ld %127s %d%zn"
      , str_stn
      , str_stype
      , &angle_
      , &repeat_count_
      , str_qtype
      , &time
      , str_dtype
      , &video_res_
      , &pos);
  if (ret != 7)
    throw std::runtime_error{"corrupt message detected"};

  // check/parse the individual tokens
  station_id_ = parse_station_id(str_stn);
  std::tie(scan_type_, volume_id_) = parse_scan_type(str_stype);
  query_type_ = parse_query_type(str_qtype);
  time_ = time;
  data_types_ = parse_data_types(str_dtype);

  // find the end of the line
  end = find_eol(in, size, pos);

  return end + 1;
}
catch (...)
{
  std::throw_with_nested(decode_error{message_type::status, in, size});
}

filter::filter()
{
  reset();
}

auto filter::type() const -> message_type
{
  return message_type::filter;
}

auto filter::reset() -> void
{
  station_id_ = -1;
  scan_type_ = rapic::scan_type::any;
  volume_id_ = -1;
  video_res_ = -1;
  source_.clear();
  data_types_.clear();
}

auto filter::encode(uint8_t* out, size_t size) const -> size_t
{
  // TODO
  return 0;
}

auto filter::decode(uint8_t const* in, size_t size) -> size_t
try
{
  char str_stn[21], str_stype[21], str_src[21], str_dtype[128];
  size_t pos = 0, end = 0;

  str_src[0] = '\0';

  // read the header
  auto ret = sscanf(
        reinterpret_cast<char const*>(in)
      , "RPFILTER:%20[^:]:%20[^:]:%d:%20[^:]:%127s%zn"
      , str_stn
      , str_stype
      , &video_res_
      , str_src
      , str_dtype
      , &pos);
  if (ret != 5)
    throw std::runtime_error{"corrupt message detected"};

  // check/parse the individual tokens
  station_id_ = parse_station_id(str_stn);
  std::tie(scan_type_, volume_id_) = parse_scan_type(str_stype);
  source_.assign(str_src);
  data_types_ = parse_data_types(str_dtype);

  // find the end of the line
  end = find_eol(in, size, pos);

  return end + 1;
}
catch (...)
{
  std::throw_with_nested(decode_error{message_type::status, in, size});
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

auto scan::encode(uint8_t* out, size_t size) const -> size_t
{
  // TODO
  return 0;
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

client::buffer::buffer(size_t capacity)
  : capacity_{capacity}
  , data_{new uint8_t[capacity_]}
  , wcount_{0}
  , rcount_{0}
{ }

client::buffer::buffer(buffer&& rhs) noexcept
  : capacity_{rhs.capacity_}
  , data_{std::move(rhs.data_)}
  , wcount_{static_cast<unsigned int>(rhs.wcount_)}
  , rcount_{static_cast<unsigned int>(rhs.rcount_)}
{ }

inline auto client::buffer::clear() -> void
{
  wcount_ = 0;
  rcount_ = 0;
}

inline auto client::buffer::full() const -> bool
{
  return wcount_ - rcount_ == capacity_;
}

inline auto client::buffer::write_acquire() -> std::pair<uint8_t*, size_t>
{
  unsigned int rc = rcount_;
  unsigned int wc = wcount_;

  // is the buffer full?
  if (wc - rc >= capacity_)
    return { nullptr, 0 };

  // determine current read and write positions
  auto rpos = rc % capacity_;
  auto wpos = wc % capacity_;

  // see how much _contiguous_ space is left in our buffer (may be less than total available write space)
  // this does not distinguish between full and empty, hence the explicit check above
  return { &data_[wpos], wpos < rpos ? rpos - wpos : capacity_ - wpos };
}

inline auto client::buffer::write_commit(size_t size) -> void
{
  wcount_ += size;
}

inline auto client::buffer::read_acquire(size_t offset) -> std::pair<uint8_t const*, size_t>
{
  unsigned int rc = rcount_ + offset;
  unsigned int wc = wcount_;

  // is the buffer empty?
  if (rc >= wc)
    return { nullptr, 0 };

  // determine current read and write positions
  auto rpos = rc % capacity_;
  auto wpos = wc % capacity_;

  // see how much _contiguous_ space is left in our buffer (may be less than total available read
  // this does not distinguish between full and empty, hence the explicit check above
  return { &data_[rpos], rpos < wpos ? wpos - rpos : capacity_ - rpos };
}

inline auto client::buffer::read_commit(size_t size) -> void
{
  rcount_ += size;
}

auto client::buffer::read_ignore_whitespace() -> void
{
  while (true)
  {
    if (wcount_ == rcount_)
      return;
    if (data_[rcount_ % capacity_] > 0x20)
      break;
    ++rcount_;
  }
}

auto client::buffer::read_starts_with(std::string const& str) const -> bool
{
  // cache rcount_ to reduce performance drop of atomic reads
  size_t rc = rcount_;
  size_t size = wcount_ - rc;

  // is there even enough data in the buffer?
  if (size < str.size())
    return false;

  // check each character for a match
  for (size_t i = 0; i < str.size(); ++i)
    if (str[i] != data_[(rc + i) % capacity_])
      return false;

  return true;
}

auto client::buffer::read_find(std::string const& str, size_t& offset) const -> bool
{
  // cache rcount_ to reduce performance drop of atomic reads
  size_t rc = rcount_;
  size_t size = wcount_ - rc;

  // is there even enough data in the buffer?
  if (size < str.size())
    return false;

  // naive search through the buffer
  for (size_t i = 0; i < size - (str.size() - 1); ++i)
  {
    for (size_t j = 0; j < str.size(); ++j)
      if (str[j] != data_[(rc + i + j) % capacity_])
        goto next_i;
    offset = i;
    return true;
next_i:
    ;
  }

  return false;
}

auto client::buffer::read_find_eol(size_t& offset) const -> bool
{
  // cache rcount_ to reduce performance drop of atomic reads
  size_t rc = rcount_;
  size_t size = wcount_ - rc;

  for (size_t i = 0; i < size; ++i)
  {
    if (   data_[(rc + i) % capacity_] == '\n'
        || data_[(rc + i) % capacity_] == '\r')
    {
      offset = i;
      return true;
    }
  }
  return false;
}

client::client(size_t buffer_size, time_t keepalive_period)
  : keepalive_period_{keepalive_period}
  , establish_wait_{false}
  , last_keepalive_{0}
  , rbuf_{buffer_size}
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
    auto space = rbuf_.write_acquire();

    // if our buffer is full return and allow the client to do some reading
    if (space.second == 0)
      return true;

    // read some data off the wire
    auto bytes = recv(socket_, space.first, space.second, 0);
    if (bytes > 0)
    {
      // commit the read bytes to the buffer
      rbuf_.write_commit(bytes);

      // if we read as much as we asked for there may be more still waiting so return true
      return static_cast<size_t>(bytes) == space.second;
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
    rbuf_.read_commit(cur_size_);
    cur_type_ = no_message;
    cur_size_ = 0;
  }

  // ignore leading whitespace (and return if no data at all)
  rbuf_.read_ignore_whitespace();

  // check fullness for overflow check at the end of this function (must be before dequeuing anything)
  auto full = rbuf_.full();

  // is it an MSSG style message?
  if (rbuf_.read_starts_with(msg_mssg_head))
  {
    // status 30 is multi-line terminated by "END STATUS"
    if (rbuf_.read_starts_with(msg_mssg30_head))
    {
      if (rbuf_.read_find(msg_mssg30_term, cur_size_))
      {
        cur_type_ = type = message_type::mssg;
        cur_size_ += msg_mssg30_term.size();
        return true;
      }
    }
    // otherwise assume it is a single line message and look for an end of line
    else
    {
      if (rbuf_.read_find_eol(cur_size_))
      {
        cur_type_ = type = message_type::mssg;
        cur_size_ += msg_mssg_term.size();
        return true;
      }
    }
  }
  // is it an RDRSTAT message?
  else if (rbuf_.read_starts_with(msg_status_head))
  {
    if (rbuf_.read_find_eol(cur_size_))
    {
      cur_type_ = type = message_type::status;
      cur_size_ += msg_permcon_term.size();
      return true;
    }
  }
  // is it a SEMIPERMANENT CONNECTION message? (must check this before RPQUERY due to header similarity)
  else if (rbuf_.read_starts_with(msg_permcon_head))
  {
    if (rbuf_.read_find_eol(cur_size_))
    {
      cur_type_ = type = message_type::permcon;
      cur_size_ += msg_permcon_term.size();
      return true;
    }
  }
  // is it a RPQUERY style message?
  else if (rbuf_.read_starts_with(msg_query_head))
  {
    if (rbuf_.read_find_eol(cur_size_))
    {
      cur_type_ = type = message_type::query;
      cur_size_ += msg_query_term.size();
      return true;
    }
  }
  // is it an RPFILTER styles message?
  else if (rbuf_.read_starts_with(msg_filter_head))
  {
    if (rbuf_.read_find_eol(cur_size_))
    {
      cur_type_ = type = message_type::filter;
      cur_size_ += msg_filter_term.size();
      return true;
    }
  }
  // otherwise assume it is a scan message and look for "END RADAR IMAGE"
  else
  {
    if (rbuf_.read_find(msg_scan_term, cur_size_))
    {
      cur_type_ = type = message_type::scan;
      cur_size_ += msg_scan_term.size();
      return true;
    }
  }

  // if the buffer was full when entering but we could not read a message then we are in overflow, fail hard
  if (full)
    throw std::runtime_error{"rapic: buffer overflow (try increasing buffer size)"};

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

  try
  {
    // if the message spans the buffer wrap around point, copy it into a temporary location to ease parsing
    auto space = rbuf_.read_acquire();
    if (space.second < cur_size_)
    {
      std::unique_ptr<uint8_t[]> buf{new uint8_t[cur_size_]};
      std::memcpy(buf.get(), space.first, space.second);
      std::memcpy(buf.get() + space.second, rbuf_.read_acquire(space.second).first, cur_size_ - space.second);
      msg.decode(buf.get(), cur_size_);
    }
    else
      msg.decode(space.first, cur_size_);
  }
  catch (...)
  {
    rbuf_.read_commit(cur_size_);
    cur_type_ = no_message;
    cur_size_ = 0;
  }

  rbuf_.read_commit(cur_size_);
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
    throw std::runtime_error{"unknown or invalid service or port '" + service + "'"};

  // create the listen socket
  socket_.reset(::socket(ipv6 ? AF_INET6 : AF_INET, SOCK_STREAM, 0));
  if (socket_ < 0)
    throw std::system_error{errno, std::generic_category(), "socket creation failed"};

  // allow immediate reuse of server socket after failure
  int on = 1;
  if (setsockopt(socket_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0)
    throw std::system_error{errno, std::generic_category(), "socket reuse mode set failed"};

  if (ipv6)
  {
    // allow connections from ipv4 clients when using an ipv6 socket
    on = 0;
    if (setsockopt(socket_, IPPROTO_IPV6, IPV6_V6ONLY, &on, sizeof(on)) < 0)
      throw std::system_error{errno, std::generic_category(), "socket failed to disable ipv6 only"};

    // build a local address
    struct sockaddr_in6 addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons(port);
    addr.sin6_addr = in6addr_any;

    // bind the address
    if (bind(socket_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0)
      throw std::system_error{errno, std::generic_category(), "socket bind failed"};
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
      throw std::system_error{errno, std::generic_category(), "socket bind failed"};
  }

  // mark as a passive socket
  if (::listen(socket_, SOMAXCONN) < 0)
    throw std::system_error{errno, std::generic_category(), "socket listen failed"};

  // set as a non-blocking socket
  int flags = fcntl(socket_, F_GETFL);
  if (flags == -1)
    throw std::system_error{errno, std::generic_category(), "failed to read socket flags"};
  if (fcntl(socket_, F_SETFL, flags | O_NONBLOCK) == -1)
    throw std::system_error{errno, std::generic_category(), "failed to set socket flags"};
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
      throw std::system_error{errno, std::generic_category(), "failed to accept socket"};
    }

    // convert the address into something readable
    char hostbuf[128]; char servbuf[32];
    if (getnameinfo(
              (struct sockaddr*) &sa, salen
            , hostbuf, sizeof(hostbuf)
            , servbuf, sizeof(servbuf)
            , NI_NUMERICHOST | NI_NUMERICSERV) != 0)
      throw std::system_error{errno, std::generic_category(), "getnameinfo failure"};

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
