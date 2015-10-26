/*------------------------------------------------------------------------------
 * Rapic Data Server client connection API for C++11
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rapic_ds.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <ctime>
#include <stdexcept>
#include <sstream>
#include <system_error>
#include <tuple>

// TEMP
#include <iostream>

using namespace rapic;

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
};
struct lookup_value
{
  enc_type type;
  int val;
  int val2;
};
constexpr lookup_value lnul() { return { enc_type::error, 0, 0 }; }
constexpr lookup_value lval(int x) { return { enc_type::value, x, 0 }; }
constexpr lookup_value lrel(int x) { return { enc_type::digit, x, 0 }; }
constexpr lookup_value ldel(int x, int y) { return { enc_type::delta, x, y }; }
constexpr lookup_value lookup[] = 
{
  lnul(),     lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      // 00-07
  lnul(),     lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      lnul(),      // 08-0f
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

static void getline(std::istream& fin, std::string& buf)
{
  // the delimiters in .rapic files may be any of \n, \r and \0, so we can't use std::getline
  buf.clear();
  while (true)
  {
    char c = fin.get();
    if (!fin.good() || c == '\0' || c == '\n' || c == '\r')
      break;
    buf.push_back(c);
  }
}

static auto tokenize(std::string const& str) -> std::vector<double>
{
  std::vector<double> ret;
  size_t pos = 0;
  while ((pos = str.find_first_not_of(" ", pos)) != std::string::npos)
  {
    size_t end = str.find_first_of(" ", pos + 1);
    size_t len = (end == std::string::npos ? str.size() : end) - pos;
    ret.push_back(std::strtod(str.c_str() + pos, nullptr));
    pos += len;
  }
  return std::move(ret);
}

auto rapic::release_tag() -> char const*
{
  return RAPIC_DS_RELEASE_TAG;
}

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
        if (   std::isnan(angres)
            || std::isnan(rngres) || std::isnan(startrng) || std::isnan(endrng)
            || product == product_type::unknown
            || product == product_type::scan_error
            || imgfmt == image_format::unknown)
          throw std::runtime_error("missing or invalid scan headers");

        if (std::isnan(angle1) || std::isnan(angle2))
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

        data_rows = rows;
        data_cols = cols;
        data.assign(data_rows * data_cols, 0);
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
        if (ray >= data_rows || std::abs(remainder(ang - azi_min, angres)) > 0.001)
          throw std::runtime_error("invalid azimuth angle in ray encoding");
        auto out = &data[ray * data_cols];

        if (first_ray == -1)
          first_ray = ray;

        // decode the data into levels
        // NOTE: corrupt encodings can cause buffer overruns here, in the future check bin < data_cols at each
        //       write of a bin
        int prev = 0;
        size_t bin = 0;
        for (auto i = buf1.begin() + 3; i != buf1.end(); ++i)
        {
          auto& cur = lookup[(unsigned char)*i];

          //  absolute pixel value
          if (cur.type == enc_type::value)
            out[bin++] = prev = cur.val;
          // run length encoding of the previous value
          else if (cur.type == enc_type::digit)
          {
            auto count = cur.val;
            while (i + 1 != buf1.end() && lookup[(unsigned char)*(i + 1)].type == enc_type::digit)
            {
              count *= 10;
              count += lookup[(unsigned char)*++i].val;
            }
            for (int j = 0; j < count; ++j)
              out[bin++] = prev;
          }
          // delta encoding
          else if (cur.type == enc_type::delta)
          {
            out[bin++] = prev += cur.val;
            out[bin++] = prev += cur.val2;
          }
          else
            throw std::runtime_error("invalid character encountered in ray encoding");
        }

        // sanity check (too late - overrun has already occured at this point)
        if (bin > data_cols)
          throw std::runtime_error("BUFFER OVERRUN");
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
        if (ray >= data_rows || std::abs(remainder(ang - azi_min, angres)) > 0.001)
          throw std::runtime_error("invalid azimuth angle in ray encoding");
        auto out = &data[ray * data_cols];

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
            for (size_t i = 0; i < count; ++i)
              out[bin++] = val;
          }
          else
            out[bin++] = val;
        }

        // sanity check (too late - overrun has already occured at this point)
        if (bin > data_cols)
          throw std::runtime_error("BUFFER OVERRUN");
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
        throw std::runtime_error("invalid header");
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
#if 0
  trace::debug() << "header: " << key << " value: " << value;
#endif
  if (key == "ANGLERATE")
    anglerate = std::stod(value);
  else if (key == "ANGRES")
    angres = std::stod(value);
  else if (key == "ANTDIAM")
    antdiam = std::stod(value);
  else if (key == "AZCORR")
    azcorr = std::stod(value);
  else if (key == "AZIM")
    azim = std::stod(value);
  else if (key == "COMPPPIID")
    compppiid = std::stoi(value);
  else if (key == "COPYRIGHT")
    copyright = value;
  else if (key == "COUNTRY")
    country = std::stoi(value);
  else if (key == "DATE")
    date = std::stoi(value);
  else if (key == "DBM2DBZ")
    dbm2dbz = std::stod(value);
  else if (key == "DBMLVL")
    dbmlvl = tokenize(value);
  else if (key == "DBZLVL")
    dbzlvl = tokenize(value);
  else if (key == "ELCORR")
    elcorr = std::stod(value);
  else if (key == "ELEV")
    elev = std::stod(value);
  else if (key == "ENDRNG")
    endrng = std::stod(value);
  else if (key == "FREQUENCY")
    frequency = std::stod(value);
  else if (key == "FAULT")
    fault = value;
  else if (key == "HBEAMWIDTH")
    hbeamwidth = std::stod(value);
  else if (key == "HEIGHT")
    height = std::stod(value);
  else if (key == "HIPRF")
  {
    if (value == "EVENS")
      hiprf = dual_prf_strategy::high_evens;
    else if (value == "ODDS")
      hiprf = dual_prf_strategy::high_odds;
    else if (value == "UNKNOWN")
      hiprf = dual_prf_strategy::unknown;
#if 0
    else
      trace::error() << "unknown HIPRF value (" << value << ") ignored";
#endif
  }
  else if (key == "IMGFMT")
  {
    if (value == "RHI")
      imgfmt = image_format::rhi;
    else if (value == "CompPPI")
      imgfmt = image_format::compppi;
    else if (value == "PPI")
      imgfmt = image_format::ppi;
#if 0
    else
      trace::error() << "unknown IMGFMT value (" << value << ") ignored";
#endif
  }
  else if (key == "LATITUDE")
    latitude = std::stod(value);
  else if (key == "LONGITUDE")
    longitude = std::stod(value);
  else if (key == "NAME")
    name = value;
  else if (key == "NYQUIST")
    nyquist = std::stod(value);
  else if (key == "PASS")
  {
    if (sscanf(value.c_str(), "%ld of %ld", &pass, &pass_count) != 2)
      throw std::runtime_error{"invalid PASS header"};
  }
  else if (key == "PEAKPOWER")
    peakpower = std::stod(value);
  else if (key == "PEAKPOWERH")
    peakpowerh = std::stod(value);
  else if (key == "PEAKPOWERV")
    peakpowerv = std::stod(value);
  else if (key == "POLARISATION")
  {
    if (value == "H")
      polarisation = polarisation_type::horizontal;
    else if (value == "V")
      polarisation = polarisation_type::vertical;
    else if (value == "ALT_HV")
      polarisation = polarisation_type::alternating;
#if 0
    else
      trace::error() << "unknown POLARISATION value (" << value << ") ignored";
#endif
  }
  else if (key == "PRF")
    prf = std::stod(value);
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
#if 0
    else
      trace::error() << "unknown PRODUCT value (" << value << ") ignored";
#endif
  }
  else if (key == "PULSELENGTH")
    pulselength = std::stod(value);
  else if (key == "RADARTYPE")
    radartype = value;
  else if (key == "RNGRES")
    rngres = std::stod(value);
  else if (key == "RXGAIN_H")
    rxgain_h = std::stod(value);
  else if (key == "RXGAIN_V")
    rxgain_v = std::stod(value);
  else if (key == "RXNOISE_H")
    rxnoise_h = std::stod(value);
  else if (key == "RXNOISE_V")
    rxnoise_v = std::stod(value);
  else if (key == "STARTRNG")
    startrng = std::stod(value);
  else if (key == "STN_NUM")
    stn_num = std::stoi(value);
  else if (key == "STNID")
    stnid = std::stoi(value);
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
    struct tm tmm;
    if (sscanf(
              value.c_str()
            , "%04d%02d%02d%02d%02d%02d"
            , &tmm.tm_year
            , &tmm.tm_mon
            , &tmm.tm_mon
            , &tmm.tm_hour
            , &tmm.tm_min
            , &tmm.tm_sec
            ) != 6)
      throw std::runtime_error{"invalid TIMESTAMP header"};
    tmm.tm_year -= 1900;
    tmm.tm_mon -= 1;
    timestamp = timegm(&tmm);
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
#if 0
    else
      trace::error() << "unknown UNFOLDING value (" << value << ") ignored";
#endif
  }
  else if (key == "VBEAMWIDTH")
    vbeamwidth = std::stod(value);
  else if (key == "VERS")
    vers = value;
  else if (key == "VIDEO")
    video = value;
  else if (key == "VIDEOGAIN")
  {
    if (value == "THRESH")
      videogain = -999.0;
    else
      videogain = std::stod(value);
  }
  else if (key == "VIDEOOFFSET")
  {
    if (value == "THRESH")
      videooffset = -999.0;
    else
      videooffset = std::stod(value);
  }
  else if (key == "VIDEOUNITS")
    videounits = value;
  else if (key == "VIDRES")
  {
    auto vr = std::stol(value);
    if (   vr != 6
        && vr != 16
        && vr != 32
        && vr != 32
        && vr != 64
        && vr != 160
        && vr != 256)
    {
#if 0
      trace::error() << "unknown VIDRES value (" << value << ") ignored";
#endif
    }
    if (vr == 6)
      throw std::runtime_error("six level ASCII encoding not supported");

    vidres = static_cast<video_format>(vr);
  }
  else if (key == "VOLUMEID")
    volumeid = std::stoi(value);
#if 0
  else
    trace::warning() << "unknown header encountered: " << key << " = " << value;
#endif
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

auto client::add_filter(
      int station
    , product_type type
    , int scan_id
    , std::vector<std::string> const& moments
    ) -> void
{
  if (socket_ != -1)
    throw std::runtime_error{"rapic_ds: add_filter called while connected"};

  std::ostringstream oss;

  oss << "RPFILTER";

  // station number (-1 == all)
  oss << ":" << station;

  // product type (COMPPPI and VOLUME support additional scan_id)
  switch (type)
  {
  case product_type::unknown:
    oss << ":ANY";
    break;
  case product_type::ppi:
    oss << ":PPI";
    break;
  case product_type::rhi:
    oss << ":RHI";
    break;
  case product_type::comp_ppi:
    oss << ":COMPPPI";
    break;
  case product_type::image:
    oss << ":IMAGE";
    break;
  case product_type::volume:
    oss << ":VOLUME";
    break;
  case product_type::rhi_set:
    oss << ":RHI_SET";
    break;
  case product_type::merge:
    oss << ":MERGE";
    break;
  case product_type::scan_error:
    oss << ":SCAN_ERROR";
    break;
  }

  // VOLUMEx and COMPPIx support
  if (scan_id != -1)
    oss << scan_id;

  // video format - always take whatever is on offer
  oss << ":-1";

  // data source - unused
  oss << ":-1";

  // moments to retrieve
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
  auto const data = &buffer_[rcount_ % capacity_];

  // TEMP horrible slow implementation.  need to remove streams
  std::string buf;
  buf.reserve(cur_size_);
  auto pos = rcount_ % capacity_;
  if (pos + cur_size_ > capacity_)
  {
    buf.assign(reinterpret_cast<char const*>(&buffer_[pos]), capacity_ - pos);
    buf.append(reinterpret_cast<char const*>(&buffer_[0]), cur_size_ - (capacity_ - pos));
  }
  else
  {
    buf.assign(reinterpret_cast<char const*>(&buffer_[pos]), cur_size_);
  }
  std::istringstream in(buf);
  msg = scan{in};
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
