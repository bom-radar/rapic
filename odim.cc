/*------------------------------------------------------------------------------
 * Rapic Protocol Support Library
 *
 * Copyright (C) 2015 Commonwealth of Australia, Bureau of Meteorology
 * See COPYING for licensing and warranty details
 *
 * Author: Mark Curtis (m.curtis@bom.gov.au)
 *----------------------------------------------------------------------------*/
#include "rapic.h"
#include <odim_h5.h>
#include <cmath>
#include <cstring>
#include <map>
#include <sstream>

using namespace rapic;

static auto rapic_timestamp_to_time_t(char const* str) -> time_t
{
  struct tm t;
  if (sscanf(str, "%04d%02d%02d%02d%02d%02d", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec) != 6)
    throw std::runtime_error{"invalid rapic timestamp"};
  t.tm_year -= 1900;
  t.tm_mon -= 1;
  return timegm(&t);
}

namespace
{
  struct quantity
  {
    quantity(char const* hname, char const* vname, float gain = std::numeric_limits<float>::quiet_NaN(), float offset = std::numeric_limits<float>::quiet_NaN())
      : hname{hname}, vname{vname}, odim_gain{gain}, odim_offset{offset}
    { }

    char const* hname;        // odim name for unknown or horizontal polarized
    char const* vname;        // odim name for vertical polarized
    float       odim_gain;    // gain to use when encoding to 8-bit for odim output
    float       odim_offset;  // offset to use when encoding to 8-bit for odim output
  };

  struct meta_extra
  {
    meta_extra(scan const& s, odim_h5::polar_volume& v, odim_h5::scan& t, odim_h5::data& d)
      : s(s), v(v), t(t), d(d)
    { }

    scan const& s;
    odim_h5::polar_volume& v;
    odim_h5::scan& t;
    odim_h5::data& d;

    bool        vpol = false;
    std::string video;
    std::string vidgain;
    std::string vidoffset;
    std::vector<double>  thresholds;
    double      maxvel = std::numeric_limits<double>::quiet_NaN();
    long        vidres = 0;
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
  , { "LATITUDE",     METAFN { }} // ignored - special processing
  , { "LONGITUDE",    METAFN { }} // ignored - special processing
  , { "HEIGHT",       METAFN { }} // ignored - special processing
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
        if (isnan(m.maxvel))
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
static auto angle_to_index(scan const& s, float angle) -> int
{
  while (angle >= s.angle_max())
    angle -= 360.0f;
  while (angle < s.angle_min())
    angle += 360.0f;
  int ray = std::lround((angle - s.angle_min()) / s.angle_resolution());
  if (ray < 0 || ray >= s.rays() || std::abs(remainder(angle - s.angle_min(), s.angle_resolution())) > 0.001)
    throw std::runtime_error{"invalid azimuth angle specified by ray"};
  return ray;
}

auto rapic::write_odim_h5_volume(
      std::string const& path
    , std::list<scan> const& scan_set
    , std::function<void(char const*)> log_fn
    ) -> time_t
{
  time_t vol_time = 0;
  std::vector<uint8_t> ibuf;
  std::vector<int> level_convert;

  // sanity check
  if (scan_set.empty())
    throw std::runtime_error{"empty scan set"};

  // initialize the volume file
  auto hvol = odim_h5::polar_volume{path, odim_h5::file::io_mode::create};

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
        log_fn("unknown country code, using 000 and XX as placeholders");
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
  if (auto p = scan_set.front().find_header("LATITUDE"))
  {
    hvol.set_latitude(p->get_real() * -1.0);
  }
  else
  {
    log_fn("missing LATITUDE header, using -999.0 as placeholder");
    hvol.set_latitude(-999.0);
  }
  if (auto p = scan_set.front().find_header("LONGITUDE"))
  {
    hvol.set_longitude(p->get_real());
  }
  else
  {
    log_fn("missing LONGITUDE header, using -999.0 as placeholder");
    hvol.set_longitude(-999.0);
  }
  if (auto p = scan_set.front().find_header("HEIGHT"))
  {
    hvol.set_height(p->get_real());
  }
  else
  {
    log_fn("missing HEIGHT header, using -999.0 as placeholder");
    hvol.set_height(-999.0);
  }

  // add each scan to the volume
  auto end_tilt = scan_set.begin();
  int bins = 0;
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
      bins = s->bins();
      while (end_tilt != scan_set.end())
      {
        bins = std::max(bins, end_tilt->bins());
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

      // resize our temporary buffer
      ibuf.resize(s->rays() * bins);
    }

    // determine the appropriate data type and size
    size_t dims[2] = { static_cast<size_t>(s->rays()), static_cast<size_t>(bins) };
    auto hdata = hscan.data_append(odim_h5::data::data_type::u8, 2, dims);

    // process each header
    meta_extra m{*s, hvol, hscan, hdata};
    for (auto& h : s->headers())
    {
      auto i = header_map.find(h.name());
      if (i == header_map.end())
      {
        log_fn(("unknown rapic header encountered: " + h.name() + " = " + h.value()).c_str());
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
      hscan.set_ray_count(s->rays());
      hscan.set_ray_start(-0.5);
      hscan.set_first_ray_radiated(s->ray_headers().empty() ? 0 : angle_to_index(*s, s->ray_headers().front().azimuth()));

      // automatically determine scan end time
      if (!s->ray_headers().empty() && s->ray_headers().back().time_offset() != -1)
      {
        // use time of last ray if available
        hscan.set_end_date_time(hscan.start_date_time() + s->ray_headers().back().time_offset());
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
        log_fn(("missing VIDEO header, assuming reflectivity (VERS: " + s->find_header("VERS")->value() + ")").c_str());
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
    std::fill(ibuf.begin(), ibuf.end(), 0);
    for (size_t r = 0; r < s->ray_headers().size(); ++r)
    {
      std::memcpy(
            &ibuf[angle_to_index(*s, s->ray_headers()[r].azimuth()) * s->bins()]
          , &s->level_data()[r * s->bins()]
          , s->bins() * sizeof(uint8_t));
    }

    // determine the conversion (if any) to real moment values
    // thresholded data?
    if (!m.thresholds.empty())
    {
      // check that we know how to repack this moment
      if (vm == video_map.end() || isnan(vm->second.odim_gain))
        throw std::runtime_error{std::string("thresholded encoding used for unexpected video type: ") + m.video};

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
          std::ostringstream oss;
          oss
            << "threshold value '" << m.thresholds[i] << "' cannot be represented exactly by 8bit encoding with gain "
            << vm->second.odim_gain << " offset " << vm->second.odim_offset
            << " will be encoded as " << level_convert[i + 1] << " -> " << (level_convert[i + 1] * vm->second.odim_gain + vm->second.odim_offset);
          log_fn(oss.str().c_str());
        }
      }

      // convert between the rapic and odim levels
      for (size_t i = 0; i < ibuf.size(); ++i)
      {
        int lvl = ibuf[i];
        if (lvl >= static_cast<int>(level_convert.size()))
          throw std::runtime_error{"level exceeding threshold table size encountered"};
        ibuf[i] = level_convert[lvl];
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
      auto gain = std::stod(m.vidgain);
      hdata.set_gain(gain);
      hdata.set_offset(std::stod(m.vidoffset) + 0.5 * gain);
      hdata.write(ibuf.data());
    }
    // velocity moment with nyquist or VELLVL supplied?
    else if (m.video == "Vel")
    {
      if (isnan(m.maxvel))
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
      log_fn(("unable to determine encoding for VIDEO '" + m.video + "', writing levels directly").c_str());
      hdata.set_gain(1.0);
      hdata.set_offset(0.0);
      hdata.write(ibuf.data());
    }
  }

  return vol_time;
}
