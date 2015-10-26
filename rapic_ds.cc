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

auto rapic::release_tag() -> char const*
{
  return RAPIC_DS_RELEASE_TAG;
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
