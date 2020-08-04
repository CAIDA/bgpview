/*
 * Copyright (C) 2014 The Regents of the University of California.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __BGPVIEW_IO_ZMQ_INT_H
#define __BGPVIEW_IO_ZMQ_INT_H

#include "bgpview_io_zmq.h"

/**
 * @name Private Constants
 *
 * @{ */

#ifdef DEBUG_TIMING

#define TIMER_START(timer)                                                     \
  struct timeval timer##_start;                                                \
  do {                                                                         \
    gettimeofday_wrap(&timer##_start);                                         \
  } while (0)

#define TIMER_END(timer)                                                       \
  struct timeval timer##_end, timer##_diff;                                    \
  do {                                                                         \
    gettimeofday_wrap(&timer##_end);                                           \
    timeval_subtract(&timer##_diff, &timer##_end, &timer##_start);             \
  } while (0)

#define TIMER_VAL(timer)                                                       \
  ((timer##_diff.tv_sec * 1000000) + timer##_diff.tv_usec)
#else

#define TIMER_START(timer)
#define TIMER_END(timer)
#define TIMER_VAL(timer) (uint64_t)(0)

#endif

#define BW_PFX_ROW_BUFFER_LEN 17 + (BGPVIEW_PEER_MAX_CNT * 5)

/* shared constants are in bgpview_io_zmq.h */

/** @} */

/**
 * @name Private Enums
 *
 * @{ */

/** Enumeration of message types
 *
 * @note these will be cast to a uint8_t, so be sure that there are fewer than
 * 2^8 values
 */
typedef enum {
  /** Invalid message */
  BGPVIEW_IO_ZMQ_MSG_TYPE_UNKNOWN = 0,

  /** Client is ready to send requests/Server is ready for requests */
  BGPVIEW_IO_ZMQ_MSG_TYPE_READY = 1,

  /** Client is explicitly disconnecting (clean shutdown) */
  BGPVIEW_IO_ZMQ_MSG_TYPE_TERM = 2,

  /** Server/Client is still alive */
  BGPVIEW_IO_ZMQ_MSG_TYPE_HEARTBEAT = 3,

  /** A view for the server to process */
  BGPVIEW_IO_ZMQ_MSG_TYPE_VIEW = 4,

  /** Server is sending a response to a client */
  BGPVIEW_IO_ZMQ_MSG_TYPE_REPLY = 5,

  /** Highest message number in use */
  BGPVIEW_IO_ZMQ_MSG_TYPE_MAX = BGPVIEW_IO_ZMQ_MSG_TYPE_REPLY,

} bgpview_io_zmq_msg_type_t;

#define bgpview_io_zmq_msg_type_size_t sizeof(uint8_t)

/** @} */

/* ========== MESSAGE TYPES ========== */

/** Receives one message from the given socket and decodes as a message type
 *
 * @param src          socket to receive on
 * @param flags        flags passed directed to zmq_recv (e.g. ZMQ_DONTWAIT)
 * @return the type of the message, or BGPVIEW_MSG_TYPE_UNKNOWN
 */
bgpview_io_zmq_msg_type_t bgpview_io_zmq_recv_type(void *src, int flags);

/* ========== VIEW IO ========== */

/** Send the given view to the given socket
 *
 * @param dest          socket to send the view to
 * @param view          pointer to the view to send
 * @param cb            callback function to use to filter entries (may be NULL)
 * @return 0 if the view was sent successfully, -1 otherwise
 */
int bgpview_io_zmq_send(void *dest, bgpview_t *view, bgpview_io_filter_cb_t *cb,
                        void *cb_user);

/** Receive a view from the given socket
 *
 * @param src           socket to receive on
 * @param view          pointer to the clear/new view to receive into
 * @param cb            callback function to use to filter entries (may be NULL)
 * @return pointer to the view instance received, NULL if an error occurred.
 */
int bgpview_io_zmq_recv(void *src, bgpview_t *view,
                        bgpview_io_filter_peer_cb_t *peer_cb,
                        bgpview_io_filter_pfx_cb_t *pfx_cb,
                        bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

#endif /* __BGPVIEW_IO_ZMQ_H */
