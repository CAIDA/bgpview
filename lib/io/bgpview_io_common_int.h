/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef __BGPVIEW_IO_COMMON_INT_H
#define __BGPVIEW_IO_COMMON_INT_H

#include <czmq.h>
#include <stdint.h>
#include <sys/socket.h>

#include "bgpview_io_common.h"
#include "bgpview_consumer_manager.h" /* for interests */
#include "bgpstream_utils_pfx.h"

/** @file
 *
 * @brief Header file that exposes the public structures used by both
 * bgpview_io_client and bgpview_io_server
 *
 * @author Alistair King
 *
 */

/**
 * @name Public Constants
 *
 * @{ */


#ifdef DEBUG_TIMING

#define TIMER_START(timer)			\
  struct timeval timer##_start;			\
  do {						\
  gettimeofday_wrap(&timer##_start);		\
  } while(0)

#define TIMER_END(timer)					\
  struct timeval timer##_end, timer##_diff;				\
  do {								\
    gettimeofday_wrap(&timer##_end);				\
    timeval_subtract(&timer##_diff, &timer##_end, &timer##_start);	\
  } while(0)

#define TIMER_VAL(timer)			\
  ((timer##_diff.tv_sec*1000000) + timer##_diff.tv_usec)
#else

#define TIMER_START(timer)
#define TIMER_END(timer)
#define TIMER_VAL(timer) (uint64_t)(0)

#endif

#define BW_PFX_ROW_BUFFER_LEN 17 + (BGPVIEW_PEER_MAX_CNT*5)

/* shared constants are in bgpview_io_common.h */

/** @} */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */


/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** Enumeration of message types
 *
 * @note these will be cast to a uint8_t, so be sure that there are fewer than
 * 2^8 values
 */
typedef enum {
  /** Invalid message */
  BGPVIEW_MSG_TYPE_UNKNOWN   = 0,

  /** Client is ready to send requests/Server is ready for requests */
  BGPVIEW_MSG_TYPE_READY     = 1,

  /** Client is explicitly disconnecting (clean shutdown) */
  BGPVIEW_MSG_TYPE_TERM      = 2,

  /** Server/Client is still alive */
  BGPVIEW_MSG_TYPE_HEARTBEAT = 3,

  /** A view for the server to process */
  BGPVIEW_MSG_TYPE_VIEW   = 4,

  /** Server is sending a response to a client */
  BGPVIEW_MSG_TYPE_REPLY     = 5,

  /** Highest message number in use */
  BGPVIEW_MSG_TYPE_MAX      = BGPVIEW_MSG_TYPE_REPLY,

} bgpview_msg_type_t;

#define bgpview_msg_type_size_t sizeof(uint8_t)


/** @} */

/* ========== MESSAGE TYPES ========== */

/** Receives one message from the given socket and decodes as a message type
 *
 * @param src          socket to receive on
 * @param flags        flags passed directed to zmq_recv (e.g. ZMQ_DONTWAIT)
 * @return the type of the message, or BGPVIEW_MSG_TYPE_UNKNOWN
 */
bgpview_msg_type_t bgpview_io_recv_type(void *src, int flags);


/* ========== INTERESTS/VIEWS ========== */

/** Given a set of interests that are satisfied by a view, find the most
 *  specific and return the publication string
 *
 * @param interests     set of interests
 * @return most-specific publication string that satisfies the interests
 */
const char *bgpview_consumer_interest_pub(int interests);

/** Given a set of interests, find the least specific return the subscription
 * string
 *
 * @param interests     set of interests
 * @return least-specific subscription string that satisfies the interests
 */
const char *bgpview_consumer_interest_sub(int interests);

/** Receive an interest publication prefix and convert to an interests set
 *
 * @param src           socket to receive on
 * @return set of interest flags if successful, 0 otherwise
 */
uint8_t bgpview_consumer_interest_recv(void *src);

#endif
