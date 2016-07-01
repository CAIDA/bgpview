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

#ifndef __BGPVIEW_IO_ZMQ_H
#define __BGPVIEW_IO_ZMQ_H

#include <stdint.h>

/** @file
 *
 * @brief Header file that exposes both bgpview_io_zmq_client and
 * bgpview_io_zmq_server.
 *
 * @author Alistair King
 *
 */

/**
 * @name Public Constants
 *
 * @{ */

/** Default URI for the server to listen for client requests on */
#define BGPVIEW_IO_ZMQ_CLIENT_URI_DEFAULT "tcp://*:6300"

/** Default URI for the server to publish tables on (subscribed to by consumer
    clients) */
#define BGPVIEW_IO_ZMQ_CLIENT_PUB_URI_DEFAULT "tcp://*:6301"

/** Default the server/client heartbeat interval to 2000 msec */
#define BGPVIEW_IO_ZMQ_HEARTBEAT_INTERVAL_DEFAULT 2000

/** Default the server/client heartbeat liveness to 450 beats (15min) */
#define BGPVIEW_IO_ZMQ_HEARTBEAT_LIVENESS_DEFAULT 450

/** Default the client reconnect minimum interval to 1 second */
#define BGPVIEW_IO_ZMQ_RECONNECT_INTERVAL_MIN 1000

/** Default the client reconnect maximum interval to 32 seconds */
#define BGPVIEW_IO_ZMQ_RECONNECT_INTERVAL_MAX 32000

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

/** Type of a sequence number */
typedef uint32_t seq_num_t;

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** Producer Intents
 *
 * A producer has intents: it intends to send messages about something. E.g. a
 * new prefix table.
 */
typedef enum {

  /** Prefix Table */
  BGPVIEW_PRODUCER_INTENT_PREFIX = 0x01,

} bgpview_producer_intent_t;

#include "bgpview_io_zmq_client.h"
#include "bgpview_io_zmq_server.h"

#endif /* __BGPVIEW_IO_ZMQ_H */
