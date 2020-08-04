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
