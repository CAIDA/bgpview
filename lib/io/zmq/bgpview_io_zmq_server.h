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

#ifndef __BGPVIEW_IO_ZMQ_SERVER_H
#define __BGPVIEW_IO_ZMQ_SERVER_H

#include <czmq.h>
#include <stdint.h>

/** @file
 *
 * @brief Header file that exposes the public interface of the bgpview
 * server.
 *
 * @author Alistair King
 *
 */

/**
 * @name Public Constants
 *
 * @{ */

/* shared constants are in bgpview_io_zmq.h */

/** The default number of views in the window */
#define BGPVIEW_IO_ZMQ_SERVER_WINDOW_LEN 6

/** Maximum length of the metric prefix string */
#define BGPVIEW_IO_ZMQ_SERVER_METRIC_PREFIX_LEN 1024

/** Default value of the metric prefix string */
#define BGPVIEW_IO_ZMQ_SERVER_METRIC_PREFIX_DEFAULT "bgp"

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** @} */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

typedef struct bgpview_io_zmq_server bgpview_io_zmq_server_t;

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

/** Public information about a client given to bgpview when a client connects
 *  or disconnects
 */
typedef struct bgpview_io_zmq_server_client_info {
  /** Client name */
  char *name;

  /** Producer Intents (bgpview_consumer_intent_t flags) */
  uint8_t intents;

} bgpview_io_zmq_server_client_info_t;

/** @} */

/** Initialize a new BGPView Server instance
 *
 * @return a pointer to a bgpview server instance if successful, NULL if an
 * error occurred.
 */
bgpview_io_zmq_server_t *bgpview_io_zmq_server_init();

/** Set bgpview prefix metric
 *
 * @param server        pointer to a bgpview server instance
 * @param metric_prefix string that represents the prefix to prepend to metrics
 */
void bgpview_io_zmq_server_set_metric_prefix(bgpview_io_zmq_server_t *server,
                                             char *metric_prefix);

/** Start the given bgpview server instance
 *
 * @param server       pointer to a bgpview server instance to start
 * @return 0 if the server started successfully, -1 otherwise.
 *
 * This function will block and run until the server is stopped.
 */
int bgpview_io_zmq_server_start(bgpview_io_zmq_server_t *server);

/** Stop the given bgpview server instance at the next safe occasion.
 *
 * This is useful to initiate a clean shutdown if you are handling signals in
 * bgpview. Call this from within your signal handler. It should also be
 * called from bgpview_stop to pass the signal through.
 *
 * @param server       pointer to the bgpview instance to stop
 */
void bgpview_io_zmq_server_stop(bgpview_io_zmq_server_t *server);

/** Free the given bgpview server instance
 *
 * @param server       pointer to the bgpview server instance to free
 */
void bgpview_io_zmq_server_free(bgpview_io_zmq_server_t *server);

/** Set the size of the view window
 *
 * @param               pointer to a bgpview server instance to configure
 * @param               length of the view window (in number of views)
 */
void bgpview_io_zmq_server_set_window_len(bgpview_io_zmq_server_t *server,
                                          int window_len);

/** Set the URI for the server to listen for client connections on
 *
 * @param server        pointer to a bgpview server instance to update
 * @param uri           pointer to a uri string
 * @return 0 if the uri was set successfully, -1 otherwise
 *
 * @note defaults to BGPVIEW_IO_ZMQ_CLIENT_URI_DEFAULT
 */
int bgpview_io_zmq_server_set_client_uri(bgpview_io_zmq_server_t *server,
                                         const char *uri);

/** Set the URI for the server to publish tables on
 *  (subscribed to by consumer clients)
 *
 * @param server        pointer to a bgpview server instance to update
 * @param uri           pointer to a uri string
 * @return 0 if the uri was set successfully, -1 otherwise
 *
 * @note defaults to BGPVIEW_IO_ZMQ_CLIENT_PUB_URI_DEFAULT
 */
int bgpview_io_zmq_server_set_client_pub_uri(bgpview_io_zmq_server_t *server,
                                             const char *uri);

/** Set the heartbeat interval
 *
 * @param server        pointer to a bgpview server instance to update
 * @param interval_ms   time in ms between heartbeats
 *
 * @note defaults to BGPVIEW_IO_ZMQ_HEARTBEAT_INTERVAL_DEFAULT
 */
void bgpview_io_zmq_server_set_heartbeat_interval(
  bgpview_io_zmq_server_t *server, uint64_t interval_ms);

/** Set the heartbeat liveness
 *
 * @param server        pointer to a bgpview server instance to update
 * @param beats         number of heartbeats that can go by before a server is
 *                      declared dead
 *
 * @note defaults to BGPVIEW_IO_ZMQ_HEARTBEAT_LIVENESS_DEFAULT
 */
void bgpview_io_zmq_server_set_heartbeat_liveness(
  bgpview_io_zmq_server_t *server, int beats);

#endif
