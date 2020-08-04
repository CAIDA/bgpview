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

#ifndef __BGPVIEW_IO_ZMQ_SERVER_INT_H
#define __BGPVIEW_IO_ZMQ_SERVER_INT_H

#include "bgpview.h"
#include "bgpview_io_zmq_server.h"
#include "bgpview_io_zmq_store.h"
#include "khash.h"
#include <czmq.h>
#include <stdint.h>

/** @file
 *
 * @brief Header file that exposes the (protected) interface of the bgpview
 * server. This interface is only used by bgpview
 *
 * @author Alistair King
 *
 */

/**
 * @name Public Constants
 *
 * @{ */

/* shared constants are in bgpview_io_zmq.h */

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

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

/** Protected information about a client used to handle client connections */
typedef struct bgpview_io_zmq_server_client {

  /** Identity frame data that the client sent us */
  zmq_msg_t identity;

  /** Hex id of the client (may be the same as the printable ID */
  char *hexid;

  /** Printable ID of client (for debugging and logging) */
  char *id;

  /** Time at which the client expires */
  uint64_t expiry;

  /** info about this client that we will send to the client connect handler */
  bgpview_io_zmq_server_client_info_t info;

} bgpview_io_zmq_server_client_t;

KHASH_INIT(strclient, char *, bgpview_io_zmq_server_client_t *, 1,
           kh_str_hash_func, kh_str_hash_equal);

struct bgpview_io_zmq_server {

  /** Metric prefix to output metrics */
  char metric_prefix[BGPVIEW_IO_ZMQ_SERVER_METRIC_PREFIX_LEN];

  /** 0MQ context pointer */
  zctx_t *ctx;

  /** URI to listen for clients on */
  char *client_uri;

  /** URI to pub tables on */
  char *client_pub_uri;

  /** Socket to bind to for client connections */
  void *client_socket;

  /** Socket to pub tables on */
  void *client_pub_socket;

  /** List of clients that are connected */
  khash_t(strclient) * clients;

  /** Time (in ms) between heartbeats sent to clients */
  uint64_t heartbeat_interval;

  /** Time (in ms) to send the next heartbeat to clients */
  uint64_t heartbeat_next;

  /** The number of heartbeats that can go by before a client is declared
      dead */
  int heartbeat_liveness;

  /** Indicates that the server should shutdown at the next opportunity */
  int shutdown;

  /** Next view number */
  uint64_t view_num;

  /** BGPView Store instance */
  bgpview_io_zmq_store_t *store;

  /** The number of heartbeats that have gone by since the last timeout check */
  int store_timeout_cnt;

  /** The number of views in the store */
  int store_window_len;
};

/** @} */

/**
 * @name Server Publish Functions
 *
 * @{ */

/** Publish the given BGP View to any subscribed consumers
 *
 * @param server        pointer to the bgpview server instance
 * @param table         pointer to a bgp view to publish
 */
int bgpview_io_zmq_server_publish_view(bgpview_io_zmq_server_t *server,
                                       bgpview_t *view);

/** @} */

#endif
