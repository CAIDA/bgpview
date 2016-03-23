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

#ifndef __BGPVIEW_IO_ZMQ_SERVER_INT_H
#define __BGPVIEW_IO_ZMQ_SERVER_INT_H

#include "bgpview_io_zmq_server.h"
#include "bgpview.h"
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

KHASH_INIT(strclient, char*, bgpview_io_zmq_server_client_t*, 1,
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
  khash_t(strclient) *clients;

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
