/*
 * This file is part of bgpstream
 *
 * Copyright (C) 2015 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
 *
 * All rights reserved.
 *
 * This code has been developed by CAIDA at UC San Diego.
 * For more information, contact bgpstream-info@caida.org
 *
 * This source code is proprietary to the CAIDA group at UC San Diego and may
 * not be redistributed, published or disclosed without prior permission from
 * CAIDA.
 *
 * Report any bugs, questions or comments to bgpstream-info@caida.org
 *
 */

#ifndef __BGPVIEW_IO_SERVER_INT_H
#define __BGPVIEW_IO_SERVER_INT_H

#include <czmq.h>
#include <stdint.h>

#include "bgpview_io_server.h"
#include "bgpview.h"
#include "bgpview_io_common.h"
#include "bgpview_io_common_int.h"
#include "bgpview_io_store.h"

#include "khash.h"

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

/* shared constants are in bgpview_io_common.h */

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
typedef struct bgpview_io_server_client {

  /** Identity frame data that the client sent us */
  zmq_msg_t identity;

  /** Hex id of the client (may be the same as the printable ID */
  char *hexid;

  /** Printable ID of client (for debugging and logging) */
  char *id;

  /** Time at which the client expires */
  uint64_t expiry;

  /** info about this client that we will send to the client connect handler */
  bgpview_io_server_client_info_t info;

} bgpview_io_server_client_t;

KHASH_INIT(strclient, char*, bgpview_io_server_client_t*, 1,
	   kh_str_hash_func, kh_str_hash_equal);

struct bgpview_io_server {

  /** Metric prefix to output metrics */
  char metric_prefix[BGPVIEW_IO_SERVER_METRIC_PREFIX_LEN];
  
  /** Error status */
  bgpview_io_err_t err;

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
  bgpview_io_store_t *store;

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

/** Publish the given BGP View to any interested consumers
 *
 * @param server        pointer to the bgpview server instance
 * @param table         pointer to a bgp view to publish
 * @param interests     flags indicating which interests this table satisfies
 */
int bgpview_io_server_publish_view(bgpview_io_server_t *server,
                                   bgpview_t *view,
                                   int interests);

/** @} */

#endif
