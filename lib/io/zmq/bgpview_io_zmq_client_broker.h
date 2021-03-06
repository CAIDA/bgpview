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

#ifndef __BGPVIEW_IO_ZMQ_CLIENT_BROKER_H
#define __BGPVIEW_IO_ZMQ_CLIENT_BROKER_H

#include "bgpview_io_zmq_int.h"
#include "khash.h"
#include <czmq.h>
#include <stdint.h>

/** @file
 *
 * @brief Header file that exposes the private interface of the bgpview
 * client broker
 *
 * @author Alistair King
 *
 */

/** The maximum number of requests that we allow to be outstanding at any time
 */
#define MAX_OUTSTANDING_REQ 2

/** The number of frames that we allocate each time we need more messages */
#define BGPVIEW_IO_ZMQ_CLIENT_BROKER_REQ_MSG_FRAME_CHUNK 256000

/** The maximum number of messages that we receive from the server before
    yielding back to the reactor */
#define BGPVIEW_IO_ZMQ_CLIENT_BROKER_GREEDY_MAX_MSG 10

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

/** Holds information about a single outstanding request sent to the server */
typedef struct bgpview_io_zmq_client_broker_req {

  /** Is this request in use? */
  int in_use;

  /** Message type in the request (and reply) */
  bgpview_io_zmq_msg_type_t msg_type;

  /** The sequence number in the request (used to match replies) */
  seq_num_t seq_num;

  /** The time that this request should next be retried */
  uint64_t retry_at;

  /** The number of retries that remain */
  uint8_t retries_remaining;

  /** Messages to send to the server */
  zmq_msg_t *msg_frames;

  /** Number of used msg frames */
  int msg_frames_cnt;

  /** Number of allocated msg frames */
  int msg_frames_alloc;

} bgpview_io_zmq_client_broker_req_t;

/** Config for the broker. Populated by the client */
typedef struct bgpview_io_zmq_client_broker_config {

  /** set of bgpview_producer_intent_t flags */
  uint8_t intents;

  /** Pointer to the master's state (passed to callbacks) */
  struct bgpview_io_zmq_client *master;

  /** 0MQ context pointer (for broker->server comms) */
  zctx_t *ctx;

  /** URI to connect to the server on */
  char *server_uri;

  /** URI to subscribe to server table messages on */
  char *server_sub_uri;

  /** Time (in ms) between heartbeats sent to the server */
  uint64_t heartbeat_interval;

  /** The number of heartbeats that can go by before the server is declared
      dead */
  int heartbeat_liveness;

  /** The minimum time (in ms) after a server disconnect before we try to
      reconnect */
  uint64_t reconnect_interval_min;

  /** The maximum time (in ms) after a server disconnect before we try to
      reconnect (after exponential back-off) */
  uint64_t reconnect_interval_max;

  /** The time that we will linger once a shutdown request has been received */
  uint64_t shutdown_linger;

  /** Request timeout in msec */
  uint64_t request_timeout;

  /** Request retries */
  int request_retries;

  /** Set if the broker is in an error state */
  int err;

  /** Identity of this client. MUST be globally unique.  If this field is set
   * when the broker is started, it will be used to set the identity of the zmq
   * socket
   */
  char *identity;

  /** Pointer to the pipe used to talk to the master */
  zsock_t *master_pipe;

} bgpview_io_zmq_client_broker_config_t;

/** State for a broker instance */
typedef struct bgpview_io_zmq_client_broker {

  /** Pointer to the config info that our master prepared for us (READ-ONLY) */
  bgpview_io_zmq_client_broker_config_t *cfg;

  /** Pointer to the pipe used to get/send signals to/from the master */
  zsock_t *signal_pipe;

  /** Pointer to the pipe used to talk to the master */
  zsock_t *master_pipe;

  /** Pointer to the resolved zmq socket used to talk to the master */
  void *master_zocket;

  /** Has the master pipe been removed from the reactor? */
  int master_removed;

  /** Socket used to connect to the server */
  void *server_socket;

  /** Socket used to receive server table messages (for consumers) */
  void *server_sub_socket;

  /** Ordered list of outstanding requests (used for re-transmits) */
  bgpview_io_zmq_client_broker_req_t req_list[MAX_OUTSTANDING_REQ];

  /** Number of currently outstanding requests (<= MAX_OUTSTANDING_REQ) */
  int req_count;

  /** Time (in ms) to send the next heartbeat to server */
  uint64_t heartbeat_next;

  /** The number of beats before the server is declared dead */
  int heartbeat_liveness_remaining;

  /** The time before we will next attempt to reconnect */
  uint64_t reconnect_interval_next;

  /** Indicates the time that the broker must shut down by (calculated as
      $TERM.time + shutdown_linger) */
  uint64_t shutdown_time;

  /** Event loop */
  zloop_t *loop;

  /** Heartbeat timer ID */
  int timer_id;

} bgpview_io_zmq_client_broker_t;

/** @} */

/** Main event loop of the client broker. Conforms to the zactor_fn spec.
 *
 * @param pipe          pointer to a pipe used to communicate with the client
 * @param args          pointer to a pre-populated client broker state struct
 *
 * @note all communication with the broker must be through the pipe. NO shared
 * memory is to be used.
 */
void bgpview_io_zmq_client_broker_run(zsock_t *pipe, void *args);

/** Initialize a request instance
 *
 * @return pointer to a request instance if successful, NULL otherwise
 */
bgpview_io_zmq_client_broker_req_t *bgpview_io_zmq_client_broker_req_init();

/** Free a request instance
 *
 * @param req_p           double-pointer to a request instance
 */
void bgpview_io_zmq_client_broker_req_free(
  bgpview_io_zmq_client_broker_req_t **req_p);

#endif
