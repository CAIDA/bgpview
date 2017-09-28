/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Danilo Giordano, Alistair King, Chiara Orsini
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

#ifndef __BGPVIEW_IO_KAFKA_INT_H
#define __BGPVIEW_IO_KAFKA_INT_H

#define WITH_THREADS

#include "bgpview_io_kafka.h"
#include "khash.h"
#include <librdkafka/rdkafka.h>
#ifdef WITH_THREADS
#include <pthread.h>
#endif

/**
 * @name Protected Macros
 *
 * @{ */

/** Shortcut to get the BGPView topic ref for the given topic ID */
#define TOPIC(tid) (&(client->topics[(tid)]))

/** Shortcut to get the RD Kafka topic ref for the given topic ID */
#define RKT(tid) (client->topics[(tid)].rkt)

/** Shortcut to get the FQ name for the given topic ID */
#define TNAME(tid) (client->topics[(tid)].name)

#define IDENTITY_MAX_LEN 1024

/* @} */

/**
 * @name Protected Enums
 *
 * @{ */

/** IDs of topics used */
typedef enum {

  BGPVIEW_IO_KAFKA_TOPIC_ID_PFXS = 0,

  BGPVIEW_IO_KAFKA_TOPIC_ID_PEERS = 1,

  BGPVIEW_IO_KAFKA_TOPIC_ID_META = 2,

  BGPVIEW_IO_KAFKA_TOPIC_ID_MEMBERS = 3,

  BGPVIEW_IO_KAFKA_TOPIC_ID_GLOBALMETA = 4,

  BGPVIEW_IO_KAFKA_TOPIC_ID_CNT = 5,

} bgpview_io_kafka_topic_id_t;

/* @} */

/**
 * @name Protected Data Structures
 *
 * @{ */

typedef struct bgpview_io_kafka_topic {

  /** Fully-qualified name of the topic (includes namespace and possibly
      identity) */
  char name[IDENTITY_MAX_LEN];

  /** RD Kafka topic handle */
  rd_kafka_topic_t *rkt;

} bgpview_io_kafka_topic_t;

typedef struct bgpview_io_kafka_peeridmap {

  /** Mapping from Kafka peer ID to local peer ID */
  bgpstream_peer_id_t *map;

  /** Length of the peerid_map array */
  int alloc_cnt;

} bgpview_io_kafka_peeridmap_t;

typedef struct producer_state {

  /** Structure to store tx statistics */
  bgpview_io_kafka_stats_t stats;

  /** The metadata offset of the last sync view sent */
  int64_t last_sync_offset;

  /** The walltime at which we should write another members update */
  uint32_t next_members_update;

} producer_state_t;

typedef struct direct_consumer_state {

  bgpview_io_kafka_peeridmap_t idmap;

} direct_consumer_state_t;

enum {
  WORKER_BUSY = 0,
  WORKER_IDLE = 1,
  WORKER_VIEW_EMPTY = 0,
  WORKER_VIEW_READY = 1,
  WORKER_JOB_IDLE = 0,
  WORKER_JOB_ASSIGNED = 1,
  WORKER_JOB_COMPLETE = 2,
};

/** Topic state for a member */
typedef struct gc_topics {

#ifdef WITH_THREADS
  /** Borrowed pointer to the global consumer state */
  struct global_consumer_state *global;

  /** Borrowed pointer to RD Kafka connection handle */
  rd_kafka_t *rdk_conn;

  /** The thread that reads data from this topic */
  pthread_t worker;

  /** Should the worker shutdown at the next chance it gets? */
  int shutdown;

  /** Was there an error receiving the view */
  int recv_error;

  /** Is there a job waiting for the worker to read? */
  pthread_cond_t job_state_cond;

  /** Is the worker ready to process a view? */
  int worker_state; /* WORKER_IDLE, WORKER_BUSY */
  pthread_cond_t worker_state_cond;

  /* Mutex for the worker conditions */
  pthread_mutex_t mutex;

  /** Filter callbacks (use VIEW mutex) */
  bgpview_io_filter_peer_cb_t *peer_cb;
  bgpview_io_filter_pfx_cb_t *pfx_cb;
  bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb;
#endif

  /** Borrowed pointer to the view metadata to work on receiving */
  struct bgpview_io_kafka_md *meta;

  /** Is this "worker" assigned a view */
  int job_state; /* WORKER_JOB_IDLE, WORKER_JOB_ASSIGNED */

  /** Has the worker touched the view? */
  int view_state; /* WORKER_VIEW_EMPTY, WORKER_VIEW_READY */

  /** The peer topic for this member */
  bgpview_io_kafka_topic_t peers;

  /** The prefix topic for this member */
  bgpview_io_kafka_topic_t pfxs;

  /** Mapping of remote to local peer IDs */
  bgpview_io_kafka_peeridmap_t idmap;

  /** The time of the last view we successfully received */
  uint32_t parent_view_time;

  /** Private partial-view (only contains info from this producer) */
  bgpview_t *view;

} gc_topics_t;

/** Maps a member identity string (e.g., collector name) to a topic structure */
KHASH_INIT(str_topic, char *, gc_topics_t *, 1, kh_str_hash_func,
           kh_str_hash_equal);

typedef struct global_consumer_state {

  khash_t(str_topic) * topics;

#ifdef WITH_THREADS
  /** Global view mutex */
  pthread_mutex_t mutex;
#endif

} global_consumer_state_t;

struct bgpview_io_kafka {

  /** Is this a producer, direct consumer, or global consumer? */
  bgpview_io_kafka_mode_t mode;

  /* SETTINGS */

  /**
   * The broker address/es. It is possible to use more than one broker by
   * separating them with a ","
   */
  char *brokers;

  /** Namespace of this producer (all topic names will have this string prefixed
      to them) */
  char *namespace;

  /** String that uniquely IDs a producer within the namespace */
  char *identity;

  /** Global meta channel to use (to allow multiple global meta servers to be
      run) */
  char *channel;

  /* STATE */

  /** RD Kafka connection handle */
  rd_kafka_t *rdk_conn;

  /** Are we connected to Kafka? */
  int connected;

  /** Has there been a fatal error? */
  int fatal_error;

  /** State for the various topics that we use (only some will be connected) */
  bgpview_io_kafka_topic_t topics[BGPVIEW_IO_KAFKA_TOPIC_ID_CNT];

  /* Various state info used by specific client modes */
  producer_state_t prod_state;
  direct_consumer_state_t dc_state;
  global_consumer_state_t gc_state;
};

typedef struct bgpview_io_kafka_md {

  /** The identity of the producer */
  char identity[IDENTITY_MAX_LEN];

  /** The time of the the view */
  uint32_t time;

  /** The number of peers in this view */
  uint32_t peers_cnt;

  /** The type of this view dump (S[ync]/D[iff]) */
  char type;

  /** Where to find the prefixes */
  int64_t pfxs_offset;

  /** Where to find the peers */
  int64_t peers_offset;

  /** Only populated in the case of a Diff: */
  /** Offset of the most recent sync frame */
  int64_t sync_md_offset;
  /** Time of the parent view */
  uint32_t parent_time;

} bgpview_io_kafka_md_t;

/** @} */

/** Set up config options common to both producer and consumer */
int bgpview_io_kafka_common_config(bgpview_io_kafka_t *client,
                                   rd_kafka_conf_t *conf);

/** Generate the topic name from the given producer identity and topic id,
    create a connection to the generated topic */
int bgpview_io_kafka_single_topic_connect(bgpview_io_kafka_t *client,
                                          char *identity,
                                          bgpview_io_kafka_topic_id_t id,
                                          bgpview_io_kafka_topic_t *topic);

/* PRODUCER FUNCTIONS */

/** Create a producer connection to Kafka */
int bgpview_io_kafka_producer_connect(bgpview_io_kafka_t *client);

/** Create a connection to the given topic */
int bgpview_io_kafka_producer_topic_connect(bgpview_io_kafka_t *client,
                                            rd_kafka_topic_t **rkt,
                                            char *topic);

/** Send the given view to the given socket
 *
 * @param dest          kafka broker and topic to send the view to
 * @param view          pointer to the view to send
 * @param view          pointer to the parent view to send
 * @param cb            callback function to use to filter peers (may be NULL)
 * @return 0 if the view was sent successfully, -1 otherwise
 */
int bgpview_io_kafka_producer_send(bgpview_io_kafka_t *client, bgpview_t *view,
                                   bgpview_t *parent_view,
                                   bgpview_io_filter_cb_t *cb, void *cb_user);

/** Manually trigger an update to the members topic (used to signal producer is
 * shutting down)
 *
 * @param client
 * @param time_now      Current wall time, or 0 if the producer is shutting down
 */
int bgpview_io_kafka_producer_send_members_update(bgpview_io_kafka_t *client,
                                                  uint32_t time_now);

/* CONSUMER FUNCTIONS */

/** Create a connection to the given topic and start consuming */
int bgpview_io_kafka_consumer_topic_connect(bgpview_io_kafka_t *client,
                                            rd_kafka_topic_t **rkt,
                                            char *topic);

/** Create a consumer connection to Kafka */
int bgpview_io_kafka_consumer_connect(bgpview_io_kafka_t *client);

/** Receive a view from the given socket
 *
 * @param src           information about broker to find metadata about views
 * @param view          pointer to the clear/new view to receive into
 * @return pointer to the view instance received, NULL if an error occurred.
 */
int bgpview_io_kafka_consumer_recv(
  bgpview_io_kafka_t *client, bgpview_t *view,
  bgpview_io_filter_peer_cb_t *peer_cb, bgpview_io_filter_pfx_cb_t *pfx_cb,
  bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

#endif /* __BGPVIEW_IO_KAFKA_INT_H */
