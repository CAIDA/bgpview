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

#include "bgpview_io_kafka.h"
#include <librdkafka/rdkafka.h>

/**
 * @name Protected Data Structures
 *
 * @{ */

struct bgpview_io_kafka {

  /** Is this a producer or a consumer? */
  bgpview_io_kafka_mode_t mode;

  /** Structure to store tx statistics */
  bgpview_io_kafka_stats_t stats;

  /**
   * The broker address/es. It is possible to use more than one broker by
   * separating them with a ","
   */
  char *brokers;

  /** RD Kafka connection handle */
  rd_kafka_t *rdk_conn;

  /** Are we connected to Kafka? */
  int connected;

  /** Has there been a fatal error? */
  int fatal_error;

  /** Name of the topic to read/write prefix info to/from */
  char *pfxs_topic;

  /** Name of the topic to read/write peer info to/from */
  char *peers_topic;

  /** Name of the topic to read/write metadata info to/from */
  char *metadata_topic;

  /** RD Kafka prefix topic handle */
  rd_kafka_topic_t *pfxs_rkt;

  /** RD Kafka peer topic handle */
  rd_kafka_topic_t *peers_rkt;

  /** RD Kafka metadata topic handle */
  rd_kafka_topic_t *metadata_rkt;

  /** The following fields are only used by the consumer */

  /** Mapping from Kafka peer ID to local peer ID */
  bgpstream_peer_id_t *peerid_map;

  /** Length of the peerid_map array */
  int peerid_map_alloc_cnt;

  /** The following fields are only used by the producer. */

  /** The maximum number of diffs that can be sent before a sync frame must be
      sent */
  int max_diffs;

  /** The number of diffs that have been sent since the last sync frame */
  int num_diffs;

  /* Historical View*/
  bgpview_t *parent_view;
  bgpview_iter_t *parent_view_it;

  /** The metadata offset of the last sync view sent */
  int64_t last_sync_offset;
};

typedef struct bgpviewio_kafka_md {

  /** The time of the the view */
  uint32_t time;

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
 * @param cb            callback function to use to filter peers (may be NULL)
 * @return 0 if the view was sent successfully, -1 otherwise
 */
int bgpview_io_kafka_producer_send(bgpview_io_kafka_t *client,
                                   bgpview_t *view, bgpview_io_filter_cb_t *cb);

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
