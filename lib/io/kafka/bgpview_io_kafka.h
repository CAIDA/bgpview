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

#ifndef __BGPVIEW_IO_KAFKA_H
#define __BGPVIEW_IO_KAFKA_H

#include "bgpview.h"
#include "bgpview_io.h"

/** @file
 *
 * @brief Header file that exposes the public interface of the bgpview kafka
 * client
 *
 * @author Danilo Giordano
 *
 */

/**
 * @name Public Constants
 *
 * @{ */

#define BGPVIEW_IO_KAFKA_BROKER_URI_DEFAULT "127.0.0.1:9092"

#define BGPVIEW_IO_KAFKA_PFXS_TOPIC_DEFAULT "bgpview-pfxs"

#define BGPVIEW_IO_KAFKA_PEERS_TOPIC_DEFAULT "bgpview-peers"

#define BGPVIEW_IO_KAFKA_METADATA_TOPIC_DEFAULT "bgpview-metadata"

#define BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT 0

#define BGPVIEW_IO_KAFKA_METADATA_PARTITION_DEFAULT 0

#define BGPVIEW_IO_KAFKA_PEERS_OFFSET_DEFAULT 0

#define BGPVIEW_IO_KAFKA_METADATA_OFFSET_DEFAULT 0

/** A Sync frame will be sent once per N views */
#define BGPVIEW_IO_KAFKA_SYNC_FREQUENCY 12

/** @} */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

typedef struct bgpview_io_kafka bgpview_io_kafka_t;

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

typedef struct bgpview_io_kafka_stats {

  int send_time;
  int clone_time;
  int total_time;
  int arrival_time;
  int processed_time;

  int add;
  int remove;
  int common;
  int change;
  int current_pfx_cnt;
  int historical_pfx_cnt;

  int sync_cnt;

} bgpview_io_kafka_stats_t;

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** The mode that the client will operate in */
typedef enum {

  /** This instance will consume data from Kafka */
  BGPVIEW_IO_KAFKA_MODE_CONSUMER = 0,

  /** This instance will produce data into Kafka */
  BGPVIEW_IO_KAFKA_MODE_PRODUCER = 1,

} bgpview_io_kafka_mode_t;

/** @} */

/** Initialize a new BGPView Kafka IO client
 *
 * @param mode          whether to act as a producer or a consumer
 * @return a pointer to a BGPView Kafka IO instance if successful, NULL if an
 * error occurred.
 */
bgpview_io_kafka_t *bgpview_io_kafka_init(bgpview_io_kafka_mode_t mode);

/** Destroy the given BGPView Kafka IO client
 *
 * @param client       pointer to the bgpview kafka client instance to free
 */
void bgpview_io_kafka_destroy(bgpview_io_kafka_t *client);

/** Start the given bgpview kafka client
 *
 * @param client       pointer to a kafka client instance to start
 * @return 0 if the client started successfully, -1 otherwise.
 */
int bgpview_io_kafka_start(bgpview_io_kafka_t *client);


/** Set the broker addresses (comma separated) of the Kafka server
 *
 * @param client        pointer to a bgpview kafka client instance to update
 * @param uri           pointer to a uri string
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_kafka_set_broker_addresses(bgpview_io_kafka_t *client,
                                          const char *addresses);

/** How often should a Sync frame be sent
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param frequency     how often a sync frame should be sent
 *
 * A Sync view will be sent every N views (i.e. after N-1 diffs)
 */
void bgpview_io_kafka_set_sync_frequency(bgpview_io_kafka_t *client,
                                         int frequency);

/** Set the topic that prefix information will be written into/read from
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param topic         pointer to the topic name
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_kafka_set_pfxs_topic(bgpview_io_kafka_t *client,
                                    const char *topic);

/** Set the topic that peer information will be written into/read from
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param topic         pointer to the topic name
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_kafka_set_peers_topic(bgpview_io_kafka_t *client,
                                     const char *topic);

/** Set the topic that metadata information will be written into/read from
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param topic         pointer to the topic name
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_kafka_set_metadata_topic(bgpview_io_kafka_t *client,
                                        const char *topic);

/** Set the partition that prefixes information will be written into/read from
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param partition     partition to use
 * @return 0 if successful, -1 otherwise
 */
void bgpview_io_kafka_set_pfxs_partition(bgpview_io_kafka_t *client,
                                         int partition);

/** Set the partition that peers information will be written into/read from
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param partition     partition to use
 * @return 0 if successful, -1 otherwise
 */
void bgpview_io_kafka_set_peers_partition(bgpview_io_kafka_t *client,
                                          int partition);

/** Set the partition that metadata information will be written into/read from
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param partition     partition to use
 * @return 0 if successful, -1 otherwise
 */
void bgpview_io_kafka_set_metadata_partition(bgpview_io_kafka_t *client,
                                             int partition);

/** Queue the given View for transmission to Kafka
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param stats         pointer to a bgpview kafka stats structure to fill
 * @param view          pointer to the view to transmit
 * @param cb            callback function to use to filter entries (may be NULL)
 * @return 0 if the view was transmitted successfully, -1 otherwise
 *
 * This function only sends 'active' fields. Any fields that are 'inactive' in
 * the view **will not** be present in the view received by Kafka.
 */
int bgpview_io_kafka_send_view(bgpview_io_kafka_t *client,
                               bgpview_io_kafka_stats_t *stats,
                               bgpview_t *view,
                               bgpview_io_filter_cb_t *cb);


/** Attempt to receive an BGP View from the bgpview server
 *
 * @param client        pointer to the client instance to receive from
 * @param view          pointer to the view to fill
 * @param peer_cb       callback function to use to filter peer entries
 *                      (may be NULL)
 * @param pfx_cb        callback function to use to filter prefix entries
 *                      (may be NULL)
 * @param pfx_peer_cb   callback function to use to filter prefix-peer entries
 *                      (may be NULL)
 * @return 0 or -1 if an error occurred.
 *
 * The view provided to this function must have been created using
 * bgpview_create, and if diffs are not in use, it *must* have been cleared
 * using bgpview_clear. If diffs are in use, it *must* not have been cleared,
 * and instead *must* contain information about the previously received view.
 */
int bgpview_io_kafka_recv_view(bgpview_io_kafka_t *client,
                               bgpview_t *view,
                               bgpview_io_filter_peer_cb_t *peer_cb,
                               bgpview_io_filter_pfx_cb_t *pfx_cb,
                               bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

#endif /* __BGPVIEW_IO_KAFKA_H */
