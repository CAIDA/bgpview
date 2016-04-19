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

/** Default Kafaka broker(s) */
#define BGPVIEW_IO_KAFKA_BROKER_URI_DEFAULT "127.0.0.1:9092"

/** Default namespace */
#define BGPVIEW_IO_KAFKA_NAMESPACE_DEFAULT "bgpview-test"

/** Default topic for prefixes */
#define BGPVIEW_IO_KAFKA_PFXS_TOPIC_DEFAULT "pfxs"

/** Default topic for peers */
#define BGPVIEW_IO_KAFKA_PEERS_TOPIC_DEFAULT "peers"

/** Default topic for metadata */
#define BGPVIEW_IO_KAFKA_METADATA_TOPIC_DEFAULT "metadata"

/** Default partition for prefixes */
#define BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT 0

/** Default partition for peers */
#define BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT 0

/** Default partition for metadata */
#define BGPVIEW_IO_KAFKA_METADATA_PARTITION_DEFAULT 0

/** Default partition for members */
#define BGPVIEW_IO_KAFKA_MEMBERS_PARTITION_DEFAULT 0

/** Default number of times to retry a failed connection to Kafka */
#define BGPVIEW_IO_KAFKA_CONNECT_MAX_RETRIES 8

/** Number of seconds (wall time) between updates to the members topic */
#define BGPVIEW_IO_KAFKA_MEMBERS_UPDATE_INTERVAL_DEFAULT 3600

/** @} */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

/** Opaque structure representing a BGPView Kafka IO instance */
typedef struct bgpview_io_kafka bgpview_io_kafka_t;

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

typedef struct bgpview_io_kafka_stats {

  /** The number of prefixes in common between this view and the previous view
      (only set when sending a diff) */
  int common_pfxs_cnt;

  /** The number of prefixes that were added wrt the previous view (only set
      when sending a diff) */
  int added_pfxs_cnt;

  /** The number of prefixes that were removed wrt the previous view (only set
      when sending a diff) */
  int removed_pfxs_cnt;

  /** The number of prefixes that were changed wrt the previous view (only set
      when sending a diff) */
  int changed_pfxs_cnt;

  /** The number of pfx-peer cells that were added wrt the previous view (only
      set when sending a diff) */
  int added_pfx_peer_cnt;

  /** The total number of pfx-peer cells that were changed wrt the previous view
      (only set when sending a diff) */
  int changed_pfx_peer_cnt;

  /** The total number of pfx-peer cells that were removed wrt the previous view
      (only set when sending a diff) */
  int removed_pfx_peer_cnt;

  /** The number of prefixes sent in the current view (added + changed + removed
      in the case of a diff) */
  int pfx_cnt;

  /** The number of prefixes sent as part of a sync frame */
  int sync_pfx_cnt;

} bgpview_io_kafka_stats_t;

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** The mode that the client will operate in */
typedef enum {

  /** This instance will consume data from Kafka (directly from a single
      producer) */
  BGPVIEW_IO_KAFKA_MODE_DIRECT_CONSUMER = 0,

  /** This instance will consume data from Kafka (from all registered producer
      -- requires the server to be running) */
  BGPVIEW_IO_KAFKA_MODE_GLOBAL_CONSUMER = 1,

  /** This instance will produce data into Kafka */
  BGPVIEW_IO_KAFKA_MODE_PRODUCER = 2,

} bgpview_io_kafka_mode_t;

/** @} */

/** Initialize a new BGPView Kafka IO client
 *
 * @param mode          whether to act as a producer or a consumer
 * @param opts          string containing options to be parsed with getopt
 * @return a pointer to a BGPView Kafka IO instance if successful, NULL if an
 * error occurred.
 */
bgpview_io_kafka_t *bgpview_io_kafka_init(bgpview_io_kafka_mode_t mode,
                                          const char *opts);

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

/** Set the topic namespace to use
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param namespace     pointer to a namespace string
 * @return 0 if successful, -1 otherwise
 *
 * Namespaces allow multiple producers to operate completely independently of
 * each other (e.g., production and test producers).
 *
 * It is **completely** up to the user to ensure that it is safe to write into
 * the provided namespace. If multiple producers write into the same namespace,
 * at the same time, bad things will happen. You have been warned.
 *
 * If the namespace is not set, then BGPVIEW_IO_KAFKA_NAMESPACE_DEFAULT will be
 * used.
 */
int bgpview_io_kafka_set_namespace(bgpview_io_kafka_t *client,
                                   const char *namespace);

/** Queue the given View for transmission to Kafka
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param view          pointer to the view to transmit
 * @param parent_view   pointer to the parent view to diff agains (may be NULL)
 * @param cb            callback function to use to filter entries (may be NULL)
 * @return 0 if the view was transmitted successfully, -1 otherwise
 *
 * This function only sends 'active' fields. Any fields that are 'inactive' in
 * the view **will not** be present in the view received by Kafka.
 *
 * If the `parent_view` parameter is NULL, then a sync frame will be sent
 * (i.e. the entire view will be transmitted), otherwise, `view` will be
 * compared against `parent_view` and only prefixes and peers that have changed
 * will be sent.
 */
int bgpview_io_kafka_send_view(bgpview_io_kafka_t *client,
                               bgpview_t *view,
                               bgpview_t *parent_view,
                               bgpview_io_filter_cb_t *cb,
                               void *cb_user);

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
int bgpview_io_kafka_recv_view(bgpview_io_kafka_t *client, bgpview_t *view,
                               bgpview_io_filter_peer_cb_t *peer_cb,
                               bgpview_io_filter_pfx_cb_t *pfx_cb,
                               bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

/** Get statistics about the last view that was sent
 * (currently only valid for a producer)
 *
 * @param client        pointer to the client instance to get stats for
 * @return borrowed pointer to a stats structure filled with information
 * about the last sent view (values will be all zero if no views have been sent)
 */
bgpview_io_kafka_stats_t *
bgpview_io_kafka_get_stats(bgpview_io_kafka_t *client);

#endif /* __BGPVIEW_IO_KAFKA_H */
