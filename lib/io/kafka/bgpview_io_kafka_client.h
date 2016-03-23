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

#ifndef __BGPVIEW_IO_KAFKA_CLIENT_H
#define __BGPVIEW_IO_KAFKA_CLIENT_H


#include <stdint.h>

#include "bgpview_io_kafka.h"


/** @file
 *
 * @brief Header file that exposes the public interface of the bgpview kafka client
 *
 * @author Danilo Giordano
 *
 */

/**
 * @name Public Constants
 *
 * @{ */

/** Default URI for the server -> client connection */
#define BGPVIEW_IO_KAFKA_CLIENT_SERVER_URI_DEFAULT "192.172.226.44:9092,192.172.226.46:9092"

#define BGPVIEW_IO_KAFKA_CLIENT_PFXS_PATHS_TOPIC_DEFAULT "views"

#define BGPVIEW_IO_KAFKA_CLIENT_PEERS_TOPIC_DEFAULT "peers"

#define BGPVIEW_IO_KAFKA_CLIENT_METADATA_TOPIC_DEFAULT "metadata"

#define BGPVIEW_IO_KAFKA_CLIENT_PEERS_PARTITION_DEFAULT 0

#define BGPVIEW_IO_KAFKA_CLIENT_METADATA_PARTITION_DEFAULT 0

#define BGPVIEW_IO_KAFKA_CLIENT_PEERS_OFFSET_DEFAULT 0

#define BGPVIEW_IO_KAFKA_CLIENT_METADATA_OFFSET_DEFAULT 0

#define BGPVIEW_IO_KAFKA_CLIENT_DIFF_FREQUENCY 11

/*
 * PFXS PAHTS PARTIOTION AND TOPIC FROM METADATA TOPIC
 *
 */

/** @} */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

typedef struct bgpview_io_kafka_client bgpview_io_kafka_client_t;

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

/** @} */

/**
 * @name Public Enums
 *
 * @{ */


/** Initialize a new BGPView Client instance
 *
 * @return a pointer to a bgpview kafka client instance if successful, NULL if an
 * error occurred.
 */
bgpview_io_kafka_client_t *bgpview_io_kafka_client_init();

/** Queue the given View for transmission to the server
 *
 * @param client        pointer to a bgpview kafka client instance
 * @param view          pointer to the view to transmit
 * @param cb            callback function to use to filter entries (may be NULL)
 * @return 0 if the view was transmitted successfully, -1 otherwise
 *
 * This function only sends 'active' fields. Any fields that are 'inactive' in
 * the view **will not** be present in the view received by the server.
 *
 * @note The actual transmission may happen asynchronously, so a return from
 * this function simply means that the view was queued for transmission.
 */
int bgpview_io_kafka_client_send_view(bgpview_io_kafka_client_t *client,
                                bgpview_t *view,
								kafka_performance_t *metrics,
                                bgpview_io_filter_cb_t *cb);


/** Attempt to receive an BGP View from the bgpview server
 *
 * @param client        pointer to the client instance to receive from
 * @param view          pointer to the view to fill
 * @param cb            callback functions to use to filter entries (may be NULL)
 * @return 0 or -1 if an error occurred.
 *
 * The view provided to this function must have been created using
 * bgpview_create, and if it is being re-used, it *must* have been
 * cleared using bgpview_clear.
 */
int bgpview_io_kafka_client_recv_view(bgpview_io_kafka_client_t *client,
                                      bgpview_t *view,
                                      bgpview_io_filter_peer_cb_t *peer_cb,
                                      bgpview_io_filter_pfx_cb_t *pfx_cb,
                                      bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);



/** Free the given bgpview client instance
 *
 * @param client       pointer to the bgpview kafka client instance to free
 */
void bgpview_io_kafka_client_free(bgpview_io_kafka_client_t *client);


/** Set the URI for the client to connect to the kakfa server on
 *
 * @param client        pointer to a bgpview kafka client instance to update
 * @param uri           pointer to a uri string
 * @return 0 if successful, -1 otherwise
 */


/** Start the given bgpview kafka client to be a producer instance
 *
 * @param client       pointer to a bgpview client instance to start
 * @return 0 if the client started successfully, -1 otherwise.
 */
int bgpview_io_kafka_client_start_producer(bgpview_io_kafka_client_t *client);

/** Start the given bgpview client to be a consumer instance
 *
 * @param client       pointer to a bgpview kafka client instance to start
 * @return 0 if the client started successfully, -1 otherwise.
 */
int bgpview_io_kafka_client_start_consumer(bgpview_io_kafka_client_t *client);

void bgpview_io_kafka_client_set_diff_frequency(bgpview_io_kafka_client_t *client, int frequency);

int bgpview_io_kafka_client_set_broker_addresses(bgpview_io_kafka_client_t *client, const char *addresses);

int bgpview_io_kafka_client_set_pfxs_paths_topic(bgpview_io_kafka_client_t *client, const char *topic);
int bgpview_io_kafka_client_set_peers_topic(bgpview_io_kafka_client_t *client, const char *topic);
int bgpview_io_kafka_client_set_metadata_topic(bgpview_io_kafka_client_t *client, const char *topic);

void bgpview_io_kafka_client_set_pfxs_paths_partition(bgpview_io_kafka_client_t *client, int partition);
void bgpview_io_kafka_client_set_peers_partition(bgpview_io_kafka_client_t *client, int partition);
void bgpview_io_kafka_client_set_metadata_partition(bgpview_io_kafka_client_t *client, int partition);

#endif

