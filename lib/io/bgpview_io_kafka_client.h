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
#include "bgpview_io_common.h" //bgpview_io_err_perr


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

#define BGPVIEW_IO_KAFKA_CLIENT_METADATA_OFFSET_DEFAULT RD_KAFKA_OFFSET_BEGINNING

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
 *
 * @note calling a producer function or registering a consumer callback for an
 * intent/interest not registered will trigger an assert.
 */
bgpview_io_kafka_client_t *bgpview_io_kafka_client_init();


/** Prints the error status (if any) to standard error and clears the error
 * state
 *
 * @param client       pointer to bgpview kafka client instance to print error for
 */
void bgpview_io_kafka_client_perr(bgpview_io_kafka_client_t *client);

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
                                bgpview_io_filter_cb_t *cb);


/** Attempt to receive an BGP View from the bgpview server
 *
 * @param client        pointer to the client instance to receive from
 * @param interest_view	ID in form of timestamp of the view to receive
 * @param view          pointer to the view to fill
 * @param cb            callback functions to use to filter entries (may be NULL)
 * @return 0 or -1 if an error occurred.
 *
 * @note this function will only receive messages for which an interest was set
 * when initializing the client, but a view may satisfy *more* interests than
 * were explicitly asked for. For example, when subscribing to PARTIAL tables, a
 * table that is marked as PARTIAL could also be marked as FIRSTFULL (if it also
 * satisfies that interest).
 *
 * The view provided to this function must have been created using
 * bgpview_create, and if it is being re-used, it *must* have been
 * cleared using bgpview_clear.
 */
int bgpview_io_kafka_client_recv_view(bgpview_io_kafka_client_t *client,
								bgpview_t *view,
								int interest_view,
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
int bgpview_io_kafka_client_start_producer(bgpview_io_kafka_client_t *client, char* topic);

/** Start the given bgpview client to be a consumer instance
 *
 * @param client       pointer to a bgpview kafka client instance to start
 * @return 0 if the client started successfully, -1 otherwise.
 */
int bgpview_io_kafka_client_start_consumer(bgpview_io_kafka_client_t *client, char* topic);


int bgpview_io_kafka_client_set_server_uri(bgpview_io_kafka_client_t *client,
					 const char *uri);


int bgpview_io_kafka_client_send_diffs(bgpview_io_kafka_client_t *dest, char *topic, void* messages[], int messages_len[], int num_messages);

int bgpview_io_kafka_client_send_message_to_topic(bgpview_io_kafka_client_t *dest, char *topic, char* message, int len);

int bgpview_kafka_client_publish_metadata(bgpview_io_kafka_client_t *dest, bgpview_t *view, kafka_sync_view_data_t sync_view_data, char *type);

int bgpview_view_set_sync_view_data(bgpview_io_kafka_client_t *dest,bgpview_t *view, kafka_sync_view_data_t *sync_view_data);

#endif

