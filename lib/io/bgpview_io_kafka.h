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

#include <wandio.h>

#include "bgpview_io.h"
#include "bgpview.h"
#include <librdkafka/rdkafka.h>


typedef struct kafka_data{

	  /*
	   * The broker address/es. It is possible to use
	   * more than one broker by separating them with a ","
	   */
	  char *brokers;

	  /**Name of the topic for:
	   *
	   * @pfxs_paths_topic: default views
	   * @peers_topic: default peers
	   * @metadata_topic: default metadata
	   *
	   */
	  char *pfxs_paths_topic;
	  char *peers_topic;
	  char *metadata_topic;

	  /** Information about which partition of the topic the user wants to read
	   *
	   * @pfxs_paths_partition should be set automatically by the metadata topic
	   * @peers_partition should be always equal to 0 in case of a single partition
	   * @metadata_partition should be always equal to 0 in case of a single partition
	   *  as the program crawl the topic to get the view offset
	   *
	   */
	  int pfxs_paths_partition;
	  int peers_partition;
	  int metadata_partition;

	  /** Information about which offset of the topic the user wants to read
	   *
	   * @pfxs_paths_offset should be set automatically by the metadata topic
	   * @peers_offset should be always equal to 0
	   * @metadata_offset should be always equal to 0 as the program crawl the topic
	   * to get the view offset
	   *
	   */
	  int pfxs_paths_offset;
	  int peers_offset;
	  int metadata_offset;

	  int pfxs_paths_sync_partition;
	  int pfxs_paths_sync_offset;
	  int pfxs_paths_sync_view_id;

	  rd_kafka_t *pfxs_paths_rk;
	  rd_kafka_t *peers_rk;
	  rd_kafka_t *metadata_rk;

	  rd_kafka_topic_t *pfxs_paths_rkt;
	  rd_kafka_topic_t *peers_rkt;
	  rd_kafka_topic_t *metadata_rkt;

	  rd_kafka_conf_t *pfxs_paths_conf;
	  rd_kafka_conf_t *peers_conf;
	  rd_kafka_conf_t *metadata_conf;

	  rd_kafka_topic_conf_t *pfxs_paths_topic_conf;
	  rd_kafka_topic_conf_t *peers_topic_conf;
	  rd_kafka_topic_conf_t *metadata_topic_conf;


} kafka_data_t;


typedef struct kafka_sync_view_data{

	int pfxs_paths_sync_partition;
	uint32_t pfxs_paths_sync_offset;
	int pfxs_paths_sync_view_id;

} kafka_sync_view_data_t;

int set_sync_view_data(kafka_data_t dest,bgpview_t *view, kafka_sync_view_data_t *sync_view_data);

int send_diffs(kafka_data_t dest, char *topic ,void* messages[], int messages_len[] ,int num_messages);

int send_message_to_topic(kafka_data_t dest, char *topic, char* message, int len);

void *row_serialize(char operation, bgpview_iter_t *it, int *len);

int publish_metadata(kafka_data_t dest, bgpview_t *view, kafka_sync_view_data_t *sync_view_data, char *type);

int set_metadata(kafka_data_t src,int interest_view);

rd_kafka_topic_t * initialize_producer_connection(rd_kafka_t **rk,
												  rd_kafka_conf_t **conf,
												  rd_kafka_topic_conf_t **topic_conf,
												  char *brokers, char *topic, int partition, int offset);

rd_kafka_topic_t * initialize_consumer_connection(rd_kafka_t **rk,
												  rd_kafka_conf_t **conf,
												  rd_kafka_topic_conf_t **topic_conf,
												  char *brokers, char *topic, int partition, int offset);



/** Send the given view to the given socket
 *
 * @param dest          kafka broker and topic to send the view to
 * @param view          pointer to the view to send
 * @param cb            callback function to use to filter peers (may be NULL)
 * @return 0 if the view was sent successfully, -1 otherwise
 */
int bgpview_io_kafka_send(kafka_data_t dest,
						  bgpview_t *view,
						  bgpview_io_filter_cb_t *cb);

/** Receive a view from the given socket
 *
 * @param src           information about broker to find metadata about views
 * @param view          pointer to the clear/new view to receive into
 * @param interest      timestamp of the view
 * @return pointer to the view instance received, NULL if an error occurred.
 */
int bgpview_io_kafka_recv(kafka_data_t src,
						  bgpview_t *view,
						  int interest_view,
						  bgpview_io_filter_peer_cb_t *peer_cb,
						  bgpview_io_filter_pfx_cb_t *pfx_cb,
						  bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

#endif /* __BGPVIEW_IO_KAFKA_H */
