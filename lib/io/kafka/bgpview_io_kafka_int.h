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


#define KAFKA_PEER_MAP_SIZE 2048

/**
 * @name Protected Data Structures
 *
 * @{ */

struct bgpview_io_kafka {

  /** Is this a producer or a consumer? */
  bgpview_io_kafka_mode_t mode;

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
  char *pfxs_topic;
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
  int32_t pfxs_partition;
  int32_t peers_partition;
  int32_t metadata_partition;

  /** Information about which offset of the topic the user wants to read
   *
   * @pfxs_paths_offset should be set automatically by the metadata topic
   * @peers_offset should be always equal to 0
   * @metadata_offset should be always equal to 0 as the program crawl the topic
   * to get the view offset
   *
   */
  int64_t pfxs_offset;
  int64_t peers_offset;
  int64_t metadata_offset;

  rd_kafka_t *pfxs_rk;
  rd_kafka_t *peers_rk;
  rd_kafka_t *metadata_rk;

  rd_kafka_topic_t *pfxs_rkt;
  rd_kafka_topic_t *peers_rkt;
  rd_kafka_topic_t *metadata_rkt;

  int view_frequency;

  bgpstream_peer_id_t *peerid_map;
  int peerid_map_alloc_cnt;

  uint32_t sync_view_time;

  int64_t current_pfxs_offset;
  int64_t current_peers_offset;

  int32_t pfxs_sync_partition;
  int64_t pfxs_sync_offset;
  int64_t peers_sync_offset;

  int32_t pfxs_diffs_partition[BGPVIEW_IO_KAFKA_DIFF_FREQUENCY];

  int64_t pfxs_diffs_offset[BGPVIEW_IO_KAFKA_DIFF_FREQUENCY];
  int64_t peers_diffs_offset[BGPVIEW_IO_KAFKA_DIFF_FREQUENCY];

  int num_diffs;

  /* Historical View*/
  bgpview_t *viewH;
  bgpview_iter_t *itH;
};

/** @} */



int initialize_producer_connection(rd_kafka_t **rk,
                                   rd_kafka_topic_t **rkt,
                                   char *brokers, char *topic);


int initialize_consumer_connection(rd_kafka_t **rk,
                                   rd_kafka_topic_t **rkt,
                                   char *brokers,
                                   char *topic);



/** Send the given view to the given socket
 *
 * @param dest          kafka broker and topic to send the view to
 * @param view          pointer to the view to send
 * @param cb            callback function to use to filter peers (may be NULL)
 * @return 0 if the view was sent successfully, -1 otherwise
 */
int bgpview_io_kafka_send(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats,
                          bgpview_t *view,
                          bgpview_io_filter_cb_t *cb);

/** Receive a view from the given socket
 *
 * @param src           information about broker to find metadata about views
 * @param view          pointer to the clear/new view to receive into
 * @return pointer to the view instance received, NULL if an error occurred.
 */
int bgpview_io_kafka_recv(bgpview_io_kafka_t *client,
                          bgpview_t *view,
                          bgpview_io_filter_peer_cb_t *peer_cb,
                          bgpview_io_filter_pfx_cb_t *pfx_cb,
                          bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

#endif /* __BGPVIEW_IO_KAFKA_INT_H */
