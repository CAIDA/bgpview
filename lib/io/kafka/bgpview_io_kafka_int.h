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

  /**
   * The broker address/es. It is possible to use more than one broker by
   * separating them with a ","
   */
  char *brokers;

  /** RD Kafka connection handle */
  rd_kafka_t *rdk_conn;

  char *pfxs_topic;
  char *peers_topic;
  char *metadata_topic;

  int32_t pfxs_partition;
  int32_t peers_partition;
  int32_t metadata_partition;

  rd_kafka_topic_t *pfxs_rkt;
  rd_kafka_topic_t *peers_rkt;
  rd_kafka_topic_t *metadata_rkt;

  /** The following fields are only used by the consumer */

  bgpstream_peer_id_t *peerid_map;
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

/** @} */

#endif /* __BGPVIEW_IO_KAFKA_INT_H */
