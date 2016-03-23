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

#ifndef __BGPVIEW_IO_KAFKA_CODEC_H
#define __BGPVIEW_IO_KAFKA_CODEC_H

#include "bgpview_io_kafka_int.h"
#include <librdkafka/rdkafka.h>

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
int bgpview_io_kafka_send(kafka_data_t dest,
                          kafka_view_data_t *view_data,
                          bgpview_t *view,
                          kafka_performance_t *metrics,
                          bgpview_io_filter_cb_t *cb);

/** Receive a view from the given socket
 *
 * @param src           information about broker to find metadata about views
 * @param view          pointer to the clear/new view to receive into
 * @return pointer to the view instance received, NULL if an error occurred.
 */
int bgpview_io_kafka_recv(kafka_data_t *src,
                          kafka_view_data_t *view_data,
                          bgpview_t *view,
                          bgpview_io_filter_peer_cb_t *peer_cb,
                          bgpview_io_filter_pfx_cb_t *pfx_cb,
                          bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

#endif /* __BGPVIEW_IO_KAFKA_CODEC_H */
