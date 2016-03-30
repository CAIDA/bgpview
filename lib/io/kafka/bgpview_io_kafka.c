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

#include "config.h"
#include "bgpview_io_kafka_int.h"
#include "bgpview_io_kafka_codec.h"
#include "bgpview.h"
#include "utils.h"
#include <assert.h>
#include <errno.h>
#include <librdkafka/rdkafka.h>
#include <string.h>
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif


/* ========== PUBLIC FUNCTIONS ========== */

bgpview_io_kafka_t *bgpview_io_kafka_init(bgpview_io_kafka_mode_t mode)
{
  bgpview_io_kafka_t *client;
  if((client = malloc_zero(sizeof(bgpview_io_kafka_t))) == NULL)
    {
      return NULL;
    }

  client->mode = mode;

  if((client->brokers =
      strdup(BGPVIEW_IO_KAFKA_BROKER_URI_DEFAULT)) == NULL)
    {
      fprintf(stderr, "Failed to duplicate kafka server uri string\n");
      goto err;
    }

  if((client->pfxs_topic =
      strdup(BGPVIEW_IO_KAFKA_PFXS_TOPIC_DEFAULT)) == NULL)
    {
      fprintf(stderr,
              "Failed to duplicate kafka prefixes topic string\n");
      goto err;
    }

  if((client->peers_topic =
      strdup(BGPVIEW_IO_KAFKA_PEERS_TOPIC_DEFAULT)) == NULL)
    {
      fprintf(stderr, "Failed to duplicate kafka peers topic string\n");
      goto err;
    }

  if((client->metadata_topic =
      strdup(BGPVIEW_IO_KAFKA_METADATA_TOPIC_DEFAULT)) == NULL)
    {
      fprintf(stderr, "Failed to duplicate kafka metadata topic string\n");
      goto err;
    }


  client->peers_partition =
    BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT;
  client->metadata_partition =
    BGPVIEW_IO_KAFKA_METADATA_PARTITION_DEFAULT;

  client->max_diffs =
    BGPVIEW_IO_KAFKA_SYNC_FREQUENCY-1;

  return client;

 err:
  bgpview_io_kafka_destroy(client);
  return NULL;
}

void bgpview_io_kafka_destroy(bgpview_io_kafka_t *client)
{
  if (client == NULL)
    {
      return;
    }

  free(client->brokers);
  client->brokers = NULL;

  free(client->pfxs_topic);
  client->pfxs_topic = NULL;

  free(client->peers_topic);
  client->peers_topic = NULL;

  free(client->metadata_topic);
  client->metadata_topic = NULL;

  if(client->peers_rkt != NULL)
    {
      rd_kafka_topic_destroy(client->peers_rkt);
      client->peers_rkt = NULL;
    }

  if(client->pfxs_rkt != NULL)
    {
      rd_kafka_topic_destroy(client->pfxs_rkt);
      client->pfxs_rkt = NULL;
    }

  if(client->metadata_rkt != NULL)
    {
      rd_kafka_topic_destroy(client->metadata_rkt);
      client->metadata_rkt = NULL;
    }

  if(client->rdk_conn != NULL)
    {
      rd_kafka_destroy(client->rdk_conn);
      client->rdk_conn = NULL;
    }

  free(client->peerid_map);
  client->peerid_map = NULL;
  client->peerid_map_alloc_cnt = 0;

  bgpview_iter_destroy(client->parent_view_it);
  client->parent_view_it = NULL;

  bgpview_destroy(client->parent_view);
  client->parent_view = NULL;

  free(client);
  return;
}

int bgpview_io_kafka_start(bgpview_io_kafka_t *client)
{
  switch(client->mode)
    {
    case BGPVIEW_IO_KAFKA_MODE_PRODUCER:
      if(bgpview_io_kafka_producer_connect(client) != 0 ||
         bgpview_io_kafka_producer_topic_connect(client, &client->metadata_rkt,
                                                 client->metadata_topic) != 0 ||
         bgpview_io_kafka_producer_topic_connect(client, &client->peers_rkt,
                                                 client->peers_topic) != 0 ||
         bgpview_io_kafka_producer_topic_connect(client, &client->pfxs_rkt,
                                                 client->pfxs_topic) != 0)
        {
          goto err;
        }
      break;

    case BGPVIEW_IO_KAFKA_MODE_CONSUMER:
      if(bgpview_io_kafka_consumer_connect(client) != 0 ||
         bgpview_io_kafka_consumer_topic_connect(client, &client->metadata_rkt,
                                                 client->metadata_topic) != 0 ||
         bgpview_io_kafka_consumer_topic_connect(client, &client->peers_rkt,
                                                 client->peers_topic) != 0 ||
         bgpview_io_kafka_consumer_topic_connect(client, &client->pfxs_rkt,
                                                 client->pfxs_topic) != 0)
        {
          goto err;
        }
      break;
    }

  return 0;

 err:
  return -1;

}

int bgpview_io_kafka_set_broker_addresses(bgpview_io_kafka_t *client,
                                          const char *addresses)
{
  if(client->brokers != NULL)
    {
      free(client->brokers);
    }

  if((client->brokers = strdup(addresses)) == NULL)
    {
      fprintf(stderr, "Could not set broker addresses\n");
      return -1;
    }

  return 0;
}

void bgpview_io_kafka_set_sync_frequency(bgpview_io_kafka_t *client,
                                         int frequency)
{
  client->max_diffs = frequency-1;
}

int bgpview_io_kafka_set_pfxs_topic(bgpview_io_kafka_t *client,
                                    const char *topic)
{
  if(client->pfxs_topic != NULL)
    {
      free(client->pfxs_topic);
    }

  if((client->pfxs_topic = strdup(topic)) == NULL)
    {
      fprintf(stderr, "Could not set prefixes topic\n");
      return -1;
    }

  return 0;
}

int bgpview_io_kafka_set_peers_topic(bgpview_io_kafka_t *client,
                                     const char *topic)
{
  if(client->peers_topic != NULL)
    free(client->peers_topic);

  if((client->peers_topic = strdup(topic)) == NULL)
    {
      fprintf(stderr, "Could not set peer topic\n");
      return -1;
    }

  return 0;
}

int bgpview_io_kafka_set_metadata_topic(bgpview_io_kafka_t *client,
                                        const char *topic)
{
  if(client->metadata_topic != NULL)
    free(client->metadata_topic);

  if((client->metadata_topic = strdup(topic)) == NULL)
    {
      fprintf(stderr, "Could not set metadata topic\n");
      return -1;
    }

  return 0;
}

void bgpview_io_kafka_set_pfxs_partition(bgpview_io_kafka_t *client,
                                         int partition)
{
  client->pfxs_partition = partition;
}

void bgpview_io_kafka_set_peers_partition(bgpview_io_kafka_t *client,
                                          int partition)
{
  client->peers_partition = partition;
}

void bgpview_io_kafka_set_metadata_partition(bgpview_io_kafka_t *client,
                                             int partition)
{
  client->metadata_partition = partition;
}


int bgpview_io_kafka_send_view(bgpview_io_kafka_t *client,
                               bgpview_io_kafka_stats_t *stats,
                               bgpview_t *view,
                               bgpview_io_filter_cb_t *cb)
{

  return bgpview_io_kafka_send(client, stats, view, cb);
}

int bgpview_io_kafka_recv_view(bgpview_io_kafka_t *client,
                               bgpview_t *view,
                               bgpview_io_filter_peer_cb_t *peer_cb,
                               bgpview_io_filter_pfx_cb_t *pfx_cb,
                               bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)

{
  return bgpview_io_kafka_recv(client, view, peer_cb, pfx_cb, pfx_peer_cb);
}
