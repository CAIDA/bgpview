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

#include "bgpview.h"
#include "bgpview_io_kafka_int.h"
#include "config.h"
#include "utils.h"
#include <assert.h>
#include <errno.h>
#include <librdkafka/rdkafka.h>
#include <string.h>
#include <unistd.h>
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

/* ========== PRIVATE FUNCTIONS ========== */

static void kafka_error_callback(rd_kafka_t *rk, int err,
                                 const char *reason,
                                 void *opaque)
{
  bgpview_io_kafka_t *client = (bgpview_io_kafka_t *)opaque;

  switch (err) {
    // fatal errors:
  case RD_KAFKA_RESP_ERR__BAD_COMPRESSION:
  case RD_KAFKA_RESP_ERR__RESOLVE:
    client->fatal_error = 1;
    // fall through

    // recoverable? errors:
  case RD_KAFKA_RESP_ERR__DESTROY:
  case RD_KAFKA_RESP_ERR__FAIL:
  case RD_KAFKA_RESP_ERR__TRANSPORT:
  case RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN:
    client->connected = 0;
    break;
  }

  fprintf(stderr, "ERROR: %s (%d): %s\n", rd_kafka_err2str(err), err, reason);

  // TODO: handle other errors
}

static int kafka_topic_connect(bgpview_io_kafka_t *client)
{
  typedef int (topic_connect_func_t)(bgpview_io_kafka_t *client,
                                     rd_kafka_topic_t **rkt,
                                     char *topic);

  topic_connect_func_t *topic_connect_funcs[] = {
    // BGPVIEW_IO_KAFKA_MODE_CONSUMER
    bgpview_io_kafka_consumer_topic_connect,

    // BGPVIEW_IO_KAFKA_MODE_PRODUCER
    bgpview_io_kafka_producer_topic_connect,
  };

  fprintf(stderr, "INFO: Checking topic connections...\n");

  if ((client->metadata_rkt == NULL &&
       topic_connect_funcs[client->mode](client, &client->metadata_rkt,
                                         client->metadata_topic) != 0)) {
    goto err;
  }
  if ((client->peers_rkt == NULL &&
       topic_connect_funcs[client->mode](client, &client->peers_rkt,
                                         client->peers_topic) != 0)) {
    goto err;
  }
  if ((client->pfxs_rkt == NULL &&
       topic_connect_funcs[client->mode](client, &client->pfxs_rkt,
                                         client->pfxs_topic) != 0)) {
    goto err;
  }

  return 0;

 err:
  return -1;
}

/* ========== PROTECTED FUNCTIONS ========== */

int bgpview_io_kafka_common_config(bgpview_io_kafka_t *client,
                                   rd_kafka_conf_t *conf)
{
  char errstr[512];

  // Set the opaque pointer that will be passed to callbacks
  rd_kafka_conf_set_opaque(conf, client);

  // Set our error handler
  rd_kafka_conf_set_error_cb(conf, kafka_error_callback);

  // Disable logging of connection close/idle timeouts caused by Kafka 0.9.x
  //   See https://github.com/edenhill/librdkafka/issues/437 for more details.
  // TODO: change this when librdkafka has better handling of idle disconnects
  if (rd_kafka_conf_set(conf, "log.connection.close", "false", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }

  return 0;

 err:
  return -1;
}

/* ========== PUBLIC FUNCTIONS ========== */

bgpview_io_kafka_t *bgpview_io_kafka_init(bgpview_io_kafka_mode_t mode)
{
  bgpview_io_kafka_t *client;
  if ((client = malloc_zero(sizeof(bgpview_io_kafka_t))) == NULL) {
    return NULL;
  }

  client->mode = mode;

  if ((client->brokers = strdup(BGPVIEW_IO_KAFKA_BROKER_URI_DEFAULT)) == NULL) {
    fprintf(stderr, "Failed to duplicate kafka server uri string\n");
    goto err;
  }

  if ((client->pfxs_topic = strdup(BGPVIEW_IO_KAFKA_PFXS_TOPIC_DEFAULT)) ==
      NULL) {
    fprintf(stderr, "Failed to duplicate kafka prefixes topic string\n");
    goto err;
  }

  if ((client->peers_topic = strdup(BGPVIEW_IO_KAFKA_PEERS_TOPIC_DEFAULT)) ==
      NULL) {
    fprintf(stderr, "Failed to duplicate kafka peers topic string\n");
    goto err;
  }

  if ((client->metadata_topic =
         strdup(BGPVIEW_IO_KAFKA_METADATA_TOPIC_DEFAULT)) == NULL) {
    fprintf(stderr, "Failed to duplicate kafka metadata topic string\n");
    goto err;
  }

  return client;

err:
  bgpview_io_kafka_destroy(client);
  return NULL;
}

void bgpview_io_kafka_destroy(bgpview_io_kafka_t *client)
{
  if (client == NULL) {
    return;
  }

  int drain_wait_cnt = 12;
  while (rd_kafka_outq_len(client->rdk_conn) > 0 && drain_wait_cnt > 0) {
    fprintf(stderr,
            "INFO: Waiting for Kafka queue to drain (currently %d messages)\n",
            rd_kafka_outq_len(client->rdk_conn));
    rd_kafka_poll(client->rdk_conn, 5000);
    drain_wait_cnt--;
  }

  free(client->brokers);
  client->brokers = NULL;

  free(client->pfxs_topic);
  client->pfxs_topic = NULL;

  free(client->peers_topic);
  client->peers_topic = NULL;

  free(client->metadata_topic);
  client->metadata_topic = NULL;

  if (client->peers_rkt != NULL) {
    rd_kafka_topic_destroy(client->peers_rkt);
    client->peers_rkt = NULL;
  }

  if (client->pfxs_rkt != NULL) {
    rd_kafka_topic_destroy(client->pfxs_rkt);
    client->pfxs_rkt = NULL;
  }

  if (client->metadata_rkt != NULL) {
    rd_kafka_topic_destroy(client->metadata_rkt);
    client->metadata_rkt = NULL;
  }

  if (client->rdk_conn != NULL) {
    rd_kafka_destroy(client->rdk_conn);
    client->rdk_conn = NULL;
  }

  free(client->peerid_map);
  client->peerid_map = NULL;
  client->peerid_map_alloc_cnt = 0;

  free(client);
  return;
}

typedef int (kafka_connect_func_t)(bgpview_io_kafka_t *client);

static kafka_connect_func_t *kafka_connect_funcs[] = {
  // BGPVIEW_IO_KAFKA_MODE_CONSUMER
  bgpview_io_kafka_consumer_connect,

  // BGPVIEW_IO_KAFKA_MODE_PRODUCER
  bgpview_io_kafka_producer_connect,
};

int bgpview_io_kafka_start(bgpview_io_kafka_t *client)
{
  int wait = 10;
  int connect_retries = BGPVIEW_IO_KAFKA_CONNECT_MAX_RETRIES;

  while (client->connected == 0 && connect_retries > 0) {
    if (kafka_connect_funcs[client->mode](client) != 0) {
      goto err;
    }

    connect_retries--;
    if (client->connected == 0 && connect_retries > 0) {
      fprintf(stderr,
              "WARN: Failed to connect to Kafka. Retrying in %d seconds\n",
              wait);
      sleep(wait);
      wait *= 2;
      if (wait > 180) {
        wait = 180;
      }
    }
  }

  if (client->connected == 0) {
    fprintf(stderr,
            "ERROR: Failed to connect to Kafka after %d retries. Giving up\n",
            BGPVIEW_IO_KAFKA_CONNECT_MAX_RETRIES);
    goto err;
  }

  return 0;

err:
  return -1;
}

int bgpview_io_kafka_set_broker_addresses(bgpview_io_kafka_t *client,
                                          const char *addresses)
{
  if (client->brokers != NULL) {
    free(client->brokers);
  }

  if ((client->brokers = strdup(addresses)) == NULL) {
    fprintf(stderr, "Could not set broker addresses\n");
    return -1;
  }

  return 0;
}

int bgpview_io_kafka_set_pfxs_topic(bgpview_io_kafka_t *client,
                                    const char *topic)
{
  if (client->pfxs_topic != NULL) {
    free(client->pfxs_topic);
  }

  if ((client->pfxs_topic = strdup(topic)) == NULL) {
    fprintf(stderr, "Could not set prefixes topic\n");
    return -1;
  }

  return 0;
}

int bgpview_io_kafka_set_peers_topic(bgpview_io_kafka_t *client,
                                     const char *topic)
{
  if (client->peers_topic != NULL)
    free(client->peers_topic);

  if ((client->peers_topic = strdup(topic)) == NULL) {
    fprintf(stderr, "Could not set peer topic\n");
    return -1;
  }

  return 0;
}

int bgpview_io_kafka_set_metadata_topic(bgpview_io_kafka_t *client,
                                        const char *topic)
{
  if (client->metadata_topic != NULL)
    free(client->metadata_topic);

  if ((client->metadata_topic = strdup(topic)) == NULL) {
    fprintf(stderr, "Could not set metadata topic\n");
    return -1;
  }

  return 0;
}

int bgpview_io_kafka_send_view(bgpview_io_kafka_t *client,
                               bgpview_t *view,
                               bgpview_t *parent_view,
                               bgpview_io_filter_cb_t *cb)
{
  // first, ensure all topics are connected
  if (kafka_topic_connect(client) != 0) {
    return -1;
  }
  return bgpview_io_kafka_producer_send(client, view, parent_view, cb);
}

int bgpview_io_kafka_recv_view(bgpview_io_kafka_t *client, bgpview_t *view,
                               bgpview_io_filter_peer_cb_t *peer_cb,
                               bgpview_io_filter_pfx_cb_t *pfx_cb,
                               bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)

{
  // first, ensure all topics are connected
  if (kafka_topic_connect(client) != 0) {
    return -1;
  }
  return bgpview_io_kafka_consumer_recv(client, view, peer_cb, pfx_cb,
                                        pfx_peer_cb);
}

bgpview_io_kafka_stats_t *
bgpview_io_kafka_get_stats(bgpview_io_kafka_t *client)
{
  return &client->stats;
}
