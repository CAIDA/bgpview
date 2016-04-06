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
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#define BUFFER_LEN 16384

static int add_peerid_mapping(bgpview_io_kafka_t *client, bgpview_iter_t *it,
                              bgpstream_peer_sig_t *sig,
                              bgpstream_peer_id_t remote_id)
{
  int j;
  bgpstream_peer_id_t local_id;

  /* first, is the array big enough to possibly already contain remote_id? */
  if (remote_id >= client->dc_state.peerid_map_alloc_cnt) {
    if ((client->dc_state.peerid_map =
           realloc(client->dc_state.peerid_map,
                   sizeof(bgpstream_peer_id_t) * (remote_id + 1))) == NULL) {
      return -1;
    }

    /* now set all ids to 0 (reserved) */
    for (j = client->dc_state.peerid_map_alloc_cnt; j <= remote_id; j++) {
      client->dc_state.peerid_map[j] = 0;
    }
    client->dc_state.peerid_map_alloc_cnt = remote_id + 1;
  }

  /* do we need to add this peer */
  if (client->dc_state.peerid_map[remote_id] == 0) {
    if ((local_id = bgpview_iter_add_peer(
           it, sig->collector_str, (bgpstream_ip_addr_t *)&sig->peer_ip_addr,
           sig->peer_asnumber)) == 0) {
      return -1;
    }
    bgpview_iter_activate_peer(it);
    client->dc_state.peerid_map[remote_id] = local_id;
  }

  /* by here we are guaranteed to have a valid mapping */
  return client->dc_state.peerid_map[remote_id];
}

static void clear_peerid_mapping(bgpview_io_kafka_t *client)
{
  memset(client->dc_state.peerid_map, 0,
         sizeof(bgpstream_peer_id_t) * client->dc_state.peerid_map_alloc_cnt);
}

static int seek_topic(rd_kafka_topic_t *rkt, int32_t partition, int64_t offset)
{
  int err;

  if ((err = rd_kafka_seek(rkt, partition, offset, 1000)) != 0) {
    fprintf(stderr, "consume_seek(%s, %d, %" PRIu64 ") failed: %s\n",
            rd_kafka_topic_name(rkt), partition, offset, rd_kafka_err2str(err));
    return -1;
  }

  return 0;
}

static int deserialize_metadata(bgpview_io_kafka_md_t *meta,
                                uint8_t *buf,
                                size_t len)
{
  size_t read = 0;

  uint16_t ident_len;

  /* Deserialize the common metadata header */

  /* Identity */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, ident_len);
  assert((len-read) >= ident_len);
  memcpy(meta->identity, buf, ident_len);
  meta->identity[ident_len] = '\0';
  buf += ident_len;
  read += ident_len;

  /* Time */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->time);

  /* Prefixes offset */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->pfxs_offset);

  /* Peers offset (not partition for peers) */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->peers_offset);

  /* Dump type */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->type);

  switch (meta->type) {
  case 'S':
    /* nothing extra for a sync frame */

    /** This field is meaningless since this **is** a sync frame */
    meta->sync_md_offset = RD_KAFKA_OFFSET_END;

    /** Also meaningless as a sync frame has no parent */
    meta->parent_time = meta->time;
    break;

  case 'D':
    /** Offset of most recent sync frame */
    BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->sync_md_offset);

    /** Time of the parent view */
    BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->parent_time);
    break;

  default:
    goto err;
  }

  return 0;

err:
  return -1;
}

/* ==========END SUPPORT FUNCTIONS ========== */

/* ==========START SEND/RECEIVE FUNCTIONS ========== */

static int recv_metadata(bgpview_io_kafka_t *client, bgpview_t *view,
                         bgpview_io_kafka_md_t *meta)
{
  rd_kafka_message_t *msg = NULL;

again:
  /* Grab the last metadata message */
  if ((msg = rd_kafka_consume(RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_META),
                              BGPVIEW_IO_KAFKA_METADATA_PARTITION_DEFAULT,
                              2000000000)) == NULL) {
    goto err;
  }

  if (msg->payload == NULL) {
    if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
      rd_kafka_message_destroy(msg);
      msg = NULL;
      goto again;
    }
    /* TODO: handle this failure -- maybe reconnect? */
    fprintf(stderr, "ERROR: Could not consume metadata message\n");
    goto err;
  }

  /* extract the information from the message */
  if (deserialize_metadata(meta, msg->payload, msg->len) != 0) {
    fprintf(stderr, "ERROR: Could not deserialize metadata message\n");
    goto err;
  }
  /* we're done with this message */
  rd_kafka_message_destroy(msg);
  msg = NULL;

  /* Can we use this view? */
  // these asserts are anywhere the code assumes direct consumers...
  assert(client->mode == BGPVIEW_IO_KAFKA_MODE_DIRECT_CONSUMER);
  if (strncmp(meta->identity, client->identity, IDENTITY_MAX_LEN) != 0) {
    fprintf(stderr,
            "INFO: Skipping view from producer '%s' (looking for '%s')\n",
            meta->identity, client->identity);
    goto again;
  }
  if (meta->type != 'S' && meta->parent_time != bgpview_get_time(view)) {
    /* this is a diff frame with a parent time that does not match the time of
       the view that we are given */
    fprintf(stderr, "WARN: Found Diff frame against %d, but view time is %d\n",
            meta->parent_time, bgpview_get_time(view));
    fprintf(stderr, "INFO: Rewinding to last sync frame\n");
    if (seek_topic(RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_META),
                   BGPVIEW_IO_KAFKA_METADATA_PARTITION_DEFAULT,
                   meta->sync_md_offset) != 0) {
      fprintf(stderr, "ERROR: Could not seek to last sync metadata\n");
      goto err;
    }
    goto again;
  }

  /* We can use this metadata! */

  /* if it is a Sync frame we need to clean up the view that we were given, and
     also our peer mapping */
  if (meta->type == 'S') {
    bgpview_clear(view);
    clear_peerid_mapping(client);
  }

  assert(msg == NULL);
  return 0;

err:
  if (msg != NULL) {
    rd_kafka_message_destroy(msg);
  }
  return -1;
}

static int recv_peers(bgpview_io_kafka_t *client, bgpview_iter_t *iter,
                      bgpview_io_filter_peer_cb_t *peer_cb, int64_t offset,
                      uint32_t exp_time)
{
  rd_kafka_message_t *msg = NULL;
  size_t read = 0;
  ssize_t s;
  uint8_t *ptr;
  char type;
  uint16_t peer_cnt;
  uint32_t vtime;

  bgpstream_peer_id_t peerid_remote;
  bgpstream_peer_sig_t ps;

  int peers_rx = 0;
  int filter;

  if (seek_topic(RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_PEERS),
                 BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT,
                 offset) != 0) {
    fprintf(stderr, "Error changing the offset");
    goto err;
  }

  /* receive the peers */
  while (1) {
    msg = rd_kafka_consume(RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_PEERS),
                           BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT, 1000);
    if (msg->payload == NULL) {
      fprintf(stderr, "Cannot not receive peer message\n");
      goto err;
    }
    ptr = msg->payload;
    read = 0;

    BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, type);

    if (type == 'E') {
      /* end of peers */
      BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, vtime);
      assert(vtime == exp_time);
      BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, peer_cnt);
      assert(peers_rx == peer_cnt);

      rd_kafka_message_destroy(msg);
      msg = NULL;
      break;
    }

    assert(type == 'P');

    if ((s = bgpview_io_deserialize_peer(ptr, msg->len, &peerid_remote, &ps)) <
        0) {
      goto err;
    }
    read += s;
    ptr += s;

    rd_kafka_message_destroy(msg);
    msg = NULL;

    peers_rx++;

    if (iter == NULL) {
      continue;
    }

    if (peer_cb != NULL) {
      /* ask the caller if they want this peer */
      if ((filter = peer_cb(&ps)) < 0) {
        goto err;
      }
      if (filter == 0) {
        continue;
      }
    }
    /* all code below here has a valid view */

    if (add_peerid_mapping(client, iter, &ps, peerid_remote) <= 0) {
      goto err;
    }
  }

  assert(msg == NULL);
  return 0;

err:
  if (msg != NULL) {
    rd_kafka_message_destroy(msg);
  }
  return -1;
}

static int recv_pfxs(bgpview_io_kafka_t *client, bgpview_iter_t *iter,
                     bgpview_io_filter_pfx_cb_t *pfx_cb,
                     bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
                     int64_t offset, uint32_t exp_time)
{
  bgpview_t *view = NULL;
  uint32_t view_time;

  size_t read = 0;
  uint8_t *ptr;
  ssize_t s;

  char type;

  uint32_t pfx_cnt = 0;
  uint32_t cell_cnt = 0;
  int pfx_rx = 0;

  int tor = 0;
  int tom = 0;

  rd_kafka_message_t *msg = NULL;

  if (seek_topic(RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_PFXS),
                 BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT,
                 offset) != 0) {
    fprintf(stderr, "Error changing the offset");
    goto err;
  }

  if (iter != NULL) {
    view = bgpview_iter_get_view(iter);
  }

  while (1) {
    msg = rd_kafka_consume(RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_PFXS),
                           BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT, 1000);
    if (msg->payload == NULL) {
      fprintf(stderr, "Cannot receive prefixes and paths\n");
      goto err;
    }

    ptr = msg->payload;
    read = 0;

    BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, type);

    if (type == 'E') {
      /* end of prefixes */
      BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, view_time);
      if (iter != NULL) {
        bgpview_set_time(view, view_time);
      }
      assert(view_time == exp_time);
      BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, pfx_cnt);
      assert(pfx_rx == pfx_cnt);
      BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, cell_cnt);
      assert(read == msg->len);

      rd_kafka_message_destroy(msg);
      msg = NULL;
      break;
    }

    /* this is a prefix row message */
    pfx_rx++;

    switch (type) {
    case 'U':
      /* an update row */
      tom++;
      if ((s = bgpview_io_deserialize_pfx_row(
             ptr, (msg->len - read), iter, pfx_cb, pfx_peer_cb,
             client->dc_state.peerid_map,
             client->dc_state.peerid_map_alloc_cnt, NULL, -1,
             BGPVIEW_FIELD_ACTIVE)) ==
          -1) {
        goto err;
      }
      read += s;
      ptr += s;
      break;

    case 'R':
      /* a remove row */
      tor++;
      if ((s = bgpview_io_deserialize_pfx_row(
             ptr, (msg->len - read), iter, pfx_cb, pfx_peer_cb,
             client->dc_state.peerid_map,
             client->dc_state.peerid_map_alloc_cnt, NULL, -1,
             BGPVIEW_FIELD_INACTIVE)) ==
          -1) {
        goto err;
      }
      read += s;
      ptr += s;
      break;
    }

    rd_kafka_message_destroy(msg);
    msg = NULL;
  }

  assert(msg == NULL);
  return 0;

err:
  if (msg != NULL) {
    rd_kafka_message_destroy(msg);
  }
  return -1;
}

static int recv_view(bgpview_io_kafka_t *client, bgpview_t *view,
                     bgpview_io_kafka_md_t *meta,
                     bgpview_io_filter_peer_cb_t *peer_cb,
                     bgpview_io_filter_pfx_cb_t *pfx_cb,
                     bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{
  bgpview_iter_t *it = NULL;

  if (view != NULL && (it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  if (recv_peers(client, it, peer_cb, meta->peers_offset, meta->time) < 0) {
    fprintf(stderr, "Could not receive peers\n");
    return -1;
  }

  if (recv_pfxs(client, it, pfx_cb, pfx_peer_cb, meta->pfxs_offset,
                meta->time) != 0) {
    fprintf(stderr, "Could not receive prefixes\n");
    goto err;
  }

  for (bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(it); bgpview_iter_next_peer(it)) {
    if (bgpview_iter_peer_get_pfx_cnt(it, 0, BGPVIEW_FIELD_ACTIVE) == 0 &&
        bgpview_iter_deactivate_peer(it) != 1) {
      fprintf(stderr, "Fail to deactivate peer\n");
      goto err;
    }
  }

  if (it != NULL) {
    bgpview_iter_destroy(it);
  }

  return 0;

err:
  if (it != NULL) {
    bgpview_iter_destroy(it);
  }
  return -1;
}

/* ==========END SEND/RECEIVE FUNCTIONS ========== */

/* ========== PROTECTED FUNCTIONS ========== */

int bgpview_io_kafka_consumer_connect(bgpview_io_kafka_t *client)
{
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  char errstr[512];

  // Create Kafka handle
  if ((client->rdk_conn = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr,
                                       sizeof(errstr))) == NULL) {
    fprintf(stderr, "ERROR: Failed to create new consumer: %s\n", errstr);
    goto err;
  }

  // Add brokers
  if (rd_kafka_brokers_add(client->rdk_conn, client->brokers) == 0) {
    fprintf(stderr, "ERROR: No valid brokers specified\n");
    goto err;
  }

  client->connected = 1;

  // poll for connection errors
  rd_kafka_poll(client->rdk_conn, 5000);

  return client->fatal_error;

 err:
  return -1;
}

int bgpview_io_kafka_consumer_topic_connect(bgpview_io_kafka_t *client,
                                            rd_kafka_topic_t **rkt, char *topic)
{
  if ((*rkt = rd_kafka_topic_new(client->rdk_conn, topic, NULL)) == NULL) {
    return -1;
  }

  if (rd_kafka_consume_start(*rkt, 0, RD_KAFKA_OFFSET_TAIL(1)) == -1) {
    fprintf(stderr, "ERROR: Failed to start consuming: %s\n",
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    return -1;
  }

  return 0;
}

int bgpview_io_kafka_consumer_recv(bgpview_io_kafka_t *client, bgpview_t *view,
                                   bgpview_io_filter_peer_cb_t *peer_cb,
                                   bgpview_io_filter_pfx_cb_t *pfx_cb,
                                   bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{
  bgpview_io_kafka_md_t meta;

  /* find the view that we will receive */
  if (recv_metadata(client, view, &meta) != 0) {
    return -1;
  }

  return recv_view(client, view, &meta, peer_cb, pfx_cb, pfx_peer_cb);
}
