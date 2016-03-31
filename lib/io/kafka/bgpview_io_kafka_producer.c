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

static int64_t get_offset(bgpview_io_kafka_t *client, char *topic,
                          int32_t partition)
{
  int64_t low = 0;
  int64_t high = 0;

  if (rd_kafka_query_watermark_offsets(client->rdk_conn, topic, partition, &low,
                                       &high,
                                       10000) == RD_KAFKA_RESP_ERR_NO_ERROR) {
    return high;
  }

  return -1;
}

static int pfx_row_serialize(uint8_t *buf, size_t len, char operation,
                             bgpview_iter_t *it, bgpview_io_filter_cb_t *cb)
{
  size_t written = 0;
  ssize_t s;

  // serialize the operation that must be done with this row
  // "Update" or "Remove"
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, operation);

  switch (operation) {
  case 'U':
    if ((s = bgpview_io_serialize_pfx_row(buf, (len - written), it, cb, 0)) ==
        -1) {
      goto err;
    }
    written += s;
    buf += s;
    break;

  case 'R':
    /* simply serialize the prefix */
    if ((s = bgpview_io_serialize_pfx(buf, (len - written),
                                      bgpview_iter_pfx_get_pfx(it))) == -1) {
      goto err;
    }
    written += s;
    buf += s;
    break;
  }

  return written;

err:
  return -1;
}

static int diff_paths(bgpview_iter_t *parent_view_it, bgpview_iter_t *itC)
{
  bgpstream_as_path_store_path_id_t idxH =
    bgpview_iter_pfx_peer_get_as_path_store_path_id(parent_view_it);
  bgpstream_as_path_store_path_id_t idxC =
    bgpview_iter_pfx_peer_get_as_path_store_path_id(itC);

  return bcmp(&idxH, &idxC, sizeof(bgpstream_as_path_store_path_id_t));
}

static int diff_rows(bgpview_iter_t *parent_view_it, bgpview_iter_t *itC)
{
  int npeersH =
    bgpview_iter_pfx_get_peer_cnt(parent_view_it, BGPVIEW_FIELD_ACTIVE);
  int npeersC = bgpview_iter_pfx_get_peer_cnt(itC, BGPVIEW_FIELD_ACTIVE);

  if (npeersH != npeersC)
    return 1;

  bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itC);
  bgpstream_peer_id_t peerid;

  for (bgpview_iter_pfx_first_peer(itC, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_pfx_has_more_peer(itC); bgpview_iter_pfx_next_peer(itC)) {
    peerid = bgpview_iter_peer_get_peer_id(itC);
    if (bgpview_iter_seek_pfx_peer(parent_view_it, pfx, peerid,
                                   BGPVIEW_FIELD_ACTIVE,
                                   BGPVIEW_FIELD_ACTIVE) == 1 ||
        diff_paths(parent_view_it, itC) == 1) {
      return 1;
    }
  }
  return 0;
}

/* ==========END SUPPORT FUNCTIONS ========== */

/* ==========START SEND/RECEIVE FUNCTIONS ========== */

static int send_metadata(bgpview_io_kafka_t *client,
                         bgpview_io_kafka_md_t *meta)
{
  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t len = BUFFER_LEN;
  size_t written = 0;

  /* Serialize the common metadata header */

  /* Time */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->time);

  /* Prefixes offset */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->pfxs_offset);

  /* Peers offset (no partitions for peers) */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->peers_offset);

  /* Serialize the dump type ('S' -> Sync, or 'D' -> Diff) */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->type);

  /* now serialize info specific to to the dump type */
  switch (meta->type) {
  case 'S':
    /* nothing additional */
    break;

  case 'D':
    /** Offset of most recent sync frame */
    BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->sync_md_offset);

    /** Time of the parent view */
    BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->parent_time);
    break;

  default:
    goto err;
  }

  if (rd_kafka_produce(
        client->metadata_rkt, BGPVIEW_IO_KAFKA_METADATA_PARTITION_DEFAULT,
        RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1) {
    fprintf(stderr, "ERROR: Failed to produce to topic %s partition %i: %s\n",
            rd_kafka_topic_name(client->metadata_rkt),
            BGPVIEW_IO_KAFKA_METADATA_PARTITION_DEFAULT,
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    rd_kafka_poll(client->rdk_conn, 0);
    goto err;
  }

  /* Wait for messages to be delivered */
  while (rd_kafka_outq_len(client->rdk_conn) > 0) {
    rd_kafka_poll(client->rdk_conn, 100);
  }

  return 0;

err:
  return -1;
}

static int send_peers(bgpview_io_kafka_t *client, bgpview_io_kafka_md_t *meta,
                      bgpview_iter_t *it, bgpview_t *view,
                      bgpview_io_filter_cb_t *cb)
{
  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t len = BUFFER_LEN;
  ssize_t written = 0;

  char type;

  uint16_t peers_tx = 0;
  int filter;

  /* find our current offset and update the metadata */
  if ((meta->peers_offset =
         get_offset(client, client->peers_topic,
                    BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT)) < 0) {
    fprintf(stderr, "ERROR: Could not get peer offset\n");
    goto err;
  }

  for (bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(it); bgpview_iter_next_peer(it)) {
    /* if we are sending a diff, only send if this peer is not in our
       reference diff */
    if (meta->type == 'D' &&
        bgpview_iter_seek_peer(client->parent_view_it,
                               bgpview_iter_peer_get_peer_id(it),
                               BGPVIEW_FIELD_ACTIVE) == 1) {
      continue;
    }

    if (cb != NULL) {
      /* ask the caller if they want this peer */
      if ((filter = cb(it, BGPVIEW_IO_FILTER_PEER)) < 0) {
        goto err;
      }
      if (filter == 0) {
        continue;
      }
    }
    /* past here means this peer is being sent */
    peers_tx++;

    written = 0;
    ptr = buf;

    type = 'P';
    BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, type);

    if ((written = bgpview_io_serialize_peer(
           ptr, (len - written), bgpview_iter_peer_get_peer_id(it),
           bgpview_iter_peer_get_sig(it))) < 0) {
      goto err;
    }

    if (rd_kafka_produce(
          client->peers_rkt, BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT,
          RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1) {
      fprintf(stderr, "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(client->peers_rkt),
              BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(client->rdk_conn, 0);
      goto err;
    }
  }

  written = 0;
  ptr = buf;

  /* End message */
  type = 'E';
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, type);
  /* View time */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->time);
  /* Peer Count */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, peers_tx);

  if (rd_kafka_produce(
        client->peers_rkt, BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT,
        RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1) {
    fprintf(stderr, "ERROR: Failed to produce to topic %s partition %i: %s\n",
            rd_kafka_topic_name(client->peers_rkt),
            BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT,
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    rd_kafka_poll(client->rdk_conn, 0);
    goto err;
  }

  while (rd_kafka_outq_len(client->rdk_conn) > 0) {
    rd_kafka_poll(client->rdk_conn, 100);
  }

  return 0;

err:
  return -1;
}

static int send_pfxs(bgpview_io_kafka_t *client, bgpview_io_kafka_md_t *meta,
                     bgpview_io_kafka_stats_t *stats, bgpview_iter_t *it,
                     bgpview_io_filter_cb_t *cb)
{
  int filter;

  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t len = BUFFER_LEN;
  size_t written = 0;
  ssize_t s = 0;

  bgpview_t *view = bgpview_iter_get_view(it);
  assert(view != NULL);
  bgpstream_pfx_t *pfx;

  int send;
  int pfxs_tx = 0;

  int pfxs_cnt_ref;
  int pfxs_cnt_cur;

  /* reset the stats */
  memset(stats, 0, sizeof(bgpview_io_kafka_stats_t));

  /* find our current offset and update the metadata */
  if ((meta->pfxs_offset =
         get_offset(client, client->pfxs_topic,
                    BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT)) < 0) {
    fprintf(stderr, "ERROR: Could not get prefix offset\n");
    goto err;
  }

  if (meta->type == 'D') {
    pfxs_cnt_ref = bgpview_pfx_cnt(client->parent_view, BGPVIEW_FIELD_ACTIVE);
    pfxs_cnt_cur = bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE);
  }

  for (bgpview_iter_first_pfx(it, 0, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {
    /* reset the buffer */
    written = 0;
    ptr = buf;
    send = 1;

    if (cb != NULL) {
      if ((filter = cb(it, BGPVIEW_IO_FILTER_PFX)) < 0) {
        goto err;
      }
      if (filter == 0) {
        continue;
      }
    }

    if (meta->type == 'D') {
      pfx = bgpview_iter_pfx_get_pfx(it);
      if (bgpview_iter_seek_pfx(client->parent_view_it, pfx,
                                BGPVIEW_FIELD_ACTIVE) == 1) {
        stats->common_pfxs_cnt++;
        if (diff_rows(client->parent_view_it, it) == 1) {
          stats->changed_pfxs_cnt++;
          /* pfx has changed, send */
        } else {
          send = 0;
        }
      }
    }

    if (send == 0) {
      continue;
    }

    if ((s = pfx_row_serialize(ptr, len, 'U', it, cb)) < 0) {
      goto err;
    }
    written += s;
    ptr += s;

    pfxs_tx++;

    if (rd_kafka_produce(
          client->pfxs_rkt, BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT,
          RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1) {
      fprintf(stderr, "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(client->pfxs_rkt),
              BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      // Poll to handle delivery reports
      rd_kafka_poll(client->rdk_conn, 0);
      goto err;
    }
  }
  rd_kafka_poll(client->rdk_conn, 0);

  if (meta->type == 'D') {
    /* send removals */
    stats->removed_pfxs_cnt = pfxs_cnt_ref - stats->common_pfxs_cnt;
    assert(stats->removed_pfxs_cnt >= 0);

    fprintf(stderr, "DEBUG: HC %d CC %d common %d remove %d change %d\n",
            pfxs_cnt_ref, pfxs_cnt_cur, stats->common_pfxs_cnt,
            stats->removed_pfxs_cnt, stats->changed_pfxs_cnt);

    stats->added_pfxs_cnt =
      (pfxs_cnt_cur - stats->common_pfxs_cnt) + stats->removed_pfxs_cnt;
    if (stats->added_pfxs_cnt < 0) {
      stats->added_pfxs_cnt = 0;
    }

    stats->pfx_cnt = stats->common_pfxs_cnt + stats->added_pfxs_cnt;
    stats->sync_pfx_cnt = 0;

    if (stats->removed_pfxs_cnt > 0) {
      int remain = stats->removed_pfxs_cnt;
      for (bgpview_iter_first_pfx(client->parent_view_it, 0,
                                  BGPVIEW_FIELD_ACTIVE);
           bgpview_iter_has_more_pfx(client->parent_view_it);
           bgpview_iter_next_pfx(client->parent_view_it)) {
        written = 0;
        ptr = buf;

        pfx = bgpview_iter_pfx_get_pfx(client->parent_view_it);

        if (bgpview_iter_seek_pfx(it, pfx, BGPVIEW_FIELD_ACTIVE) != 0) {
          continue;
        }

        if ((s = pfx_row_serialize(ptr, (len - written), 'R',
                                   client->parent_view_it, cb)) < 0) {
          goto err;
        }
        written += s;
        ptr += s;

        remain--;
        pfxs_tx++;
        if (rd_kafka_produce(
              client->pfxs_rkt, BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT,
              RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1) {
          fprintf(stderr, "ERROR: Failed to produce to topic %s partition %i\n"
                          "Kafka Error: %s\n",
                  rd_kafka_topic_name(client->pfxs_rkt),
                  BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT,
                  rd_kafka_err2str(rd_kafka_errno2err(errno)));
          rd_kafka_poll(client->rdk_conn, 0);
          goto err;
        }

        /* stop looking if we have removed all we need to */
        if (remain == 0) {
          break;
        }
      }
    }
  } else {
    stats->added_pfxs_cnt = 0;
    stats->removed_pfxs_cnt = 0;
    stats->changed_pfxs_cnt = 0;
    stats->common_pfxs_cnt = 0;
    stats->pfx_cnt = stats->sync_pfx_cnt =
      bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE);
  }

  /* send the end message */
  written = 0;
  ptr = buf;
  char type = 'E';
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, type);
  /* Time */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->time);
  /* Prefix count */
  uint32_t pfx_cnt = bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE);
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, pfx_cnt);
  /* Cell count */
  /* @todo */

  if (rd_kafka_produce(
        client->pfxs_rkt, BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT,
        RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1) {
    fprintf(stderr, "ERROR: Failed to produce to topic %s partition %i: %s\n",
            rd_kafka_topic_name(client->pfxs_rkt),
            BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT,
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    // Poll to handle delivery reports
    rd_kafka_poll(client->rdk_conn, 0);
    return -1;
  }

  /* @todo figure out how to defer this until the connection gets shut down */
  while (rd_kafka_outq_len(client->rdk_conn) > 0) {
    rd_kafka_poll(client->rdk_conn, 100);
  }

  return 0;

err:
  return -1;
}

static int send_sync_view(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats, bgpview_t *view,
                          bgpview_io_filter_cb_t *cb)
{
  bgpview_iter_t *it = NULL;
  bgpview_io_kafka_md_t meta;

  if ((it = bgpview_iter_create(view)) == NULL) {
    goto err;
  }

  meta.time = bgpview_get_time(view);
  meta.type = 'S';

  if (send_peers(client, &meta, it, view, cb) != 0) {
    goto err;
  }
  if (send_pfxs(client, &meta, stats, it, cb) != 0) {
    goto err;
  }

  /* find the current metadata offset and update the sync info */
  if ((client->last_sync_offset =
         get_offset(client, client->metadata_topic,
                    BGPVIEW_IO_KAFKA_METADATA_PARTITION_DEFAULT)) < 0) {
    fprintf(stderr, "ERROR: Could not get metadata offset\n");
    goto err;
  }

  if (send_metadata(client, &meta) == -1) {
    fprintf(stderr, "Error publishing metadata\n");
    goto err;
  }

  bgpview_iter_destroy(it);

  client->num_diffs = 0;
  return 0;

err:
  return -1;
}

static int send_diff_view(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats, bgpview_t *view,
                          bgpview_io_filter_cb_t *cb)
{
  bgpview_iter_t *it = NULL;
  bgpview_io_kafka_md_t meta;

  if ((it = bgpview_iter_create(view)) == NULL) {
    goto err;
  }

  meta.time = bgpview_get_time(view);
  meta.type = 'D';

  assert(client->parent_view != NULL &&
         bgpview_get_time(client->parent_view) != 0);
  meta.parent_time = bgpview_get_time(client->parent_view);
  meta.sync_md_offset = client->last_sync_offset;

  if (send_peers(client, &meta, it, view, cb) == -1) {
    goto err;
  }

  if (send_pfxs(client, &meta, stats, it, cb) == -1) {
    goto err;
  }

  if (send_metadata(client, &meta) == -1) {
    fprintf(stderr, "Error on publishing the offset\n");
    goto err;
  }

  client->num_diffs++;

  bgpview_iter_destroy(it);

  return 0;

err:
  bgpview_iter_destroy(it);
  return -1;
}

/* ==========END SEND/RECEIVE FUNCTIONS ========== */

/* ========== PROTECTED FUNCTIONS ========== */

int bgpview_io_kafka_producer_connect(bgpview_io_kafka_t *client)
{
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  char errstr[512];

  if (rd_kafka_conf_set(conf, "queue.buffering.max.messages", "7000000", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }

  if (rd_kafka_conf_set(conf, "compression.codec", "snappy", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }

  if ((client->rdk_conn = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr,
                                       sizeof(errstr))) == NULL) {
    fprintf(stderr, "ERROR: Failed to create new producer: %s\n", errstr);
    goto err;
  }

  if (rd_kafka_brokers_add(client->rdk_conn, client->brokers) == 0) {
    fprintf(stderr, "ERROR: No valid brokers specified\n");
    goto err;
  }

  return 0;

err:
  return -1;
}

int bgpview_io_kafka_producer_topic_connect(bgpview_io_kafka_t *client,
                                            rd_kafka_topic_t **rkt, char *topic)
{
  if ((*rkt = rd_kafka_topic_new(client->rdk_conn, topic, NULL)) == NULL) {
    return -1;
  }

  return 0;
}

int bgpview_io_kafka_producer_send(bgpview_io_kafka_t *client,
                                   bgpview_io_kafka_stats_t *stats,
                                   bgpview_t *view, bgpview_io_filter_cb_t *cb)
{
  time_t rawtime;
  time_t end;
  int length;
  time_t start1;
  time_t start2;
  struct tm *timeinfo;

  time(&start1);

  if (client->parent_view == NULL || client->num_diffs == client->max_diffs) {
    if (send_sync_view(client, stats, view, cb) != 0) {
      goto err;
    }
  } else {
    if (send_diff_view(client, stats, view, cb) != 0) {
      goto err;
    }
  }

  time(&rawtime);
  timeinfo = localtime(&rawtime);
  time(&end);
  length = difftime(end, start1);
  stats->send_time = length;

  time(&rawtime);
  timeinfo = localtime(&rawtime);
  time(&start2);

  if ((client->parent_view == NULL &&
       (client->parent_view = bgpview_dup(view)) == NULL) ||
      bgpview_copy(client->parent_view, view) != 0) {
    fprintf(stderr, "ERROR: Could not copy view\n");
    goto err;
  }

  if (client->parent_view_it == NULL &&
      (client->parent_view_it = bgpview_iter_create(client->parent_view)) ==
        NULL) {
    goto err;
  }

  time(&rawtime);
  timeinfo = localtime(&rawtime);
  time(&end);
  length = difftime(end, start2);
  stats->copy_time = length;

  return 0;

err:
  return -1;
}
