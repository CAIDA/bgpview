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
#include "bgpview_io_kafka_codec.h"
#include "bgpview_io_kafka_int.h"
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

#define BUFFER_LEN 16384
#define BUFFER_1M  1048576

typedef struct view_md {

  /** The time of the the view */
  uint32_t time;

  /** The type of this view dump (S[ync]/D[iff]) */
  char type;

  /** Where to find the prefixes */
  int32_t pfxs_partition;
  int64_t pfxs_offset;

  /** Where to find the peers */
  int64_t peers_offset;

  /** Only populated in the case of a Diff: */
  /** Offset of the most recent sync frame */
  int64_t sync_md_offset;
  /** Time of the parent view */
  uint32_t parent_time;

} view_md_t;

static int add_peerid_mapping(bgpview_io_kafka_t *client,
                              bgpview_iter_t *it,
                              bgpstream_peer_sig_t *sig,
                              bgpstream_peer_id_t remote_id)
{
  int j;
  bgpstream_peer_id_t local_id;

  /* first, is the array big enough to possibly already contain remote_id? */
  if (remote_id >= client->peerid_map_alloc_cnt)
    {
      if ((client->peerid_map =
           realloc(client->peerid_map,
                   sizeof(bgpstream_peer_id_t) * (remote_id+1))) == NULL)
        {
          return -1;
        }

      /* now set all ids to 0 (reserved) */
      for(j=client->peerid_map_alloc_cnt; j<= remote_id; j++)
        {
          client->peerid_map[j] = 0;
        }
      client->peerid_map_alloc_cnt = remote_id + 1;
    }

  /* do we need to add this peer */
  if (client->peerid_map[remote_id] == 0)
    {
      if ((local_id =
           bgpview_iter_add_peer(it,
                                 sig->collector_str,
                                 (bgpstream_ip_addr_t*)&sig->peer_ip_addr,
                                 sig->peer_asnumber)) == 0)
        {
          return -1;
        }
      bgpview_iter_activate_peer(it);
      client->peerid_map[remote_id] = local_id;
    }

  /* by here we are guaranteed to have a valid mapping */
  return client->peerid_map[remote_id];
}

static void clear_peerid_mapping(bgpview_io_kafka_t *client)
{
  memset(client->peerid_map, 0,
         sizeof(bgpstream_peer_id_t) * client->peerid_map_alloc_cnt);
}

static int seek_topic(rd_kafka_topic_t *rkt,
                      int32_t partition,
                      int64_t offset)
{
  int err;

  if ((err = rd_kafka_seek(rkt, partition, offset, 1000)) != 0) {
    fprintf(stderr,"consume_seek(%s, %d, %"PRIu64") failed: %s\n",
            rd_kafka_topic_name(rkt), partition, offset,
            rd_kafka_err2str(err));
    return -1;
  }

  return 0;
}

static int64_t get_offset(bgpview_io_kafka_t *client,
                          char *topic, int32_t partition)
{
  int64_t low = 0;
  int64_t high = 0;

  if(rd_kafka_query_watermark_offsets(client->rdk_conn, topic, partition,
                                      &low, &high, 10000)
     == RD_KAFKA_RESP_ERR_NO_ERROR)
    {
      return high;
    }

  return -1;
}

static int pfx_row_serialize(uint8_t *buf, size_t len,
                             char operation, bgpview_iter_t *it,
                             bgpview_io_filter_cb_t *cb)
{
  size_t written = 0;
  ssize_t s;

  // serialize the operation that must be done with this row
  // "Update" or "Remove"
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, operation);

  switch (operation)
    {
    case 'U':
      if ((s = bgpview_io_serialize_pfx_row(buf, (len-written),
                                            it, cb, 0)) == -1)
        {
          goto err;
        }
      written += s;
      buf += s;
      break;

    case 'R':
      /* simply serialize the prefix */
      if ((s = bgpview_io_serialize_pfx(buf, (len-written),
                                        bgpview_iter_pfx_get_pfx(it))) == -1)
        {
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

static int deserialize_metadata(view_md_t *meta, uint8_t *buf, size_t len)
{
  size_t read = 0;

  /* Deserialize the common metadata header */

  /* Time */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->time);

  /* Prefixes partition */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->pfxs_partition);

  /* Prefixes offset */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->pfxs_offset);

  /* Peers offset (not partition for peers) */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->peers_offset);

  /* Dump type */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, meta->type);

  switch(meta->type)
    {
    case 'S':
      /* nothing extra for a sync frame */
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
  int npeersH = bgpview_iter_pfx_get_peer_cnt(parent_view_it,
                                              BGPVIEW_FIELD_ACTIVE);
  int npeersC = bgpview_iter_pfx_get_peer_cnt(itC,BGPVIEW_FIELD_ACTIVE);

  if(npeersH != npeersC) return 1;

  bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itC);
  bgpstream_peer_id_t peerid;

  for(bgpview_iter_pfx_first_peer(itC, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_pfx_has_more_peer(itC);
      bgpview_iter_pfx_next_peer(itC))
    {
      peerid = bgpview_iter_peer_get_peer_id(itC);
      if(bgpview_iter_seek_pfx_peer(parent_view_it, pfx, peerid,
                                    BGPVIEW_FIELD_ACTIVE,
                                    BGPVIEW_FIELD_ACTIVE) == 1 ||
         diff_paths(parent_view_it, itC) == 1)
        {
          return 1;
        }
    }
  return 0;
}

/* ==========END SUPPORT FUNCTIONS ========== */

/* ==========START SEND/RECEIVE FUNCTIONS ========== */

static int send_metadata(bgpview_io_kafka_t *client,
                         view_md_t *meta)
{
  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t len = BUFFER_LEN;
  size_t written = 0;

  /* Serialize the common metadata header */

  /* Time */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->time);

  /* Prefixes partition */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->pfxs_partition);

  /* Prefixes offset */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->pfxs_offset);

  /* Peers offset (no partitions for peers) */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->peers_offset);

  /* Serialize the dump type ('S' -> Sync, or 'D' -> Diff) */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, meta->type);

  /* now serialize info specific to to the dump type */
  switch(meta->type)
    {
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

  if(rd_kafka_produce(client->metadata_rkt, client->metadata_partition,
                      RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1)
    {
      fprintf(stderr,"ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(client->metadata_rkt),
              client->metadata_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(client->rdk_conn, 0);
      goto err;
    }

  /* Wait for messages to be delivered */
  while (rd_kafka_outq_len(client->rdk_conn) > 0)
    {
      rd_kafka_poll(client->rdk_conn, 100);
    }

  return 0;

 err:
  return -1;
}

static int recv_metadata(bgpview_io_kafka_t *client,
                         bgpview_t *view,
                         view_md_t *meta)
{
  rd_kafka_message_t *msg = NULL;

 again:
  /* Grab the last metadata message */
  if ((msg = rd_kafka_consume(client->metadata_rkt,
                              client->metadata_partition,
                              2000000000)) == NULL)
    {
      goto err;
    }

  if(msg->payload == NULL)
    {
      if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
        {
          rd_kafka_message_destroy(msg);
          msg = NULL;
          goto again;
        }
      /* TODO: handle this failure -- maybe reconnect? */
      fprintf(stderr, "ERROR: Could not consume metadata message\n");
      goto err;
    }

  /* extract the information from the message */
  if (deserialize_metadata(meta, msg->payload, msg->len) != 0)
    {
      fprintf(stderr, "ERROR: Could not deserialize metadata message\n");
      goto err;
    }

  /* we're done with this message */
  rd_kafka_message_destroy(msg);
  msg = NULL;

  /* Can we use this view? */
  if (meta->type != 'S' && meta->parent_time != bgpview_get_time(view))
    {
      /* this is a diff frame with a parent time that does not match the time of
         the view that we are given */
      fprintf(stderr, "WARN: Found Diff frame against %d, but view time is %d\n",
              meta->parent_time, bgpview_get_time(view));
      fprintf(stderr, "INFO: Rewinding to last sync frame\n");
      if (seek_topic(client->metadata_rkt, client->metadata_partition,
                     meta->sync_md_offset) != 0)
        {
          fprintf(stderr, "ERROR: Could not seek to last sync metadata\n");
          goto err;
        }
      goto again;
    }

  /* We can use this metadata! */

  /* if it is a Sync frame we need to clean up the view that we were given, and
     also our peer mapping */
  if (meta->type == 'S')
    {
      bgpview_clear(view);
      clear_peerid_mapping(client);
    }

  assert(msg == NULL);
  return 0;

 err:
  if(msg != NULL)
    {
      rd_kafka_message_destroy(msg);
    }
  return -1;
}

static int send_peers(bgpview_io_kafka_t *client,
                      view_md_t *meta,
                      bgpview_iter_t *it,
                      bgpview_t *view,
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
  if ((meta->peers_offset = get_offset(client, client->peers_topic,
                                      client->peers_partition)) < 0)
    {
      fprintf(stderr, "ERROR: Could not get peer offset\n");
      goto err;
    }

  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
      /* if we are sending a diff, only send if this peer is not in our
         reference diff */
      if (meta->type == 'D' &&
          bgpview_iter_seek_peer(client->parent_view_it,
                                 bgpview_iter_peer_get_peer_id(it),
                                 BGPVIEW_FIELD_ACTIVE) == 1)
        {
          continue;
        }

      if(cb != NULL)
        {
          /* ask the caller if they want this peer */
          if((filter = cb(it, BGPVIEW_IO_FILTER_PEER)) < 0)
            {
              goto err;
            }
          if(filter == 0)
            {
              continue;
            }
        }
      /* past here means this peer is being sent */
      peers_tx++;

      written = 0;
      ptr = buf;

      type = 'P';
      BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, type);

      if ((written =
           bgpview_io_serialize_peer(ptr, (len-written),
                                     bgpview_iter_peer_get_peer_id(it),
                                     bgpview_iter_peer_get_sig(it))) < 0)
        {
          goto err;
        }

      if(rd_kafka_produce(client->peers_rkt, client->peers_partition,
                          RD_KAFKA_MSG_F_COPY, buf, written,
                          NULL, 0, NULL) == -1)
        {
          fprintf(stderr,
                  "ERROR: Failed to produce to topic %s partition %i: %s\n",
                  rd_kafka_topic_name(client->peers_rkt),
                  client->peers_partition,
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

  if(rd_kafka_produce(client->peers_rkt, client->peers_partition,
                      RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(client->peers_rkt), client->peers_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(client->rdk_conn, 0);
      goto err;
    }

  while (rd_kafka_outq_len(client->rdk_conn) > 0)
    {
      rd_kafka_poll(client->rdk_conn, 100);
    }

  return 0;

 err:
  return -1;
}

static int recv_peers(bgpview_io_kafka_t *client, bgpview_iter_t *iter,
                      bgpview_io_filter_peer_cb_t *peer_cb,
                      int64_t offset, uint32_t exp_time)
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

  if(seek_topic(client->peers_rkt, client->peers_partition, offset) != 0)
    {
      fprintf(stderr,"Error changing the offset");
      goto err;
    }

  /* receive the peers */
  while(1)
    {
      msg = rd_kafka_consume(client->peers_rkt, client->peers_partition, 1000);
      if(msg->payload == NULL)
        {
          fprintf(stderr, "Cannot not receive peer message\n");
          goto err;
        }
      ptr = msg->payload;
      read = 0;

      BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, type);

      if (type == 'E')
        {
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

      if ((s = bgpview_io_deserialize_peer(ptr, msg->len,
                                           &peerid_remote, &ps)) < 0)
        {
          goto err;
        }
      read += s;
      ptr += s;

      rd_kafka_message_destroy(msg);
      msg = NULL;

      peers_rx++;

      if(iter == NULL)
        {
          continue;
        }

      if(peer_cb != NULL)
        {
          /* ask the caller if they want this peer */
          if((filter = peer_cb(&ps)) < 0)
            {
              goto err;
            }
          if(filter == 0)
            {
              continue;
            }
        }
      /* all code below here has a valid view */

      if(add_peerid_mapping(client, iter, &ps, peerid_remote) <= 0)
        {
          goto err;
        }
    }

  assert(msg == NULL);
  return 0;

 err:
  if (msg != NULL)
    {
      rd_kafka_message_destroy(msg);
    }
  return -1;
}

static int send_pfxs(bgpview_io_kafka_t *client,
                     view_md_t *meta,
                     bgpview_io_kafka_stats_t *stats,
                     bgpview_iter_t *it,
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
  if ((meta->pfxs_offset = get_offset(client,
                                      client->pfxs_topic,
                                      client->pfxs_partition)) < 0)
    {
      fprintf(stderr, "ERROR: Could not get prefix offset\n");
      goto err;
    }
  meta->pfxs_partition = client->pfxs_partition;

  if (meta->type == 'D')
    {
      pfxs_cnt_ref = bgpview_pfx_cnt(client->parent_view, BGPVIEW_FIELD_ACTIVE);
      pfxs_cnt_cur = bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE);
    }

  for(bgpview_iter_first_pfx(it, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(it);
      bgpview_iter_next_pfx(it))
    {
      /* reset the buffer */
      written = 0;
      ptr = buf;
      send = 1;

      if(cb != NULL)
        {
          if((filter = cb(it, BGPVIEW_IO_FILTER_PFX)) < 0)
            {
              goto err;
            }
          if(filter == 0)
            {
              continue;
            }
        }

      if (meta->type == 'D')
        {
          pfx = bgpview_iter_pfx_get_pfx(it);
          if(bgpview_iter_seek_pfx(client->parent_view_it, pfx,
                                   BGPVIEW_FIELD_ACTIVE) == 1)
            {
              stats->common++;
              if(diff_rows(client->parent_view_it, it) == 1)
                {
                  stats->change++;
                  /* pfx has changed, send */
                }
              else
                {
                  send = 0;
                }
            }
        }

      if (send == 0)
        {
          continue;
        }

      if ((s = pfx_row_serialize(ptr, len, 'U', it, cb)) < 0)
        {
          goto err;
        }
      written += s;
      ptr += s;

      pfxs_tx++;

      if(rd_kafka_produce(client->pfxs_rkt, client->pfxs_partition,
                          RD_KAFKA_MSG_F_COPY,
                          buf, written, NULL, 0, NULL) == -1)
        {
          fprintf(stderr,
                  "ERROR: Failed to produce to topic %s partition %i: %s\n",
                  rd_kafka_topic_name(client->pfxs_rkt),
                  client->pfxs_partition,
                  rd_kafka_err2str(rd_kafka_errno2err(errno)));
          //Poll to handle delivery reports
          rd_kafka_poll(client->rdk_conn, 0);
          goto err;
        }
    }
  rd_kafka_poll(client->rdk_conn, 0);

  if (meta->type == 'D')
    {
      /* send removals */
      stats->remove = pfxs_cnt_ref - stats->common;
      assert(stats->remove >= 0);

      fprintf(stderr, "DEBUG: HC %d CC %d common %d remove %d change %d\n",
              pfxs_cnt_ref, pfxs_cnt_cur, stats->common,
              stats->remove, stats->change);

      stats->add = (pfxs_cnt_cur - stats->common) + stats->remove;
      if(stats->add < 0)
        {
          stats->add = 0;
        }

      stats->current_pfx_cnt = stats->common + stats->add;
      stats->historical_pfx_cnt = stats->common + stats->remove;
      stats->sync_cnt = 0;

      if (stats->remove > 0)
        {
          int remain = stats->remove;
          for(bgpview_iter_first_pfx(client->parent_view_it, 0,
                                     BGPVIEW_FIELD_ACTIVE);
              bgpview_iter_has_more_pfx(client->parent_view_it);
              bgpview_iter_next_pfx(client->parent_view_it))
            {
              written = 0;
              ptr = buf;

              pfx = bgpview_iter_pfx_get_pfx(client->parent_view_it);

              if(bgpview_iter_seek_pfx(it, pfx, BGPVIEW_FIELD_ACTIVE) != 0)
                {
                  continue;
                }

              if ((s = pfx_row_serialize(ptr, (len-written), 'R',
                                         client->parent_view_it, cb)) < 0)
                {
                  goto err;
                }
              written += s;
              ptr += s;

              remain--;
              pfxs_tx++;
              if(rd_kafka_produce(client->pfxs_rkt, client->pfxs_partition,
                                  RD_KAFKA_MSG_F_COPY,
                                  buf, written, NULL, 0, NULL) == -1)
                {
                  fprintf(stderr,
                          "ERROR: Failed to produce to topic %s partition %i: %s\n",
                          rd_kafka_topic_name(client->pfxs_rkt),
                          client->pfxs_partition,
                          rd_kafka_err2str(rd_kafka_errno2err(errno)));
                  rd_kafka_poll(client->rdk_conn, 0);
                  goto err;
                }

              /* stop looking if we have removed all we need to */
              if(remain == 0)
                {
                  break;
                }
            }
        }
    }
  else
    {
      stats->add = 0;
      stats->remove = 0;
      stats->change = 0;
      stats->common = 0;
      stats->historical_pfx_cnt = 0;
      stats->current_pfx_cnt = stats->sync_cnt =
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

  if(rd_kafka_produce(client->pfxs_rkt, client->pfxs_partition,
                      RD_KAFKA_MSG_F_COPY,
                      buf, written, NULL, 0, NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(client->pfxs_rkt), client->pfxs_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      //Poll to handle delivery reports
      rd_kafka_poll(client->rdk_conn, 0);
      return -1;
    }

  /* @todo figure out how to defer this until the connection gets shut down */
  while (rd_kafka_outq_len(client->rdk_conn) > 0)
    {
      rd_kafka_poll(client->rdk_conn, 100);
    }

  return 0;

 err:
  return -1;
}

static int recv_pfxs(bgpview_io_kafka_t *client,
                     bgpview_iter_t *iter,
                     bgpview_io_filter_pfx_cb_t *pfx_cb,
                     bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
                     int32_t partition, int64_t offset, uint32_t exp_time)
{
  bgpview_t *view = NULL;
  uint32_t view_time;

  size_t read = 0;
  uint8_t *ptr;
  ssize_t s;

  char type;

  bgpstream_pfx_storage_t pfx;
  int pfx_cnt = 0;
  int pfx_rx = 0;

  int tor = 0;
  int tom = 0;

  rd_kafka_message_t *msg = NULL;

  if(seek_topic(client->pfxs_rkt, partition, offset) != 0)
    {
      fprintf(stderr,"Error changing the offset");
      goto err;
    }

  if(iter != NULL)
    {
      view = bgpview_iter_get_view(iter);
    }

  while(1)
    {
      msg = rd_kafka_consume(client->pfxs_rkt, client->pfxs_partition, 1000);
      if(msg->payload == NULL)
        {
          fprintf(stderr, "Cannot receive prefixes and paths\n");
          goto err;
        }

      ptr = msg->payload;
      read = 0;

      BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, type);

      if (type == 'E')
        {
          /* end of prefixes */
          BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, view_time);
          if (iter != NULL)
            {
              bgpview_set_time(view, view_time);
            }
          assert(view_time == exp_time);
          BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, pfx_cnt);
          assert(pfx_rx == pfx_cnt);
          assert(read == msg->len);

          rd_kafka_message_destroy(msg);
          msg = NULL;
          break;
        }

      /* this is a prefix row message */
      pfx_rx++;

      switch(type)
        {
        case 'U':
          /* an update row */
          tom++;
          if ((s = bgpview_io_deserialize_pfx_row(ptr, (msg->len-read), iter,
                                                  pfx_cb, pfx_peer_cb,
                                                  client->peerid_map,
                                                  client->peerid_map_alloc_cnt,
                                                  NULL, -1)) == -1)
            {
              goto err;
            }
          read += s;
          ptr += s;
          break;

        case 'R':
          /* a remove row */
          tor++;
          /* just grab the prefix and then deactivate it */
          if((s = bgpview_io_deserialize_pfx(ptr, (msg->len-read), &pfx)) == -1)
            {
              goto err;
            }
          read += s;
          ptr += s;
          if((bgpview_iter_seek_pfx(iter, (bgpstream_pfx_t *)&pfx,
                                    BGPVIEW_FIELD_ACTIVE) != 0) &&
             (bgpview_iter_deactivate_pfx(iter) != 1))
            {
              goto err;
            }
          break;
        }

      rd_kafka_message_destroy(msg);
      msg = NULL;
    }

  assert(msg == NULL);
  return 0;

 err:
  if (msg != NULL)
    {
      rd_kafka_message_destroy(msg);
    }
  return -1;
}

static int send_sync_view(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats,
                          bgpview_t *view,
                          bgpview_io_filter_cb_t *cb)
{
  bgpview_iter_t *it = NULL;
  view_md_t meta;

  if((it = bgpview_iter_create(view)) == NULL)
    {
      goto err;
    }

  meta.time = bgpview_get_time(view);
  meta.type = 'S';


  if(send_peers(client, &meta, it, view, cb) != 0)
    {
      goto err;
    }
  if(send_pfxs(client, &meta, stats, it, cb) != 0)
    {
      goto err;
    }

  /* find the current metadata offset and update the sync info */
  if ((client->last_sync_offset = get_offset(client,
                                             client->metadata_topic,
                                             client->metadata_partition)) < 0)
    {
      fprintf(stderr, "ERROR: Could not get metadata offset\n");
      goto err;
    }

  if(send_metadata(client, &meta) == -1)
    {
      fprintf(stderr,"Error publishing metadata\n");
      goto err;
    }

  bgpview_iter_destroy(it);

  client->num_diffs = 0;
  return 0;

 err:
  return -1;
}

static int send_diff_view(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats,
                          bgpview_t *view,
                          bgpview_io_filter_cb_t *cb)
{
  bgpview_iter_t *it = NULL;
  view_md_t meta;

  if((it = bgpview_iter_create(view)) == NULL)
    {
      goto err;
    }

  meta.time = bgpview_get_time(view);
  meta.type = 'D';

  assert(client->parent_view != NULL &&
         bgpview_get_time(client->parent_view) != 0);
  meta.parent_time = bgpview_get_time(client->parent_view);
  meta.sync_md_offset = client->last_sync_offset;

  if(send_peers(client, &meta, it, view, cb) == -1)
    {
      goto err;
    }

  if(send_pfxs(client, &meta, stats, it, cb) ==- 1)
    {
      goto err;
    }

  if(send_metadata(client, &meta) == -1)
    {
      fprintf(stderr,"Error on publishing the offset\n");
      goto err;
    }

  client->num_diffs++;

  bgpview_iter_destroy(it);

  return 0;

 err:
  bgpview_iter_destroy(it);
  return -1;
}

static int read_view(bgpview_io_kafka_t *client,
                     bgpview_t *view,
                     view_md_t *meta,
                     bgpview_io_filter_peer_cb_t *peer_cb,
                     bgpview_io_filter_pfx_cb_t *pfx_cb,
                     bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{
  bgpview_iter_t *it = NULL;

  if(view != NULL && (it = bgpview_iter_create(view)) == NULL)
    {
      return -1;
    }

  if(recv_peers(client, it, peer_cb, meta->peers_offset, meta->time) < 0)
    {
      fprintf(stderr, "Could not receive peers\n");
      return -1;
    }

  if(recv_pfxs(client, it, pfx_cb, pfx_peer_cb,
               meta->pfxs_partition, meta->pfxs_offset, meta->time) != 0)
    {
      fprintf(stderr, "Could not receive prefixes\n");
      goto err;
    }

  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
      if(bgpview_iter_peer_get_pfx_cnt(it, 0, BGPVIEW_FIELD_ACTIVE) == 0 &&
         bgpview_iter_deactivate_peer(it) != 1)
        {
          fprintf(stderr, "Fail to deactivate peer\n");
          goto err;
        }
    }

  if(it != NULL)
    {
      bgpview_iter_destroy(it);
    }

  return 0;

 err:
  if(it != NULL)
    {
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
  if ((client->rdk_conn =
       rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr))) == NULL) {
    fprintf(stderr,"ERROR: Failed to create new consumer: %s\n", errstr);
    return -1;
  }

  // Add brokers
  if (rd_kafka_brokers_add(client->rdk_conn, client->brokers) == 0) {
    fprintf(stderr, "ERROR: No valid brokers specified\n");
    return -1;
  }

  return 0;
}

int bgpview_io_kafka_consumer_topic_connect(bgpview_io_kafka_t *client,
                                            rd_kafka_topic_t **rkt,
                                            char *topic)
{
  if((*rkt = rd_kafka_topic_new(client->rdk_conn, topic, NULL)) == NULL)
    {
      return -1;
    }

  if (rd_kafka_consume_start(*rkt, 0, RD_KAFKA_OFFSET_TAIL(1)) == -1){
    fprintf(stderr, "ERROR: Failed to start consuming: %s\n",
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    return -1;
  }

  return 0;
}

int bgpview_io_kafka_producer_connect(bgpview_io_kafka_t *client)
{
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  char errstr[512];

  if (rd_kafka_conf_set(conf, "queue.buffering.max.messages","7000000",
                        errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }

  if (rd_kafka_conf_set(conf, "compression.codec", "snappy",
                        errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }

  if ((client->rdk_conn =
       rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                    errstr, sizeof(errstr))) == NULL) {
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
                                            rd_kafka_topic_t **rkt,
                                            char *topic)
{
  if((*rkt = rd_kafka_topic_new(client->rdk_conn, topic, NULL)) == NULL)
    {
      return -1;
    }

  return 0;
}

int bgpview_io_kafka_send(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats,
                          bgpview_t *view,
                          bgpview_io_filter_cb_t *cb)
{
  time_t rawtime;
  time_t end;
  int length;
  time_t start1;
  time_t start2;
  struct tm * timeinfo;

  time(&start1);

  stats->arrival_time=(int)time(NULL);

  if(client->parent_view == NULL || client->num_diffs == client->max_diffs)
    {
      if(send_sync_view(client, stats, view, cb) != 0){
        goto err;
      }
    }
  else
    {
      if(send_diff_view(client, stats, view, cb) != 0){
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

  if((client->parent_view == NULL &&
      (client->parent_view = bgpview_dup(view)) == NULL) ||
     bgpview_copy(client->parent_view, view) != 0)
    {
      fprintf(stderr,"ERROR: Could not copy view\n");
      goto err;
    }

  if(client->parent_view_it == NULL &&
     (client->parent_view_it =
      bgpview_iter_create(client->parent_view)) == NULL)
    {
      goto err;
    }

  time(&rawtime);
  timeinfo = localtime(&rawtime);
  time(&end);
  length = difftime(end,start2);
  stats->clone_time=length;
  length = difftime(end,start1);
  stats->total_time=length;

  stats->processed_time=(int)time(NULL);

  return 0;

 err:
  return -1;
}

int bgpview_io_kafka_recv(bgpview_io_kafka_t *client,
                          bgpview_t *view,
                          bgpview_io_filter_peer_cb_t *peer_cb,
                          bgpview_io_filter_pfx_cb_t *pfx_cb,
                          bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{
  view_md_t meta;

  /* find the view that we will receive */
  if(recv_metadata(client, view, &meta) != 0)
    {
      return -1;
    }

  return read_view(client, view, &meta, peer_cb, pfx_cb, pfx_peer_cb);
}
