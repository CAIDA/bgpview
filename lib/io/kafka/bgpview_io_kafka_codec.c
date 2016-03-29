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


static int change_consumer_offset_partition(rd_kafka_topic_t *rkt,
                                            int partition, long int offset)
{
  int err;

  if ((err = rd_kafka_seek(rkt, partition, offset, 1000)) != 0) {
    fprintf(stderr,"consume_seek(%s, %d, %ld) failed: %s\n",
            rd_kafka_topic_name(rkt), partition, offset,
            rd_kafka_err2str(err));
    return -1;
  }

  return 0;
}

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

static long int get_offset(bgpview_io_kafka_t *client,
                           char *topic, int partition)
{
  rd_kafka_t *rk = NULL;
  rd_kafka_topic_t *rkt = NULL;
  rd_kafka_message_t *rkmessage;

  long int offset;

  char *brokers = client->brokers;

  if(initialize_consumer_connection(&rk, &rkt, brokers, topic) != 0)
    {
      fprintf(stderr,"Error initializing consumer connection");
      goto err;
    }

  if(change_consumer_offset_partition(rkt, partition, RD_KAFKA_OFFSET_END) !=0)
    {
      fprintf(stderr,"Error changing the offset");
      goto err;
    }

  rkmessage = rd_kafka_consume(rkt, partition, 1000);
  if(rkmessage == NULL){
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);
    return 0;
  }

  offset = rkmessage->offset;

  rd_kafka_message_destroy(rkmessage);
  //printf("offset %ld\n",offset);
  // Destroy topic
  rd_kafka_topic_destroy(rkt);
  // Destroy the handle
  rd_kafka_destroy(rk);
  return offset;

 err:
  rd_kafka_topic_destroy(rkt);
  // Destroy the handle
  rd_kafka_destroy(rk);
  return -1;
}

static long int set_current_offsets(bgpview_io_kafka_t *client)
{
  if ((client->current_pfxs_offset =
       get_offset(client, client->pfxs_topic, client->pfxs_partition)) == -1)
    {
      goto err;
    }

  if((client->current_peers_offset =
      get_offset(client, client->peers_topic, client->peers_partition)) == -1)
    {
      goto err;
    }
  return 0;

 err:

  return -1;
}

static int set_sync_view_data(bgpview_io_kafka_t *client,
                              bgpview_t *view)
{
  client->pfxs_sync_partition = client->pfxs_partition;
  client->sync_view_time = bgpview_get_time(view);

  if((client->pfxs_sync_offset = get_offset(client, client->pfxs_topic,
                                            client->pfxs_partition)) == -1)
    {
      goto err;
    }

  return 0;

 err:
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

static int deserialize_metadata(uint8_t *buf, size_t len,
                                bgpview_io_kafka_t *client)
{
  size_t read = 0;

  /* Deserialize the common metadata header */

  /* Time */
  uint32_t time;
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, time);

  /* Prefixes partition */
  int32_t pfxs_partition;
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, pfxs_partition);

  /* Prefixes offset */
  int64_t pfxs_offset;
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, pfxs_offset);

  /* Peers offset (not partition for peers) */
  int64_t peers_offset;
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, peers_offset);

  /* Dump type */
  char type;
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, type);

  switch(type)
    {
    case 'S':
      client->sync_view_time = time;
      client->pfxs_sync_partition = pfxs_partition;
      client->pfxs_sync_offset = pfxs_offset;
      client->peers_sync_offset = peers_offset;
      client->num_diffs = 0;
      break;

    case 'D':
      client->pfxs_diffs_partition[client->num_diffs] = pfxs_partition;
      client->pfxs_diffs_offset[client->num_diffs] = pfxs_offset;
      client->peers_diffs_offset[client->num_diffs] = peers_offset;
      client->num_diffs++;
      break;

    default:
      goto err;
    }

  return 0;

 err:
  return -1;
}

static int diff_paths(bgpview_iter_t *itH, bgpview_iter_t *itC)
{
  bgpstream_as_path_store_path_id_t idxH =
    bgpview_iter_pfx_peer_get_as_path_store_path_id(itH);
  bgpstream_as_path_store_path_id_t idxC =
    bgpview_iter_pfx_peer_get_as_path_store_path_id(itC);

  return bcmp(&idxH, &idxC, sizeof(bgpstream_as_path_store_path_id_t));
}

static int diff_rows(bgpview_iter_t *itH, bgpview_iter_t *itC)
{
  int npeersH = bgpview_iter_pfx_get_peer_cnt(itH,BGPVIEW_FIELD_ACTIVE);
  int npeersC = bgpview_iter_pfx_get_peer_cnt(itC,BGPVIEW_FIELD_ACTIVE);

  if(npeersH != npeersC) return 1;

  bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itC);
  bgpstream_peer_id_t peerid;

  for(bgpview_iter_pfx_first_peer(itC, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_pfx_has_more_peer(itC);
      bgpview_iter_pfx_next_peer(itC))
    {
      peerid = bgpview_iter_peer_get_peer_id(itC);
      if(bgpview_iter_seek_pfx_peer(itH, pfx, peerid,
                                    BGPVIEW_FIELD_ACTIVE,
                                    BGPVIEW_FIELD_ACTIVE) == 1 ||
         diff_paths(itH, itC) == 1)
        {
          return 1;
        }
    }
  return 0;
}


/* ==========END SUPPORT FUNCTIONS ========== */

/* ==========START SEND/RECEIVE FUNCTIONS ========== */

static int send_metadata(bgpview_io_kafka_t *client,
                         bgpview_t *view,
                         char type)
{
  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t len = BUFFER_LEN;
  size_t written = 0;

  /* Serialize the common metadata header */

  /* Time */
  uint32_t time = bgpview_get_time(view);
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, time);

  /* Prefixes partition */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, client->pfxs_partition);

  /* Prefixes offset */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, client->current_pfxs_offset);

  /* Peers offset (no partitions for peers) */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, client->current_peers_offset);

  /* Serialize the dump type ('S' -> Sync, or 'D' -> Diff) */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, type);

  /* now serialize info specific to to the dump type */
  switch(type)
    {
    case 'S':
      /* nothing additional */
      break;

    case 'D':
      /* TODO: Sync View Metadata Offset (so that we can jump there) */
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
      rd_kafka_poll(client->metadata_rk, 0);
      goto err;
    }

  /* Wait for messages to be delivered */
  while (rd_kafka_outq_len(client->metadata_rk) > 0)
    {
      rd_kafka_poll(client->metadata_rk, 100);
    }

  return 0;

 err:
  return -1;
}

static int recv_metadata(bgpview_io_kafka_t *client)
{
  rd_kafka_message_t *rkmessage = NULL;
  int i;

  /* gets the offset of the last message in the topic */
  int64_t offset =
    get_offset(client, client->metadata_topic, client->metadata_partition);
  /* rewinds by enough MD messages to be sure that we can get an sync */
  int64_t history_offset = offset - client->view_frequency - 1;

  /* If there aren't enough messages already in Kafka, then just go to the
     beginning */
  if(history_offset < 0)
    {
      history_offset = 0;
    }

  if(client->metadata_offset != 0)
    {
      history_offset = client->metadata_offset;
    }

  /* Seek to the given offset on the given topic/partition */
  if(change_consumer_offset_partition(client->metadata_rkt,
                                      client->metadata_partition,
                                      history_offset) != 0)
    {
      fprintf(stderr,"Error changing the offset");
      goto err;
    }

  /* Preload all the diff information? */
  for(i=0; i < client->view_frequency+2; i++)
    {
      if ((rkmessage = rd_kafka_consume(client->metadata_rkt,
                                        client->metadata_partition,
                                        2000000000)) == NULL)
        {
          goto err;
        }

      if(rkmessage->payload == NULL)
        {
          if(rkmessage->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
              fprintf(stderr, "Cannot not receive metadata\n");
              goto err;
            }
          /* Got EOF from Kafka */
          if(i != 0)
            {
              /* We've already read some metadata, so go away and process
                 that first */
              break;
            }
          /* Otherwise, block here until something is generated */
          rkmessage = rd_kafka_consume(client->metadata_rkt,
                                       client->metadata_partition,
                                       2000000000);
        }

      client->metadata_offset++;

      if (deserialize_metadata(rkmessage->payload, rkmessage->len, client) != 0)
        {
          fprintf(stderr, "ERROR: Failed to deserialize metadata\n");
          goto err;
        }

      rd_kafka_message_destroy(rkmessage);
    }

  return 0;

 err:
  fprintf(stderr, "ERROR: Failed to receive metadata\n");
  return -1;
}

static int send_peers(bgpview_io_kafka_t *client,
                      bgpview_iter_t *it,
                      bgpview_t *view,
                      bgpview_io_filter_cb_t *cb,
                      int send_diff)
{
  time_t rawtime;
  time (&rawtime);

  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t len = BUFFER_LEN;
  ssize_t written = 0;

  char type;

  uint16_t peers_tx = 0;
  int filter;

  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
      /* if we are sending a diff, only send if this peer is not in our
         reference diff */
      if (send_diff != 0 &&
          bgpview_iter_seek_peer(client->itH,
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
          rd_kafka_poll(client->peers_rk, 0);
          goto err;
        }
    }

  written = 0;
  ptr = buf;

  /* End message */
  type = 'E';
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, type);
  /* Peer Count */
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, peers_tx);

  if(rd_kafka_produce(client->peers_rkt, client->peers_partition,
                      RD_KAFKA_MSG_F_COPY, buf, written, NULL, 0, NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(client->peers_rkt), client->peers_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(client->peers_rk, 0);
      goto err;
    }

  while (rd_kafka_outq_len(client->peers_rk) > 0)
    {
      rd_kafka_poll(client->peers_rk, 100);
    }

  return 0;

 err:
  return -1;
}

static int recv_peers(bgpview_io_kafka_t *client, bgpview_iter_t *iter,
                      bgpview_io_filter_peer_cb_t *peer_cb,
                      int64_t offset)
{
  rd_kafka_message_t *msg = NULL;
  size_t read = 0;
  ssize_t s;
  uint8_t *ptr;
  char type;
  uint16_t peer_cnt;

  bgpstream_peer_id_t peerid_remote;
  bgpstream_peer_sig_t ps;

  int peers_rx = 0;
  int filter;

  if(change_consumer_offset_partition(client->peers_rkt,
                                      client->peers_partition,
                                      offset) != 0)
    {
      fprintf(stderr,"Error changing the offset");
      goto err;
    }

  /* receive the peers */
  while(1)
    {
      if (msg != NULL)
        {
          rd_kafka_message_destroy(msg);
          msg = NULL;
        }
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
          BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, peer_cnt);
          assert(peers_rx == peer_cnt);
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

  if (msg != NULL)
    {
      rd_kafka_message_destroy(msg);
    }
  return 0;

 err:
  if (msg != NULL)
    {
      rd_kafka_message_destroy(msg);
    }
  return -1;
}

static int send_pfxs(bgpview_io_kafka_t *client,
                     bgpview_io_kafka_stats_t *stats,
                     bgpview_iter_t *it,
                     bgpview_io_filter_cb_t *cb,
                     int send_diff)
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

  if (send_diff != 0)
    {
      pfxs_cnt_ref = bgpview_pfx_cnt(client->viewH, BGPVIEW_FIELD_ACTIVE);
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

      if (send_diff != 0)
        {
          pfx = bgpview_iter_pfx_get_pfx(it);
          if(bgpview_iter_seek_pfx(client->itH, pfx, BGPVIEW_FIELD_ACTIVE) == 1)
            {
              stats->common++;
              if(diff_rows(client->itH, it) == 1)
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
          rd_kafka_poll(client->pfxs_rk, 0);
          goto err;
        }
    }
  rd_kafka_poll(client->pfxs_rk, 0);

  if (send_diff != 0)
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
          for(bgpview_iter_first_pfx(client->itH, 0 , BGPVIEW_FIELD_ACTIVE);
              bgpview_iter_has_more_pfx(client->itH);
              bgpview_iter_next_pfx(client->itH))
            {
              written = 0;
              ptr = buf;

              pfx = bgpview_iter_pfx_get_pfx(client->itH);

              if(bgpview_iter_seek_pfx(it, pfx, BGPVIEW_FIELD_ACTIVE) != 0)
                {
                  continue;
                }

              if ((s = pfx_row_serialize(ptr, (len-written), 'R',
                                         client->itH, cb)) < 0)
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
                  rd_kafka_poll(client->pfxs_rk, 0);
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
  uint32_t vtime = bgpview_get_time(view);
  BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, vtime);
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
      rd_kafka_poll(client->pfxs_rk, 0);
      return -1;
    }

  /* @todo figure out how to defer this until the connection gets shut down */
  while (rd_kafka_outq_len(client->pfxs_rk) > 0)
    {
      rd_kafka_poll(client->pfxs_rk, 100);
    }

  return 0;

 err:
  return -1;
}

static int recv_pfxs(bgpview_io_kafka_t *client,
                     bgpview_iter_t *iter,
                     bgpview_io_filter_pfx_cb_t *pfx_cb,
                     bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
                     int partition, long int offset)
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

  rd_kafka_message_t *msg;

  if(change_consumer_offset_partition(client->pfxs_rkt, partition, offset) != 0)
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
          BGPVIEW_IO_DESERIALIZE_VAL(ptr, msg->len, read, pfx_cnt);
          assert(pfx_rx == pfx_cnt);
          assert(read == msg->len);
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

  return 0;

 err:
  return -1;
}

static int send_sync_view(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats,
                          bgpview_t *view,
                          bgpview_io_filter_cb_t *cb)
{
  bgpview_iter_t *it = NULL;

  if((it = bgpview_iter_create(view)) == NULL)
    {
      goto err;
    }

  set_sync_view_data(client, view);

  set_current_offsets(client);

  if(send_peers(client, it, view, cb, 0) != 0)
    {
      goto err;
    }
  if(send_pfxs(client, stats, it, cb, 0) != 0)
    {
      goto err;
    }
  if(send_metadata(client, view, 'S') == -1)
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

  if((it = bgpview_iter_create(view)) == NULL)
    {
      goto err;
    }

  if(set_current_offsets(client) == -1)
    {
      goto err;
    }

  if(send_peers(client, it, view, cb, 1) == -1)
    {
      goto err;
    }

  if(send_pfxs(client, stats, it, cb, 1) ==- 1)
    {
      goto err;
    }

  if(send_metadata(client, view, 'D') == -1)
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

static int read_sync_view(bgpview_io_kafka_t *client,
                          bgpview_t *view,
                          bgpview_io_filter_peer_cb_t *peer_cb,
                          bgpview_io_filter_pfx_cb_t *pfx_cb,
                          bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{
  bgpview_iter_t *it = NULL;

  if(view != NULL && (it = bgpview_iter_create(view)) == NULL)
    {
      goto err;
    }

  if(recv_peers(client, it, peer_cb, client->peers_sync_offset) < 0)
    {
      fprintf(stderr, "Could not receive peers\n");
      goto err;
    }

  if(recv_pfxs(client, it, pfx_cb, pfx_peer_cb,
               client->pfxs_sync_partition, client->pfxs_sync_offset) !=0)
    {
      fprintf(stderr, "Could not receive prefixes and paths\n");
      goto err;
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

static int read_diff_view(bgpview_io_kafka_t *client,
                          bgpview_t *view,
                          bgpview_io_filter_peer_cb_t *peer_cb,
                          bgpview_io_filter_pfx_cb_t *pfx_cb,
                          bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{
  bgpview_iter_t *it = NULL;

  if(view != NULL && (it = bgpview_iter_create(view)) == NULL)
    {
      return -1;
    }

  int i;
  for(i=0; i<client->num_diffs; i++)
    {
      if(recv_peers(client, it, peer_cb, client->peers_diffs_offset[i]) < 0)
        {
          fprintf(stderr, "Could not receive peers\n");
          return -1;
        }

      if(recv_pfxs(client, it, pfx_cb, pfx_peer_cb,
                   client->pfxs_diffs_partition[i],
                   client->pfxs_diffs_offset[i]) !=0)
        {
          fprintf(stderr, "Could not receive prefixes\n");
          goto err;
        }
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

int initialize_consumer_connection(rd_kafka_t **rk,
                                   rd_kafka_topic_t **rkt,
                                   char *brokers,
                                   char *topic)
{
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;

  char errstr[512];

  // Kafka configuration
  conf = rd_kafka_conf_new();

  // Topic configuration
  topic_conf = rd_kafka_topic_conf_new();
  //printf("%s %s %d\n",brokers,topic,partition);

  // Create Kafka handle
  if (!(*rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,errstr, sizeof(errstr)))) {
    fprintf(stderr,"ERROR: Failed to create new consumer: %s\n",errstr);
    goto err;
  }

  // Add brokers
  if (rd_kafka_brokers_add(*rk, brokers) == 0) {
    fprintf(stderr, "ERROR: No valid brokers specified\n");
    goto err;
  }

  // Create topic
  *rkt = rd_kafka_topic_new(*rk, topic, topic_conf);

  // Start consuming
  if (rd_kafka_consume_start(*rkt, 0, RD_KAFKA_OFFSET_BEGINNING) == -1){
    fprintf(stderr, "ERROR: Failed to start consuming: %s\n",
            rd_kafka_err2str(rd_kafka_errno2err(errno)));
    if (errno == EINVAL)
      {
        fprintf(stderr,
                "ERROR: Broker based offset storage requires a group.id, "
                "add: -X group.id=yourGroup\n");
      }
    goto err;
  }

  return 0;

 err:
  return -1;
}

int initialize_producer_connection(rd_kafka_t **rk,
                                   rd_kafka_topic_t **rkt,
                                   char *brokers,
                                   char *topic)
{
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;

  char errstr[512];

  conf = rd_kafka_conf_new();
  topic_conf = rd_kafka_topic_conf_new();

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

  if (!(*rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,errstr, sizeof(errstr)))) {
    fprintf(stderr, "ERROR: Failed to create new producer: %s\n", errstr);
    goto err;
  }

  if (rd_kafka_brokers_add(*rk, brokers) == 0) {
    fprintf(stderr, "ERROR: No valid brokers specified\n");
    goto err;
  }

  *rkt = rd_kafka_topic_new(*rk, topic, topic_conf);

  return 0;

 err:
  return -1;
}

int bgpview_io_kafka_send(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats,
                          bgpview_t *view,
                          bgpview_io_filter_cb_t *cb)
{
  time_t rawtime;
  struct tm * timeinfo;
  time(&rawtime);
  timeinfo = localtime(&rawtime);

  time_t start1,start2, end;
  int length;

  time(&start1);

  fprintf(stderr,"#####START sending view at: %s", asctime(timeinfo));

  stats->arrival_time=(int)time(NULL);

  if(client->viewH == NULL || client->num_diffs == client->view_frequency){
    if(send_sync_view(client, stats, view, cb) != 0){
      goto err;
    }
  }
  else{
    if(send_diff_view(client, stats, view, cb) != 0){
      goto err;
    }
  }

  time(&rawtime);
  timeinfo = localtime(&rawtime);
  time(&end);
  length = difftime(end, start1);
  stats->send_time = length;
  fprintf(stderr,"#####END sending view in %d at: %s",length, asctime(timeinfo));

  time(&rawtime);
  timeinfo = localtime(&rawtime);
  time(&start2);

  fprintf(stderr,"#####START CLONING view at: %s", asctime(timeinfo));

  if((client->viewH == NULL && (client->viewH = bgpview_dup(view)) == NULL) ||
     bgpview_copy(client->viewH, view) != 0)
    {
      fprintf(stderr,"error cloning the view\n");
      goto err;
    }

  if(client->itH == NULL &&
     (client->itH = bgpview_iter_create(client->viewH)) == NULL)
    {
      goto err;
    }

  time(&rawtime);
  timeinfo = localtime(&rawtime);
  time(&end);
  length = difftime(end,start2);
  fprintf(stderr,"#####END CLONING view in: %d at: %s",
          length, asctime(timeinfo));
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
  int first_view = 0;

  /* @todo make this more robust */
  if(bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE) == 0)
    {
      first_view = 1;
    }

  if(recv_metadata(client) != 0)
    {
      goto err;
    }

  if(client->num_diffs == 0 || first_view == 1)
    {
      bgpview_clear(view);
      clear_peerid_mapping(client);
      read_sync_view(client, view, peer_cb, pfx_cb, pfx_peer_cb);
    }
  else
    {
      read_diff_view(client, view, peer_cb, pfx_cb, pfx_peer_cb);
    }

  return 0;

 err:
  return -1;
}
