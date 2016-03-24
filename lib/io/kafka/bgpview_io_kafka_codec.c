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

// deleteme
#include <czmq.h>

#define BUFFER_LEN 16384
#define BUFFER_1M  1048576

/* ========== START KAFKA FUNCTIONS ========== */


static int change_consumer_offset_partition(rd_kafka_topic_t *rkt,
                                            int partition, long int offset)
{
  int err;

  if ((err = rd_kafka_seek(rkt, partition, offset, 1000)) != 0){
    fprintf(stderr,"consume_seek(%s, %d, %ld) "
            "failed: %s\n",
            rd_kafka_topic_name(rkt), partition, offset,
            rd_kafka_err2str(err));
    return -1;
  }

  return 0;
}

/* ==========END KAFKA FUNCTIONS ========== */

/* ==========START SUPPORT FUNCTIONS ========== */

static int add_peerid_mapping(bgpview_io_kafka_t *client,
                              bgpview_iter_t *it,
                              bgpstream_peer_sig_t *sig,
                              bgpstream_peer_id_t remote_id)
{
  int j;
  bgpstream_peer_id_t local_id;

  /* first, is the array big enough to possibly already contain remote_id? */
  if ((remote_id+1) > client->peerid_map_alloc_cnt)
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

/** Returns the nth token of real_string as an integer */
static int num_elements(char *real_string, int position)
{
  char *token, *cpy, *str;
  int j = 0;
  int res = -1;

  if ((cpy = str = strdup(real_string)) == NULL)
    {
      return -1;
    }

  while ((token = strsep(&str, " ")) != NULL)
    {
      if(j == position)
        {
          res = atoi(token);
          break;
        }
        j++;
    }

  free(cpy);
  return res;
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
  client->sync_view_id = bgpview_get_time(view);

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
                         bgpview_t *view, char *type)
{
  rd_kafka_t *rk = client->metadata_rk;
  rd_kafka_topic_t *rkt = client->metadata_rkt;

  char offset_message[256];
  int nc;

  int publish_partition = client->pfxs_partition;

  long int peers_offset = client->current_peers_offset;
  long int pfxs_offset = client->current_pfxs_offset;

  if(strcmp(type,"SYNC") == 0)
    {
      nc = sprintf(offset_message,
                   "SYNC VIEW: %"PRIu32" PUBLISHED IN PARTITION: %d "
                   "AT OFFSET: %ld. PEERS AT OFFSET, %ld",
                   bgpview_get_time(view),
                   publish_partition,
                   pfxs_offset,
                   peers_offset);
    }
  else if(strcmp(type,"DIFF") == 0)
    {
      int view_sync_id = client->sync_view_id;
      int view_sync_partition = client->pfxs_sync_partition;
      long int view_sync_offset = client->pfxs_sync_offset;
      nc = sprintf(offset_message,
                   "DIFF VIEW: %"PRIu32" PUBLISHED IN PARTITION: %d "
                   "AT OFFSET: %ld WITH SYNC VIEW: %d IN PARTITION %d "
                   "AT OFFSET: %ld. PEERS AT OFFSET, %ld",
                   bgpview_get_time(view),
                   publish_partition,
                   pfxs_offset,
                   view_sync_id,
                   view_sync_partition,
                   view_sync_offset,
                   peers_offset);
  }
  else
    {
      goto err;
    }

  if (nc>0)
    {
      offset_message[nc]='\0';
    }

  if(rd_kafka_produce(rkt, client->metadata_partition,
                      RD_KAFKA_MSG_F_COPY,
                      offset_message, strlen(offset_message),
                      NULL, 0, NULL) == -1)
    {
      fprintf(stderr,"ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt),
              client->metadata_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      //Poll to handle delivery reports
      rd_kafka_poll(rk, 0);
      goto err;
    }

  /* Wait for messages to be delivered */
  while (rd_kafka_outq_len(rk) > 0)
    {
      rd_kafka_poll(rk, 100);
    }

  return 0;

 err:
  return -1;

}

static int recv_metadata(bgpview_io_kafka_t *client)
{
  rd_kafka_message_t *rkmessage = NULL;
  int i, nf = 0;
  long int offset =
    get_offset(client, client->metadata_topic, client->metadata_partition);

  long int history_offset = offset - client->view_frequency - 1;

  if(history_offset < 0)
    {
      history_offset = 0;
    }

  if(client->metadata_offset != 0)
    {
      history_offset = client->metadata_offset;
    }

  if(change_consumer_offset_partition(client->metadata_rkt,
                                      client->metadata_partition,
                                      history_offset) != 0)
    {
      fprintf(stderr,"Error changing the offset");
      return -1;
    }

  for(i=0; i < client->view_frequency+2; i++)
    {
      rkmessage = rd_kafka_consume(client->metadata_rkt,
                                   client->metadata_partition,
                                   2000000000);

      if(rkmessage == NULL)
        {
          return -1;
        }
      if(rkmessage->payload == NULL)
        {
          if(rkmessage->err!=RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
              fprintf(stderr, "Cannot not receive metadata\n");
              return -1;
            }
          if(i!=0)
            {
              break;
            }
          rkmessage = rd_kafka_consume(client->metadata_rkt,
                                       client->metadata_partition,
                                       2000000000);
        }
      client->metadata_offset++;
      if(strstr(rkmessage->payload, "DIFF") == NULL)
        {
          nf=0;
          client->pfxs_sync_partition = num_elements(rkmessage->payload, 6);
          client->pfxs_sync_offset = num_elements(rkmessage->payload, 9);
          client->sync_view_id = num_elements(rkmessage->payload, 2);
          client->peers_sync_offset = num_elements(rkmessage->payload, 12);
          client->num_diffs = 0;
        }
      else
        {
          client->peers_diffs_offset[nf] =
            num_elements(rkmessage->payload, 23);
          client->pfxs_diffs_partition[nf] =
            num_elements(rkmessage->payload, 6);
          client->pfxs_diffs_offset[nf] = num_elements(rkmessage->payload, 9);
          nf++;
          client->num_diffs = nf;
        }
      rd_kafka_message_destroy(rkmessage);
    }

  return 0;
}

static int send_peers(bgpview_io_kafka_t *client,
                      bgpview_iter_t *it, bgpview_t *view,
                      bgpview_io_filter_cb_t *cb)
{
  time_t rawtime;
  time ( &rawtime );

  rd_kafka_topic_t *rkt = client->peers_rkt;
  rd_kafka_t *rk = client->peers_rk;

  char begin_message[256];

  uint8_t buf[BUFFER_LEN];
  size_t len = BUFFER_LEN;
  ssize_t written = 0;

  int peers_tx=0;
  int filter;

  //SEND BEGIN MESSAGE
  int np = bgpview_peer_cnt(view, BGPVIEW_FIELD_ACTIVE);
  int nc = sprintf(begin_message, "BEGIN - PEER: %d",np);
  if(nc > 0)
    {
      begin_message[nc]='\0';
    }

  if(rd_kafka_produce(rkt, client->peers_partition,
                      RD_KAFKA_MSG_F_COPY, begin_message,
                      strlen(begin_message), NULL, 0,NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt), client->peers_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0); //Poll to handle delivery reports
      goto err;
    }

  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
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

      if ((written =
           bgpview_io_serialize_peer(buf, len,
                                     bgpview_iter_peer_get_peer_id(it),
                                     bgpview_iter_peer_get_sig(it))) < 0)
        {
          goto err;
        }

      if(rd_kafka_produce(rkt, client->peers_partition,
                          RD_KAFKA_MSG_F_COPY, buf, written,
                          NULL, 0, NULL) == -1)
        {
          fprintf(stderr,
                  "ERROR: Failed to produce to topic %s partition %i: %s\n",
                  rd_kafka_topic_name(rkt), client->peers_partition,
                  rd_kafka_err2str(rd_kafka_errno2err(errno)));
          rd_kafka_poll(rk, 0); //Poll to handle delivery reports
          goto err;
        }
    }

  assert(peers_tx <= UINT16_MAX);
  nc = sprintf(begin_message, "END PEER: %d",peers_tx);
  if(nc > 0)
    {
      begin_message[nc]='\0';
    }
  if(rd_kafka_produce(rkt, client->peers_partition, RD_KAFKA_MSG_F_COPY,
                      begin_message, strlen(begin_message),
                      NULL, 0, NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt), client->peers_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0);
      goto err;
    }

  while (rd_kafka_outq_len(rk) > 0)
    {
      rd_kafka_poll(rk, 100);
    }

  /*
  printf("End writing peers in partition 0. Current local time and date: %s",
         asctime (timeinfo));
  */

  return 0;

 err:
  return -1;
}

static int send_peer_diffs(bgpview_io_kafka_t *client, bgpview_iter_t *itH,
                           bgpview_iter_t *itC, uint32_t view_id,
                           uint32_t sync_view_id)
{
  uint8_t buf[BUFFER_LEN];
  size_t len = BUFFER_LEN;
  ssize_t written = 0;

  rd_kafka_t *rk = client->peers_rk;
  rd_kafka_topic_t *rkt = client->peers_rkt;
  int partition = client->peers_partition;
  int total_np=0;
  char message[256];

  len = sprintf(message, "BEGIN DIFF PEERS VIEW %d WITH SYNC VIEW: %d",
                view_id, sync_view_id);
  message[len]='\0';

  if(rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                      message, strlen(message),
                      NULL, 0,NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt), partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0); //Poll to handle delivery reports
      return -1;
  }

  for(bgpview_iter_pfx_first_peer(itC, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_pfx_has_more_peer(itC);
      bgpview_iter_pfx_next_peer(itC))
    {
      //check if current peer id exist in old view
      bgpstream_peer_id_t peerid = bgpview_iter_peer_get_peer_id(itC);
      if(bgpview_iter_seek_peer(itH, peerid,BGPVIEW_FIELD_ACTIVE) == 0)
        {
          if ((written =
               bgpview_io_serialize_peer(buf, len,
                                         bgpview_iter_peer_get_peer_id(itC),
                                         bgpview_iter_peer_get_sig(itC))) < 0)
            {
              return -1;
            }

          if(rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                              buf, written, NULL, 0,NULL) == -1)
            {
              fprintf(stderr,
                      "ERROR: Failed to produce to topic %s partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(rd_kafka_errno2err(errno)));
              rd_kafka_poll(rk, 0); //Poll to handle delivery reports
              return -1;
            }
          total_np++;
        }
    }

  len = sprintf(message,
                "END DIFF PEERS VIEW %d WITH SYNC VIEW: %d NEW PEERS %d",
                view_id,sync_view_id,total_np);
  message[len]='\0';

  if(rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                      message, strlen(message), NULL, 0, NULL) == -1)
    {
      fprintf(stderr,"ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt), partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0); //Poll to handle delivery reports
      return -1;
    }

  return 0;
}

static int recv_peers(bgpview_io_kafka_t *client, bgpview_iter_t *iter,
                      bgpview_io_filter_peer_cb_t *peer_cb,
                      long int offset)
{
  ssize_t read;

  bgpstream_peer_id_t peerid_remote;
  bgpstream_peer_sig_t ps;

  int pc = -1;
  int peers_rx = 0;
  int filter;

  rd_kafka_topic_t *rkt = client->peers_rkt;

  if(change_consumer_offset_partition(rkt, client->peers_partition,
                                      offset) != 0)
    {
      fprintf(stderr,"Error changing the offset");
      goto err;
    }

  while(1)
    {
      rd_kafka_message_t *rkmessage;

      /* peerid (or end-of-peers)*/
      rkmessage = rd_kafka_consume(rkt, client->peers_partition, 1000);
      if(rkmessage->payload == NULL)
	{
          /* end of peers */
    	  if(rkmessage->err != RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
              fprintf(stderr, "Cannot not receive peer id\n");
              goto err;
            }
          break;
	}

      /* fix this horrible stuff when we have non-sentence begin/ends */
      if (rkmessage->len < 5)
        {
          goto err;
        }
      if(strstr(rkmessage->payload, "END") != NULL)
        {
          goto done;
        }
      if(strstr(rkmessage->payload, "BEGIN") != NULL)
        {
          if(pc == -1)
            {
              pc = num_elements(rkmessage->payload, 3);
            }
          else
            {
              fprintf(stderr,"Can not read all peers\n");
              goto err;
            }
          fprintf(stderr, "pc: %d\n", pc);
          continue;
        }

      if(pc == -1)
        {
          fprintf(stderr,"No number of peers\n");
          goto err;
        }

      if ((read = bgpview_io_deserialize_peer(rkmessage->payload,
                                              rkmessage->len,
                                              &peerid_remote,
                                              &ps)) < 0)
        {
          goto err;
        }

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

      peers_rx++;

      rd_kafka_message_destroy(rkmessage);
      continue;

    done:
      rd_kafka_message_destroy(rkmessage);
      break;
    }

  assert(pc == peers_rx);
  return 0;

 err:
  return -1;
}

static int send_pfxs(bgpview_io_kafka_t *client,
                     bgpview_io_kafka_stats_t *stats,
                     bgpview_iter_t *it, bgpview_io_filter_cb_t *cb)
{
  time_t rawtime;
  time(&rawtime);

  rd_kafka_topic_t *rkt = client->pfxs_rkt;
  rd_kafka_t *rk = client->pfxs_rk;

  int filter;
  int peers_cnt;
  int paths_tx = 0;
  int npfx = 0;

  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t len = BUFFER_LEN;
  size_t written = 0;
  ssize_t s = 0;

  char begin_message[256];

  bgpview_t *view = bgpview_iter_get_view(it);
  assert(view != NULL);

  int nc = sprintf(begin_message,
                   "BEGIN - VIEW: %"PRIu32" WITH %d PREFIXES",
                   bgpview_get_time(view),
                   bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE));
  if(nc > 0)
    {
      begin_message[nc] = '\0';
    }

  while(rd_kafka_produce(rkt, client->pfxs_partition, RD_KAFKA_MSG_F_COPY,
                         begin_message, strlen(begin_message),
                         NULL, 0, NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt), client->pfxs_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0); //Poll to handle delivery reports
      goto err;
  }

  for(bgpview_iter_first_pfx(it, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(it);
      bgpview_iter_next_pfx(it))
    {
      /* reset the buffer */
      written = 0;
      ptr = buf;

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

      peers_cnt = bgpview_iter_pfx_get_peer_cnt(it, BGPVIEW_FIELD_ACTIVE);

      if(peers_cnt == 0)
        {
          continue;
        }

      if ((s = pfx_row_serialize(ptr, len, 'U', it, cb)) < 0)
        {
          goto err;
        }
      written += s;
      ptr += s;

      if(rd_kafka_produce(rkt, client->pfxs_partition, RD_KAFKA_MSG_F_COPY,
                          buf, written, NULL, 0, NULL) == -1)
        {
          fprintf(stderr,
                  "ERROR: Failed to produce to topic %s partition %i: %s\n",
                  rd_kafka_topic_name(rkt), client->pfxs_partition,
                  rd_kafka_err2str(rd_kafka_errno2err(errno)));
          //Poll to handle delivery reports
          rd_kafka_poll(rk, 0);
          goto err;
        }
      rd_kafka_poll(rk, 0);

      npfx++;
    }

  assert(paths_tx <= UINT32_MAX);

  stats->add = 0;
  stats->remove = 0;
  stats->change = 0;
  stats->common = 0;
  stats->historical_pfx_cnt = 0;
  stats->current_pfx_cnt = stats->sync_cnt =
    bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE);

  nc = sprintf(begin_message,
               "END - VIEW: %"PRIu32" WITH %d PREFIXES AND %d PATHS",
               bgpview_get_time(view),
               bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE),
               paths_tx);
  if(nc > 0)
    {
      begin_message[nc] = '\0';
    }

  if(rd_kafka_produce(rkt, client->pfxs_partition, RD_KAFKA_MSG_F_COPY,
                      begin_message, strlen(begin_message),
                      NULL, 0,NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt), client->pfxs_partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      //Poll to handle delivery reports
      rd_kafka_poll(rk, 0);
      return -1;
    }

  while (rd_kafka_outq_len(rk) > 0)
    {
      rd_kafka_poll(rk, 100);
    }

  time(&rawtime);

  return 0;

 err:
  return -1;
}

static int send_pfxs_diffs(bgpview_io_kafka_t *client,
                           bgpview_io_kafka_stats_t *stats,
                           bgpview_t *viewH, bgpview_t *viewC,
                           bgpview_iter_t *itH, bgpview_iter_t *itC,
                           bgpview_io_filter_cb_t *cb,
                           uint32_t view_id, uint32_t sync_view_id)
{
  int npfxsH = bgpview_pfx_cnt(viewH,BGPVIEW_FIELD_ACTIVE);
  int npfxsC = bgpview_pfx_cnt(viewC,BGPVIEW_FIELD_ACTIVE);

  int total_prefixes = 0;

  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t len = BUFFER_LEN;
  size_t written = 0;
  ssize_t s;
  int send;

  rd_kafka_t *rk = client->pfxs_rk;
  rd_kafka_topic_t *rkt = client->pfxs_rkt;
  int partition = client->pfxs_partition;

  char message[256];

  len= sprintf(message, "BEGIN DIFF VIEW %d WITH SYNC VIEW: %d",
               view_id, sync_view_id);
  message[len]='\0';

  if(rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                      message, strlen(message), NULL, 0, NULL) == -1)
    {
      fprintf(stderr,"ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt), partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0);
      goto err;
    }
  int common = 0;
  int change = 0;
  total_prefixes = 0;

  for(bgpview_iter_first_pfx(itC, 0 , BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(itC);
      bgpview_iter_next_pfx(itC))
    {
      bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itC);

      /* reset the buffer (@todo optimize this) */
      written = 0;
      ptr = buf;
      send = 1;

      if(bgpview_iter_seek_pfx(itH, pfx, BGPVIEW_FIELD_ACTIVE) == 1)
        {
          common++;
          if(diff_rows(itH, itC) == 1)
            {
              change++;
              /* pfx has changed, send */
            }
          else
            {
              send = 0;
            }
        }
      /* else, send the pfx */

      if (send != 0)
        {
          if ((s = pfx_row_serialize(ptr, len, 'U', itC, cb)) < 0)
            {
              goto err;
            }
          written += s;
          ptr += s;

          total_prefixes++;

          if(rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                              buf, len, NULL, 0, NULL) == -1)
            {
              fprintf(stderr,
                      "ERROR: Failed to produce to topic %s partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(rd_kafka_errno2err(errno)));
              rd_kafka_poll(rk, 0);
              goto err;
            }
        }
    }

  int rm_pfxsH = npfxsH - common;

  fprintf(stderr, "HC %d CC %d common %d remove %d change %d\n",
          npfxsH, npfxsC, common, rm_pfxsH, change);
  assert(rm_pfxsH >= 0);

  stats->add = (npfxsC - common) + rm_pfxsH;
  if(stats->add < 0)
    {
      stats->add = 0;
    }
  stats->remove = rm_pfxsH;
  stats->change = change;
  stats->common = common;

  stats->current_pfx_cnt = stats->common + stats->add;
  stats->historical_pfx_cnt = stats->common + stats->remove;
  stats->sync_cnt = 0;

  if(rm_pfxsH > 0)
    {
      for(bgpview_iter_first_pfx(itH, 0 , BGPVIEW_FIELD_ACTIVE);
          bgpview_iter_has_more_pfx(itH);
          bgpview_iter_next_pfx(itH))
        {
          written = 0;
          ptr = buf;

          bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itH);

          if(bgpview_iter_seek_pfx(itC, pfx, BGPVIEW_FIELD_ACTIVE) != 0)
            {
              continue;
            }

          if ((s = pfx_row_serialize(ptr, len, 'R', itH, cb)) < 0)
            {
              goto err;
            }
          written += s;
          ptr += s;

          rm_pfxsH--;
          total_prefixes++;
          if(rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                              buf, len, NULL, 0, NULL) == -1)
            {
              fprintf(stderr,
                      "ERROR: Failed to produce to topic %s partition %i: %s\n",
                      rd_kafka_topic_name(rkt), partition,
                      rd_kafka_err2str(rd_kafka_errno2err(errno)));
              rd_kafka_poll(rk, 0);
              goto err;
            }

          /* stop looking if we have removed all we need to */
          if(rm_pfxsH == 0)
            {
              break;
            }
        }
    }

  len = sprintf(message,
                "END DIFF VIEW %d WITH SYNC VIEW: %d AND %d PFXS",
                view_id, sync_view_id, total_prefixes);
  message[len]='\0';

  if(rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
                      message, strlen(message), NULL, 0, NULL) == -1)
    {
      fprintf(stderr,
              "ERROR: Failed to produce to topic %s partition %i: %s\n",
              rd_kafka_topic_name(rkt), partition,
              rd_kafka_err2str(rd_kafka_errno2err(errno)));
      rd_kafka_poll(rk, 0);
      goto err;
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
  rd_kafka_topic_t *rkt = client->pfxs_rkt;

  if(change_consumer_offset_partition(rkt, partition, offset) != 0)
    {
      fprintf(stderr,"Error changing the offset");
      goto err;
    }

  bgpview_t *view = NULL;
  uint32_t view_time;

  size_t read = 0;
  size_t len;
  uint8_t *ptr;
  ssize_t s;

  bgpstream_pfx_storage_t pfx;
  int pfx_cnt = 0;
  int pfx_rx = 0;

  int tor = 0;
  int tom = 0;

  rd_kafka_message_t *rkmessage;

  if(iter != NULL)
    {
      view = bgpview_iter_get_view(iter);
    }

  while(1)
    {
      rkmessage = rd_kafka_consume(rkt, client->pfxs_partition, 1000);

      if(rkmessage->payload == NULL)
        {
          if(rkmessage->err!=RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
              fprintf(stderr, "Cannot receive prefixes and paths\n");
              goto err;
            }
          break;
        }

      /* fixme fixme fixme! */
      if (rkmessage->len < 5)
        {
          goto err;
        }
      if(strstr(rkmessage->payload, "END") != NULL)
        {
          /* this is an end message */
          if(strstr(rkmessage->payload, "DIFF") == NULL)
            {
              /* this was a diff message */
              pfx_cnt = num_elements(rkmessage->payload, 5);
              view_time = num_elements(rkmessage->payload, 3);
            }
          else
            {
              /* this was a sync message */
              pfx_cnt = num_elements(rkmessage->payload, 9);
              view_time = num_elements(rkmessage->payload, 3);
            }
          if(iter != NULL)
            {
              bgpview_set_time(view, view_time);
            }
          /* since this is the end, stop looping */
          break;
        }
      if(strstr(rkmessage->payload, "BEGIN") != NULL)
        {
          /* this is a begin message */
          /* just ignore it */
          goto next;
        }

      /* this is a prefix row message */

      pfx_rx++;
      ptr = rkmessage->payload;
      len = rkmessage->len;
      read = 0;

      switch(ptr[read])
        {
        case 'U':
          read++;
          ptr++;

          /* an update row */
          tom++;
          if ((s = bgpview_io_deserialize_pfx_row(ptr, (len-read), iter,
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
          read++;
          ptr++;

          /* a remove row */
          tor++;
          /* just grab the prefix and then deactivate it */
          if((s = bgpview_io_deserialize_pfx(ptr, (len-read), &pfx)) == -1)
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

 next:
      rd_kafka_message_destroy (rkmessage);
    }

  fprintf(stderr,"DEBUG: U: %d R: %d\n", tom, tor);

  assert(pfx_rx == pfx_cnt);

  return 0;

 err:
  return -1;
}

static int send_sync_view(bgpview_io_kafka_t *client,
                          bgpview_io_kafka_stats_t *stats,
                          bgpview_t *view,
                          bgpview_io_filter_cb_t *cb)
{
  fprintf(stderr,"Send Sync\n");
  bgpview_iter_t *it = NULL;

  if((it = bgpview_iter_create(view)) == NULL)
    {
      goto err;
    }

  set_sync_view_data(client, view);

  set_current_offsets(client);

  if(send_peers(client, it, view, cb) != 0)
    {
      goto err;
    }
  if(send_pfxs(client, stats, it, cb) != 0)
    {
      goto err;
    }
  if(send_metadata(client, view, "SYNC") == -1)
    {
      fprintf(stderr,"Error on publishing the offset\n");
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
  fprintf(stderr, "Send Diff %d\n", client->num_diffs);

  bgpview_iter_t *itH = NULL, *itC = NULL;
  uint32_t view_id = bgpview_get_time(view);
  uint32_t sync_view_id = client->sync_view_id;

  if(((itH = bgpview_iter_create(client->viewH)) == NULL) ||
     ((itC = bgpview_iter_create(view)) == NULL))
    {
      goto err;
    }

  if(set_current_offsets(client) == -1)
    {
      goto err;
    }

  if(send_peer_diffs(client, itH, itC, view_id, sync_view_id) == -1)
    {
      goto err;
    }

  if(send_pfxs_diffs(client, stats, client->viewH, view,
                     itH, itC, cb, view_id, sync_view_id) ==- 1)
    {
      goto err;
    }

  if(send_metadata(client, view, "DIFF") == -1)
    {
      fprintf(stderr,"Error on publishing the offset\n");
      goto err;
    }

  client->num_diffs++;

  bgpview_iter_destroy(itH);
  bgpview_iter_destroy(itC);

  return 0;

 err:
  bgpview_iter_destroy(itH);
  bgpview_iter_destroy(itC);
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
