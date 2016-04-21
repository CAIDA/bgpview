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

static int add_peerid_mapping(bgpview_io_kafka_peeridmap_t *idmap,
                              bgpview_iter_t *it,
                              bgpstream_peer_sig_t *sig,
                              bgpstream_peer_id_t remote_id
#ifdef WITH_THREADS
                              ,pthread_mutex_t *mutex
#endif
                              )
{
  int j;
  bgpstream_peer_id_t local_id;

  /* first, is the array big enough to possibly already contain remote_id? */
  if (remote_id >= idmap->alloc_cnt) {
    if ((idmap->map =
           realloc(idmap->map,
                   sizeof(bgpstream_peer_id_t) * (remote_id + 1))) == NULL) {
      return -1;
    }

    /* now set all ids to 0 (reserved) */
    for (j = idmap->alloc_cnt; j <= remote_id; j++) {
      idmap->map[j] = 0;
    }
    idmap->alloc_cnt = remote_id + 1;
  }

#ifdef WITH_THREADS
  pthread_mutex_lock(mutex);
#endif
  /* just blindly add the peer */
  if ((local_id = bgpview_iter_add_peer(
           it, sig->collector_str, (bgpstream_ip_addr_t *)&sig->peer_ip_addr,
           sig->peer_asnumber)) == 0) {
    return -1;
  }
  /* ensure the peer is active */
  bgpview_iter_activate_peer(it);
#ifdef WITH_THREADS
  pthread_mutex_unlock(mutex);
#endif
  idmap->map[remote_id] = local_id;

  /* by here we are guaranteed to have a valid mapping */
  return idmap->map[remote_id];
}

static void clear_peerid_mapping(bgpview_io_kafka_peeridmap_t *idmap)
{
  memset(idmap->map, 0,
         sizeof(bgpstream_peer_id_t) * idmap->alloc_cnt);
}

/* returns 1 if the offset is valid, 0 if it is below "low" */
static int check_offset(rd_kafka_t *rdk_conn, const char *topic,
                        int32_t partition, int64_t offset)
{
  int64_t low = 0;
  int64_t high = 0;

  if (rd_kafka_query_watermark_offsets(rdk_conn, topic, partition, &low,
                                       &high,
                                       1000) != RD_KAFKA_RESP_ERR_NO_ERROR) {
    return 0;
  }
  if (offset < low) {
    fprintf(stderr, "WARN: Invalid offset %"PRIi64" on %s\n",
            offset, topic);
    fprintf(stderr, "INFO: Valid offsets are %"PRIi64" - %"PRIu64"\n",
            low, high);
  }
  return (offset > low);
}

static int seek_topic(rd_kafka_t *rdk_conn, rd_kafka_topic_t *rkt,
                      int32_t partition, int64_t offset)
{
  int err;

  if (check_offset(rdk_conn, rd_kafka_topic_name(rkt),
                   partition, offset) != 1) {
    return -1;
  }

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

  return read;

err:
  return -1;
}

static int deserialize_global_metadata(bgpview_io_kafka_md_t **metasptr,
                                       int64_t *last_sync_offset,
                                       uint8_t *buf,
                                       size_t len)
{
  size_t read = 0;
  ssize_t s;

  *metasptr = NULL;

  /* get the number of members */
  uint16_t members_cnt;
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, members_cnt);

  if (members_cnt == 0) {
    fprintf(stderr, "ERROR: Empty global metadata message\n");
    goto err;
  }

  /* allocate the metas array */
  bgpview_io_kafka_md_t *metas;
  if ((metas =
       malloc_zero(sizeof(bgpview_io_kafka_md_t) * members_cnt)) == NULL) {
    return -1;
  }

  int i;
  bgpview_io_kafka_md_t common;
  /* hush anxious gcc5: */
  common.time = 0; common.type = '\0'; common.parent_time = 0;
  for (i=0; i<members_cnt; i++) {
    if ((s = deserialize_metadata(&metas[i], buf, (len-read))) <= 0) {
      goto err;
    }
    read += s;
    buf += s;
    if (i == 0) {
      // copy into common
      common = metas[i];
    } else {
      // check against common
      if (metas[i].time != common.time ||
          metas[i].type != common.type ||
          metas[i].parent_time != common.parent_time) {
        fprintf(stderr, "WARN: Found inconsistent global metadata. Skipping\n");
        return 0;
      }
    }
  }

  // NB: last_sync_offset is a ptr, but this macro expects a regular variable,
  // so we de-reference the ptr, and then the macro will take the address of
  // that
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, *last_sync_offset);

  assert(read == len);

  *metasptr = metas;
  return members_cnt;

 err:
  return -1;
}

/* ==========END SUPPORT FUNCTIONS ========== */

/* ==========START SEND/RECEIVE FUNCTIONS ========== */

static int recv_direct_metadata(bgpview_io_kafka_t *client, bgpview_t *view,
                                bgpview_io_kafka_md_t *meta, int need_sync)
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
  if (deserialize_metadata(meta, msg->payload, msg->len) != msg->len) {
    fprintf(stderr, "ERROR: Could not deserialize metadata message\n");
    goto err;
  }
  /* we're done with this message */
  rd_kafka_message_destroy(msg);
  msg = NULL;

  /* Can we use this view? */
  assert(client->mode == BGPVIEW_IO_KAFKA_MODE_DIRECT_CONSUMER);
  if (strncmp(meta->identity, client->identity, IDENTITY_MAX_LEN) != 0) {
    fprintf(stderr,
            "INFO: Skipping view from producer '%s' (looking for '%s')\n",
            meta->identity, client->identity);
    goto again;
  }
  if (meta->type == 'D' && need_sync != 0) {
    fprintf(stderr, "INFO: Found diff frame at %d but need sync frame\n",
            meta->time);
    goto again;
  }
  if (meta->type != 'S' && meta->parent_time != bgpview_get_time(view)) {
    /* this is a diff frame with a parent time that does not match the time of
       the view that we are given */
    fprintf(stderr, "WARN: Found Diff frame against %d, but view time is %d\n",
            meta->parent_time, bgpview_get_time(view));
    fprintf(stderr, "INFO: Rewinding to last sync frame\n");
    if (seek_topic(client->rdk_conn, RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_META),
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
    clear_peerid_mapping(&client->dc_state.idmap);
  }

  assert(msg == NULL);
  return 0;

err:
  if (msg != NULL) {
    rd_kafka_message_destroy(msg);
  }
  return -1;
}

static int recv_global_metadata(bgpview_io_kafka_t *client, bgpview_t *view,
                                bgpview_io_kafka_md_t **metasptr,
                                int need_sync)
{
  rd_kafka_message_t *msg = NULL;
  bgpview_io_kafka_md_t *metas = NULL;

again:
  if (metas != NULL) {
    free(metas);
    metas = NULL;
  }
  /* Grab the next metadata message */
  if ((msg = rd_kafka_consume(RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_GLOBALMETA),
                              BGPVIEW_IO_KAFKA_GLOBALMETADATA_PARTITION_DEFAULT,
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
  int metas_cnt;
  int64_t last_sync_offset = -1;
  if ((metas_cnt =
       deserialize_global_metadata(&metas, &last_sync_offset,
                                   msg->payload, msg->len)) < 0) {
    fprintf(stderr, "ERROR: Could not deserialize metadata message\n");
    goto err;
  }
  /* we're done with this message */
  rd_kafka_message_destroy(msg);
  msg = NULL;

  /* can we use this view */
  if (metas_cnt == 0) {
    /* GMD was inconsistent and thus unusable, try again */
    goto again;
  }
  if (metas[0].type == 'D' && need_sync != 0) {
    fprintf(stderr, "INFO: Found diff frame at %d but need sync frame\n",
            metas[0].time);
    goto again;
  }
  /* since by here we know all members are giving a view for the same time,
     type, and parent view, we can just check the first member's metadata */
  if (metas[0].type != 'S' && metas[0].parent_time != bgpview_get_time(view)) {
     fprintf(stderr, "WARN: Found Diff frame against %d, but view time is %d\n",
            metas[0].parent_time, bgpview_get_time(view));

     if (last_sync_offset == -1) {
       fprintf(stderr, "INFO: No rewind info. Waiting for next sync frame\n");
     } else {
       fprintf(stderr, "INFO: Rewinding to last sync frame (%"PRIi64")\n",
               last_sync_offset);
       if (seek_topic(client->rdk_conn,
                      RKT(BGPVIEW_IO_KAFKA_TOPIC_ID_GLOBALMETA),
                      BGPVIEW_IO_KAFKA_GLOBALMETADATA_PARTITION_DEFAULT,
                      last_sync_offset) != 0) {
         fprintf(stderr, "ERROR: Could not seek to last sync metadata\n");
         goto err;
       }
     }

     goto again;
  }

  /* we can use this view! */

  /* if it is a Sync frame we need to clean up the view that we were given */
  if (metas[0].type == 'S') {
    bgpview_clear(view);
  }

  assert(msg == NULL);
  assert(metasptr != NULL);
  *metasptr = metas;
  return metas_cnt;

err:
  if (msg != NULL) {
    rd_kafka_message_destroy(msg);
  }
  return -1;
}

static int recv_peers(bgpview_io_kafka_peeridmap_t *idmap,
                      bgpview_io_kafka_topic_t *topic,
                      bgpview_iter_t *iter,
                      bgpview_io_filter_peer_cb_t *peer_cb, int64_t offset,
                      uint32_t exp_time,
#ifdef WITH_THREADS
                      pthread_mutex_t *mutex,
#endif
                      rd_kafka_t *rdk_conn)
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

  if (seek_topic(rdk_conn, topic->rkt,
                 BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT,
                 offset) != 0) {
    goto err;
  }

  /* receive the peers */
  while (1) {
    msg = rd_kafka_consume(topic->rkt,
                           BGPVIEW_IO_KAFKA_PEERS_PARTITION_DEFAULT, 5000);
    if (msg == NULL) {
      fprintf(stderr, "INFO: Failed to retrieve peer message. Retrying...\n");
      continue;
    }
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

    if (add_peerid_mapping(idmap, iter, &ps, peerid_remote
#ifdef WITH_THREADS
                           ,mutex
#endif
                           ) <= 0) {
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

static int recv_pfxs(bgpview_io_kafka_peeridmap_t *idmap,
                     bgpview_io_kafka_topic_t *topic,
                     bgpview_iter_t *iter,
                     bgpview_io_filter_pfx_cb_t *pfx_cb,
                     bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
                     int64_t offset, uint32_t exp_time,
#ifdef WITH_THREADS
                     pthread_mutex_t *mutex,
#endif
                     rd_kafka_t *rdk_conn)
{
  bgpview_t *view = NULL;
  uint32_t view_time;

  size_t read = 0;
  uint8_t *ptr;
  ssize_t s;

  char type;

  uint32_t pfx_cnt = 0;
  int pfx_rx = 0;

  int tor = 0;
  int tom = 0;

  rd_kafka_message_t *msg = NULL;

  if (seek_topic(rdk_conn, topic->rkt,
                 BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT,
                 offset) != 0) {
    goto err;
  }

  if (iter != NULL) {
    view = bgpview_iter_get_view(iter);
  }

  while (1) {
    msg = rd_kafka_consume(topic->rkt,
                           BGPVIEW_IO_KAFKA_PFXS_PARTITION_DEFAULT, 5000);
    if (msg == NULL) {
      fprintf(stderr, "INFO: Failed to retrieve prefix message. Retrying...\n");
      continue;
    }
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
      assert(read == msg->len);

      rd_kafka_message_destroy(msg);
      msg = NULL;
      break;
    }

    /* this is a prefix row message */
    pfx_rx++;

    switch (type) {
    case 'S':
    case 'U':
      /* an update row */
      tom++;
#ifdef WITH_THREADS
      pthread_mutex_lock(mutex);
#endif
      if ((s = bgpview_io_deserialize_pfx_row(
             ptr, (msg->len - read), iter, pfx_cb, pfx_peer_cb,
             idmap->map,
             idmap->alloc_cnt, NULL, -1,
             BGPVIEW_FIELD_ACTIVE)) ==
          -1) {
        goto err;
      }
#ifdef WITH_THREADS
      pthread_mutex_unlock(mutex);
#endif
      read += s;
      ptr += s;
      break;

    case 'R':
      /* a remove row */
      tor++;
#ifdef WITH_THREADS
      pthread_mutex_lock(mutex);
#endif
      if ((s = bgpview_io_deserialize_pfx_row(
             ptr, (msg->len - read), iter, pfx_cb, pfx_peer_cb,
             idmap->map,
             idmap->alloc_cnt,
             NULL, -1,
             BGPVIEW_FIELD_INACTIVE)) ==
          -1) {
        goto err;
      }
#ifdef WITH_THREADS
      pthread_mutex_unlock(mutex);
#endif
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

static int recv_view(bgpview_io_kafka_peeridmap_t *idmap,
                     bgpview_t *view,
                     bgpview_io_kafka_md_t *meta,
                     bgpview_io_kafka_topic_t *peers_topic,
                     bgpview_io_kafka_topic_t *pfxs_topic,
                     bgpview_io_filter_peer_cb_t *peer_cb,
                     bgpview_io_filter_pfx_cb_t *pfx_cb,
                     bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
#ifdef WITH_THREADS
                     pthread_mutex_t *mutex,
#endif
                     rd_kafka_t *rdk_conn)
{
  bgpview_iter_t *it = NULL;

  if (view != NULL && (it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  if (recv_peers(idmap, peers_topic, it, peer_cb,
                 meta->peers_offset, meta->time,
#ifdef WITH_THREADS
                 mutex,
#endif
                 rdk_conn) < 0) {
    return -1;
  }

  if (recv_pfxs(idmap, pfxs_topic, it, pfx_cb, pfx_peer_cb,
                meta->pfxs_offset, meta->time,
#ifdef WITH_THREADS
                mutex,
#endif
                rdk_conn) != 0) {
    goto err;
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

#ifdef WITH_THREADS
static void *thread_worker(void *user)
{
  gc_topics_t *gct = (gc_topics_t *)user;

  while (gct->shutdown == 0) {
    /* signal that we are ready to do some work */
    pthread_mutex_lock(&gct->mutex);
    gct->worker_ready = 1;
    pthread_cond_signal(&gct->worker_ready_cond);

    /* block until there is something for us to do */
    while (gct->view_waiting == 0) {
      if (gct->shutdown != 0) {
        break;
      }
      pthread_cond_wait(&gct->view_waiting_cond, &gct->mutex);
    }
    if (gct->shutdown != 0) {
      break;
    }
    assert(gct->view_waiting != 0);
    gct->view_waiting = 0; /* we've workin on it! */
    gct->worker_ready = 0; /* hey, we're busy! */
    gct->recv_error = 0; /* so far, so good... */
    pthread_mutex_unlock(&gct->mutex);

    /* do some work! */
    /* ask to read each view */
    if (recv_view(&gct->idmap, gct->view, gct->meta, &gct->peers, &gct->pfxs,
                  gct->peer_cb, gct->pfx_cb, gct->pfx_peer_cb,
                  gct->view_mutex, gct->rdk_conn) != 0) {
      pthread_mutex_lock(&gct->mutex);
      gct->recv_error = 1;
      pthread_mutex_unlock(&gct->mutex);
    }
  }

  gct->shutdown = 1;
  return NULL;
}
#endif

static gc_topics_t *get_gc_topics(bgpview_io_kafka_t *client,
                                  char *identity)
{
  gc_topics_t *gct = NULL;
  char *cpy = NULL;
  khiter_t k;
  int khret;

  /* is there already a record for this identity? */
  if ((k = kh_get(str_topic, client->gc_state.topics, identity))
      == kh_end(client->gc_state.topics)) {
    /* need to create the topic and insert into the hash */
    cpy = strdup(identity);
    assert(cpy != NULL);
    k = kh_put(str_topic, client->gc_state.topics, cpy, &khret);
    if ((gct = malloc_zero(sizeof(gc_topics_t))) == NULL) {
      goto err;
    }
    kh_val(client->gc_state.topics, k) = gct;

    if (bgpview_io_kafka_single_topic_connect(client, identity,
                                              BGPVIEW_IO_KAFKA_TOPIC_ID_PEERS,
                                              &gct->peers) != 0) {
      goto err;
    }
    if (bgpview_io_kafka_single_topic_connect(client, identity,
                                              BGPVIEW_IO_KAFKA_TOPIC_ID_PFXS,
                                              &gct->pfxs) != 0) {
      goto err;
    }

#ifdef WITH_THREADS
    gct->view_mutex = &client->gc_state.view_mutex;
    gct->rdk_conn = client->rdk_conn;

    /* spin up the worker thread */
    pthread_mutex_init(&gct->mutex, NULL);
    pthread_cond_init(&gct->view_waiting_cond, NULL);
    pthread_cond_init(&gct->worker_ready_cond, NULL);
    pthread_create(&gct->worker, NULL, thread_worker, gct);

    /* wait until the worker is ready */
    pthread_mutex_lock(&gct->mutex);
    while (gct->worker_ready == 0) {
      if (gct->shutdown != 0) {
        goto err;
      }
      pthread_cond_wait(&gct->worker_ready_cond, &gct->mutex);
    }
    if (gct->shutdown != 0) {
      goto err;
    }
    assert(gct->worker_ready != 0);
    assert(gct->view_waiting == 0);
    pthread_mutex_unlock(&gct->mutex);
#endif

  } else {
    gct = kh_val(client->gc_state.topics, k);
  }

  return gct;

 err:
  return NULL;
}

static int recv_global_view(bgpview_io_kafka_t *client, bgpview_t *view,
                            bgpview_io_filter_peer_cb_t *peer_cb,
                            bgpview_io_filter_pfx_cb_t *pfx_cb,
                            bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{
  /* first, retrieve a list of per-producer metadata */

  bgpview_io_kafka_md_t *metas = NULL;
  int metas_cnt;
  int need_sync = 0;

 again:
  if (metas != NULL) {
    free(metas);
    metas = NULL;
  }
  if ((metas_cnt = recv_global_metadata(client, view, &metas,
                                        need_sync)) <= 0) {
    goto err;
  }

  fprintf(stderr, "\nDEBUG: ------ %c %d ------\n",
          metas[0].type, metas[0].time);
  struct timeval tv;
  gettimeofday(&tv, NULL);
  uint32_t start = tv.tv_sec;

  int i;
  fprintf(stderr, "DEBUG: %d members:\n", metas_cnt);
  for (i = 0; i < metas_cnt; i++) {
    fprintf(stderr,
            "DEBUG: [%d] "
            "%s|"
            "%"PRIu32"|"
            "%c|"
            "%"PRIi64"|"
            "%"PRIi64"|"
            "%"PRIi64"|"
            "%"PRIu32"\n",
            i,
            metas[i].identity,
            metas[i].time,
            metas[i].type,
            metas[i].pfxs_offset,
            metas[i].peers_offset,
            metas[i].sync_md_offset,
            metas[i].parent_time);

    gc_topics_t *gct;
    if ((gct = get_gc_topics(client, metas[i].identity)) == NULL) {
      fprintf(stderr, "ERROR: Could not create topics for '%s'\n",
              metas[i].identity);
      goto err;
    }

    /* if this is a diff, can it be applied correctly? */
    if (metas[i].type == 'D' &&
        metas[i].parent_time != gct->parent_view_time) {
      fprintf(stderr,
              "WARN: Skipping view from %s (parent time: %d, expecting %d)\n",
              metas[i].identity, metas[i].parent_time, gct->parent_view_time);
      continue;
    }
    gct->parent_view_time = metas[i].parent_time;

#ifdef WITH_THREADS
    /* the user *could* have changed the view instance they are using */
    gct->view = view;
    gct->peer_cb = peer_cb;
    gct->pfx_cb = pfx_cb;
    gct->pfx_peer_cb = pfx_peer_cb;

    /* if it is a Sync frame we need to clear the peerid map */
    if (metas[0].type == 'S') {
      clear_peerid_mapping(&gct->idmap);
    }

    gct->meta = &metas[i];
    /* tell the worker to get cracking on this */
    assert(gct->worker_ready == 1);
    pthread_mutex_lock(&gct->mutex);
    gct->view_waiting = 1;
    pthread_cond_signal(&gct->view_waiting_cond);
    pthread_mutex_unlock(&gct->mutex);
#else
    if (recv_view(&gct->idmap, view, &metas[i], &gct->peers, &gct->pfxs,
                  peer_cb, pfx_cb, pfx_peer_cb, client->rfk_conn) != 0) {
      fprintf(stderr, "WARN: Failed to receive view (%d), moving on\n",
              metas[i].time);
      need_sync = 1;
      goto again;
    }
#endif
  }

#ifdef WITH_THREADS
  /* now wait for the workers to finish */
  int is_error = 0;
  for (i=0; i < metas_cnt; i++) {
    gc_topics_t *gct;
    if ((gct = get_gc_topics(client, metas[i].identity)) == NULL) {
      fprintf(stderr, "ERROR: Could not create topics for '%s'\n",
              metas[i].identity);
      goto err;
    }

    pthread_mutex_lock(&gct->mutex);
    while (gct->worker_ready == 0) {
      if (gct->shutdown != 0) {
        break;
      }
      pthread_cond_wait(&gct->worker_ready_cond, &gct->mutex);
    }
    if (gct->shutdown != 0) {
      break;
    }
    if (gct->recv_error != 0) {
      is_error = 1;
    }
    assert(gct->worker_ready != 0);
    pthread_mutex_unlock(&gct->mutex);
  }

  if (is_error != 0) {
    /* at least one of the workers failed */
    fprintf(stderr, "WARN: Failed to receive view (%d), moving on\n",
            metas[0].time);
    need_sync = 1;
    goto again;
  }
#endif

  gettimeofday(&tv, NULL);
  uint32_t stop = tv.tv_sec;
  fprintf(stderr, "DEBUG: Processing time: %"PRIu32"\n", stop-start);

  free(metas);
  metas = NULL;

  return 0;

 err:
  free(metas);
  return -1;
}

/* ==========END SEND/RECEIVE FUNCTIONS ========== */

/* ========== PROTECTED FUNCTIONS ========== */

int bgpview_io_kafka_consumer_connect(bgpview_io_kafka_t *client)
{
  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  char errstr[512];

  //#if 0
#ifdef WITH_THREADS
  // Increase buffer sizes when using threads
  if (rd_kafka_conf_set(conf, "receive.message.max.bytes", "900000000", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }
  if (rd_kafka_conf_set(conf, "queued.min.messages", "100000", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }
  if (rd_kafka_conf_set(conf, "fetch.wait.max.ms", "5000", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }
  if (rd_kafka_conf_set(conf, "fetch.message.max.bytes", "20000000", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }
  if (rd_kafka_conf_set(conf, "fetch.min.bytes", "10485760", errstr,
                        sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "ERROR: %s\n", errstr);
    goto err;
  }
#endif
  //#endif

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

  if (rd_kafka_consume_start(*rkt, 0, RD_KAFKA_OFFSET_BEGINNING/*RD_KAFKA_OFFSET_TAIL(1)*/) == -1) {
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
  int need_sync = 0;

  switch (client->mode) {
  case BGPVIEW_IO_KAFKA_MODE_DIRECT_CONSUMER:
  again:
    /* directly find metadata for a single view frame */
    if (recv_direct_metadata(client, view, &meta, need_sync) != 0) {
      return -1;
    }
    if (recv_view(&client->dc_state.idmap, view, &meta,
                  TOPIC(BGPVIEW_IO_KAFKA_TOPIC_ID_PEERS),
                  TOPIC(BGPVIEW_IO_KAFKA_TOPIC_ID_PFXS),
                  peer_cb, pfx_cb, pfx_peer_cb,
#ifdef WITH_THREADS
                  NULL,
#endif
                  client->rdk_conn) != 0) {
      fprintf(stderr, "WARN: Failed to receive view (%d), moving on\n",
              meta.time);
      need_sync = 1;
      goto again;
    }
    break;

  case BGPVIEW_IO_KAFKA_MODE_GLOBAL_CONSUMER:
    /* retrieve global view (which will retrieve global metadata and then
       iteratively retrieve partial views) */
    if (recv_global_view(client, view, peer_cb, pfx_cb, pfx_peer_cb) != 0) {
      return -1;
    }
    break;

  default:
    fprintf(stderr, "ERROR: Invalid client mode (%d) to receive a view\n",
            client->mode);
    return -1;
  }

  bgpview_iter_t *it = NULL;
  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }
  for (bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(it); bgpview_iter_next_peer(it)) {
    if (bgpview_iter_peer_get_pfx_cnt(it, 0, BGPVIEW_FIELD_ACTIVE) == 0 &&
        bgpview_iter_deactivate_peer(it) != 1) {
      fprintf(stderr, "Failed to deactivate peer\n");
      return -1;
    }
  }
  bgpview_iter_destroy(it);

  return 0;
}
