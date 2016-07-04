/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
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

#include "bgpview_io_zmq_int.h"
#include "config.h"
#include <czmq.h>
#include <stdio.h>

#define BUFFER_LEN 16384
#define BUFFER_1M 1048576

#define ASSERT_MORE                                                            \
  if (zsocket_rcvmore(src) == 0) {                                             \
    fprintf(stderr, "ERROR: Malformed view message at line %d\n", __LINE__);   \
    goto err;                                                                  \
  }

/* ========== UTILITIES ========== */

#if 0
static int send_ip(void *dest, bgpstream_ip_addr_t *ip, int flags)
{
  switch(ip->version)
    {
    case BGPSTREAM_ADDR_VERSION_IPV4:
      if(zmq_send(dest, &((bgpstream_ipv4_addr_t *)ip)->ipv4.s_addr,
                  sizeof(uint32_t), flags) == sizeof(uint32_t))
        {
          return 0;
        }
      break;

    case BGPSTREAM_ADDR_VERSION_IPV6:
      if(zmq_send(dest, &((bgpstream_ipv6_addr_t *)ip)->ipv6.s6_addr,
                  (sizeof(uint8_t)*16), flags) == sizeof(uint8_t)*16)
        {
          return 0;
        }
      break;

    case BGPSTREAM_ADDR_VERSION_UNKNOWN:
      return -1;
    }

  return -1;
}

static int recv_ip(void *src, bgpstream_addr_storage_t *ip)
{
  zmq_msg_t llm;
  assert(ip != NULL);

  if(zmq_msg_init(&llm) == -1 || zmq_msg_recv(&llm, src, 0) == -1)
    {
      goto err;
    }

  /* 4 bytes means ipv4, 16 means ipv6 */
  if(zmq_msg_size(&llm) == sizeof(uint32_t))
    {
      /* v4 */
      ip->version = BGPSTREAM_ADDR_VERSION_IPV4;
      memcpy(&ip->ipv4.s_addr,
	     zmq_msg_data(&llm),
	     sizeof(uint32_t));
    }
  else if(zmq_msg_size(&llm) == sizeof(uint8_t)*16)
    {
      /* v6 */
      ip->version = BGPSTREAM_ADDR_VERSION_IPV6;
      memcpy(&ip->ipv6.s6_addr,
	     zmq_msg_data(&llm),
	     sizeof(uint8_t)*16);
    }
  else
    {
      /* invalid ip address */
      fprintf(stderr, "Invalid IP address\n");
      goto err;
    }

  zmq_msg_close(&llm);
  return 0;

 err:
  zmq_msg_close(&llm);
  return -1;
}
#endif

static int send_pfxs(void *dest, bgpview_iter_t *it, bgpview_io_filter_cb_t *cb,
                     void *cb_user)
{
  int filter;

  uint32_t u32;

  size_t len = BUFFER_LEN;
  uint8_t buf[BUFFER_LEN];
  uint8_t *ptr = buf;
  size_t written = 0;
  ssize_t s = 0;

  /* the number of pfxs we actually sent */
  int pfx_cnt = 0;

  for (bgpview_iter_first_pfx(it, 0, /* all pfx versions */
                              BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {
    if (cb != NULL) {
      /* ask the caller if they want this peer */
      if ((filter = cb(it, BGPVIEW_IO_FILTER_PFX, cb_user)) < 0) {
        goto err;
      }
      if (filter == 0) {
        continue;
      }
    }

    /* reset the buffer */
    len = BUFFER_LEN;
    ptr = buf;
    written = 0;
    s = 0;

    // serialize the pfx row using only path IDs
    if ((s = bgpview_io_serialize_pfx_row(ptr, len, it, NULL, cb, cb_user,
                                          1)) == -1) {
      goto err;
    }
    if (s == 0) /* prefix has no peers so skip it */
    {
      continue;
    }
    written += s;
    ptr += s;

    /* send the buffer */
    if (zmq_send(dest, buf, written, ZMQ_SNDMORE) != written) {
      goto err;
    }
    pfx_cnt++;
  }

  /* send an empty frame to signify end of pfxs */
  if (zmq_send(dest, "", 0, ZMQ_SNDMORE) != 0) {
    goto err;
  }

  /* send pfx cnt for cross-validation */
  u32 = htonl(pfx_cnt);
  if (zmq_send(dest, &u32, sizeof(u32), ZMQ_SNDMORE) != sizeof(u32)) {
    goto err;
  }

  return 0;

err:
  return -1;
}

static int recv_pfxs(void *src, bgpview_iter_t *it,
                     bgpview_io_filter_pfx_cb_t *pfx_cb,
                     bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
                     bgpstream_peer_id_t *peerid_map, int peerid_map_cnt,
                     bgpstream_as_path_store_path_id_t *pathid_map,
                     int pathid_map_cnt)
{
  uint32_t pfx_cnt;
  int i;

  zmq_msg_t msg;
  uint8_t *buf;
  size_t len;
  int read;

  int pfx_rx = 0;

  ASSERT_MORE;

  /* foreach pfx, recv pfx.ip, pfx.len, [peers_cnt, peer_info] */
  for (i = 0; i < UINT32_MAX; i++) {
    /* first receive the message */
    if (zmq_msg_init(&msg) == -1 || zmq_msg_recv(&msg, src, 0) == -1) {
      fprintf(stderr, "Could not receive pfx message\n");
      goto err;
    }
    buf = zmq_msg_data(&msg);
    len = zmq_msg_size(&msg);

    if (len == 0) {
      /* end of pfxs */
      break;
    }
    pfx_rx++;

    if ((read = bgpview_io_deserialize_pfx_row(
           buf, len, it, pfx_cb, pfx_peer_cb, peerid_map, peerid_map_cnt,
           pathid_map, pathid_map_cnt, BGPVIEW_FIELD_ACTIVE)) == -1) {
      goto err;
    }

    assert(read == len);
    zmq_msg_close(&msg);
  }

  /* pfx cnt */
  if (zmq_recv(src, &pfx_cnt, sizeof(pfx_cnt), 0) != sizeof(pfx_cnt)) {
    goto err;
  }
  pfx_cnt = ntohl(pfx_cnt);
  assert(pfx_rx == pfx_cnt);
  ASSERT_MORE; /* there will be an empty frame for end-of-pfxs */

  return 0;

err:
  return -1;
}

static int send_peers(void *dest, bgpview_iter_t *it,
                      bgpview_io_filter_cb_t *cb, void *cb_user)
{
  uint16_t u16;

  uint8_t buf[BUFFER_LEN];
  ssize_t written;

  int peers_tx = 0;

  int filter = 0;

  bgpview_t *view = bgpview_iter_get_view(it);
  assert(view != NULL);

  /* foreach peer, send peerid, collector string, peer ip (version, address),
     peer asn */
  for (bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(it); bgpview_iter_next_peer(it)) {
    if (cb != NULL) {
      /* ask the caller if they want this peer */
      if ((filter = cb(it, BGPVIEW_IO_FILTER_PEER, cb_user)) < 0) {
        goto err;
      }
      if (filter == 0) {
        continue;
      }
    }

    /* past here means this peer is being sent */
    peers_tx++;

    if ((written = bgpview_io_serialize_peer(
           buf, BUFFER_LEN, bgpview_iter_peer_get_peer_id(it),
           bgpview_iter_peer_get_sig(it))) < 0) {
      goto err;
    }

    if (zmq_send(dest, buf, written, ZMQ_SNDMORE) != written) {
      goto err;
    }
  }

  /* send an empty frame to signify end of peers */
  if (zmq_send(dest, "", 0, ZMQ_SNDMORE) != 0) {
    goto err;
  }

  /* now send the number of peers for cross validation */
  assert(peers_tx <= UINT16_MAX);
  u16 = htons(peers_tx);
  if (zmq_send(dest, &u16, sizeof(u16), ZMQ_SNDMORE) != sizeof(u16)) {
    goto err;
  }

  return 0;

err:
  return -1;
}

static int recv_peers(void *src, bgpview_iter_t *iter,
                      bgpview_io_filter_peer_cb_t *peer_cb,
                      bgpstream_peer_id_t **peerid_mapping)
{
  uint16_t pc;
  int i, j;

  zmq_msg_t msg;
  uint8_t *buf;
  size_t len;
  ssize_t read;

  bgpstream_peer_id_t peerid_orig;
  bgpstream_peer_id_t peerid_new;

  bgpstream_peer_sig_t ps;

  bgpstream_peer_id_t *idmap = NULL;
  int idmap_cnt = 0;

  int peers_rx = 0;

  int filter = 0;

  ASSERT_MORE;

  /* foreach peer, recv peerid, collector string, peer ip (version, address),
     peer asn */
  for (i = 0; i < UINT16_MAX; i++) {
    /* peerid (or end-of-peers)*/
    /* first receive the message */
    if (zmq_msg_init(&msg) == -1 || zmq_msg_recv(&msg, src, 0) == -1) {
      fprintf(stderr, "Could not receive peer message\n");
      goto err;
    }
    buf = zmq_msg_data(&msg);
    len = zmq_msg_size(&msg);

    if (len == 0) {
      /* end of peers */
      break;
    }

    if ((read = bgpview_io_deserialize_peer(buf, len, &peerid_orig, &ps)) < 0) {
      goto err;
    }

    /* by here we have a valid peer */
    peers_rx++;

    if (iter == NULL) {
      continue;
    }
    /* all code below here has a valid view */

    if (peer_cb != NULL) {
      /* ask the caller if they want this peer */
      if ((filter = peer_cb(&ps)) < 0) {
        goto err;
      }
      if (filter == 0) {
        continue;
      }
    }

    /* ensure we have enough space in the id map */
    if ((peerid_orig + 1) > idmap_cnt) {
      if ((idmap = realloc(idmap, sizeof(bgpstream_peer_id_t) *
                                    (peerid_orig + 1))) == NULL) {
        goto err;
      }

      /* now set all ids to 0 (reserved) */
      for (j = idmap_cnt; j <= peerid_orig; j++) {
        idmap[j] = 0;
      }
      idmap_cnt = peerid_orig + 1;
    }

    /* now ask the view to add this peer */
    peerid_new = bgpview_iter_add_peer(iter, ps.collector_str,
                                       (bgpstream_ip_addr_t *)&ps.peer_ip_addr,
                                       ps.peer_asnumber);
    assert(peerid_new != 0);
    idmap[peerid_orig] = peerid_new;

    bgpview_iter_activate_peer(iter);
  }

  /* receive the number of peers */
  if (zmq_recv(src, &pc, sizeof(pc), 0) != sizeof(pc)) {
    fprintf(stderr, "Could not receive peer cnt\n");
    goto err;
  }
  pc = ntohs(pc);
  assert(pc == peers_rx);

  *peerid_mapping = idmap;
  return idmap_cnt;

err:
  return -1;
}

static int send_paths(void *dest, bgpview_iter_t *it)
{
  bgpview_t *view = bgpview_iter_get_view(it);
  assert(view != NULL);
  bgpstream_as_path_store_t *ps = bgpview_get_as_path_store(view);
  assert(ps != NULL);

  size_t len = BUFFER_1M;
  uint8_t *buf = NULL;
  uint8_t *ptr = NULL;
  size_t written = 0;
  size_t s = 0;

  bgpstream_as_path_store_path_t *spath;
  uint32_t idx;

  int paths_tx = 0;
  uint32_t u32;

  /* malloc the buffer */
  if ((ptr = buf = malloc(BUFFER_1M)) == NULL) {
    goto err;
  }

  /* add paths to the buffer until full, send, repeat */

  /* foreach path, send pathid and path */
  for (bgpstream_as_path_store_iter_first_path(ps);
       bgpstream_as_path_store_iter_has_more_path(ps);
       bgpstream_as_path_store_iter_next_path(ps)) {
    paths_tx++;
    spath = bgpstream_as_path_store_iter_get_path(ps);
    assert(spath != NULL);

    /* do we need to send the buffer first? */
    if ((len - written) <
        sizeof(idx) + bgpstream_as_path_store_path_get_size(spath)) {
      if (zmq_send(dest, buf, written, ZMQ_SNDMORE) != written) {
        goto err;
      }
      s = written = 0;
      ptr = buf;
    }

    /* add the path index */
    idx = bgpstream_as_path_store_path_get_idx(spath);
    BGPVIEW_IO_SERIALIZE_VAL(ptr, len, written, idx);

    if ((s = bgpview_io_serialize_as_path_store_path(ptr, (len - written),
                                                     spath)) == -1) {
      goto err;
    }
    written += s;
    ptr += s;
  }

  /* send the last buffer */
  if (written > 0) {
    if (zmq_send(dest, buf, written, ZMQ_SNDMORE) != written) {
      goto err;
    }
  }

  /* send an empty frame to signify end of paths */
  if (zmq_send(dest, "", 0, ZMQ_SNDMORE) != 0) {
    goto err;
  }

  /* now send the number of paths for cross validation */
  assert(paths_tx <= UINT32_MAX);
  u32 = htonl(paths_tx);
  if (zmq_send(dest, &u32, sizeof(u32), ZMQ_SNDMORE) != sizeof(u32)) {
    goto err;
  }

  free(buf);

  return 0;

err:
  free(buf);
  return -1;
}

static int recv_paths(void *src, bgpview_iter_t *iter,
                      bgpstream_as_path_store_path_id_t **pathid_mapping)
{
  uint32_t pc;

  uint32_t pathidx;

  zmq_msg_t msg;
  uint8_t *buf;
  size_t len;
  size_t read = 0;
  size_t s = 0;

  bgpstream_as_path_store_path_id_t *idmap = NULL;
  int idmap_cnt = 0;

  int paths_rx = 0;

  bgpview_t *view = NULL;
  bgpstream_as_path_store_t *store = NULL;

  /* only if we have a valid iterator */
  if (iter != NULL) {
    view = bgpview_iter_get_view(iter);
    store = bgpview_get_as_path_store(view);
  }

  ASSERT_MORE;

  /* receive the first message */
  if (zmq_msg_init(&msg) == -1 || zmq_msg_recv(&msg, src, 0) == -1) {
    fprintf(stderr, "Could not receive path message\n");
    goto err;
  }
  buf = zmq_msg_data(&msg);
  len = zmq_msg_size(&msg);
  read = 0;
  s = 0;

  /* while the path message is not empty */
  while (len > 0) {
    /* by here we have a valid path to receive */
    paths_rx++;

    /* path idx */
    BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, pathidx);
    ASSERT_MORE;

    if (iter != NULL) {
      /* ensure we have enough space in the id map */
      if ((pathidx + 1) > idmap_cnt) {
        idmap_cnt = pathidx == 0 ? 1 : pathidx * 2;

        if ((idmap = realloc(idmap, sizeof(bgpstream_as_path_store_path_id_t) *
                                      idmap_cnt)) == NULL) {
          goto err;
        }

        /* WARN: ids are garbage */
      }
    }

    if ((s = bgpview_io_deserialize_as_path_store_path(
           buf, (len - read), store,
           (store == NULL) ? NULL : &idmap[pathidx])) == -1) {
      goto err;
    }
    read += s;
    buf += s;

    if (read == len) {
      /* get another message */
      zmq_msg_close(&msg);
      if (zmq_msg_init(&msg) == -1 || zmq_msg_recv(&msg, src, 0) == -1) {
        fprintf(stderr, "Could not receive path message\n");
        goto err;
      }
      buf = zmq_msg_data(&msg);
      len = zmq_msg_size(&msg);
      read = 0;
      s = 0;
    }
  }

  /* receive the number of paths */
  if (zmq_recv(src, &pc, sizeof(pc), 0) != sizeof(pc)) {
    fprintf(stderr, "Could not receive path cnt\n");
    goto err;
  }
  pc = ntohl(pc);
  assert(pc == paths_rx);

  *pathid_mapping = idmap;
  return idmap_cnt;

err:
  zmq_msg_close(&msg);
  return -1;
}

/* ========== PROTECTED FUNCTIONS ========== */

#if 0
static char *recv_str(void *src)
{
  zmq_msg_t llm;
  size_t len;
  char *str = NULL;

  if(zmq_msg_init(&llm) == -1 || zmq_msg_recv(&llm, src, 0) == -1)
    {
      goto err;
    }
  len = zmq_msg_size(&llm);
  if((str = malloc(len + 1)) == NULL)
    {
      goto err;
    }
  memcpy(str, zmq_msg_data(&llm), len);
  str[len] = '\0';
  zmq_msg_close(&llm);

  return str;

 err:
  free(str);
  return NULL;
}
#endif

/* ========== PROTECTED FUNCTIONS BELOW HERE ========== */

/* ========== MESSAGE TYPES ========== */

bgpview_io_zmq_msg_type_t bgpview_io_zmq_recv_type(void *src, int flags)
{
  bgpview_io_zmq_msg_type_t type = BGPVIEW_IO_ZMQ_MSG_TYPE_UNKNOWN;

  if ((zmq_recv(src, &type, bgpview_io_zmq_msg_type_size_t, flags) !=
       bgpview_io_zmq_msg_type_size_t) ||
      (type > BGPVIEW_IO_ZMQ_MSG_TYPE_MAX)) {
    return BGPVIEW_IO_ZMQ_MSG_TYPE_UNKNOWN;
  }

  return type;
}

int bgpview_io_zmq_send(void *dest, bgpview_t *view, bgpview_io_filter_cb_t *cb,
                        void *cb_user)
{
  uint32_t u32;

  bgpview_iter_t *it = NULL;

#ifdef DEBUG
  fprintf(stderr, "DEBUG: Sending view...\n");
#endif

  if ((it = bgpview_iter_create(view)) == NULL) {
    goto err;
  }

  /* time */
  u32 = htonl(bgpview_get_time(view));
  if (zmq_send(dest, &u32, sizeof(u32), ZMQ_SNDMORE) != sizeof(u32)) {
    goto err;
  }

  if (send_peers(dest, it, cb, cb_user) != 0) {
    goto err;
  }

  if (send_paths(dest, it) != 0) {
    goto err;
  }

  if (send_pfxs(dest, it, cb, cb_user) != 0) {
    goto err;
  }

  if (zmq_send(dest, "", 0, 0) != 0) {
    goto err;
  }

  bgpview_iter_destroy(it);

  return 0;

err:
  return -1;
}

int bgpview_io_zmq_recv(void *src, bgpview_t *view,
                        bgpview_io_filter_peer_cb_t *peer_cb,
                        bgpview_io_filter_pfx_cb_t *pfx_cb,
                        bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{
  uint32_t u32;

  bgpstream_peer_id_t *peerid_map = NULL;
  int peerid_map_cnt = 0;

  /* an array of path IDs */
  bgpstream_as_path_store_path_id_t *pathid_map = NULL;
  int pathid_map_cnt;

  bgpview_iter_t *it = NULL;
  if (view != NULL && (it = bgpview_iter_create(view)) == NULL) {
    goto err;
  }

  /* time */
  if (zmq_recv(src, &u32, sizeof(u32), 0) != sizeof(u32)) {
    fprintf(stderr, "Could not receive 'time'\n");
    goto err;
  }
  if (view != NULL) {
    bgpview_set_time(view, ntohl(u32));
  }
  ASSERT_MORE;

  if ((peerid_map_cnt = recv_peers(src, it, peer_cb, &peerid_map)) < 0) {
    fprintf(stderr, "Could not receive peers\n");
    goto err;
  }
  ASSERT_MORE;

  if ((pathid_map_cnt = recv_paths(src, it, &pathid_map)) < 0) {
    fprintf(stderr, "Could not receive paths\n");
    goto err;
  }
  ASSERT_MORE;

  /* pfxs */
  if (recv_pfxs(src, it, pfx_cb, pfx_peer_cb, peerid_map, peerid_map_cnt,
                pathid_map, pathid_map_cnt) != 0) {
    fprintf(stderr, "Could not receive prefixes\n");
    goto err;
  }
  ASSERT_MORE;

  if (zmq_recv(src, NULL, 0, 0) != 0) {
    fprintf(stderr, "Could not receive empty frame\n");
    goto err;
  }

  assert(zsocket_rcvmore(src) == 0);

  if (it != NULL) {
    bgpview_iter_destroy(it);
  }

  free(peerid_map);
  free(pathid_map);

  return 0;

err:
  if (it != NULL) {
    bgpview_iter_destroy(it);
  }
  free(peerid_map);
  free(pathid_map);
  return -1;
}
