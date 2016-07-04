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

#include "bgpview_io.h"
#include "config.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

/* because the values of AF_INET* vary from system to system we need to use
   our own encoding for the version */
#define BW_INTERNAL_AF_INET 4
#define BW_INTERNAL_AF_INET6 6

int bgpview_io_serialize_ip(uint8_t *buf, size_t len, bgpstream_ip_addr_t *ip)
{
  size_t written = 0;

  /* now serialize the actual address */
  switch (ip->version) {
  case BGPSTREAM_ADDR_VERSION_IPV4:
    /* serialize the version */
    assert(len >= 1);
    *buf = BW_INTERNAL_AF_INET;
    buf++;
    written++;

    assert((len - written) >= sizeof(uint32_t));
    memcpy(buf, &((bgpstream_ipv4_addr_t *)ip)->ipv4.s_addr, sizeof(uint32_t));
    return written + sizeof(uint32_t);
    break;

  case BGPSTREAM_ADDR_VERSION_IPV6:
    /* serialize the version */
    assert(len >= 1);
    *buf = BW_INTERNAL_AF_INET6;
    buf++;
    written++;

    assert((len - written) >= (sizeof(uint8_t) * 16));
    memcpy(buf, &((bgpstream_ipv6_addr_t *)ip)->ipv6.s6_addr,
           sizeof(uint8_t) * 16);
    return written + sizeof(uint8_t) * 16;
    break;

  case BGPSTREAM_ADDR_VERSION_UNKNOWN:
    return -1;
  }

  return -1;
}

int bgpview_io_deserialize_ip(uint8_t *buf, size_t len,
                              bgpstream_addr_storage_t *ip)
{
  size_t read = 0;

  assert(len >= 1);

  /* switch on the internal version */
  switch (*buf) {
  case BW_INTERNAL_AF_INET:
    ip->version = BGPSTREAM_ADDR_VERSION_IPV4;
    buf++;
    read++;

    assert((len - read) >= sizeof(uint32_t));
    memcpy(&ip->ipv4.s_addr, buf, sizeof(uint32_t));
    return read + sizeof(uint32_t);
    break;

  case BW_INTERNAL_AF_INET6:
    ip->version = BGPSTREAM_ADDR_VERSION_IPV6;
    buf++;
    read++;

    assert((len - read) >= (sizeof(uint8_t) * 16));
    memcpy(&ip->ipv6.s6_addr, buf, sizeof(uint8_t) * 16);
    return read + (sizeof(uint8_t) * 16);
    break;

  case BGPSTREAM_ADDR_VERSION_UNKNOWN:
    return -1;
  }

  return -1;
}

int bgpview_io_serialize_pfx(uint8_t *buf, size_t len, bgpstream_pfx_t *pfx)
{
  ssize_t s;
  size_t written = 0;

  /* pfx address */
  if ((s = bgpview_io_serialize_ip(
         buf, (len - written), (bgpstream_ip_addr_t *)(&pfx->address))) == -1) {
    goto err;
  }
  written += s;
  buf += s;

  /* pfx len */
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, pfx->mask_len);

  return written;

err:
  return -1;
}

int bgpview_io_deserialize_pfx(uint8_t *buf, size_t len,
                               bgpstream_pfx_storage_t *pfx)
{
  size_t read = 0;
  ssize_t s;

  /* pfx_ip */
  if ((s = bgpview_io_deserialize_ip(buf, (len - read), &pfx->address)) == -1) {
    goto err;
  }
  read += s;
  buf += s;

  /* pfx len */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, pfx->mask_len);

  return read;

err:
  return -1;
}

int bgpview_io_serialize_peer(uint8_t *buf, size_t len, bgpstream_peer_id_t id,
                              bgpstream_peer_sig_t *sig)
{
  size_t written = 0;
  uint16_t nlen;
  ssize_t s;

  /* Peer ID */
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, id);

  /* Collector Name */
  nlen = strlen(sig->collector_str);
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, nlen);
  assert((len - written) >= nlen);
  memcpy(buf, sig->collector_str, nlen);
  written += nlen;
  buf += nlen;

  /* Peer IP */
  if ((s = bgpview_io_serialize_ip(
         buf, len, (bgpstream_ip_addr_t *)(&sig->peer_ip_addr))) < 0) {
    goto err;
  }
  written += s;
  buf += s;

  /* Peer ASN */
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, sig->peer_asnumber);

  return written;

err:
  return -1;
}

int bgpview_io_deserialize_peer(uint8_t *buf, size_t len,
                                bgpstream_peer_id_t *id,
                                bgpstream_peer_sig_t *sig)
{
  size_t read = 0;
  bgpstream_peer_id_t peerid;
  uint16_t nlen;
  ssize_t s;

  /* grab the peer ID */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, peerid);
  *id = peerid;

  /* the collector string */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, nlen);
  assert((len - read) >= (nlen + 1));
  memcpy(sig->collector_str, buf, nlen);
  sig->collector_str[nlen] = '\0';
  read += nlen;
  buf += nlen;

  /* Peer IP */
  if ((s = bgpview_io_deserialize_ip(buf, len, &sig->peer_ip_addr)) < 0) {
    goto err;
  }
  read += s;
  buf += s;

  /* Peer ASN */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, sig->peer_asnumber);

  return read;

err:
  return -1;
}

int bgpview_io_serialize_as_path_store_path(
  uint8_t *buf, size_t len, bgpstream_as_path_store_path_t *spath)
{
  size_t written = 0;
  uint8_t *path_data;
  uint8_t is_core;
  bgpstream_as_path_t *path;
  uint16_t path_len;

  /* is this a core path? */
  is_core = bgpstream_as_path_store_path_is_core(spath);
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, is_core);

  path = bgpstream_as_path_store_path_get_int_path(spath);
  path_len = bgpstream_as_path_get_data(path, &path_data);

  /* add the path len */
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, path_len);

  /* and now the actual path */
  assert((len - written) >= path_len);
  memcpy(buf, path_data, path_len);
  written += path_len;

  return written;
}

int bgpview_io_deserialize_as_path_store_path(
  uint8_t *buf, size_t len, bgpstream_as_path_store_t *store,
  bgpstream_as_path_store_path_id_t *pathid)
{
  size_t read = 0;

  uint16_t pathlen;
  uint8_t is_core;

  /* is core */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, is_core);

  /* path len */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, pathlen);

  assert((len - read) >= pathlen);
  if (store != NULL) {
    /* now add this path to the store */
    if (bgpstream_as_path_store_insert_path(store, buf, pathlen, is_core,
                                            pathid) != 0) {
      goto err;
    }
  }

  read += pathlen;

  return read;

err:
  return -1;
}

int bgpview_io_serialize_pfx_peer(uint8_t *buf, size_t len, bgpview_iter_t *it,
                                  bgpview_io_filter_cb_t *cb, void *cb_user,
                                  int use_pathid)
{
  int filter;
  uint16_t peerid;

  bgpstream_as_path_store_path_t *spath;
  uint32_t idx;

  size_t written = 0;
  ssize_t s;

  if (cb != NULL) {
    /* ask the caller if they want this pfx-peer */
    if ((filter = cb(it, BGPVIEW_IO_FILTER_PFX_PEER, cb_user)) < 0) {
      goto err;
    }
    if (filter == 0) {
      return 0;
    }
  }

  peerid = bgpview_iter_peer_get_peer_id(it);

  /* peer id */
  assert(peerid > 0);
  assert(peerid < BGPVIEW_IO_END_OF_PEERS);
  peerid = htons(peerid);
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, peerid);

  /* AS Path */
  spath = bgpview_iter_pfx_peer_get_as_path_store_path(it);
  if (use_pathid == 1) {
    idx = bgpstream_as_path_store_path_get_idx(spath);
    BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, idx);
  } else if (use_pathid == 0) {
    if ((s = bgpview_io_serialize_as_path_store_path(buf, (len - written),
                                                     spath)) == -1) {
      goto err;
    }
    written += s;
    buf += s;
  }

  return written;

err:
  return -1;
}

int bgpview_io_serialize_pfx_peers(uint8_t *buf, size_t len, bgpview_iter_t *it,
                                   int *peers_cnt, bgpview_io_filter_cb_t *cb,
                                   void *cb_user, int use_pathid)
{
  size_t written = 0;
  ssize_t s;

  assert(peers_cnt != NULL);
  *peers_cnt = 0;

  for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_pfx_has_more_peer(it); bgpview_iter_pfx_next_peer(it)) {
    if ((s = bgpview_io_serialize_pfx_peer(buf, (len - written), it, cb,
                                           cb_user, use_pathid)) == -1) {
      goto err;
    }
    if (s > 0) { // 0 if the callback filtered the pfx-peer
      written += s;
      buf += s;
      (*peers_cnt)++;
    }
  }

  return written;

err:
  return -1;
}

int bgpview_io_serialize_pfx_row(uint8_t *buf, size_t len, bgpview_iter_t *it,
                                 int *peers_cnt, bgpview_io_filter_cb_t *cb,
                                 void *cb_user, int use_pathid)
{
  size_t written = 0;
  ssize_t s = 0;

  bgpstream_pfx_t *pfx;
  int peers_tx = 0;

  uint16_t u16;

  pfx = bgpview_iter_pfx_get_pfx(it);
  assert(pfx != NULL);

  if ((s = bgpview_io_serialize_pfx(buf, (len - written), pfx)) == -1) {
    goto err;
  }
  written += s;
  buf += s;

  /* send the peers */
  if ((s = bgpview_io_serialize_pfx_peers(buf, (len - written), it, &peers_tx,
                                          cb, cb_user, use_pathid)) == -1) {
    goto err;
  }
  written += s;
  buf += s;

  if (peers_cnt != NULL) {
    *peers_cnt = peers_tx;
  }

  /* for a pfx to be sent it must have active peers */
  if (peers_tx == 0) {
    return 0;
  }

  if (peers_cnt != NULL) {
    *peers_cnt = peers_tx;
  }

  /* send a magic peerid to indicate end of peers */
  u16 = BGPVIEW_IO_END_OF_PEERS;
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, u16);

  /* peer cnt for cross validation */
  assert(peers_tx > 0);
  u16 = htons(peers_tx);
  BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, u16);

  return written;

err:
  return -1;
}

int bgpview_io_deserialize_pfx_row(
  uint8_t *buf, size_t len, bgpview_iter_t *it,
  bgpview_io_filter_pfx_cb_t *pfx_cb,
  bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb, bgpstream_peer_id_t *peerid_map,
  int peerid_map_cnt, bgpstream_as_path_store_path_id_t *pathid_map,
  int pathid_map_cnt, bgpview_field_state_t state)
{
  size_t read = 0;
  size_t s = 0;
  int skip_pfx = 0;

  bgpstream_pfx_storage_t pfx;

  int filter = 0;

  int pfx_peers_added = 0;
  int pfx_peer_rx = 0;

  int j;

  bgpstream_peer_id_t peerid;
  uint32_t pathidx;

  bgpview_t *view = NULL;
  bgpstream_as_path_store_t *store = NULL;
  bgpstream_as_path_store_path_t *store_path = NULL;
  bgpstream_as_path_store_path_id_t pathid;

  uint16_t peer_cnt;

  if (it != NULL) {
    view = bgpview_iter_get_view(it);
    store = bgpview_get_as_path_store(view);
  }

  if ((s = bgpview_io_deserialize_pfx(buf, (len - read), &pfx)) == -1) {
    goto err;
  }
  read += s;
  buf += s;

  if (pfx_cb != NULL && state == BGPVIEW_FIELD_ACTIVE) {
    /* ask the caller if they want this pfx */
    if ((filter = pfx_cb((bgpstream_pfx_t *)&pfx)) < 0) {
      goto err;
    }
    if (filter == 0) {
      skip_pfx = 1;
    }
  }

  pfx_peers_added = 0;
  pfx_peer_rx = 0;

  for (j = 0; j < UINT16_MAX; j++) {
    /* peer id */
    BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, peerid);
    peerid = ntohs(peerid);

    if (peerid == BGPVIEW_IO_END_OF_PEERS) {
      /* end of peers */
      break;
    }

    pfx_peer_rx++;

    /* are the paths actually serialized, or just an index? */
    if (pathid_map_cnt >= 0 && state == BGPVIEW_FIELD_ACTIVE) {
      /* AS Path Index */
      BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, pathidx);
      if (view != NULL) {
        pathid = pathid_map[pathidx];
      }
    } else if (state == BGPVIEW_FIELD_ACTIVE) {
      /* we ask to deserialize (and insert) the path into the store */
      if ((s = bgpview_io_deserialize_as_path_store_path(
             buf, (len - read), store, &pathid)) == -1) {
        goto err;
      }
      read += s;
      buf += s;
    }

    if (it == NULL || skip_pfx != 0) {
      continue;
    }
    /* all code below here has a valid iter */

    assert(peerid < peerid_map_cnt);

    if (pfx_peer_cb != NULL && state == BGPVIEW_FIELD_ACTIVE) {
      /* get the store path using the id */
      store_path = bgpstream_as_path_store_get_store_path(store, pathid);
      /* ask the caller if they want this pfx-peer */
      if ((filter = pfx_peer_cb(store_path)) < 0) {
        goto err;
      }
      if (filter == 0) {
        continue;
      }
    }

    if (state == BGPVIEW_FIELD_ACTIVE) {
      if (pfx_peers_added == 0) {
        /* we have to use add_pfx_peer */
        if (bgpview_iter_add_pfx_peer_by_id(it, (bgpstream_pfx_t *)&pfx,
                                            peerid_map[peerid], pathid) != 0) {
          fprintf(stderr, "Could not add prefix\n");
          goto err;
        }
      } else {
        /* we can use pfx_add_peer for efficiency */
        if (bgpview_iter_pfx_add_peer_by_id(it, peerid_map[peerid], pathid) !=
            0) {
          fprintf(stderr, "Could not add prefix\n");
          goto err;
        }
      }
      if (bgpview_iter_pfx_activate_peer(it) < 0) {
        fprintf(stderr, "Could not activate prefix\n");
        goto err;
      }
    } else {
      if (pfx_peers_added == 0) {
        /* seek to pfx-peer */
        if (bgpview_iter_seek_pfx_peer(
              it, (bgpstream_pfx_t *)&pfx, peerid_map[peerid],
              BGPVIEW_FIELD_ALL_VALID, BGPVIEW_FIELD_ALL_VALID) == 1) {
          bgpview_iter_pfx_deactivate_peer(it);
        }
      } else {
        /* seek to peer */
        if (bgpview_iter_pfx_seek_peer(it, peerid_map[peerid],
                                       BGPVIEW_FIELD_ALL_VALID) == 1) {
          bgpview_iter_pfx_deactivate_peer(it);
        }
      }
    }

    pfx_peers_added++;
  }

  /* peer cnt */
  BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, peer_cnt);
  peer_cnt = ntohs(peer_cnt);
  assert(peer_cnt == pfx_peer_rx);

  return read;

err:
  return -1;
}
