/*
 * This file is part of bgpstream
 *
 * Copyright (C) 2015 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
 *
 * All rights reserved.
 *
 * This code has been developed by CAIDA at UC San Diego.
 * For more information, contact bgpstream-info@caida.org
 *
 * This source code is proprietary to the CAIDA group at UC San Diego and may
 * not be redistributed, published or disclosed without prior permission from
 * CAIDA.
 *
 * Report any bugs, questions or comments to bgpstream-info@caida.org
 *
 */
#include "config.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <wandio.h>

#include "utils.h"

#include "bgpview_io.h"

#define VIEW_MAGIC 0x42475056 /* BGPV */

#define VIEW_START_MAGIC    0x53545254 /* STRT */
#define VIEW_END_MAGIC      0x56454E44   /* VEND */
#define VIEW_PEER_END_MAGIC 0x50454E44 /* PEND */
#define VIEW_PFX_END_MAGIC  0x58454E44 /* XEND */

/* ========== UTILITIES ========== */

#define WRITE_VAL(from)                                                 \
  do {                                                                  \
    if(wandio_wwrite(outfile, &from, sizeof(from)) != sizeof(from))     \
      {                                                                 \
        fprintf(stderr, "%s: Could not write %s to file\n",             \
                __func__, STR(from));                                   \
      }                                                                 \
  } while(0)

#define WRITE_MAGIC(magic)                      \
  do {                                          \
    uint32_t mgc = htonl(VIEW_MAGIC);           \
    WRITE_VAL(mgc);                             \
    mgc = htonl(magic);                         \
    WRITE_VAL(mgc);                             \
  } while(0)

#define READ_VAL(to)                            \
  do {                                                          \
    if(wandio_read(infile, &to, sizeof(to)) != sizeof(to))      \
      {                                                         \
        fprintf(stderr, "%s: Could not read %s from file\n",    \
                __func__, STR(to));                             \
      }                                                         \
  } while(0)

/** Checks if the given magic number is present in the file. If it is, the magic
    is consumed, otherwise the stream is left untouched */
static int check_magic(io_t *infile, uint32_t magic)
{
  uint64_t buf;
  uint32_t mgc;
  off_t read;
  if(wandio_peek(infile, &buf, sizeof(uint64_t)) != sizeof(uint64_t))
    {
      fprintf(stderr, "Could not peek at bytes\n");
      return 0;
    }

  buf = ntohll(buf);

  /* check the generic magic */
  mgc = (uint32_t)(buf >> 32);
  if(mgc != VIEW_MAGIC)
    {
      return 0;
    }

  /* now, check the specific magic */
  mgc = (buf & 0xffffffff);
  if(mgc != magic)
    {
      return 0;
    }

  /* now consume the magic! */
  read = wandio_read(infile, &buf, sizeof(uint64_t));
  assert(read == sizeof(uint64_t));

  return 1;
}

static int write_ip(iow_t *outfile, bgpstream_ip_addr_t *ip)
{
  uint8_t len;
  switch(ip->version)
    {
    case BGPSTREAM_ADDR_VERSION_IPV4:
      len = sizeof(uint32_t);
      WRITE_VAL(len);
      if(wandio_wwrite(outfile,
                       &((bgpstream_ipv4_addr_t *)ip)->ipv4.s_addr,
                       len) == len)
        {
          return 0;
        }
      break;

    case BGPSTREAM_ADDR_VERSION_IPV6:
      len = sizeof(uint8_t)*16;
      WRITE_VAL(len);
      if(wandio_wwrite(outfile,
                       &((bgpstream_ipv6_addr_t *)ip)->ipv6.s6_addr,
                       len) == len)
        {
          return 0;
        }
      break;

    case BGPSTREAM_ADDR_VERSION_UNKNOWN:
      return -1;
    }

  return -1;
}

static int read_ip(io_t *infile, bgpstream_addr_storage_t *ip)
{
  assert(ip != NULL);

  uint8_t len;
  READ_VAL(len);

  /* 4 bytes means ipv4, 16 means ipv6 */
  if(len == sizeof(uint32_t))
    {
      /* v4 */
      ip->version = BGPSTREAM_ADDR_VERSION_IPV4;
      if(wandio_read(infile, &ip->ipv4.s_addr, len) != len)
        {
          goto err;
        }
    }
  else if(len == sizeof(uint8_t)*16)
    {
      /* v6 */
      ip->version = BGPSTREAM_ADDR_VERSION_IPV6;
      if(wandio_read(infile, &ip->ipv6.s6_addr, len) != len)
        {
          goto err;
        }
    }
  else
    {
      /* invalid ip address */
      fprintf(stderr, "Invalid IP address (len: %d)\n", len);
      goto err;
    }

  return 0;

 err:
  return -1;
}

static int send_peers(iow_t *outfile, bgpview_iter_t *it,
                      bgpview_filter_peer_cb_t *cb)
{
  uint8_t u8;
  uint16_t u16;
  uint32_t u32;

  bgpstream_peer_sig_t *ps;

  int peers_tx = 0;

  int filter = 0;

  bgpview_t *view = bgpview_iter_get_view(it);
  assert(view != NULL);

  /* an assumption we make... */
  assert(BGPSTREAM_UTILS_STR_NAME_LEN < UINT8_MAX);

  /* foreach peer, send peerid, collector string, peer ip (version, address),
     peer asn */
  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
      if(cb != NULL)
        {
          /* ask the caller if they want this peer */
          if((filter = cb(it)) < 0)
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

      /* peer id */
      u16 = bgpview_iter_peer_get_peer_id(it);
      u16 = htons(u16);
      WRITE_VAL(u16);

      ps = bgpview_iter_peer_get_sig(it);
      assert(ps);
      u8 = strlen(ps->collector_str);
      WRITE_VAL(u8);
      if(wandio_wwrite(outfile, &ps->collector_str, u8) != u8)
	{
	  goto err;
	}

      /* peer IP address */
      if(write_ip(outfile, (bgpstream_ip_addr_t *)(&ps->peer_ip_addr)) != 0)
	{
	  goto err;
	}

      /* peer AS number */
      u32 = ps->peer_asnumber;
      u32 = htonl(u32);
      WRITE_VAL(u32);
    }

  /* write end-of-peers magic number */
  WRITE_MAGIC(VIEW_PEER_END_MAGIC);

  /* now send the number of peers for cross validation */
  assert(peers_tx <= UINT16_MAX);
  u16 = htons(peers_tx);
  WRITE_VAL(u16);

  return 0;

 err:
  return -1;
}

static int send_pfx_peers(iow_t *outfile, bgpview_iter_t *it, int *peers_cnt,
                          bgpview_filter_peer_cb_t *cb)
{
  uint16_t peerid;
  uint32_t orig_asn;

  int filter;

  assert(peers_cnt != NULL);
  *peers_cnt = 0;

  for(bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_pfx_has_more_peer(it);
      bgpview_iter_pfx_next_peer(it))
    {
      if(cb != NULL)
        {
          /* ask the caller if they want this peer */
          if((filter = cb(it)) < 0)
            {
              return -1;
            }
          if(filter == 0)
            {
              continue;
            }
        }

      peerid = bgpview_iter_peer_get_peer_id(it);
      orig_asn = bgpview_iter_pfx_peer_get_orig_asn(it);

      if(orig_asn >= BGPVIEW_ASN_NOEXPORT_START)
        {
          continue;
        }

      /* peer id */
      assert(peerid > 0);
      peerid = htons(peerid);
      WRITE_VAL(peerid);

      /* orig_asn */
      assert(orig_asn > 0);
      orig_asn = htonl(orig_asn);
      WRITE_VAL(orig_asn);

      (*peers_cnt)++;
    }

  return 0;
}


static int send_pfxs(iow_t *outfile, bgpview_iter_t *it,
                     bgpview_filter_peer_cb_t *cb)
{
  uint16_t u16;
  uint32_t u32;

  /* the number of pfxs we actually sent */
  int pfx_cnt = 0;

  bgpstream_pfx_t *pfx;
  int peers_cnt = 0;

  for(bgpview_iter_first_pfx(it,
                             0, /* all pfx versions */
                             BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(it);
      bgpview_iter_next_pfx(it))
    {
      pfx = bgpview_iter_pfx_get_pfx(it);
      assert(pfx != NULL);

      /* pfx address */
      if(write_ip(outfile, (bgpstream_ip_addr_t *) (&pfx->address)) != 0)
	{
	  goto err;
	}

      /* pfx len */
      WRITE_VAL(pfx->mask_len);

      /* send the peers */
      peers_cnt = 0;
      if(send_pfx_peers(outfile, it, &peers_cnt, cb) != 0)
	{
	  goto err;
	}

      /* for a pfx to be sent it must have active peers */
      if(peers_cnt == 0)
        {
          continue;
        }

      /* write end-of-peers magic */
      WRITE_MAGIC(VIEW_PEER_END_MAGIC);

      /* peer cnt for cross validation */
      assert(peers_cnt > 0);
      u16 = htons(peers_cnt);
      WRITE_VAL(u16);

      pfx_cnt++;
    }

  /* write end-of-pfxs magic */
  WRITE_MAGIC(VIEW_PFX_END_MAGIC);

  /* send pfx cnt for cross-validation */
  u32 = htonl(pfx_cnt);
  WRITE_VAL(u32);

  return 0;

 err:
  return -1;
}

static int recv_peers(io_t *infile, bgpview_iter_t *iter,
                      bgpstream_peer_id_t **peerid_mapping)
{
  uint16_t pc;
  int i, j;

  bgpstream_peer_id_t peerid_orig;
  bgpstream_peer_id_t peerid_new;

  bgpstream_peer_sig_t ps;
  uint8_t len;

  bgpstream_peer_id_t *idmap = NULL;
  int idmap_cnt = 0;

  int peers_rx = 0;

  /* foreach peer, recv peerid, collector string, peer ip (version, address),
     peer asn */
  for(i=0; i<UINT16_MAX; i++)
    {
      /* peerid (or end-of-peers)*/
      if(check_magic(infile, VIEW_PEER_END_MAGIC) != 0)
        {
          /* end of peers */
          break;
        }

      READ_VAL(peerid_orig);
      peerid_orig = ntohs(peerid_orig);

      /* by here we have a valid peer to receive */
      peers_rx++;

      /* collector name */
      READ_VAL(len);
      if(wandio_read(infile, ps.collector_str, len) != len)
        {
          fprintf(stderr, "ERROR: Could not read collector name\n");
	  goto err;
	}
      ps.collector_str[len] = '\0';

      /* peer ip */
      if(read_ip(infile, &ps.peer_ip_addr) != 0)
	{
          fprintf(stderr, "ERROR: Could not read peer ip\n");
	  goto err;
	}

      /* peer asn */
      READ_VAL(ps.peer_asnumber);
      ps.peer_asnumber = ntohl(ps.peer_asnumber);

      if(iter == NULL)
        {
          continue;
        }
      /* all code below here has a valid view */

      /* ensure we have enough space in the id map */
      if((peerid_orig+1) > idmap_cnt)
        {
          if((idmap =
              realloc(idmap,
                      sizeof(bgpstream_peer_id_t) * (peerid_orig+1))) == NULL)
            {
              goto err;
            }

          /* now set all ids to 0 (reserved) */
          for(j=idmap_cnt; j<= peerid_orig; j++)
            {
              idmap[j] = 0;
            }
          idmap_cnt = peerid_orig+1;
        }

      /* now ask the view to add this peer */
      peerid_new = bgpview_iter_add_peer(iter,
                                         ps.collector_str,
                                         (bgpstream_ip_addr_t*)&ps.peer_ip_addr,
                                         ps.peer_asnumber);
      assert(peerid_new != 0);
      idmap[peerid_orig] = peerid_new;

      bgpview_iter_activate_peer(iter);
    }

  /* receive the number of peers */
  READ_VAL(pc);
  pc = ntohs(pc);
  assert(pc == peers_rx);

  *peerid_mapping = idmap;
  return idmap_cnt;

 err:
  return -1;
}

static int recv_pfxs(io_t *infile, bgpview_iter_t *iter,
                     bgpstream_peer_id_t *peerid_map,
                     int peerid_map_cnt)
{
  uint32_t pfx_cnt;
  uint16_t peer_cnt;
  int i, j;

  bgpstream_pfx_storage_t pfx;
  bgpstream_peer_id_t peerid;

  uint32_t orig_asn;

  int pfx_peers_added = 0;

  int pfx_rx = 0;
  int pfx_peer_rx = 0;

  /* foreach pfx, recv pfx.ip, pfx.len, [peers_cnt, peer_info] */
  for(i=0; i<UINT32_MAX; i++)
    {
      if(check_magic(infile, VIEW_PFX_END_MAGIC) != 0)
        {
          /* end of pfxs */
          break;
        }
      pfx_rx++;

      /* pfx_ip */
      if(read_ip(infile, &pfx.address) != 0)
	{
          fprintf(stderr, "ERROR: Could not read pfx ip\n");
	  goto err;
	}

      /* pfx len */
      READ_VAL(pfx.mask_len);

      pfx_peers_added = 0;
      pfx_peer_rx = 0;

      for(j=0; j<UINT16_MAX; j++)
	{
          if(check_magic(infile, VIEW_PEER_END_MAGIC) != 0)
            {
              /* end of peers */
              break;
            }

	  /* peer id */
	  READ_VAL(peerid);
	  peerid = ntohs(peerid);

          pfx_peer_rx++;

	  /* orig asn */
	  READ_VAL(orig_asn);
	  orig_asn = ntohl(orig_asn);

          if(iter == NULL)
            {
              continue;
            }
          /* all code below here has a valid iter */

          assert(peerid < peerid_map_cnt);

          if(pfx_peers_added == 0)
            {
              /* we have to use add_pfx_peer */
              if(bgpview_iter_add_pfx_peer(iter, (bgpstream_pfx_t *)&pfx,
                                           peerid_map[peerid],
                                           orig_asn) != 0)
                {
                  fprintf(stderr, "Could not add prefix\n");
                  goto err;
                }
            }
          else
            {
              /* we can use pfx_add_peer for efficiency */
              if(bgpview_iter_pfx_add_peer(iter, peerid_map[peerid],
                                           orig_asn) != 0)
                {
                  fprintf(stderr, "Could not add prefix\n");
                  goto err;
                }
            }

          pfx_peers_added++;

          /* now we have to activate it */
          if(bgpview_iter_pfx_activate_peer(iter) < 0)
            {
              fprintf(stderr, "Could not activate prefix\n");
              goto err;
            }
	}

      /* peer cnt */
      READ_VAL(peer_cnt);
      peer_cnt = ntohs(peer_cnt);
      assert(peer_cnt == pfx_peer_rx);
    }

  /* pfx cnt */
  READ_VAL(pfx_cnt);
  pfx_cnt = ntohl(pfx_cnt);
  assert(pfx_rx == pfx_cnt);

  return 0;

 err:
  return -1;
}


/* ========== PUBLIC FUNCTIONS ========== */

int bgpview_io_write(iow_t *outfile, bgpview_t *view,
                     bgpview_filter_peer_cb_t *cb)
{
  uint32_t u32;
  bgpview_iter_t *it = NULL;

  if(view == NULL)
    {
      /* no-op */
      return 0;
    }

#ifdef DEBUG
  fprintf(stderr, "DEBUG: Writing view...\n");
#endif

  if((it = bgpview_iter_create(view)) == NULL)
    {
      goto err;
    }

  /* start magic */
  WRITE_MAGIC(VIEW_START_MAGIC);

  /* time */
  u32 = htonl(bgpview_get_time(view));
  WRITE_VAL(u32);

  if(send_peers(outfile, it, cb) != 0)
    {
      goto err;
    }

  if(send_pfxs(outfile, it, cb) != 0)
    {
      goto err;
    }

  /* write end-of-view magic number */
  WRITE_MAGIC(VIEW_END_MAGIC);

  bgpview_iter_destroy(it);

  return 0;

 err:
  return -1;
}

int bgpview_io_read(io_t *infile, bgpview_t *view)
{
  uint32_t u32;

  bgpstream_peer_id_t *peerid_map = NULL;
  int peerid_map_cnt = 0;

  bgpview_iter_t *it = NULL;
  if(view != NULL && (it = bgpview_iter_create(view)) == NULL)
    {
      goto err;
    }

  /* check for eof */
  if(wandio_peek(infile, &u32, sizeof(u32)) == 0)
    {
      return 0;
    }

  if(check_magic(infile, VIEW_START_MAGIC) == 0)
    {
      fprintf(stderr, "ERROR: Missing view-start magic number\n");
      goto err;
    }

  /* time */
  READ_VAL(u32);
  if(view != NULL)
    {
      bgpview_set_time(view, ntohl(u32));
    }

  if((peerid_map_cnt = recv_peers(infile, it, &peerid_map)) < 0)
    {
      fprintf(stderr, "ERROR: Could not read peer table\n");
      goto err;
    }

  /* pfxs */
  if(recv_pfxs(infile, it, peerid_map, peerid_map_cnt) != 0)
    {
      fprintf(stderr, "ERROR: Could not read prefixes\n");
      goto err;
    }

  if(check_magic(infile, VIEW_END_MAGIC) == 0)
    {
      fprintf(stderr, "ERROR: Missing end-of-view magic number\n");
    }

  if(it != NULL)
    {
      bgpview_iter_destroy(it);
    }

  free(peerid_map);

  /* valid view */
  return 1;

 err:
  if(it != NULL)
    {
      bgpview_iter_destroy(it);
    }
  free(peerid_map);
  return -1;
}
