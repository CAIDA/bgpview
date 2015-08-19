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

static int write_ip(iow_t *outfile, bgpstream_ip_addr_t *ip)
{
  switch(ip->version)
    {
    case BGPSTREAM_ADDR_VERSION_IPV4:
      if(wandio_wwrite(outfile,
                       &((bgpstream_ipv4_addr_t *)ip)->ipv4.s_addr,
                       sizeof(uint32_t))
         == sizeof(uint32_t))
        {
          return 0;
        }
      break;

    case BGPSTREAM_ADDR_VERSION_IPV6:
      if(wandio_wwrite(outfile,
                       &((bgpstream_ipv6_addr_t *)ip)->ipv6.s6_addr,
                       (sizeof(uint8_t)*16))
         == sizeof(uint8_t)*16)
        {
          return 0;
        }
      break;

    case BGPSTREAM_ADDR_VERSION_UNKNOWN:
      return -1;
    }

  return -1;
}

static int send_peers(iow_t *outfile, bgpview_iter_t *it,
                      bgpview_filter_peer_cb_t *cb)
{
  uint16_t u16;
  uint32_t u32;

  bgpstream_peer_sig_t *ps;
  size_t len;

  int peers_tx = 0;

  int filter = 0;

  bgpview_t *view = bgpview_iter_get_view(it);
  assert(view != NULL);

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
      len = strlen(ps->collector_str);
      if(wandio_wwrite(outfile, &ps->collector_str, len) != len)
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
  /* if the view is NULL, then just read and discard */
  return -1;
}
