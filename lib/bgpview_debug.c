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
#include "bgpview.h"
#include "config.h"
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>

static void peers_dump(bgpview_t *view, bgpview_iter_t *it)
{
  bgpstream_peer_id_t peerid;
  bgpstream_peer_sig_t *ps;
  int v4pfx_cnt = -1;
  int v6pfx_cnt = -1;
  char peer_str[INET6_ADDRSTRLEN] = "";

  fprintf(stdout, "Peers (%d):\n",
          bgpview_peer_cnt(view, BGPVIEW_FIELD_ACTIVE));

  for (bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(it); bgpview_iter_next_peer(it)) {
    peerid = bgpview_iter_peer_get_peer_id(it);
    ps = bgpview_iter_peer_get_sig(it);
    assert(ps);
    v4pfx_cnt = bgpview_iter_peer_get_pfx_cnt(it, BGPSTREAM_ADDR_VERSION_IPV4,
                                              BGPVIEW_FIELD_ACTIVE);
    assert(v4pfx_cnt >= 0);
    v6pfx_cnt = bgpview_iter_peer_get_pfx_cnt(it, BGPSTREAM_ADDR_VERSION_IPV6,
                                              BGPVIEW_FIELD_ACTIVE);
    assert(v6pfx_cnt >= 0);

    bgpstream_addr_ntop(peer_str, INET6_ADDRSTRLEN, &ps->peer_ip_addr);

    fprintf(stdout,
            "  %" PRIu16 ":\t%s, %s %" PRIu32 " (%d v4 pfxs, %d v6 pfxs)\n",
            peerid, ps->collector_str, peer_str, ps->peer_asnumber, v4pfx_cnt,
            v6pfx_cnt);
  }
}

static void pfxs_dump(bgpview_t *view, bgpview_iter_t *it)
{
  bgpstream_pfx_t *pfx;
  char pfx_str[INET6_ADDRSTRLEN + 3] = "";
  char path_str[4096] = "";
  bgpstream_as_path_t *path = NULL;

  fprintf(stdout, "Prefixes (v4 %d, v6 %d):\n",
          bgpview_v4pfx_cnt(view, BGPVIEW_FIELD_ACTIVE),
          bgpview_v6pfx_cnt(view, BGPVIEW_FIELD_ACTIVE));

  for (bgpview_iter_first_pfx(it, 0, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {
    pfx = bgpview_iter_pfx_get_pfx(it);
    bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx);
    fprintf(stdout, "  %s (%d peers)\n", pfx_str,
            bgpview_iter_pfx_get_peer_cnt(it, BGPVIEW_FIELD_ACTIVE));

    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(it); bgpview_iter_pfx_next_peer(it)) {
      path = bgpview_iter_pfx_peer_get_as_path(it);
      bgpstream_as_path_snprintf(path_str, 4096, path);
      bgpstream_as_path_destroy(path);
      fprintf(stdout, "    %" PRIu16 ":\t%s\n",
              bgpview_iter_peer_get_peer_id(it), path_str);
    }
  }
}

/* ========== PUBLIC FUNCTIONS ========== */

void bgpview_debug_dump(bgpview_t *view)
{
  bgpview_iter_t *it = NULL;

  if (view == NULL) {
    fprintf(stdout, "------------------------------\n"
                    "NULL\n"
                    "------------------------------\n\n");
  } else {
    it = bgpview_iter_create(view);
    assert(it);

    fprintf(stdout, "------------------------------\n"
                    "Time:\t%" PRIu32 "\n"
                    "Created:\t%ld\n",
            bgpview_get_time(view), (long)bgpview_get_time_created(view));

    peers_dump(view, it);

    pfxs_dump(view, it);

    fprintf(stdout, "------------------------------\n\n");

    bgpview_iter_destroy(it);
  }
}
