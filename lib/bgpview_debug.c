/*
 * Copyright (C) 2014 The Regents of the University of California.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include "bgpview.h"
#include "bgpview_debug.h"
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
