/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2020 The Regents of the University of California.
 * Authors: Ken Keys
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

#include "bvc_pfx2as.h"
#include "bgpview_consumer_interface.h"
#include "bgpview_consumer_utils.h"
#include "bgpstream_utils_pfx_set.h"
#include "khash.h"
#include "utils.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <wandio.h>

#define NAME "pfx2as"

#define MAX_ORIGIN_CNT 512
#define MAX_ORIGIN_PEER_CNT 1024
#define OUTPUT_INTERVAL 86400

#define STATE (BVC_GET_STATE(consumer, pfx2as))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_pfx2as = { //
  BVC_ID_PFX2AS, //
  NAME, //
  BVC_GENERATE_PTRS(pfx2as) //
};

/// origin and duration
typedef struct origin_duration {

  /// id of path containing the origin
  bgpstream_as_path_store_path_id_t path_id;

  /// count of views in which origin was visible to peer
  uint32_t view_cnt;

} origin_duration_t;

/// additional origin/duration pairs for pfx-peers with more than one
typedef struct additional_origin_durations {

  /// count of views in which origin0 was visible to peer
  uint32_t view_cnt_0;

  /// total number of origins (including #0)
  int origin_cnt;

  /// additional origins (flexible array member)
  origin_duration_t origins[];

} additional_origin_durations_t;

/* our 'instance' */
typedef struct bvc_pfx2as_state {

  /* ----- configuration ----- */

  /** output directory */
  char *outdir;

  /** prefix origins output file name */
  char outfile_name[BVCU_PATH_MAX];

  /** prefix origins output file */
  iow_t *outfile;

  /** output interval */
  uint32_t out_interval;

  /** only output peer counts */
  int peer_count_only;

  /* ----- working state ----- */

  /** data for all pfx-peers */
  bgpview_t *view;

  /** iterator for state->view */
  bgpview_iter_t *myit;

  /** count of views since last reset */
  uint32_t view_cnt;

  /** time of first view */
  uint32_t first_view_time;

  /** when next to dump output */
  uint32_t next_output_time;

  /** time of most recent view */
  uint32_t prev_view_time;

  /** interval between previous view and the one before that */
  uint32_t prev_view_interval;

  /** first view_time in the current output interval */
  uint32_t out_interval_start;

  /** ids of pseudo-peers that represent all full- or partial-feed peers */
  bgpstream_peer_id_t full_feed_peer_id;
  bgpstream_peer_id_t partial_feed_peer_id;

} bvc_pfx2as_state_t;

typedef struct pfx2as_stats {
  int32_t pfxpeer_cnt;
  int32_t tot_origin_cnt;
  int32_t max_origin_cnt;
  int32_t mopp_cnt; // pfx-peers with multiple origins
  int32_t overwrite_cnt;
  int32_t malloc_cnt;
  int32_t realloc_cnt;
} pfx2as_stats_t;

/* ==================== CONSUMER INTERNAL FUNCTIONS ==================== */

static int open_outfiles(bvc_t *consumer, uint32_t vtime)
{
  if (!(STATE->outfile = bvcu_open_outfile(STATE->outfile_name,
        "%s/" NAME ".%" PRIu32 ".gz", STATE->outdir, vtime)))
    return -1;

  return 0;
}

static int close_outfiles(bvc_t *consumer)
{
  wandio_wdestroy(STATE->outfile);
  STATE->outfile = NULL;
  bvcu_create_donefile(STATE->outfile_name);

  return 0;
}

// In the vast majority of cases, a pfx-peer has only one origin.  In that
// case, we can use a compact storage scheme that packs the origin and
// view_cnt directly into the pfx-peer without allocating a user data
// structure:
// - origin is already stored in the pfx-peer's as path.
// - we overload the user pointer with a flag in bit0 to indicate we're
//   overloading it, and store the view_cnt in the remaining 31 (or more) bits.
// If there are multiple origins, only then do we allocate a user data
// structure to hold view_cnt_0 and origin/view_cnt pairs 1..N-1.
// This assumes that a real user pointer will always have a bit0 == 0 because
// the alignment of the struct will always be at least 2.

#pragma GCC diagnostic ignored "-Wbad-function-cast"

#define aod_size(origin_cnt)                                                   \
  offsetof(additional_origin_durations_t, origins[(origin_cnt)-1])

#define pp_is_compact(iter) /* undefined if (user==NULL) */                    \
  ((uintptr_t)bgpview_iter_pfx_peer_get_user(iter) & 0x1)

#define pp_get_aod(iter)                                                       \
  ((additional_origin_durations_t*)bgpview_iter_pfx_peer_get_user(iter))

#define pp_origin_cnt(iter)                                                    \
  (pp_is_compact(iter) ? 1 : pp_get_aod(iter)->origin_cnt)

#define pp_get_path_id(iter, i)                                                \
  (((i) > 0) ? pp_get_aod(iter)->origins[(i)-1].path_id :                      \
    bgpview_iter_pfx_peer_get_as_path_store_path_id(iter))

#define pp_get_view_cnt(iter, i)                                               \
  (((i) > 0) ? pp_get_aod(iter)->origins[(i)-1].view_cnt :                     \
    pp_is_compact(iter) ?                                                      \
    ((uintptr_t)bgpview_iter_pfx_peer_get_user(iter) >> 1) :                   \
    pp_get_aod(iter)->view_cnt_0)

#define pp_get_origin_seg(view, iter, i)                                       \
  (((i) > 0) ?                                                                 \
    bgpstream_as_path_store_path_get_origin_seg(                               \
      bgpstream_as_path_store_get_store_path(                                  \
        bgpview_get_as_path_store(view),                                       \
        pp_get_aod(iter)->origins[(i)-1].path_id)) :                           \
    bgpview_iter_pfx_peer_get_origin_seg(iter))

#define pp_set_compact_view_cnt(iter, n) \
  do {                                                                         \
    assert((n) <= 0x7FFFFFFF);                                                 \
    bgpview_iter_pfx_peer_set_user(iter, (void*)(((n) << 1) | 0x1));           \
  } while (0)

#define pp_set_view_cnt(iter, i, n) \
  do {                                                                         \
    if ((i) > 0)                                                               \
      pp_get_aod(iter)->origins[(i)-1].view_cnt = (n);                         \
    else if (pp_is_compact(iter))                                              \
      pp_set_compact_view_cnt(iter, (n));                                      \
    else                                                                       \
      pp_get_aod(iter)->view_cnt_0 = (n);                                      \
  } while (0)

#define path_id_equal(a, b) (memcmp(&a, &b, sizeof(a)) == 0) /* XXX ??? */

static void pp_destroy(void *pp)
{
  if (!((uintptr_t)pp & 0x1))
    free(pp);
}


typedef struct peer_duration {
  bgpstream_peer_id_t peer_id;
  uint32_t view_cnt;
} peer_duration_t;

typedef struct origin_peers {
  bgpstream_as_path_seg_t *origin;
  uint32_t full_feed_peer_cnt;
  peer_duration_t full_feed_peers; // psuedo peer
  uint32_t partial_feed_peer_cnt;
  peer_duration_t partial_feed_peers; // psuedo peer
  peer_duration_t peers[MAX_ORIGIN_PEER_CNT]; // real peers
  int peer_cnt;
} origin_peers_t;

static int dump_results(bvc_t *consumer, uint32_t view_interval)
{
  bgpview_iter_t *myit = STATE->myit;
  int indent = 0;
  iow_t *f = STATE->outfile;

// DUMP_LINE(delim, fmt, args...)
// depends on `f` and `indent` being in scope
#define DUMP_LINE(delim, ...) \
  do {                                                                         \
    wandio_printf(f, "%s\n%*s", delim, indent, "");                            \
    wandio_printf(f, __VA_ARGS__);                                             \
  } while (0)

  // Dump dataset metadata

  wandio_printf(f, "dataset: {");
  indent += 2;

  DUMP_LINE("", "start: %d", STATE->out_interval_start);
  DUMP_LINE(",", "duration: %d", STATE->view_cnt * view_interval);
  DUMP_LINE(",", "monitor_count: %d", -1); // XXX
  DUMP_LINE(",", "prefix_count: %d", -1); // XXX

  indent -= 2;
  DUMP_LINE("", "}"); // dataset

  // Dump monitors

  if (!STATE->peer_count_only) {
    DUMP_LINE(",", "monitors: [");
    indent += 2;

    const char *mon_delim = "";
    bgpstream_peer_sig_map_t *psmap = bgpview_get_peersigns(STATE->view);
    for (bgpview_iter_first_peer(myit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_has_more_peer(myit);
        bgpview_iter_next_peer(myit)) {
      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(myit);
      bgpstream_peer_sig_t *ps = bgpstream_peer_sig_map_get_sig(psmap, peer_id);
      DUMP_LINE(mon_delim, "{");
      mon_delim = ",";
      indent += 2;
      DUMP_LINE("", "monitor_id: %d", peer_id);
      DUMP_LINE(",", "project: \"%s\"", "XXX"); // XXX
      DUMP_LINE(",", "collector: \"%s\"", ps->collector_str);
      DUMP_LINE(",", "prefix_count: %d", -1); // XXX
      DUMP_LINE(",", "asn: %d", ps->peer_asnumber);
      indent -= 2;
      DUMP_LINE("", "}");
    }

    indent -= 2;
    DUMP_LINE("", "]"); // monitors list
  }

  // Dump prefixes

  DUMP_LINE(",", "prefix_as_meta_data: [");
  indent += 2;

  // for each prefix
  const char *pfx_delim = "";
  for (bgpview_iter_first_pfx(myit, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(myit);
      bgpview_iter_next_pfx(myit)) {

    peer_duration_t *pd;
    bgpstream_as_path_seg_t *seg;
    origin_peers_t *op;
    origin_peers_t origins[MAX_ORIGIN_CNT];
    int origin_cnt = 0;

    // convert map of peer->origin to map of origin->peer

    // for each peer in pfx
    for (bgpview_iter_pfx_first_peer(myit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_pfx_has_more_peer(myit);
        bgpview_iter_pfx_next_peer(myit)) {

      int observed = 0;

      // for each origin in pfx-peer
      for (int i = 0; i < pp_origin_cnt(myit); ++i) {
        uint32_t view_cnt = pp_get_view_cnt(myit, i);
        if (view_cnt == 0)
          continue; // skip unobserved origin
        observed++;
        seg = pp_get_origin_seg(STATE->view, myit, i);

        // linear search through array -- most prefixes should have one origin
        op = NULL;
        for (int j = 0; j < origin_cnt; ++j) {
          if (bgpstream_as_path_seg_equal(origins[j].origin, seg)) {
            op = &origins[j];
            break;
          }
        }
        if (!op) {
          // new origin
          assert(origin_cnt < MAX_ORIGIN_CNT);
          op = &origins[origin_cnt];
          origin_cnt++;
          op->origin = seg;
          op->peer_cnt = 0;
          op->full_feed_peer_cnt = 0;
          op->full_feed_peers.view_cnt = 0;
          op->partial_feed_peer_cnt = 0;
          op->partial_feed_peers.view_cnt = 0;
        }

        assert(op->peer_cnt < MAX_ORIGIN_PEER_CNT);

        bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(myit);
        if (peer_id == STATE->full_feed_peer_id) {
          pd = &op->full_feed_peers;
          op->full_feed_peer_cnt++;
        } else if (peer_id == STATE->partial_feed_peer_id) {
          pd = &op->partial_feed_peers;
          op->partial_feed_peer_cnt++;
        } else {
          pd = &op->peers[op->peer_cnt++];
        }
        pd->peer_id = bgpview_iter_peer_get_peer_id(myit);
        pd->view_cnt = view_cnt;
        pp_set_view_cnt(myit, i, 0); // reset counter
      }

      if (observed == 0) {
        // peer never observed prefix in last out_interval; delete pfx-peer
        bgpview_iter_pfx_peer_set_user(myit, NULL);
        bgpview_iter_pfx_remove_peer(myit);
      }
    }

    // dump {pfx,origin} => ...
    for (int i = 0; i < origin_cnt; ++i) {
      char pfx_str[INET6_ADDRSTRLEN + 4];
      char orig_str[4096];
      bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(myit);
      bgpstream_pfx_snprintf(pfx_str, sizeof(pfx_str), pfx);
      bgpstream_as_path_seg_snprintf(orig_str, sizeof(orig_str), origins[i].origin);

      DUMP_LINE(pfx_delim, "{"); // prefix_as_meta_data obj
      pfx_delim = ",";
      indent += 2;
      DUMP_LINE("", "network: %s", pfx_str);
      DUMP_LINE(",", "asn:\"%s\"", orig_str);

      // full/partial-feed monitor counts
      DUMP_LINE(",", "monitors: {");
      indent += 2;
      DUMP_LINE("", "full: %d", origins[i].full_feed_peer_cnt);
      DUMP_LINE(",", "partial: %d", origins[i].partial_feed_peer_cnt);
      indent -= 2;
      DUMP_LINE("", "}");

      // announced_duration
      DUMP_LINE(",", "announced_duration: {");
      indent += 2;
      pd = &origins[i].full_feed_peers;
      DUMP_LINE("", "full: %d", pd->view_cnt * view_interval);
      pd = &origins[i].partial_feed_peers;
      DUMP_LINE(",", "partial: %d", pd->view_cnt * view_interval);
      indent -= 2;
      DUMP_LINE("", "}");

      // list of {monitor_id, duration}
      if (!STATE->peer_count_only) {
        DUMP_LINE(",", "monitors: [");
        indent += 2;
        const char *pfxmon_delim = "";
        for (int j = 0; j < origins[i].peer_cnt; ++j) {
          pd = &origins[i].peers[j];
          uint32_t duration = pd->view_cnt * view_interval;
          DUMP_LINE(pfxmon_delim, "{ monitor:%"PRIu16", duration:%"PRIu32" }",
              pd->peer_id, duration);
          pfxmon_delim = ",";
        }
        indent -= 2;
        DUMP_LINE("", "]"); // monitors
      }

      indent -= 2;
      DUMP_LINE("", "}"); // prefix_as_meta_data obj
    }
  }
  indent -= 2;
  DUMP_LINE("", "]\n"); // prefix_as_meta_data list

  /* close the output files and create .done file */
  if (close_outfiles(consumer) != 0) {
    return -1;
  }

  // reset state
  STATE->view_cnt = 0;
  // don't reset first_view_time, prev_view_time, or prev_view_interval

  return 0;
}

// Accumulate info about vit's pfx-peer's origin into myit's pfx-peer
static int count_origin_peer(bvc_t *consumer, bgpview_iter_t *vit,
    bgpview_iter_t *myit, bgpstream_pfx_t *pfx, bgpstream_peer_id_t peer_id,
    bgpstream_as_path_store_path_id_t path_id, pfx2as_stats_t *stats)
{
  // Make sure pfx-peer exists in myit
  // TODO: If we can guarantee that pfx already exists in myit and myit is
  // pointing to it, we can replace verb_pfx_peer() with the faster
  // pfx_verb_peer().
  if (!bgpview_iter_seek_pfx_peer(myit, pfx, peer_id, BGPVIEW_FIELD_ACTIVE,
      BGPVIEW_FIELD_ACTIVE)) {
    bgpview_iter_add_pfx_peer_by_id(myit, pfx, peer_id, path_id);
  }

  if (bgpview_iter_pfx_peer_get_state(myit) == BGPVIEW_FIELD_INACTIVE) {
    // new pfx-peer
    bgpview_iter_pfx_activate_peer(myit);
    pp_set_compact_view_cnt(myit, 1);
    return 0;
  }

  int found_i = -1;
  bgpstream_as_path_store_path_id_t mypathid0 =
      bgpview_iter_pfx_peer_get_as_path_store_path_id(myit);
  if (path_id_equal(path_id, mypathid0)) {
    // optimize common case: myit's path[0] matches vit's path;
    // we don't need to iterate or even compare origins
    found_i = 0;
  } else {
    // general case: search every member of aod for an origin that matches
    // vit's origin
    bgpstream_as_path_seg_t *origin =
        bgpview_iter_pfx_peer_get_origin_seg(vit);
    int origin_cnt = pp_origin_cnt(myit);
    for (int i = 0; i < origin_cnt; ++i) {
      bgpstream_as_path_seg_t *myorigin =
          pp_get_origin_seg(STATE->view, myit, i);
      if (bgpstream_as_path_seg_equal(myorigin, origin)) {
        found_i = i;
        break;
      }
    }
  }

  if (found_i >= 0) {
    // use existing origin
    uintptr_t view_cnt = pp_get_view_cnt(myit, found_i);
    view_cnt++;
    pp_set_view_cnt(myit, found_i, view_cnt);
  } else if (pp_is_compact(myit) && pp_get_view_cnt(myit, 0) == 0) {
    // we can overwrite origin0
    bgpview_iter_pfx_peer_set_as_path_by_id(myit, path_id);
    pp_set_view_cnt(myit, found_i, 1);
    ++stats->overwrite_cnt;
  } else {
    // use a new aod slot
    additional_origin_durations_t *aod;
    int new_origin_cnt = pp_origin_cnt(myit) + 1; // >= 2
    if (!(aod = malloc(aod_size(new_origin_cnt))))
      return -1;
    if (new_origin_cnt == 2) {
      // replace compact storage with a new aod
      aod->view_cnt_0 = pp_get_view_cnt(myit, 0);
      ++stats->malloc_cnt;
    } else {
      // replace existing aod with a larger aod
      memcpy(aod, pp_get_aod(myit), aod_size(new_origin_cnt-1));
      ++stats->realloc_cnt;
    }
    assert(((uintptr_t)aod & 0x1) == 0);
    aod->origin_cnt = new_origin_cnt;
    aod->origins[aod->origin_cnt-2].path_id = path_id;
    aod->origins[aod->origin_cnt-2].view_cnt = 1;
    bgpview_iter_pfx_peer_set_user(myit, aod);
  }
  return 0;
}

int bvc_pfx2as_process_view(bvc_t *consumer, bgpview_t *view)
{
  uint32_t vtime = bgpview_get_time(view);
  uintptr_t view_interval = 0;
  pfx2as_stats_t stats;
  memset(&stats, 0, sizeof(stats));

  if (!STATE->view) {
    // receiving first view; initialize my state
    STATE->view = bgpview_create_shared(bgpview_get_peersigns(view),
        bgpview_get_as_path_store(view), NULL, NULL, NULL, pp_destroy);
    if (!STATE->view)
      goto err;
    STATE->view_cnt = 0;
    STATE->first_view_time = vtime;
    STATE->prev_view_time = 0;
    STATE->prev_view_interval = 0;
    STATE->next_output_time = vtime + STATE->out_interval;
    STATE->myit = bgpview_iter_create(STATE->view);
    // for counts by feed type
    bgpstream_ip_addr_t bogus_addr;
    bgpstream_str2addr("0.0.0.0", &bogus_addr);
    STATE->full_feed_peer_id = bgpview_iter_add_peer(STATE->myit,
        "FULL_FEED_PEERS", &bogus_addr, 0);
    bgpview_iter_activate_peer(STATE->myit);
    STATE->partial_feed_peer_id = bgpview_iter_add_peer(STATE->myit,
        "PARTIAL_FEED_PEERS", &bogus_addr, 0);
    bgpview_iter_activate_peer(STATE->myit);

  } else {
    view_interval = vtime - STATE->prev_view_time;
    if (STATE->prev_view_interval == 0) {
      // second view (end of first view_interval)
      if (STATE->out_interval % view_interval != 0) {
        fprintf(stderr, "WARNING: pfx2as: output interval %d is not a multiple "
            "of view interval %"PRIuPTR"\n", STATE->out_interval, view_interval);
      }
    } else {
      if (STATE->prev_view_interval != view_interval) {
        // third+ view (end of second+ view_interval)
        fprintf(stderr, "ERROR: pfx2as: view interval changed from %d to "
            "%"PRIuPTR"\n", STATE->prev_view_interval, view_interval);
        goto err;
      }
    }
    if (vtime >= STATE->next_output_time) {
      // Dump results BEFORE processing current view
      if (dump_results(consumer, view_interval) < 0)
        goto err;
      STATE->next_output_time += STATE->out_interval;
    }
  }

  if (!STATE->outfile) {
    // prepare output files for writing (doing this now instead of waiting for
    // the end of the out_interval lets us generate any error message sooner)
    STATE->out_interval_start = vtime;
    if (open_outfiles(consumer, STATE->out_interval_start) != 0) {
      return -1;
    }
  }

  STATE->view_cnt++;
  bgpview_iter_t *vit = bgpview_iter_create(view);
  bgpview_iter_t *myit = STATE->myit;

  if (!STATE->peer_count_only) {
    // make sure every peer in view exists in myview
    for (bgpview_iter_first_peer(vit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_has_more_peer(vit);
        bgpview_iter_next_peer(vit)) {

      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(vit);
      if (!bgpview_iter_seek_peer(myit, peer_id, BGPVIEW_FIELD_ACTIVE)) {
        // TODO implement bgpview_iter_add_peer_by_id() in bgpview; use it here
        bgpstream_peer_sig_t *ps = bgpview_iter_peer_get_sig(vit);
        bgpstream_peer_id_t new_peer_id = bgpview_iter_add_peer(myit,
            ps->collector_str, &ps->peer_ip_addr, ps->peer_asnumber);
        assert(new_peer_id == peer_id);
        bgpview_iter_activate_peer(myit);
      }
    }
  }

  // for each prefix
  for (bgpview_iter_first_pfx(vit, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(vit);
      bgpview_iter_next_pfx(vit)) {

    bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(vit);

    // for each peer in pfx
    for (bgpview_iter_pfx_first_peer(vit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_pfx_has_more_peer(vit);
        bgpview_iter_pfx_next_peer(vit)) {

      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(vit);
      bgpstream_as_path_store_path_id_t path_id =
          bgpview_iter_pfx_peer_get_as_path_store_path_id(vit);

      // count into full- or partial-feed pseudo-peer
      int idx = bgpstream_ipv2idx(pfx->address.version);
      int full = bgpstream_id_set_exists(CHAIN_STATE->full_feed_peer_ids[idx], peer_id);
      if (count_origin_peer(consumer, vit, myit, pfx,
          full ? STATE->full_feed_peer_id : STATE->partial_feed_peer_id,
          path_id, &stats) < 0)
        goto err;

      // count into actual peer
      if (STATE->peer_count_only == 0) {
        if (count_origin_peer(consumer, vit, myit, pfx, peer_id, path_id, &stats) < 0)
          goto err;
      }
    }
  }

  bgpview_gc(STATE->view);

  STATE->prev_view_interval = view_interval;
  STATE->prev_view_time = bgpview_get_time(view);

#if 1 // dump stats
  // for each prefix
  for (bgpview_iter_first_pfx(myit, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(myit);
      bgpview_iter_next_pfx(myit)) {

    // for each peer in pfx
    for (bgpview_iter_pfx_first_peer(myit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_pfx_has_more_peer(myit);
        bgpview_iter_pfx_next_peer(myit)) {

      stats.pfxpeer_cnt++;
      int origin_cnt = pp_origin_cnt(myit);
      stats.tot_origin_cnt += origin_cnt;
      if (origin_cnt > 1)
        stats.mopp_cnt++;
      if (origin_cnt > stats.max_origin_cnt)
        stats.max_origin_cnt = origin_cnt;
    }
  }
  printf("# pp=%d; orig: tot=%d, max=%d; orig/pp=%f; mopp=%d; overwrite=%d; malloc=%d,%d\n",
      stats.pfxpeer_cnt,
      stats.tot_origin_cnt, stats.max_origin_cnt,
      (double)stats.tot_origin_cnt/stats.pfxpeer_cnt,
      stats.mopp_cnt,
      stats.overwrite_cnt,
      stats.malloc_cnt, stats.realloc_cnt);
#endif

  bgpview_iter_destroy(vit);
  return 0;
err:
  return -1;
}

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
      "consumer usage: %s\n"
      "       -i <output-interval>  output interval in seconds (default %d)\n"
      "       -o <path>             output directory\n"
      "       -c                    output peer counts, not full list\n",
      consumer->name, OUTPUT_INTERVAL);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  assert(argc > 0 && argv != NULL);

  bvc_pfx2as_state_t *state = STATE;

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, "i:o:c?")) >= 0) {
    switch (opt) {
    case 'i':
      state->out_interval = strtoul(optarg, NULL, 10);
      break;
    case 'o':
      state->outdir = strdup(optarg);
      break;
    case 'c':
      state->peer_count_only = 1;
      break;
    case '?':
    case ':':
    default:
      usage(consumer);
      return -1;
    }
  }

  if (state->outdir == NULL) {
    fprintf(stderr, "ERROR: pfx2as output directory required\n");
    usage(consumer);
    return -1;
  }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_pfx2as_alloc()
{
  return &bvc_pfx2as;
}

int bvc_pfx2as_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_pfx2as_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_pfx2as_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  state->out_interval = OUTPUT_INTERVAL;
  state->view = NULL;

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  fprintf(stderr, "INFO: output directory: %s\n", state->outdir);

  if (CHAIN_STATE->visibility_computed == 0) {
    fprintf(stderr, "ERROR: " NAME " requires the Visibility consumer "
                    "to be run first\n");
    goto err;
  }

  return 0;

err:
  bvc_pfx2as_destroy(consumer);
  return -1;
}

void bvc_pfx2as_destroy(bvc_t *consumer)
{
  if (STATE == NULL) {
    return;
  }

  if (STATE->outfile) {
    if (STATE->prev_view_time > STATE->out_interval_start) {
      fprintf(stderr, "WARNING: omitting incomplete %s output interval %d-%d\n",
          NAME, STATE->out_interval_start, STATE->prev_view_time);
    }
    wandio_wdestroy(STATE->outfile);
    STATE->outfile = NULL;
  }

  if (STATE->myit)
    bgpview_iter_destroy(STATE->myit);

  if (STATE->view)
    bgpview_destroy(STATE->view);

  free(STATE->outdir);

  free(STATE);
  BVC_SET_STATE(consumer, NULL);
}
