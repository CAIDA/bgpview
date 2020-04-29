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

#include "bvc_pfx2as_v1.h"
#include "bgpview_consumer_interface.h"
#include "bgpview_consumer_utils.h"
#include "bgpstream_utils_addr.h"
#include "bgpstream_utils_pfx_set.h"
#include "bgpstream_utils_id_set.h"
#include "khash.h"
#include "utils.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <wandio.h>

#define NAME "pfx2as-v1"

#define MAX_ORIGIN_CNT 512
#define MAX_ORIGIN_PEER_CNT 1024
#define OUTPUT_INTERVAL 86400

#define STATE (BVC_GET_STATE(consumer, pfx2as_v1))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_pfx2as_v1 = { //
  BVC_ID_PFX2AS_V1, //
  NAME, //
  BVC_GENERATE_PTRS(pfx2as_v1) //
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
typedef struct bvc_pfx2as_v1_state {

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

  /** split prefixes into files by IP version */
  int split_ipv;

  /* ----- working state ----- */

  /** data for all pfx-peers */
  bgpview_t *view;

  /** iterator for state->view */
  bgpview_iter_t *myit;

  /** count of views in current output interval */
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

  /** sets of peers that were considered full-feed in any view within out_interval */
  bgpstream_id_set_t *full_feed_peer_set[BGPSTREAM_MAX_IP_VERSION_IDX];

} bvc_pfx2as_v1_state_t;

typedef struct pfx2as_v1_stats {
  int32_t pfxpeer_cnt[2];     // count of pfx-peers
  int32_t ppo_cnt[2];         // count of pfx-peer-origins
  int32_t max_origin_cnt[2];  // max origin count for any pfx-peer
  int32_t mopp_cnt[2];        // count of pfx-peers with multiple origins
  int32_t compactable_cnt[2]; // mopps with only 1 nonzero origin
  int32_t overwrite_cnt[2];
  int32_t new_aod_cnt[2];
  int32_t grow_aod_cnt[2];
} pfx2as_v1_stats_t;

/* ==================== CONSUMER INTERNAL FUNCTIONS ==================== */

static int open_outfiles(bvc_t *consumer, int version, uint32_t vtime)
{
  char version_str[4] = "";
  if (version != 0)
    sprintf(version_str, ".v%d", bgpstream_ipv2number(version));
  if (!(STATE->outfile = bvcu_open_outfile(STATE->outfile_name,
        "%s/" NAME "%s.%" PRIu32 ".gz", STATE->outdir, version_str, vtime)))
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
// structure to hold view_cnt_0 and {origin,view_cnt} pairs 1..N-1.
// This assumes that a real user pointer will always have a bit0 == 0 because
// the alignment of the struct will always be at least 2.

#define aod_size(origin_cnt) /* undefined if (origin_cnt <= 1) */              \
  offsetof(additional_origin_durations_t, origins[(origin_cnt)-1])

#define ppu_is_compact(ppu) /* undefined if (ppu==NULL) */                     \
  ((uintptr_t)(ppu) & 0x1)

#define ppu_get_aod(ppu) /* undefined if (ppu_is_compact(ppu)) */              \
  ((additional_origin_durations_t*)(ppu))

#define ppu_get_origin_cnt(ppu)                                                \
  (ppu_is_compact(ppu) ? 1 : ppu_get_aod(ppu)->origin_cnt)

#define ppu_get_view_cnt(ppu, i)                                               \
  (((i) > 0) ? ppu_get_aod(ppu)->origins[(i)-1].view_cnt :                     \
    ppu_is_compact(ppu) ? ((uintptr_t)(ppu) >> 1) :                            \
    ppu_get_aod(ppu)->view_cnt_0)

#define path_get_origin_seg(view, path_id)                                     \
    bgpstream_as_path_store_path_get_origin_seg(                               \
      bgpstream_as_path_store_get_store_path(                                  \
        bgpview_get_as_path_store(view), path_id))

#define ppu_get_origin_seg(view, iter, ppu, i)                                 \
  (((i) > 0) ?                                                                 \
    path_get_origin_seg(view, ppu_get_aod(ppu)->origins[(i)-1].path_id) :      \
    bgpview_iter_pfx_peer_get_origin_seg(iter))

/* note: invalidates ppu */
#define pp_set_compact_view_cnt(iter, n) \
  do {                                                                         \
    assert((n) <= 0x7FFFFFFF);                                                 \
    bgpview_iter_pfx_peer_set_user(iter, (void*)(((n) << 1) | 0x1));           \
  } while (0)

/* note: may invalidate ppu */
#define ppu_set_view_cnt(iter, ppu, i, n)                                      \
  do {                                                                         \
    if ((i) > 0)                                                               \
      ppu_get_aod(ppu)->origins[(i)-1].view_cnt = (n);                         \
    else if (ppu_is_compact(ppu))                                              \
      pp_set_compact_view_cnt(iter, (n));                                      \
    else                                                                       \
      ppu_get_aod(ppu)->view_cnt_0 = (n);                                      \
  } while (0)

// XXX This belongs in bgpstream
#define path_id_equal(a, b) (memcmp(&a, &b, sizeof(a)) == 0)

static void ppu_destroy(void *ppu)
{
  if (!ppu_is_compact(ppu))
    free(ppu);
}


typedef struct peer_duration {
  bgpstream_peer_id_t peer_id;
  uint32_t view_cnt;
} peer_duration_t;

typedef struct origin_peers {
  bgpstream_as_path_seg_t *origin;
  peer_duration_t full_feed_peers; // psuedo peer
  uint32_t full_feed_peer_cnt; // count of real peers comprising pseudo peer
  peer_duration_t partial_feed_peers; // psuedo peer
  uint32_t partial_feed_peer_cnt; // count of real peers comprising pseudo peer
  peer_duration_t peers[MAX_ORIGIN_PEER_CNT]; // real peers
  int peer_cnt;
} origin_peers_t;

static int dump_results(bvc_t *consumer, int version, uint32_t view_interval)
{
  bgpview_iter_t *myit = STATE->myit;
  int indent = 0;


  // delete pfx-peers with 0 views

  // for each prefix
  for (bgpview_iter_first_pfx(myit, version, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(myit);
      bgpview_iter_next_pfx(myit)) {

    // for each real peer in pfx
    for (bgpview_iter_pfx_first_peer(myit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_pfx_has_more_peer(myit);
        bgpview_iter_pfx_next_peer(myit)) {

      int peer_observed_pfx = 0;
      void *ppu = bgpview_iter_pfx_peer_get_user(myit);

      // for each origin in pfx-peer
      for (int i = 0; i < ppu_get_origin_cnt(ppu); ++i) {
        uint32_t view_cnt = ppu_get_view_cnt(ppu, i);
        if (view_cnt > 0) {
          peer_observed_pfx = 1;
          break;
        }
      }

      if (!peer_observed_pfx) {
        // peer never observed prefix in last out_interval; delete pfx-peer
        pp_set_compact_view_cnt(myit, 0); // deallocates user ptr
        bgpview_iter_pfx_remove_peer(myit);
      }
    }
  }

  // count real peers where pfx_cnt > 0
  uint32_t peer_cnt = 0;
  for (bgpview_iter_first_peer(myit, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(myit);
      bgpview_iter_next_peer(myit)) {
    if (bgpview_iter_peer_get_pfx_cnt(myit, version, BGPVIEW_FIELD_ACTIVE) > 0)
      peer_cnt++;
  }
  if (peer_cnt == 0) // e.g., peers are ipv-specific, and split_ipv is true
    return 0; // nothing to report

  if (open_outfiles(consumer, version, STATE->out_interval_start) != 0) {
    return -1;
  }

// DUMP_LINE(delim, fmt, args...)
// depends on `indent` being in scope
#define DUMP_LINE(delim, ...) \
  do {                                                                         \
    wandio_printf(STATE->outfile, "%s\n%*s", delim, indent, "");               \
    wandio_printf(STATE->outfile, __VA_ARGS__);                                \
  } while (0)

  // Dump dataset metadata

  wandio_printf(STATE->outfile, "dataset: {");
  indent += 2;

  DUMP_LINE("", "start: %d", STATE->out_interval_start);
  DUMP_LINE(",", "duration: %d", STATE->view_cnt * view_interval);
  DUMP_LINE(",", "monitor_count: %d", peer_cnt);
  uint32_t pfx_cnt =
    (version == BGPSTREAM_ADDR_VERSION_IPV4 ? bgpview_v4pfx_cnt :
    version == BGPSTREAM_ADDR_VERSION_IPV6 ? bgpview_v6pfx_cnt :
    bgpview_pfx_cnt)(STATE->view, BGPVIEW_FIELD_ACTIVE);
  DUMP_LINE(",", "prefix_count: %"PRIu32, pfx_cnt);

  indent -= 2;
  DUMP_LINE("", "}"); // dataset

  // Dump monitors

  if (!STATE->peer_count_only) {
    char addr_str[INET6_ADDRSTRLEN];
    DUMP_LINE(",", "monitors: [");
    indent += 2;

    const char *mon_delim = "";
    bgpstream_peer_sig_map_t *psmap = bgpview_get_peersigns(STATE->view);
    // note: ACTIVE peers excludes pseudo-peers
    for (bgpview_iter_first_peer(myit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_has_more_peer(myit);
        bgpview_iter_next_peer(myit)) {
      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(myit);
      uint32_t peer_pfx_cnt = bgpview_iter_peer_get_pfx_cnt(myit, version,
          BGPVIEW_FIELD_ACTIVE);
      if (peer_pfx_cnt == 0)
        continue; // skip peer with no prefixes with the requested ipv

      bgpstream_peer_sig_t *ps = bgpstream_peer_sig_map_get_sig(psmap, peer_id);
      DUMP_LINE(mon_delim, "{");
      mon_delim = ",";
      indent += 2;
      DUMP_LINE("", "monitor_idx: %d", peer_id);
      // DUMP_LINE(",", "project: \"%s\"", ???); // not available from bgpview
      DUMP_LINE(",", "collector: \"%s\"", ps->collector_str);
      bgpstream_addr_ntop(addr_str, sizeof(addr_str), &ps->peer_ip_addr);
      DUMP_LINE(",", "address: \"%s\"", addr_str);
      DUMP_LINE(",", "prefix_count: %"PRIu32, peer_pfx_cnt);
      DUMP_LINE(",", "asn: %"PRIu32, ps->peer_asnumber);
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
  for (bgpview_iter_first_pfx(myit, version, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(myit);
      bgpview_iter_next_pfx(myit)) {

    bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(myit);
    int vidx = bgpstream_ipv2idx(pfx->address.version);
    origin_peers_t origins[MAX_ORIGIN_CNT];
    int origin_cnt = 0;

    // convert map of peer->origin to map of origin->peer

    // for each VALID (real or pseudo) peer in pfx
    for (bgpview_iter_pfx_first_peer(myit, BGPVIEW_FIELD_ALL_VALID);
        bgpview_iter_pfx_has_more_peer(myit);
        bgpview_iter_pfx_next_peer(myit)) {

      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(myit);
      void *ppu = bgpview_iter_pfx_peer_get_user(myit);

      // for each origin in pfx-peer
      for (int i = 0; i < ppu_get_origin_cnt(ppu); ++i) {
        uint32_t view_cnt = ppu_get_view_cnt(ppu, i);
        if (view_cnt == 0)
          continue; // skip unobserved origin
        bgpstream_as_path_seg_t *seg = ppu_get_origin_seg(STATE->view, myit, ppu, i);

        // linear search through array -- most prefixes should have one origin
        origin_peers_t *op = NULL;
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

        peer_duration_t *pd;
        if (peer_id == STATE->full_feed_peer_id) {
          pd = &op->full_feed_peers;
        } else if (peer_id == STATE->partial_feed_peer_id) {
          pd = &op->partial_feed_peers;
        } else {
          pd = &op->peers[op->peer_cnt++];
          pd->peer_id = peer_id;
          if (bgpstream_id_set_exists(STATE->full_feed_peer_set[vidx], peer_id)) {
            op->full_feed_peer_cnt++;
          } else {
            op->partial_feed_peer_cnt++;
          }
        }
        pd->view_cnt = view_cnt;
        ppu_set_view_cnt(myit, ppu, i, 0); // reset counter
      }
    }

    // dump {pfx,origin} => ...
    char pfx_str[INET6_ADDRSTRLEN + 4];
    bgpstream_pfx_snprintf(pfx_str, sizeof(pfx_str), pfx);
    for (int i = 0; i < origin_cnt; ++i) {
      char orig_str[4096];
      bgpstream_as_path_seg_snprintf(orig_str, sizeof(orig_str), origins[i].origin);

      DUMP_LINE(pfx_delim, "{"); // prefix_as_meta_data obj
      pfx_delim = ",";
      indent += 2;
      DUMP_LINE("", "network: \"%s\"", pfx_str);
      DUMP_LINE(",", "asn: \"%s\"", orig_str);

      // full/partial-feed monitor counts
      DUMP_LINE(",", "monitors: { full: %d, partial: %d }",
          origins[i].full_feed_peer_cnt, origins[i].partial_feed_peer_cnt);

      // announced_duration
      peer_duration_t *pd_full = &origins[i].full_feed_peers;
      peer_duration_t *pd_partial = &origins[i].partial_feed_peers;
      DUMP_LINE(",", "announced_duration: { full: %d, partial: %d }",
          pd_full->view_cnt * view_interval, pd_partial->view_cnt * view_interval);

      // list of {monitor_idx, duration}
      if (!STATE->peer_count_only) {
        DUMP_LINE(",", "monitors: [");
        indent += 2;
        const char *pfxmon_delim = "";
        for (int j = 0; j < origins[i].peer_cnt; ++j) {
          peer_duration_t *pd = &origins[i].peers[j];
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

  return 0;
}

// Accumulate info about {peer_id, path_id} into myit's pfx-peer
static int count_origin_peer(bvc_t *consumer, bgpstream_pfx_t *pfx,
    bgpstream_peer_id_t peer_id, bgpstream_as_path_store_path_id_t path_id,
    int pfx_exists, pfx2as_v1_stats_t *stats)
{
  bgpview_iter_t *myit = STATE->myit;
  int si = // stat index: 0=real peer, 1=pseudo peer
    (peer_id == STATE->full_feed_peer_id || peer_id == STATE->partial_feed_peer_id) ? 1 : 0;

#if 1
    char pfx_str[INET6_ADDRSTRLEN + 4];
    bgpstream_pfx_snprintf(pfx_str, sizeof(pfx_str), pfx);
    char orig_str[4096];
    bgpstream_as_path_seg_snprintf(orig_str, sizeof(orig_str),
        path_get_origin_seg(STATE->view, path_id));
#endif

  // Make sure pfx-peer exists in myit
  int pfx_peer_is_new = 0;
  if (pfx_exists) {
    assert(bgpstream_pfx_equal(bgpview_iter_pfx_get_pfx(myit), pfx));
    // we can use pfx_seek_peer() instead of seek_pfx_peer()
    // Note: ALL_VALID peers includes pseudo-peers.
    if (!bgpview_iter_pfx_seek_peer(myit, peer_id, BGPVIEW_FIELD_ALL_VALID)) {
      bgpview_iter_pfx_add_peer_by_id(myit, peer_id, path_id);
      pfx_peer_is_new = 1;
    }
  } else {
    // pfx doesn't exist, so pfx-peer can't exist either
    bgpview_iter_add_pfx_peer_by_id(myit, pfx, peer_id, path_id);
    pfx_peer_is_new = 1;
  }

  if (pfx_peer_is_new) {
    if (si == 0) {
      bgpview_iter_pfx_activate_peer(myit);
    }
    pp_set_compact_view_cnt(myit, 1);
#if 1
    // printf("### new %s %d: [%d] %s %d\n", pfx_str, peer_id, 0, orig_str, 1);
#endif
    return 0;
  }

  void *ppu = bgpview_iter_pfx_peer_get_user(myit);
  int found_i = -1;
  bgpstream_as_path_store_path_id_t mypathid0 =
      bgpview_iter_pfx_peer_get_as_path_store_path_id(myit);
  if (path_id_equal(path_id, mypathid0)) {
    // optimize common case: myit's path[0] matches path_id;
    // we don't need to iterate or even compare origins
    found_i = 0;
  } else {
    // general case: search every member of aod for an origin that matches
    // path_id's origin
    bgpstream_as_path_seg_t *origin =
        path_get_origin_seg(STATE->view, path_id);
    int origin_cnt = ppu_get_origin_cnt(ppu);
    for (int i = 0; i < origin_cnt; ++i) {
      bgpstream_as_path_seg_t *myorigin =
          ppu_get_origin_seg(STATE->view, myit, ppu, i);
      if (bgpstream_as_path_seg_equal(myorigin, origin)) {
        found_i = i;
        break;
      }
    }
  }

  if (found_i >= 0) {
    // use existing matching origin
    uintptr_t view_cnt = ppu_get_view_cnt(ppu, found_i);
    view_cnt++;
    ppu_set_view_cnt(myit, ppu, found_i, view_cnt);
#if 1
    // printf("### existing %s %d: [%d] %s %d\n", pfx_str, peer_id, found_i, orig_str, (int)view_cnt);
#endif
    return 0;

  } else if (ppu_get_view_cnt(ppu, 0) == 0) {
    // we can overwrite origin0
    bgpview_iter_pfx_peer_set_as_path_by_id(myit, path_id);
    ppu_set_view_cnt(myit, ppu, 0, 1);
    ++stats->overwrite_cnt[si];
#if 1
    printf("### overwrite %s %d: [%d] %s %d\n", pfx_str, peer_id, 0, orig_str, 1);
#endif
    return 0;
  }

  // is there an existing aod slot with view_cnt==0 that we can overwrite?
  int origin_cnt = ppu_get_origin_cnt(ppu);
  for (int i = 1; i < origin_cnt; ++i) {
    if (ppu_get_view_cnt(ppu, i) == 0) {
      // we can overwrite origins[i]
      found_i = i;
    }
  }
  if (found_i >= 0) {
    // overwrite empty aod slot
#if 1
    printf("### overwrite %s %d: [%d] %s %d\n", pfx_str, peer_id, found_i, orig_str, 1);
#endif
    ++stats->overwrite_cnt[si];
    additional_origin_durations_t *aod = ppu_get_aod(ppu);
    aod->origins[found_i-1].path_id = path_id;
    aod->origins[found_i-1].view_cnt = 1;
  } else {
    // create a new aod slot
    additional_origin_durations_t *aod;
    int new_origin_cnt = ppu_get_origin_cnt(ppu) + 1; // >= 2
    if (!(aod = malloc(aod_size(new_origin_cnt))))
      return -1;
    if (new_origin_cnt == 2) {
      // replace compact storage with a new aod
      aod->view_cnt_0 = ppu_get_view_cnt(ppu, 0);
      ++stats->new_aod_cnt[si];
#if 1
      printf("### new_aod %s %d: [%d] %s %d\n", pfx_str, peer_id, new_origin_cnt-1, orig_str, 1);
#endif
    } else {
      // replace existing aod with a larger aod
      memcpy(aod, ppu_get_aod(ppu), aod_size(new_origin_cnt-1));
      ++stats->grow_aod_cnt[si];
#if 1
      printf("### grow_aod %s %d: [%d] %s %d\n", pfx_str, peer_id, new_origin_cnt-1, orig_str, 1);
#endif
    }
    assert(((uintptr_t)aod & 0x1) == 0);
    aod->origin_cnt = new_origin_cnt;
    aod->origins[aod->origin_cnt-2].path_id = path_id;
    aod->origins[aod->origin_cnt-2].view_cnt = 1;
    bgpview_iter_pfx_peer_set_user(myit, aod); // frees old aod if there was one
  }
  return 0;
}

static int init_my_view(bvc_t *consumer, bgpview_t *srcview)
{
  // receiving first view; initialize my state
  uint32_t vtime = bgpview_get_time(srcview);
  STATE->view = bgpview_create_shared(bgpview_get_peersigns(srcview),
      bgpview_get_as_path_store(srcview), NULL, NULL, NULL, ppu_destroy);
  if (!STATE->view)
    return -1;
  STATE->view_cnt = 0;
  STATE->first_view_time = vtime;
  STATE->prev_view_time = 0;
  STATE->prev_view_interval = 0;
  STATE->out_interval_start = vtime;
  STATE->next_output_time = vtime + STATE->out_interval;
  STATE->myit = bgpview_iter_create(STATE->view);

  for (int i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; ++i) {
    STATE->full_feed_peer_set[i] = bgpstream_id_set_create();
  }

  // for counts by feed type
  bgpstream_ip_addr_t bogus_addr;
  bgpstream_str2addr("0.0.0.0", &bogus_addr);
  STATE->full_feed_peer_id = bgpview_iter_add_peer(STATE->myit,
      "FULL_FEED_PEERS", &bogus_addr, 0);
  STATE->partial_feed_peer_id = bgpview_iter_add_peer(STATE->myit,
      "PARTIAL_FEED_PEERS", &bogus_addr, 0);
#if 1
  printf("## pseudo-peers: %"PRIu16 " %"PRIu16 "\n",
    STATE->full_feed_peer_id, STATE->partial_feed_peer_id);
#endif
  return 0;
}

static int end_output_interval(bvc_t *consumer, uint32_t vtime,
    uint32_t view_interval)
{
  if (STATE->split_ipv) {
    for (int idx = 0; idx < BGPSTREAM_MAX_IP_VERSION_IDX; idx++) {
      if (dump_results(consumer, bgpstream_idx2ipv(idx), view_interval) < 0)
        return -1;
    }
  } else {
    if (dump_results(consumer, 0, view_interval) < 0)
      return -1;
  }

  // reset state
  STATE->view_cnt = 0;
  for (int i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; ++i) {
    bgpstream_id_set_clear(STATE->full_feed_peer_set[i]);
  }
  // Don't bgpview_gc() yet; removed records may be re-added very soon

  STATE->out_interval_start = vtime;
  STATE->next_output_time += STATE->out_interval;
  return 0;
}

static void dump_stats(bvc_t *consumer, pfx2as_v1_stats_t *stats)
{
  bgpview_iter_t *myit = STATE->myit;
  // for each prefix
  for (bgpview_iter_first_pfx(myit, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(myit);
      bgpview_iter_next_pfx(myit)) {

    bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(myit);

    // for each peer in pfx
    for (bgpview_iter_pfx_first_peer(myit, BGPVIEW_FIELD_ALL_VALID);
        bgpview_iter_pfx_has_more_peer(myit);
        bgpview_iter_pfx_next_peer(myit)) {

      void *ppu = bgpview_iter_pfx_peer_get_user(myit);
      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(myit);
      int si = // stat index: 0=real peer, 1=pseudo peer
        (peer_id == STATE->full_feed_peer_id || peer_id == STATE->partial_feed_peer_id) ? 1 : 0;
      stats->pfxpeer_cnt[si]++;
      int origin_cnt = ppu_get_origin_cnt(ppu);
      stats->ppo_cnt[si] += origin_cnt;
      if (origin_cnt > 1) {
        char pfx_str[INET6_ADDRSTRLEN + 4];
        bgpstream_pfx_snprintf(pfx_str, sizeof(pfx_str), pfx);
        printf("## mopp %s %d:", pfx_str, peer_id);
        stats->mopp_cnt[si]++;
        int nonzero_cnt = 0;
        for (int i = 0; i < origin_cnt; ++i) {
          char orig_str[4096];
          bgpstream_as_path_seg_snprintf(orig_str, sizeof(orig_str),
              ppu_get_origin_seg(STATE->view, myit, ppu, i));
          int view_cnt = ppu_get_view_cnt(ppu, i);
          printf(" %s %d;", orig_str, view_cnt);
          if (view_cnt > 0)
            nonzero_cnt++;
        }
        if (nonzero_cnt == 1) {
          printf(" (compactable)");
          stats->compactable_cnt[si]++;
        }
        printf("\n");
      }
      if (origin_cnt > stats->max_origin_cnt[si])
        stats->max_origin_cnt[si] = origin_cnt;
    }
  }
  printf("# pp=%d,%d; ppo: tot=%d,%d, max=%d,%d; ppo/pp=%f; mopp=%d,%d (%d,%d compactable); overwrite=%d,%d; new_aod=%d,%d; grow_aod=%d,%d\n",
      stats->pfxpeer_cnt[0], stats->pfxpeer_cnt[1],
      stats->ppo_cnt[0], stats->ppo_cnt[1],
      stats->max_origin_cnt[0], stats->max_origin_cnt[1],
      (double)(stats->ppo_cnt[0] + stats->ppo_cnt[1]) / (stats->pfxpeer_cnt[0] + stats->pfxpeer_cnt[1]),
      stats->mopp_cnt[0], stats->mopp_cnt[1],
      stats->compactable_cnt[0], stats->compactable_cnt[1],
      stats->overwrite_cnt[0], stats->overwrite_cnt[1],
      stats->new_aod_cnt[0], stats->new_aod_cnt[1],
      stats->grow_aod_cnt[0], stats->grow_aod_cnt[1]);
}

int bvc_pfx2as_v1_process_view(bvc_t *consumer, bgpview_t *view)
{
  uint32_t vtime = bgpview_get_time(view);
  uintptr_t view_interval = 0;
  bgpview_iter_t *vit = NULL;
  bgpview_iter_t *myit = NULL;

  if (!STATE->view) {
    // receiving first view; initialize my state
    if (init_my_view(consumer, view) < 0)
      goto err;

  } else {
    view_interval = vtime - STATE->prev_view_time;
    if (STATE->prev_view_interval == 0) {
      // second view (end of first view_interval)
      if (STATE->out_interval % view_interval != 0) {
        fprintf(stderr, "WARNING: " NAME ": output interval %d is not a multiple "
            "of view interval %"PRIuPTR" at %"PRIu32"\n",
            STATE->out_interval, view_interval, vtime);
      }
    } else {
      if (STATE->prev_view_interval != view_interval) {
        // third+ view (end of second+ view_interval)
        fprintf(stderr, "ERROR: " NAME ": view interval changed from %d to "
            "%"PRIuPTR" at %"PRIu32"\n",
            STATE->prev_view_interval, view_interval, vtime);
        goto err;
      }
    }
    if (vtime >= STATE->next_output_time) {
      // End the output interval BEFORE processing current view
      if (end_output_interval(consumer, vtime, view_interval) < 0)
        goto err;
    }
  }

  vit = bgpview_iter_create(view);
  myit = STATE->myit;
  pfx2as_v1_stats_t stats;
  memset(&stats, 0, sizeof(stats));
  STATE->view_cnt++;

  // make sure every peer in view exists in myview
  for (bgpview_iter_first_peer(vit, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(vit);
      bgpview_iter_next_peer(vit)) {
    bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(vit);
    if (!bgpview_iter_seek_peer(myit, peer_id, BGPVIEW_FIELD_ACTIVE)) {
      bgpstream_peer_sig_t *ps = bgpview_iter_peer_get_sig(vit);
      bgpstream_peer_id_t new_peer_id = bgpview_iter_add_peer(myit,
          ps->collector_str, &ps->peer_ip_addr, ps->peer_asnumber);
      assert(new_peer_id == peer_id);
      bgpview_iter_activate_peer(myit);
    }
  }

  // Array of structs for accumulating counts of full/partial peers across
  // multiple origins for a single prefix
  struct {
    bgpstream_as_path_store_path_id_t path_id; // a path representing the origin
    // count of full-feed peers with the current pfx and same origin as path_id
    uint32_t full_cnt;
    // count of partial-feed peers with the current pfx and same origin as path_id
    uint32_t partial_cnt;
  } pfx_origins[MAX_ORIGIN_CNT];
  int pfx_origin_cnt; // count of origins in the current prefix

  // for each prefix
  for (bgpview_iter_first_pfx(vit, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(vit);
      bgpview_iter_next_pfx(vit)) {

    bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(vit);
    int vidx = bgpstream_ipv2idx(pfx->address.version);
    pfx_origin_cnt = 0;

    // does pfx already exist in myview?
    int pfx_exists = bgpview_iter_seek_pfx(myit, pfx, BGPVIEW_FIELD_ACTIVE);

    // for each peer in pfx
    for (bgpview_iter_pfx_first_peer(vit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_pfx_has_more_peer(vit);
        bgpview_iter_pfx_next_peer(vit)) {

      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(vit);
      bgpstream_as_path_store_path_id_t path_id =
          bgpview_iter_pfx_peer_get_as_path_store_path_id(vit);
      bgpstream_as_path_seg_t *origin =
          bgpview_iter_pfx_peer_get_origin_seg(vit);
      int is_full = bgpstream_id_set_exists(
          CHAIN_STATE->full_feed_peer_ids[vidx], peer_id);

      // count full-feed peer
      if (is_full) {
        // Note: feed type is always ipv-specific
        bgpstream_id_set_insert(STATE->full_feed_peer_set[vidx], peer_id);
      }

      // accumulate count for pseudo-peers
      int found_i = -1;
      for (int i = 0; i < pfx_origin_cnt; ++i) {
        // Comparing path_ids is cheaper, but if that fails we must still
        // compare origins because different paths can have the same origin.
        if (path_id_equal(path_id, pfx_origins[i].path_id) ||
            bgpstream_as_path_seg_equal(origin,
              path_get_origin_seg(view, pfx_origins[i].path_id))) {
          found_i = i;
          break;
        }
      }
      if (found_i < 0) {
        // allocate a new pfx_origin
        found_i = pfx_origin_cnt++;
        assert(pfx_origin_cnt < MAX_ORIGIN_CNT);
        pfx_origins[found_i].path_id = path_id;
        pfx_origins[found_i].full_cnt = 0;
        pfx_origins[found_i].partial_cnt = 0;
      }
      if (is_full) {
        pfx_origins[found_i].full_cnt++;
      } else {
        pfx_origins[found_i].partial_cnt++;
      }

      // count into actual peer
      if (count_origin_peer(consumer, pfx, peer_id, path_id, pfx_exists, &stats) < 0)
        goto err;
      pfx_exists = 1;
    }

    // finalize count for pseudo-peers
    for (int i = 0; i < pfx_origin_cnt; ++i) {
      if (pfx_origins[i].full_cnt > 0) {
        if (count_origin_peer(consumer, pfx, STATE->full_feed_peer_id,
            pfx_origins[i].path_id, pfx_exists, &stats) < 0)
          goto err;
        pfx_exists = 1;
      }
      if (pfx_origins[i].partial_cnt > 0) {
        if (count_origin_peer(consumer, pfx, STATE->partial_feed_peer_id,
            pfx_origins[i].path_id, pfx_exists, &stats) < 0)
          goto err;
        pfx_exists = 1;
      }
    }
  }

  bgpview_gc(STATE->view);
  bgpview_iter_destroy(vit);

  STATE->prev_view_interval = view_interval;
  STATE->prev_view_time = vtime;

  dump_stats(consumer, &stats);

  return 0;
err:
  bgpview_iter_destroy(vit);
  return -1;
}

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
      "consumer usage: %s\n"
      "       -i <output-interval>  output interval in seconds (default %d)\n"
      "       -o <path>             output directory\n"
      "       -c                    output peer counts, not full list\n"
      "       -v                    split prefixes into files by IP version\n",
      consumer->name, OUTPUT_INTERVAL);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  assert(argc > 0 && argv != NULL);

  bvc_pfx2as_v1_state_t *state = STATE;

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, "i:o:cv?")) >= 0) {
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
    case 'v':
      state->split_ipv = 1;
      break;
    case '?':
    case ':':
    default:
      usage(consumer);
      return -1;
    }
  }

  if (state->outdir == NULL) {
    fprintf(stderr, "ERROR: " NAME " output directory required\n");
    usage(consumer);
    return -1;
  }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_pfx2as_v1_alloc()
{
  return &bvc_pfx2as_v1;
}

int bvc_pfx2as_v1_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_pfx2as_v1_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_pfx2as_v1_state_t))) == NULL) {
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

  // Test ability to open output files now so user gets immediate feedback on
  // any errors, instead of waiting for the end of the first out_interval
  if (open_outfiles(consumer, 0, 0) != 0) {
    goto err;
  }
  wandio_wdestroy(STATE->outfile);
  STATE->outfile = NULL;
  remove(STATE->outfile_name);

  return 0;

err:
  bvc_pfx2as_v1_destroy(consumer);
  return -1;
}

void bvc_pfx2as_v1_destroy(bvc_t *consumer)
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

  for (int i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; ++i) {
    if (STATE->full_feed_peer_set[i]) {
      bgpstream_id_set_destroy(STATE->full_feed_peer_set[i]);
    }
  }

  free(STATE->outdir);

  free(STATE);
  BVC_SET_STATE(consumer, NULL);
}

