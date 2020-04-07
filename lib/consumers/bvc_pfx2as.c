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

  /// how long origin was visible to peer
  uint32_t duration;

} origin_duration_t;

/// additional origin/duration pairs for pfx-peers with more than one
typedef struct additional_origin_durations {

  /// how long origin0 was visible to peer
  uint32_t duration0;

  /// total number of origins (including #0)
  int origin_cnt;

  /// additional origins (flexible array member)
  origin_duration_t origins[];

} additional_origin_durations_t;

/* our 'instance' */
typedef struct bvc_pfx2as_state {

  /** output directory */
  char *outdir;

  /** prefix origins output file name */
  char outfile_name[BVCU_PATH_MAX];

  /** prefix origins output file */
  iow_t *outfile;

  /** only output peer counts */
  int peer_count_only;

  /* ----- working state ----- */
  bgpview_t *view;

  uint32_t prev_view_time;

} bvc_pfx2as_state_t;

/* ==================== CONSUMER INTERNAL FUNCTIONS ==================== */

static int open_outfiles(bvc_t *consumer, uint32_t vtime)
{
  if (!(STATE->outfile = bvcu_open_outfile(STATE->outfile_name,
        "%s/" NAME ".%" PRIu32 ".gz", STATE->outdir, vtime)))
    return -1;

  return 0;
}

static int close_outfiles(bvc_t *consumer, uint32_t vtime)
{
  wandio_wdestroy(STATE->outfile);
  STATE->outfile = NULL;
  bvcu_create_donefile(STATE->outfile_name);

  return 0;
}

// In the vast majority of cases, a pfx-peer has only one origin.  In that
// case, we can use a compact storage scheme that packs the origin and
// duration directly into the pfx-peer without allocating a user data
// structure:
// - origin is already stored in the pfx-peer's as path.
// - we overload the user pointer with a flag in bit0 to indicate we're
//   overloading it, and store the duration in the remaining 31 (or more) bits.
// If there are multiple origins, only then do we allocate a user data
// structure to hold duration0 and origin/duration pairs 1..N-1.
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

#define pp_get_duration(iter, i)                                               \
  (((i) > 0) ? pp_get_aod(iter)->origins[(i)-1].duration :                     \
    pp_is_compact(iter) ?                                                      \
    ((uintptr_t)bgpview_iter_pfx_peer_get_user(iter) >> 1) :                   \
    pp_get_aod(iter)->duration0)

#define pp_get_origin_seg(view, iter, i)                                       \
  (((i) > 0) ?                                                                 \
    bgpstream_as_path_store_path_get_origin_seg(                               \
      bgpstream_as_path_store_get_store_path(                                  \
        bgpview_get_as_path_store(view),                                       \
        pp_get_aod(iter)->origins[(i)-1].path_id)) :                           \
    bgpview_iter_pfx_peer_get_origin_seg(iter))

#define pp_set_compact_duration(iter, d) \
  bgpview_iter_pfx_peer_set_user(iter, (void*)(((d) << 1) | 0x1))

#define pp_set_duration(iter, i, d) \
  do {                                                                         \
    if ((i) > 0)                                                               \
      pp_get_aod(iter)->origins[(i)-1].duration = (d);                         \
    else if (pp_is_compact(iter))                                              \
      pp_set_compact_duration(iter, (d));                                      \
    else                                                                       \
      pp_get_aod(iter)->duration0 = (d);                                       \
  } while (0)

#define path_id_equal(a, b) (memcmp(&a, &b, sizeof(a)) == 0) /* XXX ??? */

static void pp_destroy(void *pp)
{
  if (!((uintptr_t)pp & 0x1))
    free(pp);
}

static int process_prefixes(bvc_t *consumer, bgpview_t *view)
{
  int32_t stat_pfxpeer_cnt = 0;
  int32_t stat_tot_origin_cnt = 0;
  int32_t stat_max_origin_cnt = 0;
  int32_t stat_mopp_cnt = 0; // pfx-peers with multiple origins
  int32_t stat_malloc_cnt = 0;
  int32_t stat_realloc_cnt = 0;

  if (!STATE->view) {
    STATE->view = bgpview_create_shared(bgpview_get_peersigns(view),
        bgpview_get_as_path_store(view), NULL, NULL, NULL, pp_destroy);
    if (!STATE->view)
      goto err;
    STATE->prev_view_time = bgpview_get_time(view); // XXX ???
  }
  uintptr_t interval = bgpview_get_time(view) - STATE->prev_view_time;
  bgpview_iter_t *vit = bgpview_iter_create(view);
  bgpview_iter_t *myit = bgpview_iter_create(STATE->view);

  // for each prefix
  for (bgpview_iter_first_pfx(vit, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(vit);
      bgpview_iter_next_pfx(vit)) {

    // for each peer in pfx
    for (bgpview_iter_pfx_first_peer(vit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_pfx_has_more_peer(vit);
        bgpview_iter_pfx_next_peer(vit)) {

      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(vit);
      if (!bgpview_iter_seek_peer(myit, peer_id, BGPVIEW_FIELD_ACTIVE)) {
        // TODO implement bgpview_iter_add_peer_by_id() in bgpview; use it here
        bgpstream_peer_sig_t *ps = bgpview_iter_peer_get_sig(vit);
        bgpstream_peer_id_t new_peer_id = bgpview_iter_add_peer(myit,
            ps->collector_str, &ps->peer_ip_addr, ps->peer_asnumber);
        assert(new_peer_id == peer_id);
        bgpview_iter_activate_peer(myit);
      }

      bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(vit);
      bgpstream_as_path_store_path_id_t path_id =
          bgpview_iter_pfx_peer_get_as_path_store_path_id(vit);
      if (!bgpview_iter_seek_pfx_peer(myit, pfx, peer_id, BGPVIEW_FIELD_ACTIVE,
          BGPVIEW_FIELD_ACTIVE)) {
        bgpview_iter_add_pfx_peer_by_id(myit, pfx, peer_id, path_id);
      }

      if (bgpview_iter_pfx_peer_get_state(myit) == BGPVIEW_FIELD_INACTIVE) {
        // new pfx-peer
        bgpview_iter_pfx_activate_peer(myit);
        pp_set_compact_duration(myit, interval);
        continue;
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
        uintptr_t duration = pp_get_duration(myit, found_i);
        duration += interval;
        assert(duration <= 0x7FFFFFFF);
        pp_set_duration(myit, found_i, duration);
      } else {
        additional_origin_durations_t *aod;
        int new_origin_cnt = pp_origin_cnt(myit) + 1; // >= 2
        if (!(aod = malloc(aod_size(new_origin_cnt))))
          goto err;
        if (new_origin_cnt == 2) {
          // replace compact storage with a new aod
          aod->duration0 = pp_get_duration(myit, 0);
          stat_malloc_cnt++;
        } else {
          // replace existing aod with a larger aod
          memcpy(aod, pp_get_aod(myit), aod_size(new_origin_cnt-1));
          stat_realloc_cnt++;
        }
        assert(((uintptr_t)aod & 0x1) == 0); // we overload bit0 as a flag
        aod->origin_cnt = new_origin_cnt;
        aod->origins[aod->origin_cnt-2].path_id = path_id;
        aod->origins[aod->origin_cnt-2].duration = interval;
        bgpview_iter_pfx_peer_set_user(myit, aod);
      }
    }
  }

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

      stat_pfxpeer_cnt++;
      int origin_cnt = pp_origin_cnt(myit);
      stat_tot_origin_cnt += origin_cnt;
      if (origin_cnt > 1)
        stat_mopp_cnt++;
      if (origin_cnt > stat_max_origin_cnt)
        stat_max_origin_cnt = origin_cnt;
    }
  }
  printf("# pp=%d; orig: tot=%d, max=%d; orig/pp=%f; mopp=%d; malloc=%d,%d\n",
      stat_pfxpeer_cnt,
      stat_tot_origin_cnt, stat_max_origin_cnt,
      (double)stat_tot_origin_cnt/stat_pfxpeer_cnt,
      stat_mopp_cnt,
      stat_malloc_cnt, stat_realloc_cnt);
#endif

  bgpview_iter_destroy(vit);
  bgpview_iter_destroy(myit);
  return 0;
err:
  return -1;
}

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr, "consumer usage: %s\n"
                  "       -o <path>    output directory\n"
                  "       -c           output peer counts, not full list\n",
          consumer->name);
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
  while ((opt = getopt(argc, argv, ":o:c?")) >= 0) {
    switch (opt) {
    case 'c':
      state->peer_count_only = 1;
      break;
    case 'o':
      state->outdir = strdup(optarg);
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

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  fprintf(stderr, "INFO: output directory: %s\n", state->outdir);

  return 0;

err:
  bvc_pfx2as_destroy(consumer);
  return -1;
}

void bvc_pfx2as_destroy(bvc_t *consumer)
{
  bvc_pfx2as_state_t *state = STATE;

  if (state == NULL) {
    return;
  }

  if (STATE->view)
    bgpview_destroy(STATE->view);

  free(state->outdir);

  free(state);
  BVC_SET_STATE(consumer, NULL);
}

int bvc_pfx2as_process_view(bvc_t *consumer, bgpview_t *view)
{
#if 0
  uint32_t vtime = bgpview_get_time(view);

  /* prepare output files for writing */
  if (open_outfiles(consumer, vtime) != 0) {
    return -1;
  }
#endif

  /* spin through the view and output prefix origin info */
  if (process_prefixes(consumer, view) != 0) {
    return -1;
  }

#if 0
  /* close the output files and create .done file */
  if (close_outfiles(consumer, vtime) != 0) {
    return -1;
  }
#endif

  return 0;
}
