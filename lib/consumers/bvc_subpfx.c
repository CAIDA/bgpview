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

#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "bgpview_consumer_interface.h"
#include "bgpstream_utils_patricia.h"
#include "khash.h"
#include "utils.h"
#include "wandio.h"
#include "wandio_utils.h"
#include "bvc_subpfx.h"

#define NAME "subpfx"

/* macro to access the current consumer state */
#define STATE (BVC_GET_STATE(consumer, subpfx))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

#define CUR_SUBPFXS (STATE->subpfxs[STATE->current_subpfxs_idx])
#define PREV_SUBPFXS (STATE->subpfxs[(STATE->current_subpfxs_idx + 1) % 2])

#define DEFAULT_OUTPUT_DIR "./"
#define DEFAULT_COMPRESS_LEVEL 6
#define OUTPUT_FILE_FORMAT "%s/" NAME "-%s.%" PRIu32 ".events.gz"
#define BUFFER_LEN 4096

/* IPv4 default route */
#define IPV4_DEFAULT_ROUTE "0.0.0.0/0"
/* IPv6 default route */
#define IPV6_DEFAULT_ROUTE "0::/0"

// <metric-prefix>.subpfx.<mode-str>.<metric>
#define METRIC_PREFIX_FORMAT                                                   \
  "%s." NAME ".%s.%s"
// <metric-prefix>.meta.bgpview.consumer.subpfx.<mode-str>.<metric>
#define META_METRIC_PREFIX_FORMAT                                              \
  "%s.meta.bgpview.consumer." NAME ".%s.%s"

/* stores the set of ASes that announced a prefix */
typedef struct pt_user {

  // array of ASes that announced this prefix (origins)
  uint32_t *ases;

  // number of ASes in the array
  int ases_cnt;

} pt_user_t;

/* Maps sub-prefixes to super prefixes */
KHASH_INIT(pfx2pfx, bgpstream_pfx_storage_t, bgpstream_pfx_storage_t, 1,
           bgpstream_pfx_storage_hash_val, bgpstream_pfx_storage_equal_val);

enum {
  NEW = 0,
  FINISHED = 1,
};

static char *diff_type_strs[] = {
  "NEW",
  "FINISHED",
};

enum {
  INVALID_MODE = 0,
  SUBMOAS = 1,
  DEFCON = 2,
};

static char *mode_strs[] = {
  "invalid-mode",
  "submoas",
  "defcon",
};

/* our 'class' */
static bvc_t bvc_subpfx = { //
  BVC_ID_SUBPFX,
  NAME, //
  BVC_GENERATE_PTRS(subpfx) //
};

/* our 'instance' */
typedef struct bvc_subpfx_state {

  // options:
  char *outdir;
  int mode; // SUBMOAS or DEFCON

  // Patricia tree used to find sub-prefixes in the current view
  bgpstream_patricia_tree_t *pt;

  // Re-usable result set used when finding parent prefix
  bgpstream_patricia_tree_result_set_t *pt_res;

  // Flip-flop buffer for current and previous sub-prefix to super-prefix maps
  khash_t(pfx2pfx) *subpfxs[2];

  // which subpfxs map should be filled for this view
  // ((current_subpfxs_idx+1)%2) is the map for the previous view
  int current_subpfxs_idx;

  // IPv4 default route prefix
  bgpstream_pfx_storage_t v4_default_pfx;
  // IPv6 default route prefix
  bgpstream_pfx_storage_t v6_default_pfx;

  // current output file name
  char outfile_name[BUFFER_LEN];

  // current output file handle
  iow_t *outfile;

  // Timeseries Key Package
  timeseries_kp_t *kp;

  // Metric indices
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;
  int new_subpfxs_cnt_idx;
  int finished_subpfxs_cnt_idx;

} bvc_subpfx_state_t;

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
          "consumer usage: %s\n"
          "       -m <mode>            either '%s' or '%s'\n"
          "       -o <output-dir>      output directory (default: %s)\n",
          consumer->name,
          mode_strs[SUBMOAS],
          mode_strs[DEFCON],
          DEFAULT_OUTPUT_DIR);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, ":m:o:?")) >= 0) {
    switch (opt) {
    case 'm':
      if (strcmp(optarg, mode_strs[SUBMOAS]) == 0) {
        STATE->mode = SUBMOAS;
      } else if (strcmp(optarg, mode_strs[DEFCON]) == 0) {
        STATE->mode = DEFCON;
      } else {
        fprintf(stderr, "ERROR: Invalid mode type (%s)\n", optarg);
        usage(consumer);
        return -1;
      }
      break;

    case 'o':
      STATE->outdir = strdup(optarg);
      assert(STATE->outdir != NULL);
      break;

    case '?':
    case ':':
    default:
      usage(consumer);
      return -1;
    }
  }

  if (STATE->mode == INVALID_MODE) {
    fprintf(stderr, "ERROR: sub-pfx detection mode must be set using -m\n");
    usage(consumer);
    return -1;
  }

  return 0;
}

static void pt_user_destroy(void *user)
{
  if (user == NULL) {
    return;
  }
  pt_user_t *ptu = (pt_user_t *)user;
  free(ptu->ases);
  ptu->ases = NULL;
  ptu->ases_cnt = 0;
  free(user);
}

static pt_user_t *pt_user_create()
{
  pt_user_t *ptu;

  if ((ptu = malloc_zero(sizeof(pt_user_t))) == NULL) {
    return NULL;
  }

  return ptu;
}

static int pt_user_contains_asn(pt_user_t *ptu, uint32_t asn)
{
  int i;
  for (i=0; i<ptu->ases_cnt; i++) {
    if (ptu->ases[i] == asn) {
      return 1;
    }
  }
  return 0;
}

static int pt_user_add_asn(pt_user_t *ptu, uint32_t asn)
{
  // common case: its already there, and its probably the first AS in the set
  if (pt_user_contains_asn(ptu, asn) != 0) {
    return 0;
  }

  // not there... realloc, blah
  if ((ptu->ases = realloc(ptu->ases,
                           sizeof(uint32_t) * (ptu->ases_cnt+1))) == NULL) {
    return -1;
  }
  ptu->ases[ptu->ases_cnt++] = asn;
  return 0;
}

static void find_subpfxs(bgpstream_patricia_tree_t *pt,
                         bgpstream_patricia_node_t *node, void *data)
{
  int i;
  bvc_t *consumer = (bvc_t*)data;
  bgpstream_pfx_t *pfx = bgpstream_patricia_tree_get_pfx(node);

  // does this prefix have a super-prefix?
  if (bgpstream_patricia_tree_get_mincovering_prefix(pt, node,
                                                     STATE->pt_res) != 0) {
    // TODO: change the patricia tree walk func to allow me to error out!
    assert(0);
  }
  bgpstream_patricia_node_t *super_node =
    bgpstream_patricia_tree_result_set_next(STATE->pt_res);

  if (super_node == NULL) {
    // there is no way this can be a sub-prefix
    return;
  }
  bgpstream_pfx_t *super_pfx = bgpstream_patricia_tree_get_pfx(super_node);

  // so, there is an overlapping prefix, but is it of the type we're interested in?

  pt_user_t *ptu = bgpstream_patricia_tree_get_user(node);
  assert(ptu->ases_cnt > 0);
  pt_user_t *super_ptu = bgpstream_patricia_tree_get_user(super_node);
  assert(super_ptu->ases_cnt > 0);

  int is_wanted = 0;
  if (STATE->mode == SUBMOAS) {
    // in this case the (sub)prefix must have at least one origin that is
    // DIFFERENT to the super prefix origins
    // we're doing linear searches, but hopefully this isn't very expensive :/
    // TODO: consider using an AS set in the pt_user_t
    for (i = 0; i < ptu->ases_cnt; i++) {
      if (pt_user_contains_asn(super_ptu, ptu->ases[i]) == 0) {
        // pfx is originated by an AS that is not also originating the parent,
        // so this is indeed a sub-moas
        is_wanted = 1;
        break;
      }
    }
  } else {
    assert(STATE->mode == DEFCON);
    // in this case the (sub)prefix must have THE SAME origins as the super
    // prefix
    is_wanted = 1;
    if (ptu->ases_cnt != super_ptu->ases_cnt) {
      // origin set sizes differ, so they necessarily can't be the same
      is_wanted = 0;
    } else {
      // ensure all ASes that originate this prefix also originate the super
      // prefix
      for (i = 0; i < ptu->ases_cnt; i++) {
        if (pt_user_contains_asn(super_ptu, ptu->ases[i]) == 0) {
          // an AS that originates this prefix does not announce the
          // super-prefix, so we don't want this prefix
          is_wanted = 0;
          break;
        }
      }
    }
  }

  if (is_wanted == 0) {
    // its a sub-prefix, but not one that matches our mode, so give up and move
    // on
    return;
  }

  bgpstream_pfx_storage_t tmp_pfx;
  bgpstream_pfx_copy((bgpstream_pfx_t*)&tmp_pfx, pfx);
  bgpstream_pfx_storage_t tmp_super_pfx;
  bgpstream_pfx_copy((bgpstream_pfx_t*)&tmp_super_pfx, super_pfx);

  // this is a sub-prefix, add it to our table
  int ret;
  int k = kh_put(pfx2pfx, CUR_SUBPFXS, tmp_pfx, &ret);
  assert(ret > 0); // this prefix must not already be present in the map
  kh_val(CUR_SUBPFXS, k) = tmp_super_pfx;
}

static int dump_as_paths(bvc_t *consumer, bgpview_t *view, bgpview_iter_t *it,
                         bgpstream_pfx_t *pfx)
{
  bgpstream_as_path_t *path;
  char path_str[BUFFER_LEN];

  // seek the iterator to this prefix (it is guaranteed to be in the view)
  if (bgpview_iter_seek_pfx(it, pfx, BGPVIEW_FIELD_ACTIVE) == 0) {
    fprintf(stderr, "ERROR: failed to find prefix in view\n");
    return -1;
  }

  int first_path = 1;
  // spin through the peers for this prefix and dump out their AS paths
  for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE); //
       bgpview_iter_pfx_has_more_peer(it); //
       bgpview_iter_pfx_next_peer(it)) {
    if (first_path == 0) {
      wandio_printf(STATE->outfile, ":");
    }

    // write this AS path out
    // TODO: optimize this to not construct an AS path each time
    path = bgpview_iter_pfx_peer_get_as_path(it);
    assert(path != NULL);
    bgpstream_as_path_snprintf(path_str, BUFFER_LEN, path);
    bgpstream_as_path_destroy(path);

    wandio_printf(STATE->outfile, "%s", path_str);

    first_path = 0;
  }

  return 0;
}

static int dump_subpfx(bvc_t *consumer, bgpview_t *view, bgpview_iter_t *it,
                       bgpstream_pfx_storage_t *pfx,
                       bgpstream_pfx_storage_t *super_pfx,
                       int diff_type)
{
  /* output file format: */
  /* TIME|SUPER_PFX|SUB_PFX|NEW/FINISHED|SUPER_PFX_PATHS|SUB_PFX_PATHS */
  /* NB: in FINISHED events, the PATHS fields will be empty */
  /* since AS path strings can contain commas, the AS paths with be
     colon-separated, e.g.:
     AS1 AS2 {AS3,AS4}:AS1 AS2 AS5
  */

  char pfx_str[INET6_ADDRSTRLEN+3];
  bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, (bgpstream_pfx_t*)pfx);
  char super_pfx_str[INET6_ADDRSTRLEN+3];
  bgpstream_pfx_snprintf(super_pfx_str, INET6_ADDRSTRLEN + 3,
                         (bgpstream_pfx_t*)super_pfx);

  wandio_printf(STATE->outfile,
                "%"PRIu32"|%s|%s|%s|",
                bgpview_get_time(view),
                super_pfx_str,
                pfx_str,
                diff_type_strs[diff_type]);

  if (diff_type == NEW) {
    // dump the AS paths
    dump_as_paths(consumer, view, it, (bgpstream_pfx_t*)super_pfx);
    wandio_printf(STATE->outfile, "|");
    dump_as_paths(consumer, view, it, (bgpstream_pfx_t*)pfx);
    wandio_printf(STATE->outfile, "\n");
  } else {
    // just finish the record with nulls
    wandio_printf(STATE->outfile, "|\n");
  }

  return 0;
}

static uint64_t subpfxs_diff(bvc_t *consumer, bgpview_t *view,
                             bgpview_iter_t *it, khash_t(pfx2pfx) *a,
                             khash_t(pfx2pfx) *b, int diff_type)
{
  khiter_t k, j;
  uint64_t cnt = 0;
  for (k = kh_begin(a); k < kh_end(a); k++) {
    if (kh_exist(a, k) == 0) {
      continue;
    }
    bgpstream_pfx_storage_t *pfx = &kh_key(a, k);
    bgpstream_pfx_storage_t *super_pfx = &kh_val(a, k);
    // this prefix must have been a sub-prefix in the previous view,
    // and it must have had the same super prefix
    if ((j = kh_get(pfx2pfx, b, *pfx)) != kh_end(b) &&
        bgpstream_pfx_storage_equal(super_pfx, &kh_val(b, j)) != 0) {
      continue;
    }

    // this is a new/finished sub-pfx!
    if (dump_subpfx(consumer, view, it, pfx, &kh_val(a, k), diff_type) != 0) {
      return UINT64_MAX;
    }
    cnt++;
  }
  return cnt;
}

static int create_ts_metrics(bvc_t *consumer)
{
  char buffer[BUFFER_LEN];

  /* meta metrics */
  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, mode_strs[STATE->mode], "arrival_delay");
  if ((STATE->arrival_delay_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, mode_strs[STATE->mode], "processed_delay");
  if ((STATE->processed_delay_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, mode_strs[STATE->mode], "processing_time");
  if ((STATE->processing_time_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  /* new/finished counters */
  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, mode_strs[STATE->mode],
           "new_subpfxs_cnt");
  if ((STATE->new_subpfxs_cnt_idx =
       timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, mode_strs[STATE->mode],
           "finished_subpfxs_cnt");
  if ((STATE->finished_subpfxs_cnt_idx =
       timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
    return -1;
  }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_subpfx_alloc()
{
  return &bvc_subpfx;
}

int bvc_subpfx_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_subpfx_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_subpfx_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  if ((state->pt = bgpstream_patricia_tree_create(pt_user_destroy)) == NULL) {
    fprintf(stderr, "ERROR: Could not create patricia tree\n");
    goto err;
  }

  if ((STATE->pt_res = bgpstream_patricia_tree_result_set_create()) == NULL) {
    fprintf(stderr, "ERROR: Could not create patricia tree result set\n");
    goto err;
  }

  int i;
  for (i=0; i<2; i++) {
    if ((STATE->subpfxs[i] = kh_init(pfx2pfx)) == NULL) {
      fprintf(stderr, "ERROR: Could not create subpfx map\n");
      goto err;
    }
  }
  STATE->current_subpfxs_idx = 0;

  /* build blacklist prefixes */
  if (bgpstream_str2pfx(IPV4_DEFAULT_ROUTE, &STATE->v4_default_pfx) == NULL ||
      bgpstream_str2pfx(IPV6_DEFAULT_ROUTE, &STATE->v6_default_pfx) != NULL) {
    fprintf(stderr, "Could not build blacklist prefixes\n");
    goto err;
  }

  if ((STATE->kp = timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) ==
      NULL) {
    fprintf(stderr, "Error: Could not create timeseries key package\n");
    goto err;
  }

  if (create_ts_metrics(consumer) != 0) {
    goto err;
  }

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  return 0;

err:
  return -1;
}

void bvc_subpfx_destroy(bvc_t *consumer)
{
  bvc_subpfx_state_t *state = STATE;

  if (state == NULL) {
    return;
  }

  free(STATE->outdir);
  STATE->outdir = NULL;

  bgpstream_patricia_tree_destroy(state->pt);
  state->pt = NULL;
  bgpstream_patricia_tree_result_set_destroy(&state->pt_res);

  int i;
  for (i=0; i<2; i++) {
    kh_destroy(pfx2pfx, state->subpfxs[i]);
    state->subpfxs[i] = NULL;
  }

  timeseries_kp_free(&state->kp);

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_subpfx_process_view(bvc_t *consumer, bgpview_t *view)
{
  bgpview_iter_t *it = NULL;
  bgpstream_pfx_t *pfx = NULL;
  pt_user_t *ptu = NULL;
  bgpstream_as_path_seg_t *origin_seg;
  bgpstream_patricia_node_t *node;

  uint32_t start_time = epoch_sec();
  uint32_t view_time = bgpview_get_time(view);
  uint32_t arrival_delay = start_time - view_time;
  uint32_t new_cnt = 0;
  uint32_t finished_cnt = 0;

  // open the output file
  snprintf(STATE->outfile_name, BUFFER_LEN, OUTPUT_FILE_FORMAT,
           STATE->outdir, mode_strs[STATE->mode], view_time);
  if ((STATE->outfile = wandio_wcreate(STATE->outfile_name,
                                       wandio_detect_compression_type(STATE->outfile_name),
                                       DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n",
            STATE->outfile_name);
    goto err;
  }

  /* create a new iterator */
  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  /* build the patricia tree of prefixes in the current view */
  for (bgpview_iter_first_pfx(it, 0 /* all ip versions*/, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); //
       bgpview_iter_next_pfx(it)) {
    // we have to walk through the peers to see if this prefix is announced by
    // at least one FF peer and to collect origin ASNs
    pfx = bgpview_iter_pfx_get_pfx(it);

    // ignore default prefixes
    if (bgpstream_pfx_equal((bgpstream_pfx_t*)&STATE->v4_default_pfx,
                            (bgpstream_pfx_t*)pfx) != 0 ||
        bgpstream_pfx_equal((bgpstream_pfx_t*)&STATE->v6_default_pfx,
                            (bgpstream_pfx_t*)pfx) != 0) {
      continue;
    }

    int ipv_idx = bgpstream_ipv2idx(pfx->address.version);

    // check if this is a "full-feed prefix", and build the origin AS set
    int is_ff = 0;
    if ((ptu = pt_user_create()) == NULL) {
      fprintf(stderr, "ERROR: Could not create patricia user structure\n");
      goto err;
    }
    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE); //
         bgpview_iter_pfx_has_more_peer(it); //
         bgpview_iter_pfx_next_peer(it)) {
      if (bgpstream_id_set_exists(
            BVC_GET_CHAIN_STATE(consumer)->full_feed_peer_ids[ipv_idx],
            bgpview_iter_peer_get_peer_id(it)) == 0) {
        continue;
      }
      is_ff = 1;
      /* get origin asn */
      if ((origin_seg = bgpview_iter_pfx_peer_get_origin_seg(it)) == NULL) {
        return -1;
      }
      if (origin_seg->type != BGPSTREAM_AS_PATH_SEG_ASN) {
        /* skip sets and confederations */
        continue;
      }
      if (pt_user_add_asn(ptu, ((bgpstream_as_path_seg_asn_t *)origin_seg)->asn)
          != 0) {
        fprintf(stderr, "ERROR: Could not add origin AS\n");
        return -1;
      }
    }

    // now, add to the patricia tree if it is full-feed
    if (is_ff == 0) {
      // not a full-feed prefix, we wasted our time finding the origins ASes
      pt_user_destroy(ptu);
      ptu = NULL;
    } else {
      // first, insert this prefix into the tree
      if ((node = bgpstream_patricia_tree_insert(STATE->pt, pfx)) == NULL) {
        fprintf(stderr, "ERROR: Could not insert prefix in patricia tree\n");
        goto err;
      }
      // now set the user data to the origin set
      if (bgpstream_patricia_tree_set_user(STATE->pt, node, ptu) != 0) {
        fprintf(stderr, "ERROR: Could not set patricia user data\n");
        goto err;
      }
      // patricia now owns ptu
      ptu = NULL;
    }
  }

  /* iterate through the prefixes in the tree and find the sub-prefixes */
  bgpstream_patricia_tree_walk(STATE->pt, find_subpfxs, consumer);

  // now that we have a table of sub-prefixes, find out which are new
  // (i.e., which are in this view but not in the previous one)
  if ((new_cnt = subpfxs_diff(consumer, view, it, CUR_SUBPFXS, PREV_SUBPFXS, NEW))
      == UINT64_MAX) {
    fprintf(stderr, "ERROR: Failed to find NEW sub prefixes\n");
    goto err;
  }
  // and then do the complement to find finished sub-pfxs
  if ((finished_cnt = subpfxs_diff(consumer, view, it,
                                   PREV_SUBPFXS, CUR_SUBPFXS, FINISHED))
      == UINT64_MAX) {
    fprintf(stderr, "ERROR: Failed to find NEW sub prefixes\n");
    goto err;
  }

  // clear the previous map and then rotate
  kh_clear(pfx2pfx, PREV_SUBPFXS);
  STATE->current_subpfxs_idx = (STATE->current_subpfxs_idx + 1) % 2;

  /* destroy the view iterator */
  bgpview_iter_destroy(it);

  /* empty the patricia tree */
  bgpstream_patricia_tree_clear(STATE->pt);

  /* close the output file */
  wandio_wdestroy(STATE->outfile);
  STATE->outfile = NULL;

  /* generate the .done file */
  snprintf(STATE->outfile_name, BUFFER_LEN, OUTPUT_FILE_FORMAT ".done",
           STATE->outdir, mode_strs[STATE->mode], view_time);
  if ((STATE->outfile = wandio_wcreate(STATE->outfile_name,
                                       wandio_detect_compression_type(STATE->outfile_name),
                                       DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n", STATE->outfile_name);
    return -1;
  }
  wandio_wdestroy(STATE->outfile);
  STATE->outfile = NULL;

  // update and dump timeseries
  uint32_t now = epoch_sec();
  // meta series
  timeseries_kp_set(STATE->kp, STATE->arrival_delay_idx, arrival_delay);
  timeseries_kp_set(STATE->kp, STATE->processed_delay_idx, (now - view_time));
  timeseries_kp_set(STATE->kp, STATE->processing_time_idx, (now - start_time));

  // event counters
  timeseries_kp_set(STATE->kp, STATE->new_subpfxs_cnt_idx, new_cnt);
  timeseries_kp_set(STATE->kp, STATE->finished_subpfxs_cnt_idx, finished_cnt);

  if (timeseries_kp_flush(STATE->kp, view_time) != 0) {
    fprintf(stderr, "Warning: %s could not flush timeseries at %" PRIu32 "\n",
            NAME, view_time);
  }

  return 0;

 err:
  bgpview_iter_destroy(it);
  wandio_wdestroy(STATE->outfile);
  return -1;
}
