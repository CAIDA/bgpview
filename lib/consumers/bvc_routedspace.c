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

#include "bvc_routedspace.h"
#include "bgpview_consumer_interface.h"
#include "utils.h"
#include "wandio_utils.h"
#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

/** Name of the consumer */
#define NAME "routed-space"

/** Name of the consumer in metrics */
#define CONSUMER_METRIC_PREFIX "routed-space"

#define METRIC_PREFIX_WIN_FORMAT                                               \
  "%s." CONSUMER_METRIC_PREFIX ".%" PRIu32 "s-window.%s"

#define META_METRIC_PREFIX_FORMAT "%s.meta.bgpview.consumer." NAME ".%s"

#define BUFFER_LEN 1024

/** Default size of window: 1 day (in seconds) */
#define WINDOW_SIZE (24 * 3600)

/** Default compression level of output file */
#define DEFAULT_COMPRESS_LEVEL 6

/* macro to access the current consumer state */
#define STATE (BVC_GET_STATE(consumer, routedspace))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* IPv4 default route */
#define IPV4_DEFAULT_ROUTE "0.0.0.0/0"

/* IPv6 default route */
#define IPV6_DEFAULT_ROUTE "0::/0"

/* our 'class' */
static bvc_t bvc_routedspace = {BVC_ID_ROUTEDSPACE, NAME,
                                BVC_GENERATE_PTRS(routedspace)};

/** Data structure associated with each prefix
 *  in the patricia tree (attached to the user
 *  pointer */
typedef struct perpfx_info {

  /** last ts the prefix was observed */
  uint32_t last_observed;

} perpfx_info_t;

/* our 'instance' */
typedef struct bvc_routedspace_state {

  /** ts when the view arrived */
  uint32_t arrival_delay;
  /** ts when the view processing finished */
  uint32_t processed_delay;
  /** processing time */
  uint32_t processing_time;

  /** Patricia Tree instance that holds the
   *  visible prefixes */
  bgpstream_patricia_tree_t *patricia;

  /** Patricia Tree result structure
   *  (re-usable memory) */
  bgpstream_patricia_tree_result_set_t *results;

  /** To be filtered-out space PrefixSet structure */
  bgpstream_pfx_set_t *filter;

  /* Currently routed prefixes */
  uint32_t routed_v4pfx_count;
  uint32_t routed_v6pfx_count;

  /* New routed prefixes */
  uint32_t new_routed_v4pfx_count;
  uint32_t new_routed_v6pfx_count;

  /* Already routed prefixes */
  uint32_t old_routed_v4pfx_count;
  uint32_t old_routed_v6pfx_count;

  /** Window size */
  uint32_t window_size;

  /** first timestamp processed by view consumer */
  uint32_t first_ts;

  /** current timestamp */
  uint32_t ts;

  /** output folder */
  char output_folder[PATH_MAX];

  /** Timeseries Key Package */
  timeseries_kp_t *kp;

  /** Metrics indices */
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;
  int routed_ipv4_prefixes_idx;
  int routed_ipv6_prefixes_idx;
  int new_routed_ipv4_prefixes_idx;
  int new_routed_ipv6_prefixes_idx;
  int old_routed_ipv4_prefixes_idx;
  int old_routed_ipv6_prefixes_idx;
  int window_size_idx;

} bvc_routedspace_state_t;

/* ================ per prefix info management ================ */

/** Create a per pfx info structure */
static perpfx_info_t *perpfx_info_create(uint32_t ts)
{
  perpfx_info_t *ppi;
  if ((ppi = (perpfx_info_t *)malloc_zero(sizeof(perpfx_info_t))) == NULL) {
    return NULL;
  }
  ppi->last_observed = ts;
  return ppi;
}

/** Set the timestamp in the perpfx info structure */
static void perpfx_info_set_ts(perpfx_info_t *ppi, uint32_t ts)
{
  assert(ppi);
  ppi->last_observed = ts;
}

/** Destroy the perpfx info structure */
static void perpfx_info_destroy(void *ppi)
{
  if (ppi != NULL) {
    free((perpfx_info_t *)ppi);
  }
}

/* ================ command line parsing management ================ */

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(
    stderr,
    "consumer usage: %s\n"
    "       -w <window-size>      window size in seconds (default %d)\n"
    "       -o <path>             output folder (default: current folder)\n",
    consumer->name, WINDOW_SIZE);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;

  assert(argc > 0 && argv != NULL);

  bvc_routedspace_state_t *state = STATE;

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, ":w:i:o:?")) >= 0) {
    switch (opt) {
    case 'w':
      state->window_size = strtoul(optarg, NULL, 10);
      break;
    case 'o':
      strncpy(state->output_folder, optarg, PATH_MAX - 1);
      if (state->output_folder[strlen(state->output_folder) - 1] != '/') {
        state->output_folder[strlen(state->output_folder)] = '/';
      }
      state->output_folder[PATH_MAX - 1] = '\0';
      break;
    case '?':
    case ':':
    default:
      usage(consumer);
      return -1;
    }
  }

  return 0;
}

/* ================ Metrics/output functions ================ */

/** Create timeseries metrics */

static int create_ts_metrics(bvc_t *consumer)
{

  char buffer[BUFFER_LEN];
  bvc_routedspace_state_t *state = STATE;

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "arrival_delay");
  if ((state->arrival_delay_idx = timeseries_kp_add_key(state->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "processed_delay");
  if ((state->processed_delay_idx = timeseries_kp_add_key(state->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "processing_time");
  if ((state->processing_time_idx = timeseries_kp_add_key(state->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_WIN_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ipv4.routed_pfxs_count");
  if ((state->routed_ipv4_prefixes_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_WIN_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ipv6.routed_pfxs_count");
  if ((state->routed_ipv6_prefixes_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_WIN_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ipv4.new_routed_pfxs_count");
  if ((state->new_routed_ipv4_prefixes_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }
  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_WIN_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ipv6.new_routed_pfxs_count");
  if ((state->new_routed_ipv6_prefixes_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_WIN_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ipv4.old_routed_pfxs_count");
  if ((state->old_routed_ipv4_prefixes_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_WIN_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ipv6.old_routed_pfxs_count");
  if ((state->old_routed_ipv6_prefixes_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_WIN_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "window_size");
  if ((state->window_size_idx = timeseries_kp_add_key(state->kp, buffer)) ==
      -1) {
    return -1;
  }

  return 0;
}

static void output_metrics(bvc_t *consumer, uint32_t ts,
                           uint32_t current_window_size)
{
  bvc_routedspace_state_t *state = STATE;

  /* dump timeseries metrics */
  timeseries_kp_set(state->kp, state->arrival_delay_idx, state->arrival_delay);

  timeseries_kp_set(state->kp, state->processed_delay_idx,
                    state->processed_delay);

  timeseries_kp_set(state->kp, state->processing_time_idx,
                    state->processing_time);

  timeseries_kp_set(state->kp, state->routed_ipv4_prefixes_idx,
                    state->routed_v4pfx_count);

  timeseries_kp_set(state->kp, state->routed_ipv6_prefixes_idx,
                    state->routed_v6pfx_count);

  timeseries_kp_set(state->kp, state->new_routed_ipv4_prefixes_idx,
                    state->new_routed_v4pfx_count);

  timeseries_kp_set(state->kp, state->new_routed_ipv6_prefixes_idx,
                    state->new_routed_v6pfx_count);

  timeseries_kp_set(state->kp, state->old_routed_ipv4_prefixes_idx,
                    state->old_routed_v4pfx_count);

  timeseries_kp_set(state->kp, state->old_routed_ipv6_prefixes_idx,
                    state->old_routed_v6pfx_count);

  timeseries_kp_set(state->kp, state->window_size_idx, current_window_size);

  if (timeseries_kp_flush(state->kp, ts) != 0) {
    fprintf(stderr, "Warning: could not flush %s %" PRIu32 "\n", NAME, ts);
  }
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_routedspace_alloc()
{
  return &bvc_routedspace;
}

int bvc_routedspace_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_routedspace_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_routedspace_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  /* allocate dynamic memory */
  if ((state->patricia = bgpstream_patricia_tree_create(perpfx_info_destroy)) ==
      NULL) {
    fprintf(stderr, "ERROR: routedspace could not create Patricia Tree\n");
    goto err;
  }

  if ((state->results = bgpstream_patricia_tree_result_set_create()) == NULL) {
    fprintf(stderr,
            "ERROR: routedspace could not create Patricia Tree results\n");
    goto err;
  }

  if ((state->filter = bgpstream_pfx_set_create()) == NULL) {
    fprintf(stderr, "ERROR: routedspace could not create to-be-filtered-out "
                    "space Prefix Set\n");
    goto err;
  }

  bgpstream_pfx_t pfx;
  if (!(bgpstream_str2pfx(IPV4_DEFAULT_ROUTE, &pfx) != NULL &&
        bgpstream_pfx_set_insert(state->filter, &pfx) >= 0)) {
    fprintf(stderr, "Could not insert prefix in filter\n");
    goto err;
  }
  if (!(bgpstream_str2pfx(IPV6_DEFAULT_ROUTE, &pfx) != NULL &&
        bgpstream_pfx_set_insert(state->filter, &pfx) >= 0)) {
    fprintf(stderr, "Could not insert prefix in filter\n");
    goto err;
  }

  /* set defaults */
  state->first_ts = 0;
  state->window_size = WINDOW_SIZE;
  strncpy(state->output_folder, "./", PATH_MAX);

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  /* react to args HERE */

  if ((state->kp = timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) ==
      NULL) {
    fprintf(stderr, "Error: Could not create timeseries key package\n");
    goto err;
  }

  /* create metrics use options that are modified by
   * parse args, therefore it has to be put here) */
  if (create_ts_metrics(consumer) != 0) {
    goto err;
  }

  return 0;

err:
  bvc_routedspace_destroy(consumer);
  return -1;
}

void bvc_routedspace_destroy(bvc_t *consumer)
{
  bvc_routedspace_state_t *state = STATE;

  if (state == NULL) {
    return;
  }

  /* deallocate dynamic memory */
  if (state->patricia != NULL) {
    bgpstream_patricia_tree_destroy(state->patricia);
  }

  if (state->results != NULL) {
    bgpstream_patricia_tree_result_set_destroy(&state->results);
  }

  if (state->filter != NULL) {
    bgpstream_pfx_set_destroy(state->filter);
  }

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

void remove_old_prefixes(bgpstream_patricia_tree_t *pt,
                         bgpstream_patricia_node_t *node, void *data)
{
  bvc_t *consumer = (bvc_t *)data;
  bvc_routedspace_state_t *state = STATE;

  perpfx_info_t *info = (perpfx_info_t *)bgpstream_patricia_tree_get_user(node);
  if (info != NULL) {
    /* remove prefixes which have last been seen more than 1 day ago */
    if ((info->last_observed + state->window_size) < state->ts) {

      bgpstream_patricia_tree_remove_node(pt, node);
    }
  }
}

int bvc_routedspace_process_view(bvc_t *consumer, bgpview_t *view)
{
  bvc_routedspace_state_t *state = STATE;
  bgpview_iter_t *it;
  char pfx_str[INET6_ADDRSTRLEN + 3] = "";

  /* create a new iterator */
  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  /* current view timestamp */
  state->ts = bgpview_get_time(view);

  /* compute arrival delay */
  state->arrival_delay = epoch_sec() - state->ts;

  /* update first timestamp */
  if (state->first_ts == 0) {
    state->first_ts = state->ts;
  }

  /* compute the current window size */
  uint32_t current_window_size = state->window_size;
  if (state->ts - state->first_ts < state->window_size) {
    current_window_size = state->ts - state->first_ts;
  }

  /* remove stale prefixes from the patricia tree */
  bgpstream_patricia_tree_walk(state->patricia, remove_old_prefixes,
                               (void *)consumer);

  /* output newly routed prefixes into a file (one file per view) */

  /** wandio file handler */
  iow_t *wandio_fh;

  char filename[PATH_MAX];
  snprintf(filename, PATH_MAX,
           "%srouted-space.%" PRIu32 ".%" PRIu32 "s-window.gz",
           state->output_folder, state->ts, state->window_size);

  if ((wandio_fh =
         wandio_wcreate(filename, wandio_detect_compression_type(filename),
                        DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n", filename);
    return -1;
  }

  /* working variables */
  bgpstream_pfx_t *pfx;

  bgpstream_patricia_node_t *patricia_node;
  perpfx_info_t *ppi;

  int new_routed = 0;

  /* reset routed prefix counters upon reading a new view */
  state->routed_v4pfx_count = 0;
  state->routed_v6pfx_count = 0;
  state->new_routed_v4pfx_count = 0;
  state->new_routed_v6pfx_count = 0;
  state->old_routed_v4pfx_count = 0;
  state->old_routed_v6pfx_count = 0;

  /* iterate through all prefixes of the current view
   *  - both ipv4 and ipv6 prefixes are considered
   *  - active prefixes only (i.e. do not consider prefixes that have
   *    been withdrawn)
   */
  for (bgpview_iter_first_pfx(it, 0 /* all ip versions*/, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {

    /* get prefix from view */
    pfx = bgpview_iter_pfx_get_pfx(it);

    /* check if the new prefix is in the set of prefixes to filter out */
    if ((bgpstream_pfx_set_exists(state->filter, pfx))) {
      continue;
    }

    /* print the processed prefix */
    if (bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx) == NULL) {
      return -1;
    }

    /* insert the prefix in the patricia tree and check whether it overlaps
     * with something that was there before */

    if ((patricia_node =
           bgpstream_patricia_tree_insert(state->patricia, pfx)) == NULL) {
      fprintf(stderr, "ERROR: could not insert prefix in patricia tree\n");
      return -1;
    }

    /* a prefix is not new routed by default */
    new_routed = 0;
    if (state->first_ts == state->ts) {
      new_routed = 1;
    }

    // get the user pointer
    ppi = (perpfx_info_t *)bgpstream_patricia_tree_get_user(patricia_node);

    /* attach a ppi structure if it didn't exist */
    if (ppi == NULL) {
      /* if the program is here it means that this pfx did not exist */
      bgpstream_patricia_tree_set_user(state->patricia, patricia_node,
                                       perpfx_info_create(state->ts));

      /* if the prefix does not overlap with any other pfxs
       * in the tree, then it's a new routed */
      if (bgpstream_patricia_tree_get_node_overlap_info(
            state->patricia, patricia_node) == BGPSTREAM_PATRICIA_EXACT_MATCH) {
        new_routed = 1;
      }
    } else {
      /* otherwise update it with the latest ts */
      perpfx_info_set_ts(ppi, state->ts);
    }

    /* print the current prefixes on file */
    if (wandio_printf(wandio_fh, "%" PRIu32 "|%s|%d\n", state->ts, pfx_str,
                      new_routed) == -1) {
      fprintf(stderr, "ERROR: Could not write data to file\n");
      return -1;
    }

    /* update routed counters */
    if (pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV4) {
      state->routed_v4pfx_count++;
      if (new_routed)
        state->new_routed_v4pfx_count++;
      else
        state->old_routed_v4pfx_count++;
    } else {
      state->routed_v6pfx_count++;
      if (new_routed)
        state->new_routed_v6pfx_count++;
      else
        state->old_routed_v6pfx_count++;
    }
  }

  wandio_wdestroy(wandio_fh);

  /* write the .done file */
  snprintf(filename, PATH_MAX,
           "%srouted-space.%" PRIu32 ".%" PRIu32 "s-window.gz.done",
           state->output_folder, state->ts, state->window_size);
  if ((wandio_fh =
         wandio_wcreate(filename, wandio_detect_compression_type(filename),
                        DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n", filename);
    return -1;
  }
  wandio_wdestroy(wandio_fh);

  /* destroy the view iterator */
  bgpview_iter_destroy(it);

  /* compute processed delay */
  state->processed_delay = epoch_sec() - state->ts;
  state->processing_time = state->processed_delay - state->arrival_delay;

  output_metrics(consumer, state->ts, current_window_size);

  return 0;
}
