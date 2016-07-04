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

#include "bvc_visibility.h"
#include "bgpview_consumer_interface.h"
#include "bgpstream_utils_id_set.h"
#include "bgpstream_utils_pfx_set.h"
#include "khash.h"
#include "utils.h"
#include <assert.h>
#include <libipmeta.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define NAME "visibility"
#define CONSUMER_METRIC_PREFIX "prefix-visibility.overall"

#define BUFFER_LEN 1024
#define METRIC_PREFIX_FORMAT "%s." CONSUMER_METRIC_PREFIX ".ipv%d_view.%s"
#define META_METRIC_PREFIX_FORMAT "%s.meta.bgpview.consumer." NAME ".%s"

#define ROUTED_PFX_MIN_PEERCNT 10
#define ROUTED_PFX_MIN_MASK_LEN 6
#define IPV4_FULLFEED_SIZE 400000
#define IPV6_FULLFEED_SIZE 10000

#define STATE (BVC_GET_STATE(consumer, visibility))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_visibility = {BVC_ID_VISIBILITY, NAME,
                               BVC_GENERATE_PTRS(visibility)};

/** key package ids related to
 *  generic metrics
 */
typedef struct gen_metrics {

  int peers_idx[BGPSTREAM_MAX_IP_VERSION_IDX];
  int ff_peers_idx[BGPSTREAM_MAX_IP_VERSION_IDX];
  int ff_asns_idx[BGPSTREAM_MAX_IP_VERSION_IDX];

  /* META metrics */
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;
} gen_metrics_t;

/* our 'instance' */
typedef struct bvc_visibility_state {

  int arrival_delay;
  int processed_delay;
  int processing_time;

  /** # pfxs in a full-feed table */
  int full_feed_size[BGPSTREAM_MAX_IP_VERSION_IDX];

  /** set of ASns providing a full-feed table */
  bgpstream_id_set_t *full_feed_asns[BGPSTREAM_MAX_IP_VERSION_IDX];

  /** Timeseries Key Package */
  timeseries_kp_t *kp;

  /** General metric indexes */
  gen_metrics_t gen_metrics;

} bvc_visibility_state_t;

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(
    stderr,
    "consumer usage: %s\n"
    "       -4 <pfx-cnt>  # pfxs in a IPv4 full-feed table (default: %d)\n"
    "       -6 <pfx-cnt>  # pfxs in a IPv6 full-feed table (default: %d)\n"
    "       -m <mask-len> minimum mask length for pfxs (default: %d)\n"
    "       -p <peer-cnt> # peers that must observe a pfx (default: %d)\n",
    consumer->name, IPV4_FULLFEED_SIZE, IPV6_FULLFEED_SIZE,
    ROUTED_PFX_MIN_MASK_LEN, ROUTED_PFX_MIN_PEERCNT);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, ":4:6:m:p:?")) >= 0) {
    switch (opt) {
    case '4':
      STATE->full_feed_size[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)] =
        atoi(optarg);
      break;

    case '6':
      STATE->full_feed_size[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)] =
        atoi(optarg);
      break;

    case 'm':
      CHAIN_STATE->pfx_vis_mask_len_threshold = atoi(optarg);
      break;

    case 'p':
      CHAIN_STATE->pfx_vis_peers_threshold = atoi(optarg);
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

static int create_gen_metrics(bvc_t *consumer)
{
  char buffer[BUFFER_LEN];
  uint8_t i = 0;

  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, bgpstream_idx2number(i), "peers_cnt");
    if ((STATE->gen_metrics.peers_idx[i] =
           timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
      return -1;
    }

    snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, bgpstream_idx2number(i),
             "ff_peers_cnt");
    if ((STATE->gen_metrics.ff_peers_idx[i] =
           timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
      return -1;
    }

    snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, bgpstream_idx2number(i),
             "ff_peers_asns_cnt");
    if ((STATE->gen_metrics.ff_asns_idx[i] =
           timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
      return -1;
    }
  }

  /* META Metrics */

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "arrival_delay");

  if ((STATE->gen_metrics.arrival_delay_idx =
         timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "processed_delay");

  if ((STATE->gen_metrics.processed_delay_idx =
         timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "processing_time");

  if ((STATE->gen_metrics.processing_time_idx =
         timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
    return -1;
  }

  return 0;
}

static void find_ff_peers(bvc_t *consumer, bgpview_iter_t *it)
{
  bgpstream_peer_id_t peerid;
  bgpstream_peer_sig_t *sg;
  int pfx_cnt;
  int i;

  for (bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(it); bgpview_iter_next_peer(it)) {
    /* grab the peer id and its signature */
    peerid = bgpview_iter_peer_get_peer_id(it);
    sg = bgpview_iter_peer_get_sig(it);

    for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
      pfx_cnt = bgpview_iter_peer_get_pfx_cnt(it, bgpstream_idx2ipv(i),
                                              BGPVIEW_FIELD_ACTIVE);
      /* does this peer have any v4 tables? */
      if (pfx_cnt > 0) {
        CHAIN_STATE->peer_ids_cnt[i]++;
      }

      /* does this peer have a full-feed table? */
      if (pfx_cnt >= STATE->full_feed_size[i]) {
        /* add to the  full_feed set */
        bgpstream_id_set_insert(CHAIN_STATE->full_feed_peer_ids[i], peerid);
        bgpstream_id_set_insert(STATE->full_feed_asns[i], sg->peer_asnumber);
      }
    }
  }

  // fprintf(stderr, "IDS: %"PRIu32"\n", CHAIN_STATE->peer_ids_cnt[0]);

  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    CHAIN_STATE->full_feed_peer_asns_cnt[i] =
      bgpstream_id_set_size(STATE->full_feed_asns[i]);
  }
}

static void dump_gen_metrics(bvc_t *consumer)
{
  int i;
  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    timeseries_kp_set(STATE->kp, STATE->gen_metrics.peers_idx[i],
                      CHAIN_STATE->peer_ids_cnt[i]);
    timeseries_kp_set(
      STATE->kp, STATE->gen_metrics.ff_peers_idx[i],
      bgpstream_id_set_size(CHAIN_STATE->full_feed_peer_ids[i]));
    timeseries_kp_set(STATE->kp, STATE->gen_metrics.ff_asns_idx[i],
                      CHAIN_STATE->full_feed_peer_asns_cnt[i]);
  }

  /* META metrics */
  timeseries_kp_set(STATE->kp, STATE->gen_metrics.arrival_delay_idx,
                    STATE->arrival_delay);

  timeseries_kp_set(STATE->kp, STATE->gen_metrics.processed_delay_idx,
                    STATE->processed_delay);

  timeseries_kp_set(STATE->kp, STATE->gen_metrics.processing_time_idx,
                    STATE->processing_time);

  STATE->arrival_delay = 0;
  STATE->processed_delay = 0;
  STATE->processing_time = 0;
}

static void reset_chain_state(bvc_t *consumer)
{
  int i;
  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    CHAIN_STATE->peer_ids_cnt[i] = 0;
    bgpstream_id_set_clear(CHAIN_STATE->full_feed_peer_ids[i]);
    CHAIN_STATE->full_feed_peer_asns_cnt[i] = 0;
    CHAIN_STATE->usable_table_flag[i] = 0;
  }
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_visibility_alloc()
{
  return &bvc_visibility;
}

int bvc_visibility_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_visibility_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_visibility_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  /* set defaults here */
  CHAIN_STATE->pfx_vis_peers_threshold = ROUTED_PFX_MIN_PEERCNT;
  CHAIN_STATE->pfx_vis_mask_len_threshold = ROUTED_PFX_MIN_MASK_LEN;

  state->full_feed_size[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)] =
    IPV4_FULLFEED_SIZE;
  state->full_feed_size[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV6)] =
    IPV6_FULLFEED_SIZE;

  int i;

  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    /* we asssume chain state data are already allocated by the consumer mgr */
    if ((state->full_feed_asns[i] = bgpstream_id_set_create()) == NULL) {
      fprintf(stderr, "Error: unable to create full-feed ASns set\n");
      goto err;
    }
  }

  if ((state->kp = timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) ==
      NULL) {
    fprintf(stderr, "Error: Could not create timeseries key package\n");
    goto err;
  }

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  if (create_gen_metrics(consumer) != 0) {
    goto err;
  }

  return 0;

err:
  bvc_visibility_destroy(consumer);
  return -1;
}

void bvc_visibility_destroy(bvc_t *consumer)
{

  bvc_visibility_state_t *state = STATE;

  if (state == NULL) {
    return;
  }

  int i;

  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    if (state->full_feed_asns[i] != NULL) {
      bgpstream_id_set_destroy(state->full_feed_asns[i]);
    }
  }

  timeseries_kp_free(&state->kp);

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_visibility_process_view(bvc_t *consumer, bgpview_t *view)
{
  bgpview_iter_t *it;
  int i;

  // compute arrival delay
  STATE->arrival_delay = epoch_sec() - bgpview_get_time(view);

  /* this MUST come before any computation  */
  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    bgpstream_id_set_clear(STATE->full_feed_asns[i]);
  }

  reset_chain_state(consumer);

  /* create a new iterator */
  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  /* find the full-feed peers */
  find_ff_peers(consumer, it);

  bgpview_iter_destroy(it);

  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    if (CHAIN_STATE->full_feed_peer_asns_cnt[i] > 0) {
      CHAIN_STATE->usable_table_flag[i] = 1;
    }
  }

  CHAIN_STATE->visibility_computed = 1;

  /* @todo decide later what are the usability rules */

  /* compute processed delay (must come prior to dump_gen_metrics) */
  STATE->processed_delay = epoch_sec() - bgpview_get_time(view);

  STATE->processing_time = STATE->processed_delay - STATE->arrival_delay;
  /* dump metrics and tables */
  dump_gen_metrics(consumer);

  /* now flush the kp */
  if (timeseries_kp_flush(STATE->kp, bgpview_get_time(view)) != 0) {
    fprintf(stderr, "Warning: could not flush %s %" PRIu32 "\n", NAME,
            bgpview_get_time(view));
  }

  return 0;
}
