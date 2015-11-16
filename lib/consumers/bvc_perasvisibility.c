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
#include <stdio.h>
#include <stdlib.h>

#include <czmq.h>

#include "utils.h"
#include "khash.h"

#include "bgpview_consumer_interface.h"

#include "bvc_perasvisibility.h"
#include "bgpstream_utils_patricia.h"


#define NAME                         "per-as-visibility"
#define CONSUMER_METRIC_PREFIX       "prefix-visibility.asn"

#define METRIC_PREFIX_TH_FORMAT    "%s."CONSUMER_METRIC_PREFIX".%"PRIu32".v%d.visibility_threshold.%s.%s"
#define META_METRIC_PREFIX_FORMAT  "%s.meta.bgpview.consumer."NAME".%s"

#define BUFFER_LEN 1024
#define MAX_NUM_PEERS 1024


#define STATE					\
  (BVC_GET_STATE(consumer, perasvisibility))

#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))


/* our 'class' */
static bvc_t bvc_perasvisibility = {
  BVC_ID_PERASVISIBILITY,
  NAME,
  BVC_GENERATE_PTRS(perasvisibility)
};

/* Visibility Thresholds indexes */
typedef enum {
  VIS_1_FF_ASN = 0,
  VIS_25_PERCENT = 1,
  VIS_50_PERCENT = 2,
  VIS_75_PERCENT = 3,
  VIS_100_PERCENT = 4,
} vis_thresholds_t;

#define VIS_THRESHOLDS_CNT 5


typedef struct pervis_info {

  /* All the prefixes that belong to
   * this threshold, i.e. they have been
   * observed by at least TH full feed ASn,
   * but they do not belong to the higher threshold
   */
  bgpstream_patricia_tree_t *pt;

  int pfx_cnt_idx[BGPSTREAM_MAX_IP_VERSION_IDX];
  int subnet_cnt_idx[BGPSTREAM_MAX_IP_VERSION_IDX];

  /* TODO: other infos here ? */

} pervis_info_t;


/** peras_info_t
 *  network visibility information related to a single
 *  origin AS number
 */
typedef struct peras_info {

  pervis_info_t info [VIS_THRESHOLDS_CNT];

} peras_info_t;


/** Hash table: <origin ASn,pfxs info> */
KHASH_INIT(as_pfxs_info,
	   uint32_t, peras_info_t, 1,
	   kh_int_hash_func, kh_int_hash_equal);


/* our 'instance' */
typedef struct bvc_perasvisibility_state {

  /** Contains information about the prefixes
   *  announced by a specific ASn */
  khash_t(as_pfxs_info) *as_pfxs_vis;

  /** Re-usable variables that are used in the flip table
   *  function: we make them part of the state to avoid
   *  allocating new memory each time we process a new
   *  view */
  bgpstream_id_set_t *ff_asns;
  uint32_t origin_asns[MAX_NUM_PEERS];
  uint16_t valid_origins;

  /* Thresholds values */
  double thresholds[VIS_THRESHOLDS_CNT];

  /** Timeseries Key Package */
  timeseries_kp_t *kp;

  /* META metric values */
  uint32_t arrival_delay;
  uint32_t processed_delay;
  uint32_t processing_time;

  /* META metric values */
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;

} bvc_perasvisibility_state_t;


/* ==================== PARSE ARGS FUNCTIONS ==================== */

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s\n",
	  consumer->name);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while((opt = getopt(argc, argv, ":?")) >= 0)
    {
      switch(opt)
	{
	case '?':
	case ':':
	default:
	  usage(consumer);
	  return -1;
	}
    }

  return 0;
}

/* ==================== THRESHOLDS FUNCTIONS ==================== */

static void init_thresholds(bvc_perasvisibility_state_t *state)
{
  state->thresholds[VIS_1_FF_ASN] = 0;
  state->thresholds[VIS_25_PERCENT] = 0.25;
  state->thresholds[VIS_50_PERCENT] = 0.50;
  state->thresholds[VIS_75_PERCENT] = 0.75;
  state->thresholds[VIS_100_PERCENT] =1;
}

static char *threshold_string(int i)
{
  switch(i)
    {
    case VIS_1_FF_ASN:
      return "min_1_ff_peer_asn";
    case VIS_25_PERCENT:
      return "min_25%_ff_peer_asns";
    case VIS_50_PERCENT:
      return "min_50%_ff_peer_asns";
    case VIS_75_PERCENT:
      return "min_75%_ff_peer_asns";
    case VIS_100_PERCENT:
      return "min_100%_ff_peer_asns";
    default:
      return "ERROR";
    }
  return "ERROR";
}

/* ==================== ORIGINS FUNCTIONS ==================== */

/* reset the array of origin ASn*/
static void reset_origins(bvc_perasvisibility_state_t *state)
{
  state->valid_origins = 0;
}

/* check if an origin ASn is already part of the array and
 * if not it adds it */
static void add_origin(bvc_perasvisibility_state_t *state, bgpstream_as_path_seg_t *origin_seg)
{
  uint32_t origin_asn;
  if(origin_seg == NULL || origin_seg->type != BGPSTREAM_AS_PATH_SEG_ASN)
    {
      return;
    }
  else
    {
      origin_asn = ((bgpstream_as_path_seg_asn_t*)origin_seg)->asn;
    }
  
  int i;
  for(i = 0; i < state->valid_origins; i++)
    {
      if(state->origin_asns[i] == origin_asn)
        {
          /* origin_asn is already there */
          return;
        }
    }
  /* if we did not find the origin, we add it*/
  state->origin_asns[state->valid_origins] = origin_asn;
  state->valid_origins++;
}


/* ==================== PER-AS-INFO FUNCTIONS ==================== */

static int peras_info_init(bvc_t *consumer, peras_info_t *per_as, uint32_t asn)
{
  bvc_perasvisibility_state_t *state = STATE;
  char buffer[BUFFER_LEN];
  int i, v;
  for(i = 0; i < VIS_THRESHOLDS_CNT; i++)
    {
      /* create all Patricia Trees */
      if((per_as->info[i].pt = bgpstream_patricia_tree_create(NULL)) == NULL)
        {
          return -1;
        }
      /* create indexes for timeseries */
      for(v=0; v < BGPSTREAM_MAX_IP_VERSION_IDX; v++)
        {
          /* visible_prefixes_cnt */
          snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_TH_FORMAT,
                   CHAIN_STATE->metric_prefix, asn, bgpstream_idx2number(v), threshold_string(i),
                   "visible_prefixes_cnt");
          if((per_as->info[i].pfx_cnt_idx[v] =
              timeseries_kp_add_key(state->kp, buffer)) == -1)
            {
              return -1;
            }
          /* visible_ips_cnt */
          snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_TH_FORMAT,
                   CHAIN_STATE->metric_prefix, asn, bgpstream_idx2number(v), threshold_string(i),
                   v == bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4) ? "visible_slash24_cnt" : "visible_slash64_cnt");
          if((per_as->info[i].subnet_cnt_idx[v] =
              timeseries_kp_add_key(state->kp, buffer)) == -1)
            {
              return -1;
            }
        }
    }
  return 0;
}

static int peras_info_update(bvc_t *consumer, peras_info_t *per_as, bgpstream_pfx_t *pfx)
{
  bvc_perasvisibility_state_t *state = STATE;

  /* number of full feed ASns for the current IP version*/
  int totalfullfeed = CHAIN_STATE->full_feed_peer_asns_cnt[bgpstream_ipv2idx(pfx->address.version)];
  assert(totalfullfeed > 0);

  /* number of full feed ASns observing the current prefix*/
  int pfx_ff_cnt = bgpstream_id_set_size(state->ff_asns);
  assert(pfx_ff_cnt > 0);

  float ratio = (float) pfx_ff_cnt / (float) totalfullfeed;

  /* we navigate the thresholds array starting from the
   * higher one, and populate each threshold information
   * only if the prefix belongs there */
  int i;
  for(i = VIS_THRESHOLDS_CNT-1; i >= 0; i--)
    {
      if(ratio >= state->thresholds[i])
        {
          /* add prefix to the Patricia Tree */
          if(bgpstream_patricia_tree_insert(per_as->info[i].pt, pfx) == NULL)
            {
              return -1;
            }
          break;
        }
    }
  return 0;
}


static void peras_info_destroy(peras_info_t per_as)
{
  int i;
  for(i = 0; i < VIS_THRESHOLDS_CNT; i++)
    {
      bgpstream_patricia_tree_destroy(per_as.info[i].pt);
    }
}


/* ==================== UTILITY FUNCTIONS ==================== */

static int update_pfx_asns_information(bvc_t *consumer, bgpstream_pfx_t *pfx)
{
  bvc_perasvisibility_state_t *state = STATE;

  /* Requirements:
   * state->origin_asns contains <state->valid_origins> (> 0) that currently
   * announce the prefix */
  assert(state->valid_origins > 0);

  khiter_t k = 0;
  int khret;
  peras_info_t *all_infos;
  int i;
  /* check if the origin ASn is already in the as_pfxs_vis hash map */
  for(i = 0; i < state->valid_origins; i++)
    {
      /* if it is the first time we observe this ASn, then we
       * have to initialized its information */
      if((k = kh_get(as_pfxs_info, state->as_pfxs_vis, state->origin_asns[i])) == kh_end(state->as_pfxs_vis))
        {
          k = kh_put(as_pfxs_info, state->as_pfxs_vis, state->origin_asns[i], &khret);
          /* init peras_info_t */
          all_infos = &kh_val(state->as_pfxs_vis, k);
          if(peras_info_init(consumer, all_infos, state->origin_asns[i]) != 0)
            {
              return -1;
            }
        }
      else
        {
          all_infos = &kh_val(state->as_pfxs_vis, k);
        }
      /* once we have a pointer to the AS information, we update it */
      if(peras_info_update(consumer, all_infos, pfx) != 0)
        {
          return -1;
        }
    }
  return 0;
}

static int compute_origin_pfx_visibility(bvc_t *consumer, bgpview_iter_t *it)
{

  bvc_perasvisibility_state_t *state = STATE;

  bgpstream_pfx_t *pfx;
  bgpstream_peer_sig_t *sg;

  /* for each prefix in the view */
  for(bgpview_iter_first_pfx(it, 0 /* all ip versions*/, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(it);
      bgpview_iter_next_pfx(it))
    {

      pfx = bgpview_iter_pfx_get_pfx(it);

      /* only consider ipv4 prefixes whose mask is greater than a /6 */
      if(pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV4 &&
         pfx->mask_len < BVC_GET_CHAIN_STATE(consumer)->pfx_vis_mask_len_threshold)
        {
          continue;
        }

      /* reset information for the current prefix */
      bgpstream_id_set_clear(state->ff_asns);
      reset_origins(state);

      /* iterate over the peers for the current pfx and get the number of unique
       * full feed AS numbers observing this prefix as well as the unique set of
       * origin ASes */
      for(bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
          bgpview_iter_pfx_has_more_peer(it);
          bgpview_iter_pfx_next_peer(it))
        {

          /* only consider peers that are full-feed (checking if peer id is a full feed
           * for the current pfx IP version ) */
          if(bgpstream_id_set_exists(BVC_GET_CHAIN_STATE(consumer)->full_feed_peer_ids[bgpstream_ipv2idx(pfx->address.version)],
                                     bgpview_iter_peer_get_peer_id(it)) == 0)
            {
              continue;
            }

          /* get peer signature */
          sg = bgpview_iter_peer_get_sig(it);
          assert(sg);

          /* Add peer AS number to the set of full feed peers observing the prefix */
          bgpstream_id_set_insert(state->ff_asns, sg->peer_asnumber);

          /* Add origin AS number to the array of origin ASns for the prefix */
          add_origin(state, bgpview_iter_pfx_peer_get_origin_seg(it));
        }

      if(state->valid_origins > 0)
        {
          if(update_pfx_asns_information(consumer, pfx) != 0)
            {
              return -1;
            }
        }
    }

  return 0;
}


static int output_metrics_and_reset(bvc_t *consumer)
{
  bvc_perasvisibility_state_t *state = STATE;
  int i;
  khiter_t k;
  peras_info_t *per_as;

  /* for each AS number */
  for(k = kh_begin(state->as_pfxs_vis); k != kh_end(state->as_pfxs_vis); ++k)
    {
      if (kh_exist(state->as_pfxs_vis, k))
        {
          /* collect the information for all thresholds */
          per_as = &kh_val(state->as_pfxs_vis, k);
          for(i = VIS_THRESHOLDS_CNT-1; i >=0 ; i--)
            {
              /* we merge all the trees with the previous one,
               * except the first */
              if(i != (VIS_THRESHOLDS_CNT-1))
                {
                  bgpstream_patricia_tree_merge(per_as->info[i].pt, per_as->info[i+1].pt);
                }

              /* now that the tree represents all the prefixes
               * that match the threshold, we extract the information
               * that we want to output */

              /* IPv4*/
              timeseries_kp_set(state->kp,
                                per_as->info[i].pfx_cnt_idx[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)],
                                bgpstream_patricia_prefix_count(per_as->info[i].pt, BGPSTREAM_ADDR_VERSION_IPV4));
              timeseries_kp_set(state->kp,
                                per_as->info[i].subnet_cnt_idx[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)],
                                bgpstream_patricia_tree_count_24subnets(per_as->info[i].pt));

              /* IPv6*/
              timeseries_kp_set(state->kp,
                                per_as->info[i].pfx_cnt_idx[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV6)],
                                bgpstream_patricia_prefix_count(per_as->info[i].pt, BGPSTREAM_ADDR_VERSION_IPV6));
              timeseries_kp_set(state->kp,
                                per_as->info[i].subnet_cnt_idx[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV6)],
                                bgpstream_patricia_tree_count_64subnets(per_as->info[i].pt));
            }

          /* metrics are set, now we have to clean the patricia trees */
          for(i = VIS_THRESHOLDS_CNT-1; i >=0 ; i--)
            {
              bgpstream_patricia_tree_clear(per_as->info[i].pt);
            }
        }
    }

  return 0;
}


/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_perasvisibility_alloc()
{
  return &bvc_perasvisibility;
}

int bvc_perasvisibility_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_perasvisibility_state_t *state = NULL;

  if((state = malloc_zero(sizeof(bvc_perasvisibility_state_t))) == NULL)
    {
      return -1;
    }
  BVC_SET_STATE(consumer, state);

  /* init and set defaults */

  if((state->as_pfxs_vis = kh_init(as_pfxs_info)) == NULL)
    {
      fprintf(stderr, "Error: Unable to create AS visibility map\n");
      goto err;
    }

  if((state->ff_asns = bgpstream_id_set_create()) == NULL)
    {
      fprintf(stderr, "Error: Could not create full feed origin ASns set\n");
      goto err;
    }

  init_thresholds(state);

  /* init key package and meta metrics */
  if((state->kp =
      timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) == NULL)
    {
      fprintf(stderr, "Error: Could not create timeseries key package\n");
      goto err;
    }

  char buffer[BUFFER_LEN];
  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT, CHAIN_STATE->metric_prefix, "arrival_delay");
  if((state->arrival_delay_idx = timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }
  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT, CHAIN_STATE->metric_prefix, "processed_delay");
  if((state->processed_delay_idx = timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }
  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT, CHAIN_STATE->metric_prefix, "processing_time");
  if((state->processing_time_idx = timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }

  /* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      goto err;
    }

  /* react to args here */

  return 0;

 err:
  bvc_perasvisibility_destroy(consumer);
  return -1;
}

void bvc_perasvisibility_destroy(bvc_t *consumer)
{
  bvc_perasvisibility_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  /* destroy things here */
  if(state->as_pfxs_vis != NULL)
    {
      kh_free_vals(as_pfxs_info, state->as_pfxs_vis, peras_info_destroy);
      kh_destroy(as_pfxs_info, state->as_pfxs_vis);
      state->as_pfxs_vis = NULL;
    }

  if(state->ff_asns != NULL)
    {
      bgpstream_id_set_destroy(state->ff_asns);
      state->ff_asns = NULL;
    }

  timeseries_kp_free(&state->kp);

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_perasvisibility_process_view(bvc_t *consumer, uint8_t interests,
				     bgpview_t *view)
{
  bvc_perasvisibility_state_t *state = STATE;

  if(BVC_GET_CHAIN_STATE(consumer)->usable_table_flag[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)] == 0 &&
     BVC_GET_CHAIN_STATE(consumer)->usable_table_flag[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV6)] == 0 )
    {
      fprintf(stderr,
              "ERROR: Per-AS Visibility can't use this table %"PRIu32"\n", bgpview_get_time(view));
      return 0;
    }

  /* compute arrival delay */
  state->arrival_delay = zclock_time()/1000 - bgpview_get_time(view);

  /* get full feed peer ids from Visibility */
  if(BVC_GET_CHAIN_STATE(consumer)->visibility_computed == 0)
    {
      fprintf(stderr,
              "ERROR: The Per-AS Visibility requires the Visibility consumer "
              "to be run first\n");
      return -1;
    }

  /* create a new iterator */
  bgpview_iter_t *it;
  if((it = bgpview_iter_create(view)) == NULL)
    {
      return -1;
    }

  /* compute the pfx visibility of each origin AS */
  if(compute_origin_pfx_visibility(consumer, it) != 0)
    {
      return -1;
    }

  /* destroy the view iterator */
  bgpview_iter_destroy(it);

  /* compute the metrics and reset the state variables */
  if(output_metrics_and_reset(consumer) != 0)
    {
      return -1;
    }

  /* compute delays */
  state->processed_delay = zclock_time()/1000 - bgpview_get_time(view);
  state->processing_time = state->processed_delay - state->arrival_delay;

  /* set delays metrics */
  timeseries_kp_set(state->kp, state->arrival_delay_idx, state->arrival_delay);
  timeseries_kp_set(state->kp, state->processed_delay_idx, state->processed_delay);
  timeseries_kp_set(state->kp, state->processing_time_idx, state->processing_time);
  
  /* now flush the gen kp */
  if(timeseries_kp_flush(state->kp, bgpview_get_time(view)) != 0)
    {
      fprintf(stderr, "Warning: could not flush %s %"PRIu32"\n",
              NAME, bgpview_get_time(view));
    }

  return 0;
}

