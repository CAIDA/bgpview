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

#include "utils.h"
#include "libipmeta.h"
#include "khash.h"
#include "czmq.h"

#include "bgpview_consumer_interface.h"

#include "bvc_pergeovisibility.h"
#include "bgpstream_utils_patricia.h"


#define NAME                       "per-geo-visibility"
#define CONSUMER_METRIC_PREFIX     "prefix-visibility.geo.netacuity"
#define GEO_PROVIDER_NAME          "netacq-edge"

#define METRIC_PREFIX_IP_TH_FORMAT "%s."CONSUMER_METRIC_PREFIX".%s.%s.v%d.visibility_threshold.%s.%s"
#define META_METRIC_PREFIX_FORMAT  "%s.meta.bgpview.consumer."NAME".%s"

#define BUFFER_LEN                 1024
#define MAX_NUM_PEERS              1024
#define MAX_IP_VERSION_ALLOWED     1 /* replace with BGPSTREAM_MAX_IP_VERSION_IDX, once we have ipv6 */

#define STATE					\
  (BVC_GET_STATE(consumer, pergeovisibility))

#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))


/* our 'class' */
static bvc_t bvc_pergeovisibility = {
  BVC_ID_PERGEOVISIBILITY,
  NAME,
  BVC_GENERATE_PTRS(pergeovisibility)
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

  /* All the prefixes that announce a prefix
   * in this geographical region */
  bgpstream_id_set_t *asns;

  int asns_cnt_idx;

  /* TODO: other infos here ? */

} pervis_info_t;


/** pergeo_info_t
 *  network visibility information related to a single
 *  geographical location (currently country codes)
 */
typedef struct pergeo_info {

  pervis_info_t info [VIS_THRESHOLDS_CNT];

} pergeo_info_t;

/** Hash table: <geo,pfxs info> */
KHASH_INIT(geo_pfxs_info, char *, pergeo_info_t, 1,
	   kh_str_hash_func, kh_str_hash_equal);

/** It contains a k value for each country
 *  that a prefix geo-locate in, such k
 *  identify the corresponding country entry
 *  in geo_pfxs_info */
KHASH_INIT(country_k_set,uint32_t, char, 0,
           kh_int_hash_func, kh_int_hash_equal);

/* our 'instance' */
typedef struct bvc_pergeovisibility_state {

  /** Contains information about the prefixes
   *  that belong to a specific country */
  khash_t(geo_pfxs_info) *geo_pfxs_vis;

  /** netacq-edge files */
  char blocks_file[BUFFER_LEN];
  char locations_file[BUFFER_LEN];
  char countries_file[BUFFER_LEN];

  /** ipmeta structures */
  ipmeta_t *ipmeta;
  ipmeta_provider_t *provider;
  ipmeta_record_set_t *records;

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

} bvc_pergeovisibility_state_t;


/* ==================== PARSE ARGS FUNCTIONS ==================== */

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s\n"
	  "       -c <file>     country decode file (mandatory option)\n"
	  "       -b <file>     blocks file (mandatory option)\n"
	  "       -l <file>     locations file (mandatory option)\n",
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
  while((opt = getopt(argc, argv, ":b:c:l:?")) >= 0)
    {
      switch(opt)
	{
	case 'b':
	  strcpy(STATE->blocks_file, optarg);
	  break;
	case 'c':
	  strcpy(STATE->countries_file, optarg);
	  break;
	case 'l':
	  strcpy(STATE->locations_file, optarg);
	  break;
	case '?':
	case ':':
	default:
	  usage(consumer);
	  return -1;
	}
    }

  // blocks countries and locations are mandatory options
  if(STATE->blocks_file[0] == '\0' ||
     STATE->countries_file[0] ==  '\0' ||
     STATE->locations_file[0] == '\0')
    {
      usage(consumer);
      return -1;
    }

  return 0;
}


/* ==================== IPMETA FUNCTIONS ==================== */

static int init_ipmeta(bvc_t *consumer)
{

  bvc_pergeovisibility_state_t *state = STATE;

  char provider_options[BUFFER_LEN] = "";

  /* lookup the provider using the name  */
  if((state->provider =
      ipmeta_get_provider_by_name(state->ipmeta, GEO_PROVIDER_NAME)) == NULL)
    {
      fprintf(stderr, "ERROR: Invalid provider name: %s\n", GEO_PROVIDER_NAME);
      return -1;
    }

  /* enable the provider  */
  snprintf(provider_options, BUFFER_LEN, "-b %s -l %s -c %s -D intervaltree",
	   state->blocks_file,
	   state->locations_file,
	   state->countries_file);

  if(ipmeta_enable_provider(state->ipmeta,
			    state->provider,
			    provider_options,
			    IPMETA_PROVIDER_DEFAULT_YES) != 0)
    {
      fprintf(stderr, "ERROR: Could not enable provider %s\n",
              GEO_PROVIDER_NAME);
      return -1;
    }

  /* initialize a (reusable) record set structure  */
  if((state->records = ipmeta_record_set_init()) == NULL)
    {
      fprintf(stderr, "ERROR: Could not init record set\n");
      return -1;
    }

  return 0;
}


/* ==================== THRESHOLDS FUNCTIONS ==================== */

static void init_thresholds(bvc_pergeovisibility_state_t *state)
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
static void reset_origins(bvc_pergeovisibility_state_t *state)
{
  state->valid_origins = 0;
}

/* check if an origin ASn is already part of the array and
 * if not it adds it */
static void add_origin(bvc_pergeovisibility_state_t *state, bgpstream_as_path_seg_t *origin_seg)
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

/* ==================== PER-GEO-INFO FUNCTIONS ==================== */

static int pergeo_info_init(bvc_t *consumer, pergeo_info_t *per_geo, char *continent, char *iso2_cc)
{
  bvc_pergeovisibility_state_t *state = STATE;
  char buffer[BUFFER_LEN];
  int i,v;
  for(i = 0; i < VIS_THRESHOLDS_CNT; i++)
    {
      /* create all Patricia Trees */
      if((per_geo->info[i].pt = bgpstream_patricia_tree_create(NULL)) == NULL)
        {
          return -1;
        }
      /* create all asns sets  */
      if((per_geo->info[i].asns = bgpstream_id_set_create()) == NULL)
        {
          return -1;
        }
      /* create indexes for timeseries */
      for(v = 0; v < MAX_IP_VERSION_ALLOWED; v++)
        {
          /* visible_prefixes_cnt */
          snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_IP_TH_FORMAT,
                   CHAIN_STATE->metric_prefix, continent, iso2_cc, bgpstream_idx2number(v), threshold_string(i),
                   "visible_prefixes_cnt");
          if((per_geo->info[i].pfx_cnt_idx[v] = timeseries_kp_add_key(state->kp, buffer)) == -1)
            {
              return -1;
            }
          /* visible_ips_cnt */
          snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_IP_TH_FORMAT,
                   CHAIN_STATE->metric_prefix, continent, iso2_cc, bgpstream_idx2number(v), threshold_string(i),
                   v == bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4) ? "visible_slash24_cnt" : "visible_slash64_cnt");
          if((per_geo->info[i].subnet_cnt_idx[v] = timeseries_kp_add_key(state->kp, buffer)) == -1)
            {
              return -1;
            }
          /* visible_asns_cnt */
          snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_IP_TH_FORMAT,
                   CHAIN_STATE->metric_prefix, continent, iso2_cc, bgpstream_idx2number(v), threshold_string(i),
                   "visible_asns_cnt");
          if((per_geo->info[i].asns_cnt_idx = timeseries_kp_add_key(state->kp, buffer)) == -1)
            {
              return -1;
            }
        }
    }
  return 0;
}

static int pergeo_info_update(bvc_t *consumer, pergeo_info_t *per_geo, bgpstream_pfx_t *pfx)
{
  bvc_pergeovisibility_state_t *state = STATE;

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
  int i, j;
  for(i = VIS_THRESHOLDS_CNT-1; i >= 0; i--)
    {
      if(ratio >= state->thresholds[i])
        {
          /* add prefix to the Patricia Tree */
          if(bgpstream_patricia_tree_insert(per_geo->info[i].pt, pfx) == NULL)
            {
              return -1;
            }
          /* add origin ASns to asns set */
          for(j = 0; j < state->valid_origins; j++)
            {
              bgpstream_id_set_insert(per_geo->info[i].asns, state->origin_asns[j]);
            }
          break;
        }
    }
  return 0;
}

static void pergeo_info_destroy(pergeo_info_t *per_geo)
{
  int i;
  for(i = 0; i < VIS_THRESHOLDS_CNT; i++)
    {
      bgpstream_patricia_tree_destroy(per_geo->info[i].pt);
      bgpstream_id_set_destroy(per_geo->info[i].asns);
    }
}


/* ==================== UTILITY FUNCTIONS ==================== */

static int create_geo_pfxs_vis(bvc_t *consumer)
{
  bvc_pergeovisibility_state_t *state = STATE;

  if((state->geo_pfxs_vis = kh_init(geo_pfxs_info)) == NULL)
    {
      fprintf(stderr, "Error: Unable to create Geo visibility map\n");
      return -1;
    }

  int i;
  int khret;
  khiter_t k;
  pergeo_info_t *all_infos;

  ipmeta_provider_netacq_edge_country_t **countries = NULL;
  int num_countries =
    ipmeta_provider_netacq_edge_get_countries(state->provider, &countries);

  /* we create an entry in the geo_pfxs_vis state for each country */
  for(i=0; i < num_countries; i++)
    {
      /* Netacq should return a set of unique countries however we still
       * check if this iso2 is already in the countrycode map */
      if((k = kh_get(geo_pfxs_info, state->geo_pfxs_vis, countries[i]->iso2)) == kh_end(state->geo_pfxs_vis))
        {
          k = kh_put(geo_pfxs_info, state->geo_pfxs_vis,
                     strdup(countries[i]->iso2), &khret);
          all_infos = &kh_val(state->geo_pfxs_vis, k);
          if(pergeo_info_init(consumer, all_infos, countries[i]->continent, countries[i]->iso2) != 0)
            {
              return -1;
            }
        }
    }
  return 0;
}

static int update_pfx_geo_information(bvc_t *consumer, bgpview_iter_t *it)
{
  bvc_pergeovisibility_state_t *state = STATE;

  bgpstream_pfx_t * pfx = bgpview_iter_pfx_get_pfx(it);
  assert(pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV4);

  khash_t(country_k_set) *cck_set = (khash_t(country_k_set) *) bgpview_iter_pfx_get_user(it);

  /* if the user pointer does not contain geographical information (i.e., it is
   * NULL), then create new geo info */
  if(cck_set == NULL)
    {
      if((cck_set = kh_init(country_k_set)) == NULL)
        {
          fprintf(stderr, "Error: cannot create country_k_set\n");
          return -1;
        }
      int khret;
      khiter_t k;
      khiter_t setk;
      ipmeta_record_t *rec;
      uint32_t num_ips;
      /* lookup for the geo location of IPv4*/
      ipmeta_lookup(state->provider, (uint32_t) ((bgpstream_ipv4_pfx_t *)pfx)->address.ipv4.s_addr,
                        pfx->mask_len, state->records);
      ipmeta_record_set_rewind(state->records);
      while ( (rec = ipmeta_record_set_next(state->records, &num_ips)) )
        {
          /* check that we already had this country in our dataset */
          if((k = kh_get(geo_pfxs_info, state->geo_pfxs_vis,
                         rec->country_code)) == kh_end(state->geo_pfxs_vis))
                {
                  fprintf(stderr, "Warning: country (%s) not found in the countries file\n", rec->country_code);
                  continue;
                }

          /* insert k in the country-code-k set */
          if((setk = kh_get(country_k_set, cck_set, k)) == kh_end(cck_set))
            {
              setk = kh_put(country_k_set, cck_set, k, &khret);
            }
        }
      /* link the set to the appropriate user ptr */
      bgpview_iter_pfx_set_user(it, (void *) cck_set);
    }

  /* now the prefix holds geographical information
   * we can update the counters for each country in cc_k_set
   * note that cc_k_set contains the k position of a country in
   * the state->geo_pfxs_vis hash map */
  khiter_t cck;
  khiter_t idk;
  pergeo_info_t *per_geo_infos;
  for(idk = kh_begin(cck_set);
      idk != kh_end(cck_set); ++idk)
    {
      if (kh_exist(cck_set, idk))
        {
          /* get the key */
          cck = kh_key(cck_set, idk);
          /* and update the correspoding value in geo_pfxs_vis*/
          per_geo_infos = &kh_val(state->geo_pfxs_vis, cck);
          /* once we have a pointer to the geo information, we update it */
          if(pergeo_info_update(consumer, per_geo_infos, pfx) != 0)
            {
              return -1;
            }
        }
    }

  return 0;
}


static int compute_geo_pfx_visibility(bvc_t *consumer, bgpview_iter_t *it)
{
  bvc_pergeovisibility_state_t *state = STATE;

  bgpstream_pfx_t *pfx;
  bgpstream_peer_sig_t *sg;

  /* for each prefix in the view */
  for(bgpview_iter_first_pfx(it, BGPSTREAM_ADDR_VERSION_IPV4 /* IPv4 only*/, BGPVIEW_FIELD_ACTIVE);

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
          if(update_pfx_geo_information(consumer, it) != 0)
            {
              return -1;
            }
        }
    }


  return 0;
}


static int output_metrics_and_reset(bvc_t *consumer)
{
  bvc_pergeovisibility_state_t *state = STATE;
  int i;
  khiter_t k;
  pergeo_info_t *per_geo;

  /* for each country code */
  for(k = kh_begin(state->geo_pfxs_vis); k != kh_end(state->geo_pfxs_vis); ++k)
    {
      if (kh_exist(state->geo_pfxs_vis, k))
        {
          /* collect the information for all thresholds */
          per_geo = &kh_val(state->geo_pfxs_vis, k);
          for(i = VIS_THRESHOLDS_CNT-1; i >=0 ; i--)
            {
              /* we merge all the trees (asn sets) with the previous one,
               * except the first */
              if(i != (VIS_THRESHOLDS_CNT-1))
                {
                  bgpstream_patricia_tree_merge(per_geo->info[i].pt, per_geo->info[i+1].pt);
                  bgpstream_id_set_merge(per_geo->info[i].asns, per_geo->info[i+1].asns);
                }

              /* now that the tree represents all the prefixes
               * that match the threshold, we extract the information
               * that we want to output */

              /* IPv4*/
              timeseries_kp_set(state->kp,
                                per_geo->info[i].pfx_cnt_idx[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)],
                                bgpstream_patricia_prefix_count(per_geo->info[i].pt, BGPSTREAM_ADDR_VERSION_IPV4));
              timeseries_kp_set(state->kp,
                                per_geo->info[i].subnet_cnt_idx[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)],
                                bgpstream_patricia_tree_count_24subnets(per_geo->info[i].pt));

              /* IPv6 is not here yet */
              /* timeseries_kp_set(state->kp, */
              /*                   per_as->info[i].pfx_cnt_idx[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV6)], */
              /*                   bgpstream_patricia_prefix_count(per_as->info[i].pt, BGPSTREAM_ADDR_VERSION_IPV6)); */
              /* timeseries_kp_set(state->kp, */
              /*                   per_as->info[i].subnet_cnt_idx[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV6)], */
              /*                   bgpstream_patricia_tree_count_64subnets(per_as->info[i].pt)); */

              /* ASns*/
              timeseries_kp_set(state->kp, per_geo->info[i].asns_cnt_idx,
                                bgpstream_id_set_size(per_geo->info[i].asns));
            }

          /* metrics are set, now we have to clean the patricia trees */
          for(i = VIS_THRESHOLDS_CNT-1; i >=0 ; i--)
            {
              bgpstream_patricia_tree_clear(per_geo->info[i].pt);
              bgpstream_id_set_clear(per_geo->info[i].asns);
            }
        }
    }
  return 0;
}




/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_pergeovisibility_alloc()
{
  return &bvc_pergeovisibility;
}

int bvc_pergeovisibility_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_pergeovisibility_state_t *state = NULL;

  if((state = malloc_zero(sizeof(bvc_pergeovisibility_state_t))) == NULL)
    {
      return -1;
    }
  BVC_SET_STATE(consumer, state);

  /* set defaults here */

  /* init and set defaults */

  if((state->ff_asns = bgpstream_id_set_create()) == NULL)
    {
      fprintf(stderr, "Error: Could not create full feed origin ASns set\n");
      goto err;
    }

  init_thresholds(state);

  /* initialize ipmeta structure */
  if((state->ipmeta = ipmeta_init()) == NULL)
    {
      fprintf(stderr, "Error: Could not initialize ipmeta \n");
      goto err;
    }

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

  /* initialize ipmeta and provider */
  if(init_ipmeta(consumer) != 0)
    {
      goto err;
    }

  /* the main hash table can be created only when ipmet has been
   * properly initialized */
  if(create_geo_pfxs_vis(consumer) != 0)
    {
      fprintf(stderr, "Error: Unable to create Geo visibility map\n");
      goto err;
    }

  return 0;

 err:
  bvc_pergeovisibility_destroy(consumer);
  return -1;
}


static void
bvc_destroy_pfx_user_ptr(void *user)
{
  khash_t(country_k_set) *cck_set = (khash_t(country_k_set) *) user;
  kh_destroy(country_k_set, cck_set);
}


void bvc_pergeovisibility_destroy(bvc_t *consumer)
{
  bvc_pergeovisibility_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  /* destroy things here */
  if(state->geo_pfxs_vis != NULL)
    {
      khiter_t k;
      for(k = kh_begin(state->geo_pfxs_vis); k != kh_end(state->geo_pfxs_vis); ++k)
        {
          if (kh_exist(state->geo_pfxs_vis, k))
            { /* free country string */
              free(kh_key(state->geo_pfxs_vis, k));
              /* free country infos */
              pergeo_info_destroy(&kh_val(state->geo_pfxs_vis, k));
            }
        }
      kh_destroy(geo_pfxs_info, state->geo_pfxs_vis);
      state->geo_pfxs_vis = NULL;
    }

  if(state->ipmeta != NULL)
    {
      ipmeta_free(state->ipmeta);
      state->ipmeta = NULL;
    }

  if(state->records != NULL)
    {
      ipmeta_record_set_free(&state->records);
      state->records = NULL;
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


int bvc_pergeovisibility_process_view(bvc_t *consumer, bgpview_t *view)
{
  bvc_pergeovisibility_state_t *state = STATE;

  if(BVC_GET_CHAIN_STATE(consumer)->usable_table_flag[bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4)] == 0)
    {
      fprintf(stderr,
              "ERROR: Per-Geo Visibility can't use this table %"PRIu32"\n", bgpview_get_time(view));
      return 0;
    }

  /* compute arrival delay */
  state->arrival_delay = zclock_time()/1000 - bgpview_get_time(view);

  /* get full feed peer ids from Visibility */
  if(BVC_GET_CHAIN_STATE(consumer)->visibility_computed == 0)
    {
      fprintf(stderr,
              "ERROR: The Per-Geo Visibility requires the Visibility consumer "
              "to be run first\n");
      return -1;
    }

  /* create a new iterator */
  bgpview_iter_t *it;
  if((it = bgpview_iter_create(view)) == NULL)
    {
      return -1;
    }

  /* set the pfx user pointer destructor function */
  bgpview_set_pfx_user_destructor(view, bvc_destroy_pfx_user_ptr);

  /* compute the pfx visibility of each origin country */
  if(compute_geo_pfx_visibility(consumer, it) != 0)
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
