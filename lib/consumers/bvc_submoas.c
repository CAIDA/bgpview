/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini, Ruwaifa Anwar
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

#include "bvc_submoas.h"
#include "bgpview_consumer_interface.h"
#include "bgpview_consumer_utils.h"
#include "bgpstream_utils_pfx.h"
#include "bgpstream_utils_pfx_set.h"
#include "khash.h"
#include "utils.h"
#include <wandio.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define NAME "submoas"
#define CONSUMER_METRIC_PREFIX "submoas"

#define OUTPUT_FILE_FORMAT                                                     \
  "%s/" NAME ".%" PRIu32 ".%" PRIu32 "s-window.events.gz"

#define METRIC_PREFIX_FORMAT                                                   \
  "%s." CONSUMER_METRIC_PREFIX ".%" PRIu32 "s-window.%s"
#define META_METRIC_PREFIX_FORMAT                                              \
  "%s.meta.bgpview.consumer." NAME ".%" PRIu32 "s-window.%s"

/** Maximum size of the str output buffer */
#define MAX_BUFFER_LEN 1024

/** Maximum number of origin ASns */
#define MAX_UNIQUE_ORIGINS 128

/** Default size of window: 1 week (s) */
#define DEFAULT_WINDOW_SIZE (7 * 24 * 3600)

/** Default output folder: current folder */
#define DEFAULT_OUTPUT_FOLDER "./"

/* IPv4 default route */
#define IPV4_DEFAULT_ROUTE "0.0.0.0/0"

/* IPv6 default route */
#define IPV6_DEFAULT_ROUTE "0::/0"

#define STATE (BVC_GET_STATE(consumer, submoas))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_submoas = {BVC_ID_SUBMOAS, NAME, BVC_GENERATE_PTRS(submoas)};

/* Stores ANS information for prefixes in patricia tree */
typedef struct asn_info {
  uint32_t asn;
  uint32_t last_seen;
} asn_info_t;

/* Node of patricia tree. Contains prefix based information */
typedef struct pref_info {
  uint32_t first_seen;
  uint32_t start;
  uint32_t end;
  uint32_t last_seen;
  asn_info_t origin_asns[MAX_UNIQUE_ORIGINS];
  uint32_t number_of_asns;
} pref_info_t;

/* Subprefix stores. Used for each subprefix
    Stores differnt timestamps and superprefix */
typedef struct subprefix_info {
  bgpstream_pfx_t superprefix;
  uint32_t subasn;
  uint32_t superasn;
  uint32_t prev_start;
  uint32_t start;
  uint32_t end;
  uint32_t last_seen;

} subprefix_info_t;

typedef struct submoas_prefix {
  bgpstream_pfx_t subprefix;
  bgpstream_pfx_t superprefix;
  subprefix_info_t submoases[MAX_UNIQUE_ORIGINS];
  int number_of_subasns;
  uint32_t first_seen;
  uint32_t start;
  uint32_t end;
} submoas_prefix_t;

/* Stores subprefixes for a given superprefix */
KHASH_INIT(subprefixes_in_superprefix, bgpstream_pfx_t, int, 1,
           bgpstream_pfx_hash_val, bgpstream_pfx_equal_val)
typedef khash_t(subprefixes_in_superprefix) subprefixes_in_superprefix_t;

/* Stores all the subprefixes */
KHASH_INIT(subprefix_map, bgpstream_pfx_t, submoas_prefix_t, 1,
           bgpstream_pfx_hash_val, bgpstream_pfx_equal_val)
typedef khash_t(subprefix_map) subprefix_map_t;

/*Stores all the superprefixes */
KHASH_INIT(superprefix_map, bgpstream_pfx_t,
           subprefixes_in_superprefix_t *, 1, bgpstream_pfx_hash_val,
           bgpstream_pfx_equal_val)
typedef khash_t(superprefix_map) superprefix_map_t;

/* our 'instance' */
typedef struct bvc_submoas_state {

  iow_t *file;

  bgpstream_patricia_tree_t *pt;
  /** first processed timestamp */
  uint32_t first_ts;
  /** window size requested by user */
  uint32_t window_size;

  /** current window size (always <= window size) */
  uint32_t current_window_size;

  /** blacklist prefixes */
  bgpstream_pfx_set_t *blacklist_pfxs;

  /** output folder */
  char output_folder[MAX_BUFFER_LEN];
  char filename[BVCU_PATH_MAX];
  /** Contains all subprefixes */
  subprefix_map_t *subprefix_map;
  /** Contains all superprefix */
  superprefix_map_t *superprefix_map;

  uint32_t time_now;

  bgpview_iter_t *iter;

  /** New/recurring/ongoing/finished MOAS prefixes count */
  uint32_t new_submoas_pfxs_count;
  uint32_t newrec_submoas_pfxs_count;
  uint32_t ongoing_submoas_pfxs_count;
  uint32_t finished_submoas_pfxs_count;

  /** diff ts when the view arrived */
  uint32_t arrival_delay;
  /** diff ts when the view _ing finished */
  uint32_t processed_delay;
  /** processing time */
  uint32_t processing_time;

  /** Timeseries Key Package */
  timeseries_kp_t *kp;

  /** Metrics indices */
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;
  int new_submoas_pfxs_count_idx;
  int ongoing_submoas_pfxs_count_idx;
  int finished_submoas_pfxs_count_idx;
  int newrec_submoas_pfxs_count_idx;

} bvc_submoas_state_t;

/** Print and update current moases */
static int output_timeseries(bvc_t *consumer, uint32_t ts)
{
  bvc_submoas_state_t *state = STATE;
  timeseries_kp_set(state->kp, state->arrival_delay_idx, state->arrival_delay);
  timeseries_kp_set(state->kp, state->processed_delay_idx,
                    state->processed_delay);
  timeseries_kp_set(state->kp, state->processing_time_idx,
                    state->processing_time);

  timeseries_kp_set(state->kp, state->finished_submoas_pfxs_count_idx,
                    state->finished_submoas_pfxs_count);
  timeseries_kp_set(state->kp, state->ongoing_submoas_pfxs_count_idx,
                    state->ongoing_submoas_pfxs_count);
  timeseries_kp_set(state->kp, state->new_submoas_pfxs_count_idx,
                    state->new_submoas_pfxs_count);
  timeseries_kp_set(state->kp, state->newrec_submoas_pfxs_count_idx,
                    state->newrec_submoas_pfxs_count);

  if (timeseries_kp_flush(state->kp, ts) != 0) {
    fprintf(stderr, "Warning: could not flush %s %" PRIu32 "\n", NAME, ts);
  }

  return 0;
}

/** Create timeseries metrics */

static int create_ts_metrics(bvc_t *consumer)
{

  char buffer[MAX_BUFFER_LEN];
  bvc_submoas_state_t *state = STATE;

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "new_submoas_pfxs_count");
  if ((state->new_submoas_pfxs_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ongoing_submoas_pfxs_count");
  if ((state->ongoing_submoas_pfxs_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "finished_submoas_pfxs_count");
  if ((state->finished_submoas_pfxs_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "newrec_submoas_pfxs_count");
  if ((state->newrec_submoas_pfxs_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }
  /* Meta metrics */
  snprintf(buffer, MAX_BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "arrival_delay");
  if ((state->arrival_delay_idx = timeseries_kp_add_key(state->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "processed_delay");
  if ((state->processed_delay_idx = timeseries_kp_add_key(state->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "processing_time");
  if ((state->processing_time_idx = timeseries_kp_add_key(state->kp, buffer)) ==
      -1) {
    return -1;
  }

  return 0;
}

static void pref_info_destroy(void *pi)
{
  if (pi != NULL) {
    free((pref_info_t *)pi);
  }
}

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
          "consumer usage: %s\n"
          "       -w <window-size>      window size in seconds (default %d)\n"
          "       -o <output-folder>    output folder (default: %s)\n",
          consumer->name, DEFAULT_WINDOW_SIZE, DEFAULT_OUTPUT_FOLDER);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  assert(argc > 0 && argv != NULL);

  bvc_submoas_state_t *state = STATE;

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, ":w:o:?")) >= 0) {
    switch (opt) {
    case 'w':
      state->window_size = strtoul(optarg, NULL, 10);
      break;
    case 'o':
      strncpy(state->output_folder, optarg, MAX_BUFFER_LEN - 1);
      state->output_folder[MAX_BUFFER_LEN - 1] = '\0';
      break;
    case '?':
    case ':':
    default:
      usage(consumer);
      return -1;
    }
  }

  /* checking that output_folder is a valid folder */
  if (!bvcu_is_writable_folder(state->output_folder)) {
    usage(consumer);
    return -1;
  }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_submoas_alloc()
{
  return &bvc_submoas;
}

int bvc_submoas_init(bvc_t *consumer, int argc, char **argv)
{

  bvc_submoas_state_t *state = NULL;
  bgpstream_pfx_t pfx;

  if ((state = malloc_zero(sizeof(bvc_submoas_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);
  if ((state->subprefix_map = kh_init(subprefix_map)) == NULL) {
    fprintf(stderr, "Error: Could not create current_subprefixes\n");
    goto err;
  }

  if ((state->superprefix_map = kh_init(superprefix_map)) == NULL) {
    fprintf(stderr, "Error: Could not create current_superprefixes\n");
    goto err;
  }

  state->pt = bgpstream_patricia_tree_create(pref_info_destroy);

  /* defaults here */
  state->window_size = DEFAULT_WINDOW_SIZE;
  strncpy(state->output_folder, DEFAULT_OUTPUT_FOLDER, MAX_BUFFER_LEN);
  state->output_folder[MAX_BUFFER_LEN - 1] = '\0';

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  /* react to args here */
  fprintf(stderr, "INFO: window size: %" PRIu32 "\n", state->window_size);
  fprintf(stderr, "INFO: output folder: %s\n", state->output_folder);

  if ((state->blacklist_pfxs = bgpstream_pfx_set_create()) == NULL) {
    fprintf(stderr, "Error: Could not create blacklist pfx set\n");
    goto err;
  }

  /* add default routes to blacklist */
  if (!(bgpstream_str2pfx(IPV4_DEFAULT_ROUTE, &pfx) != NULL &&
        bgpstream_pfx_set_insert(state->blacklist_pfxs, &pfx) >= 0)) {
    fprintf(stderr, "Could not insert prefix in blacklist\n");
    goto err;
  }
  if (!(bgpstream_str2pfx(IPV6_DEFAULT_ROUTE, &pfx) != NULL &&
        bgpstream_pfx_set_insert(state->blacklist_pfxs, &pfx) >= 0)) {
    fprintf(stderr, "Could not insert prefix in blacklist\n");
    goto err;
  }

  /* init */

  if ((state->kp = timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) ==
      NULL) {
    fprintf(stderr, "Error: Could not create timeseries key package\n");
    goto err;
  }

  if (create_ts_metrics(consumer) != 0) {
    goto err;
  }

  if (BVC_GET_CHAIN_STATE(consumer)->visibility_computed == 0) {
    fprintf(stderr, "ERROR: moas requires the Visibility consumer "
                    "to be run first\n");
    goto err;
  }

  return 0;

err:
  bvc_submoas_destroy(consumer);
  return -1;
}

void bvc_submoas_destroy(bvc_t *consumer)
{
  bvc_submoas_state_t *state = STATE;
  if (state != NULL) {

    khint_t k;
    kh_destroy(subprefix_map, state->subprefix_map);
    for (k = kh_begin(state->superprefix_map);
         k != kh_end(state->superprefix_map); ++k) {
      if (kh_exist(state->superprefix_map, k)) {
        kh_destroy(subprefixes_in_superprefix,
                   kh_value(state->superprefix_map, k));
      }
    }
    if (state->blacklist_pfxs != NULL) {
      bgpstream_pfx_set_destroy(state->blacklist_pfxs);
    }

    if (state->kp != NULL) {
      timeseries_kp_free(&state->kp);
    }
    kh_destroy(superprefix_map, state->superprefix_map);
    bgpstream_patricia_tree_destroy(state->pt);
    free(state);
    BVC_SET_STATE(consumer, NULL);
  }
}

/*DIAGNOSTIC USE ONLY */
/* static void print_origin_asn(pref_info_t pref_info){ */
/*   printf("numberASN %d \n", pref_info.number_of_asns); */
/*   int i; */
/*   for (i=0;i<pref_info.number_of_asns;i++){ */
/*     printf(" %d ",pref_info.origin_asns[i].asn); */
/*   } */
/* printf("\n"); */
/* } */

/* static void print_subprefixes(bvc_t *consumer){ */
/*   printf("done printing\n"); */
/*   bvc_submoas_state_t *state = STATE; */
/*   char pfx_str[INET6_ADDRSTRLEN+3]; */
/*   khint_t k; */
/*   char pfx2_str[INET6_ADDRSTRLEN+3]; */
/*   char pfx3_str[INET6_ADDRSTRLEN+3]; */
/*   khint_t m,n; */
/*   submoas_prefix_t submoas_struct; */
/*   bgpstream_patricia_node_t *pfx_node2; */
/*   pref_info_t *temp_pref; */
/*   int i; */
/*   for (k = kh_begin(state->subprefix_map); k != kh_end(state->subprefix_map);
 * ++k) { */
/*     if (kh_exist(state->subprefix_map,k)){ */

/*       submoas_struct=kh_value(state->subprefix_map,k); */
/*       bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN+3,
 *         &submoas_struct.subprefix); */
/*       printf("***Subprefix was %s \n",pfx_str); */
/*       printf("number of subasns %d \n ",submoas_struct.number_of_subasns); */
/*       for ( i=0;i<submoas_struct.number_of_subasns;i++){ */
/*         bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN+3,
 *           &submoas_struct.submoases[i].superprefix); */
/*         printf("For superprefix %s \n",pfx2_str); */
/*         m=kh_get(superprefix_map,
 * state->superprefix_map,submoas_struct.submoases[i].superprefix); */
/*         if(m==kh_end(state->superprefix_map)){ */
/*           fprintf(stderr, "submoas:FATAL ERms1\n"); */
/*         } */
/*        subprefixes_in_superprefix_t *subprefixes_in_superprefix  =
 * kh_value(state->superprefix_map,m); */
/*         for (n = kh_begin(subprefixes_in_superprefix); n !=
 * kh_end(subprefixes_in_superprefix); ++n) { */
/*           if(kh_exist(subprefixes_in_superprefix,n)){ */
/*             bgpstream_pfx_snprintf(pfx3_str, INET6_ADDRSTRLEN+3,
 *               &kh_key(subprefixes_in_superprefix,n)); */
/*             printf("Supermoas from hash %s \n",pfx3_str); */
/*             break; */
/*           } */
/*         } */

/*         pfx_node2=bgpstream_patricia_tree_search_exact(state->pt,&submoas_struct.submoases[i].superprefix);
 */
/*         if (pfx_node2==NULL){ */
/*           printf("Fatal error \n"); */
/*         } */
/*         printf("Originally by asns \n"); */
/*         temp_pref=bgpstream_patricia_tree_get_user( pfx_node2); */
/*         print_origin_asn(*temp_pref); */
/*         printf("Submoased by asns %d\n",submoas_struct.submoases[i].subasn);
 */

/*       } */
/*     } */
/*   } */
/*   printf("done printing\n"); */
/* } */

// Given a subprefix struct, adds asn to it */
static submoas_prefix_t add_new_asn(submoas_prefix_t submoas_struct, int asn,
                                    bgpstream_pfx_t pfx,
                                    bgpstream_pfx_t parent_pfx,
                                    uint32_t time_now)
{
  int existing_subasns = submoas_struct.number_of_subasns;
  subprefix_info_t subprefix_info;
  bgpstream_pfx_t p_pfx = parent_pfx;
  subprefix_info.superprefix = p_pfx;
  subprefix_info.subasn = asn;
  subprefix_info.start = time_now;
  subprefix_info.last_seen = time_now;
  /* packing submoas structi */
  bgpstream_pfx_t subp = pfx;
  submoas_struct.subprefix = subp;
  submoas_struct.submoases[existing_subasns] = subprefix_info;
  submoas_struct.number_of_subasns++;
  // Add in supermoas_struct */

  return submoas_struct;
}

// Adds superprefix in superprefix map along with its subprefixes */
static void add_superprefix(bvc_t *consumer,
                            bgpstream_pfx_t superprefix,
                            bgpstream_pfx_t subprefix)
{
  bvc_submoas_state_t *state = STATE;
  khint_t k, j;
  int ret;
  subprefixes_in_superprefix_t *subprefixes_in_sup;
  k = kh_get(superprefix_map, state->superprefix_map, superprefix);
  /* seen this superprefix first time */
  if (k == kh_end(state->superprefix_map)) {
    // subprefixes_in_sup=kh_init(subprefixes_in_superprefix);
    j = kh_put(superprefix_map, state->superprefix_map, superprefix, &ret);
    // initializing khash for subprefixes

    if ((subprefixes_in_sup = kh_init(subprefixes_in_superprefix)) == NULL) {
      fprintf(stderr, "Error: Could not create sub_in_super\n");
    }
    kh_put(subprefixes_in_superprefix, subprefixes_in_sup, subprefix, &ret);
    kh_value(state->superprefix_map, j) = subprefixes_in_sup;
  }

  else {
    subprefixes_in_sup = kh_value(state->superprefix_map, k);
    kh_put(subprefixes_in_superprefix, subprefixes_in_sup, subprefix, &ret);
    kh_value(state->superprefix_map, k) = subprefixes_in_sup;
  }
}

// Returns char string having origin asns  */
static char *print_submoas_info(int caller, bvc_t *consumer,
                                const bgpstream_pfx_t *parent_pfx,
                                const bgpstream_pfx_t *pfx, char *buffer,
                                const int buffer_len)
{

  assert(buffer);
  assert(buffer_len > 0);
  int written;
  int ret;
  int i;
  bvc_submoas_state_t *state = STATE;
  buffer[0] = '\0';
  written = 0;
  bgpstream_patricia_node_t *pfx_node =
    bgpstream_patricia_tree_search_exact(state->pt, parent_pfx);
  const pref_info_t *this_prefix_info = bgpstream_patricia_tree_get_user(pfx_node);
  khint_t k;
  k = kh_get(subprefix_map, state->subprefix_map, *pfx);
  submoas_prefix_t submoas_struct = kh_value(state->subprefix_map, k);
  for (i = 0; i < this_prefix_info->number_of_asns; i++) {
    if (i == 0) {
      ret = snprintf(buffer + written, buffer_len - written - 1, "%" PRIu32 "",
                     this_prefix_info->origin_asns[i].asn);
    } else {
      ret = snprintf(buffer + written, buffer_len - written - 1, " %" PRIu32 "",
                     this_prefix_info->origin_asns[i].asn);
    }
    if (ret < 0 || ret >= buffer_len - written - 1) {
      fprintf(stderr, "ERROR: cannot write the current MOAS signature.\n");
      return NULL;
    }
    written += ret;
  }
  ret = snprintf(buffer + written, buffer_len - written - 1, "|");
  written += ret;

  for (i = 0; i < submoas_struct.number_of_subasns; i++) {
    if (i == 0) {
      ret = snprintf(buffer + written, buffer_len - written - 1, "%" PRIu32 "",
                     submoas_struct.submoases[i].subasn);
    } else {
      ret = snprintf(buffer + written, buffer_len - written - 1, " %" PRIu32 "",
                     submoas_struct.submoases[i].subasn);
    }

    if (ret < 0 || ret >= buffer_len - written - 1) {
      fprintf(stderr, "ERROR: cannot write the current MOAS signature.\n");
      return NULL;
    }
    written += ret;
  }

  buffer[written] = '\0';

  return buffer;
}
// Runs once a view, prints all ongoing subprefixes */
static void print_ongoing(bvc_t *consumer)
{
  bvc_submoas_state_t *state = STATE;
  char asn_buffer[MAX_BUFFER_LEN];
  char pfx_str[INET6_ADDRSTRLEN + 3];
  char pfx2_str[INET6_ADDRSTRLEN + 3];
  khint_t k;
  for (k = kh_begin(state->subprefix_map); k != kh_end(state->subprefix_map);
       ++k) {
    if (kh_exist(state->subprefix_map, k)) {
      submoas_prefix_t submoas_struct = kh_value(state->subprefix_map, k);
      if (submoas_struct.number_of_subasns == 0) {
        continue;
      }
      /* avoid printing/counting sub-moases that just started */
      if (submoas_struct.start == state->time_now) {
        continue;
      }
      state->ongoing_submoas_pfxs_count++;
      if (wandio_printf(
            state->file, "%" PRIu32 "|%s|%s|ONGOING|%" PRIu32 "|%" PRIu32
                         "|%" PRIu32 "|%s    \n",
            state->time_now, bgpstream_pfx_snprintf(
                               pfx_str, INET6_ADDRSTRLEN + 3,
                               &submoas_struct.superprefix),
            bgpstream_pfx_snprintf(
              pfx2_str, INET6_ADDRSTRLEN + 3,
              &submoas_struct.subprefix),
            submoas_struct.first_seen, submoas_struct.start, state->time_now,

            print_submoas_info(0, consumer,
                               &submoas_struct.superprefix,
                               &submoas_struct.subprefix,
                               asn_buffer, MAX_BUFFER_LEN)) == -1) {
        fprintf(stderr, "sERROR: Could not write file\n");
        return;
      }
    }
  }
}

static int print_pfx_peers(bvc_t *consumer, bvc_submoas_state_t *state,
    bgpview_iter_t *it, const bgpstream_pfx_t *pfx)
{
  int ipv_idx = bgpstream_ipv2idx(pfx->address.version);

  for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_pfx_has_more_peer(it);
       bgpview_iter_pfx_next_peer(it)) {

    // printing a path for each peer
    bgpstream_peer_id_t peerid = bgpview_iter_peer_get_peer_id(it);
    if (bgpstream_id_set_exists(
          BVC_GET_CHAIN_STATE(consumer)->full_feed_peer_ids[ipv_idx],
          peerid)) {

      if (bvcu_print_pfx_peer_as_path(state->file, it, "|", " ") < 0)
        return -1;
    }
  }
  return 0;
}

/* Adds prefix or new asn for a existing prefix to patricia tree. Check if
 * updating patricia tree results in submoas or not */
static void update_patricia(bvc_t *consumer,
                            bgpstream_patricia_node_t *pfx_node,
                            pref_info_t *pref_info, bgpview_iter_t *it,
                            uint32_t timenow)
{
  khint_t k, j;
  int ret;
  char prev_category[MAX_BUFFER_LEN];
  char asn_buffer[MAX_BUFFER_LEN];
  char pfx_str[INET6_ADDRSTRLEN + 3];
  char pfx2_str[INET6_ADDRSTRLEN + 3];
  bvc_submoas_state_t *state = STATE;
  const bgpstream_pfx_t *pfx = bgpstream_patricia_tree_get_pfx(pfx_node);
  bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx);
  bgpstream_patricia_tree_result_set_t *res_set =
    bgpstream_patricia_tree_result_set_create();
  int same_origin = 0;
  bgpstream_patricia_tree_get_less_specifics(state->pt, pfx_node, res_set);
  bgpstream_patricia_node_t *parent_node;
  parent_node = NULL;

  parent_node = bgpstream_patricia_tree_result_set_next(res_set);
  if (parent_node == NULL) {
    bgpstream_patricia_tree_result_set_destroy(&res_set);
    return;
  }

  uint32_t differ_asn[MAX_UNIQUE_ORIGINS];
  int differ_ind = 0;
  const bgpstream_pfx_t *parent_pfx = bgpstream_patricia_tree_get_pfx(parent_node);

  bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN + 3, parent_pfx);
  // pref_info_t *parent_pref=malloc(sizeof *parent_pref);
  pref_info_t *parent_pref;
  parent_pref = bgpstream_patricia_tree_get_user(parent_node);

  // Comparing origin ASNs for both given prefix and parent prefix and
  // generating list of ASNs of given prefix, not in parent prefix
  int i, y;
  int to_be_removed;
  to_be_removed = 0;
  for (i = 0; i < pref_info->number_of_asns; i++) {
    same_origin = 0;
    for (y = 0; y < parent_pref->number_of_asns; y++) {
      if (pref_info->origin_asns[i].asn == parent_pref->origin_asns[y].asn) {
        same_origin = 1;
        to_be_removed = 1;
        break;
      }
    }
    if (same_origin == 0) {
      differ_asn[differ_ind] = pref_info->origin_asns[i].asn;
      differ_ind++;
    }
  }
  // atleast one of the asns was same
  // atleast one of the asns was same
  if (to_be_removed) {
    return;
  }

  // Checking whether prefix already exists in patricia tree
  k = kh_get(subprefix_map, state->subprefix_map, *pfx);

  if (k == kh_end(state->subprefix_map)) {
    submoas_prefix_t submoas_struct;
    submoas_struct.number_of_subasns = 0;
    // Seen this subprefix first time
    int p;
    for (p = 0; p < differ_ind; p++) {
      submoas_struct = add_new_asn(
        submoas_struct, differ_asn[p], *pfx, *parent_pfx, timenow);
    }
    add_superprefix(consumer, *parent_pfx, *pfx);
    submoas_struct.superprefix = *parent_pfx;
    submoas_struct.first_seen = timenow;
    submoas_struct.start = timenow;
    submoas_struct.end = 0;
    j = kh_put(subprefix_map, state->subprefix_map, *pfx, &ret);
    kh_value(state->subprefix_map, j) = submoas_struct;
    // Printing info
    state->new_submoas_pfxs_count++;

    if (wandio_printf(
          state->file,
          "%" PRIu32 "|%s|%s|NEW|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|%s",
          state->time_now,
          bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, parent_pfx),
          bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN + 3, pfx),
          submoas_struct.first_seen, submoas_struct.start,
          submoas_struct.first_seen, // We're printing value of first seen as
                                     // end time for a new event
          print_submoas_info(1, consumer, parent_pfx, pfx, asn_buffer,
                             MAX_BUFFER_LEN)) == -1) {
      fprintf(stderr, "ERROR: Could not write %s file\n", state->filename);
      return;
    }
    if (print_pfx_peers(consumer, state, it, pfx) < 0)
      return;
    if (wandio_printf(state->file, "\n") == -1) {
      fprintf(stderr, "ERROR: Could not write data to file\n");
      return;
    }
  }
  // seen this subprefix before. ASN might be new or old
  else {
    bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx);
    bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN + 3, parent_pfx);
    // int first_seen_prior=0;
    submoas_prefix_t submoas_struct = kh_value(state->subprefix_map, k);

    // reupdating, just in case superprefix has changed
    submoas_struct.superprefix = *parent_pfx;
    // pChecking if it's still a submoas. if so then we have to finish it first
    // before updating for any new ASN
    add_superprefix(consumer, *parent_pfx, *pfx);
    if (submoas_struct.number_of_subasns == 0) {
      // first_seen_prior=1;
    } else {
      // The submoas was ongoing. a new asn is added. We will finish ongoing
      // submoas first
      if (wandio_printf(
            state->file, "%" PRIu32 "|%s|%s|FINISHED|%" PRIu32 "|%" PRIu32
                         "|%" PRIu32 "|%s    \n",
            timenow,
            bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, parent_pfx),
            bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN + 3, pfx),
            submoas_struct.first_seen, submoas_struct.start, timenow,
            print_submoas_info(2, consumer, parent_pfx, pfx, asn_buffer,
                               MAX_BUFFER_LEN)) == -1) {
        fprintf(stderr, "ERROR: Could not write %s file\n", state->filename);
        return;
      }
    }

    int found = 0;
    int p, m;
    for (p = 0; p < differ_ind; p++) {
      found = 0;
      for (m = 0; m < submoas_struct.number_of_subasns; m++) {
        // this asn already exists
        if (submoas_struct.submoases[m].subasn == differ_asn[p]) {
          found = 1;
          break;
        }
      }
      // Adding new ASN
      if (!found) {
        submoas_struct = add_new_asn(
          submoas_struct, differ_asn[p], *pfx, *parent_pfx, timenow);
      }
    }

    char category[MAX_BUFFER_LEN];
    // Checking last starting time if it's between current window that it's a
    // NEWREC event
    if (submoas_struct.start != timenow) {
      if (submoas_struct.start + state->window_size > timenow) {
        strcpy(category, "NEWREC");
        strcpy(prev_category, "NEWREC");
        state->newrec_submoas_pfxs_count++;
      } else {
        strcpy(category, "NEW");
        strcpy(prev_category, "NEW");
        state->new_submoas_pfxs_count++;
      }
    } else {
      strcpy(category, prev_category);
    }
    submoas_struct.start = timenow;
    kh_value(state->subprefix_map, k) = submoas_struct;

    if (wandio_printf(
          state->file,
          "%" PRIu32 "|%s|%s|%s|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|%s",
          timenow,
          bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, parent_pfx),
          bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN + 3, pfx), category,
          submoas_struct.first_seen, timenow,
          submoas_struct.first_seen, // We're printing value of first seen as
                                     // end time for a new event
          print_submoas_info(3, consumer, parent_pfx, pfx, asn_buffer,
                             MAX_BUFFER_LEN)) == -1) {
      fprintf(stderr, "ERROR: Could not write %s file\n", state->filename);
      return;
    }
    if (print_pfx_peers(consumer, state, it, pfx) < 0)
      return;
    if (wandio_printf(state->file, "\n") == -1) {
      fprintf(stderr, "ERROR: Could not write data to file\n");
      return;
    }

    // Update variables and store submoas struct
  }

  bgpstream_patricia_tree_result_set_destroy(&res_set);
}

// In case of an origin asn being eemoved of a subprefix, check whether that
// prefix is still a submoas or not.
static int check_submoas_over(bvc_t *consumer, submoas_prefix_t submoas_struct,
                              bgpstream_pfx_t superprefix)
{
  bvc_submoas_state_t *state = STATE;
  bgpstream_patricia_node_t *pfx_node = bgpstream_patricia_tree_search_exact(
    state->pt, &superprefix);
  pref_info_t *pref_info = bgpstream_patricia_tree_get_user(pfx_node);
  int same_origin = 0;
  int i, j;
  for (i = 0; i < submoas_struct.number_of_subasns; i++) {
    for (j = 0; j < pref_info->number_of_asns; j++) {
      if (pref_info->origin_asns[j].asn == submoas_struct.submoases[i].subasn) {
        same_origin = 1;
      }
    }
  }
  return same_origin;
}

/* Check if origin being removed requires any change in existing submoases */
static void check_remove_submoas_asn(bvc_t *consumer, const bgpstream_pfx_t *pfx,
                                     uint32_t asn)
{
  khint_t k, p, j;
  submoas_prefix_t submoas_struct;
  bvc_submoas_state_t *state = STATE;
  char pfx_str[INET6_ADDRSTRLEN + 3];
  char pfx2_str[INET6_ADDRSTRLEN + 3];
  char asn_buffer[MAX_BUFFER_LEN];
  char prev_category[MAX_BUFFER_LEN];
  strcpy(prev_category, "default");
  bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx);
  uint32_t timenow = state->time_now;
  int sub_as;

  /* Check whether preifx under consideration is a subprefix or not */
  p = kh_get(subprefix_map, state->subprefix_map, *pfx);
  if (p != kh_end(state->subprefix_map)) {
    submoas_struct = kh_value(state->subprefix_map, p);
    if (submoas_struct.number_of_subasns == 0) {
      return;
    }
  } else {
    // submoas doesnt exist
    return;
  }
  sub_as = 0;
  bgpstream_pfx_t subprefix = submoas_struct.subprefix;
  int i;
  for (i = 0; i < submoas_struct.number_of_subasns; i++) {

    if (submoas_struct.submoases[i].subasn != asn) {
      continue;
    }
    bgpstream_pfx_t superprefix = submoas_struct.superprefix;
    sub_as = 1;
    // removing superprefix
    k = kh_get(superprefix_map, state->superprefix_map, superprefix);
    if (k != kh_end(state->superprefix_map)) {
      // this khash has all subprefixes for given super
      subprefixes_in_superprefix_t *subprefixes_in_superprefix =
        kh_value(state->superprefix_map, k);
      // finding our subprefix from all subprefixes of given superprefix
      j = kh_get(subprefixes_in_superprefix, subprefixes_in_superprefix,
                 subprefix);
      // bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN+3, &superprefix);

      submoas_struct.submoases[i] =
        submoas_struct.submoases[submoas_struct.number_of_subasns - 1];
      i--;
      submoas_struct.number_of_subasns--;
      /* Check whether removing ASN from subprefix results in submoas being
       * finished or not */
      if (check_submoas_over(consumer, submoas_struct, superprefix) ||
          submoas_struct.number_of_subasns == 0) {
        kh_del(subprefixes_in_superprefix, subprefixes_in_superprefix, j);
        submoas_struct.number_of_subasns = 0;
      }
      // printing info for submoas removed when an asn is removed
      state->finished_submoas_pfxs_count++;
      uint32_t first_seen_ts = submoas_struct.first_seen;
      submoas_struct.end = timenow;
      if (wandio_printf(
            state->file, "%" PRIu32 "|%s|%s|FINISHED|%" PRIu32 "|%" PRIu32
                         "|%" PRIu32 "|%s    \n",
            timenow, bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3,
                                            &superprefix),
            bgpstream_pfx_snprintf(
              pfx2_str, INET6_ADDRSTRLEN + 3, &submoas_struct.subprefix),
            first_seen_ts, submoas_struct.start, submoas_struct.end,
            print_submoas_info(4, consumer, &superprefix,
                               &submoas_struct.subprefix,
                               asn_buffer, MAX_BUFFER_LEN)) == -1) {
        fprintf(stderr, "ERROR: Could not write %s file\n", state->filename);
        return;
      }
    }
  }

  kh_value(state->subprefix_map, p) = submoas_struct;
  if (!sub_as) {
    return;
  }
  /* Check if after removing ASNs if there is still any origin ASN left for the
   * subprefix. If it's then start as new submoas */
  if (submoas_struct.number_of_subasns > 0) {
    char category[MAX_BUFFER_LEN];
    /* Checking last starting time, if it's between current window that it's a
     * NEWREC event */
    if (submoas_struct.start != timenow) {
      if (submoas_struct.start + state->window_size > timenow) {
        strcpy(category, "NEWREC");
        strcpy(prev_category, "NEWREC");
        state->newrec_submoas_pfxs_count++;
      } else {
        strcpy(category, "NEW");
        strcpy(prev_category, "NEW");
        state->new_submoas_pfxs_count++;
      }
    } else {
      strcpy(category, prev_category);
      if (!strcmp(prev_category, "default")) {
        strcpy(category, "NEWREC");
        state->newrec_submoas_pfxs_count++;
      }
    }

    submoas_struct.start = timenow;
    bgpstream_pfx_t *sub_pfx;
    sub_pfx = &submoas_struct.subprefix;
    if (bgpview_iter_seek_pfx(state->iter, sub_pfx, BGPVIEW_FIELD_ALL_VALID) ==
        0) {
      return;
    }

    kh_value(state->subprefix_map, p) = submoas_struct;
    if (wandio_printf(
          state->file,
          "%" PRIu32 "|%s|%s|%s|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|%s",
          timenow, bgpstream_pfx_snprintf(
                     pfx_str, INET6_ADDRSTRLEN + 3,
                     &submoas_struct.superprefix),
          bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN + 3,
                                 &submoas_struct.subprefix),
          category, submoas_struct.first_seen, submoas_struct.start,
          submoas_struct.end,
          print_submoas_info(5, consumer,
                             &submoas_struct.superprefix,
                             &submoas_struct.subprefix,
                             asn_buffer, MAX_BUFFER_LEN)) == -1) {
      fprintf(stderr, "ERROR: Could not write %s file\n", state->filename);
      return;
    }
    if (print_pfx_peers(consumer, state, state->iter, sub_pfx) < 0)
      return;
    if (wandio_printf(state->file, "\n") == -1) {
      fprintf(stderr, "ERROR: Could not write data to file\n");
      return;
    }
  }
}

/*
void diag_check(bvc_t* consumer){
  bvc_submoas_state_t *state=STATE;
  char pfx_str[MAX_BUFFER_LEN];
  char pfx2_str[MAX_BUFFER_LEN];
  khint_t k;
   for (k = kh_begin(state->subprefix_map); k != kh_end(state->subprefix_map);
++k) {
      if (kh_exist(state->subprefix_map,k)){
        bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN+3,
          &kh_key(state->subprefix_map,k));
        bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN+3,
          &kh_value(state->subprefix_map,k).superprefix);
        if(!strcmp(pfx_str,"78.108.173.0/24")){
          printf("existed with %s \n",pfx2_str);
          submoas_prefix_t submoas_struct=kh_value(state->subprefix_map,k);
          if(!strcmp(pfx2_str,"78.108.172.0/23")){
          if(submoas_struct.number_of_subasns>0){
            printf("chutyappp \n");
            exit(0);
            }
            break;
        }
        }
      }
    }
}
*/

/* When a prefix is removed , check it it's a superprefix. if so then decide
 * whether its subprefixes are still part of submoas or not*/
static void check_remove_superprefix(bvc_t *consumer, const bgpstream_pfx_t *pfx)
{
  bvc_submoas_state_t *state = STATE;
  khint_t k, j, n, m;
  khint_t p;
  int ret;
  char prev_category[MAX_BUFFER_LEN];
  uint32_t timenow = state->time_now;
  bgpstream_patricia_tree_result_set_t *res =
    bgpstream_patricia_tree_result_set_create();
  snprintf(state->filename, MAX_BUFFER_LEN, OUTPUT_FILE_FORMAT,
           state->output_folder, timenow, state->current_window_size);
  char pfx_str[INET6_ADDRSTRLEN + 3];
  char asn_buffer[MAX_BUFFER_LEN];
  char pfx2_str[INET6_ADDRSTRLEN + 3];
  k = kh_get(superprefix_map, state->superprefix_map, *pfx);

  /* This prefix is not superprefix of anyone. Return */
  if (k == kh_end(state->superprefix_map)) {
    /* prefix is not superprefix */
    bgpstream_patricia_tree_result_set_destroy(&res);
    return;
  }
  bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN + 3, pfx);
  /*It's a superprefix.
    Find all the subprefixes */
  subprefixes_in_superprefix_t *super1_khash =
    kh_value(state->superprefix_map, k);

  int is_subprefix = 0;
  j = kh_get(subprefix_map, state->subprefix_map, *pfx);
  /* This superprefix might be a subprefix for some other superprefix */
  if (j != kh_end(state->subprefix_map) &&
      kh_value(state->subprefix_map, j).number_of_subasns > 0) {
    is_subprefix = 1;
  }
  if (is_subprefix) {

    bgpstream_pfx_t super_super_pfx =
      kh_value(state->subprefix_map, j).superprefix;

    p = kh_get(superprefix_map, state->superprefix_map, super_super_pfx);
    bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, &super_super_pfx);

    subprefixes_in_superprefix_t *super2_khash =
      kh_value(state->superprefix_map, p);
    for (n = kh_begin(super1_khash); n != kh_end(super1_khash); ++n) {
      if (kh_exist(super1_khash, n)) {
        kh_put(subprefixes_in_superprefix, super2_khash,
               kh_key(super1_khash, n), &ret);
        m =
          kh_get(subprefix_map, state->subprefix_map, kh_key(super1_khash, n));
        submoas_prefix_t submoas_struct = kh_value(state->subprefix_map, m);
        submoas_struct.superprefix = super_super_pfx;
        state->finished_submoas_pfxs_count++;
        uint32_t first_seen_ts = submoas_struct.first_seen;
        if (wandio_printf(
              state->file, "%" PRIu32 "|%s|%s|FINISHED|%" PRIu32 "|%" PRIu32
                           "|%" PRIu32 "|%s    \n",
              timenow,
              bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx),
              bgpstream_pfx_snprintf(
                pfx2_str, INET6_ADDRSTRLEN + 3,
                &submoas_struct.subprefix),
              first_seen_ts, submoas_struct.start, timenow,
              print_submoas_info(6, consumer, pfx,
                                 &submoas_struct.subprefix,
                                 asn_buffer, MAX_BUFFER_LEN)) == -1) {
          fprintf(stderr, "ERROR: Could not write %s file\n", state->filename);
          return;
        }

        char category[MAX_BUFFER_LEN];
        if (submoas_struct.start != timenow) {
          if (submoas_struct.start + state->window_size > timenow) {
            strcpy(category, "NEWREC");
            strcpy(prev_category, "NEWREC");
            state->newrec_submoas_pfxs_count++;
          } else {
            strcpy(category, "NEW");
            strcpy(prev_category, "NEW");
            state->new_submoas_pfxs_count++;
          }
        } else {
          strcpy(category, prev_category);
        }
        submoas_struct.end = timenow;
        submoas_struct.start = timenow;
        kh_value(state->subprefix_map, m) = submoas_struct;

        if (wandio_printf(
              state->file,
              "%" PRIu32 "|%s|%s|%s|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|%s",
              timenow,
              bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3,
                                     &super_super_pfx),
              bgpstream_pfx_snprintf(
                pfx2_str, INET6_ADDRSTRLEN + 3,
                &submoas_struct.subprefix),
              category, first_seen_ts, submoas_struct.start,
              submoas_struct.first_seen, // We're printing value of first seen
                                         // as end time for a new event
              print_submoas_info(7, consumer,
                                 &super_super_pfx,
                                 &submoas_struct.subprefix,
                                 asn_buffer, MAX_BUFFER_LEN)) == -1) {
          fprintf(stderr, "ERROR: Could not write %s file\n", state->filename);
          return;
        }
        bgpstream_pfx_t *sub_pfx;
        sub_pfx = &submoas_struct.subprefix;
        if (bgpview_iter_seek_pfx(state->iter, sub_pfx,
                                  BGPVIEW_FIELD_ALL_VALID) == 0) {
          continue;
        }

        if (print_pfx_peers(consumer, state, state->iter, sub_pfx) < 0)
          return;
        if (wandio_printf(state->file, "\n") == -1) {
          fprintf(stderr, "ERROR: Could not write data to file\n");
          return;
        }
      }
    }
    kh_value(state->superprefix_map, p) = super2_khash;

    kh_destroy(subprefixes_in_superprefix, super1_khash);
    kh_del(superprefix_map, state->superprefix_map, k);

  }

  /* This prefix is not subprefix of anyone.
     Free subprefixes and remove the superprefix from superfix hash table */
  else {
    /* Looping through all the subprefixes and finishing them */
    for (n = kh_begin(super1_khash); n != kh_end(super1_khash); ++n) {
      if (kh_exist(super1_khash, n)) {
        m =
          kh_get(subprefix_map, state->subprefix_map, kh_key(super1_khash, n));
        submoas_prefix_t submoas_struct = kh_value(state->subprefix_map, m);
        uint32_t first_seen_ts = submoas_struct.first_seen;
        state->finished_submoas_pfxs_count++;
        if (wandio_printf(
              state->file, "%" PRIu32 "|%s|%s|FINISHED|%" PRIu32 "|%" PRIu32
                           "|%" PRIu32 "|%s    \n",
              timenow,
              bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx),
              bgpstream_pfx_snprintf(
                pfx2_str, INET6_ADDRSTRLEN + 3,
                &submoas_struct.subprefix),
              first_seen_ts, submoas_struct.start, timenow,
              print_submoas_info(8, consumer, pfx,
                                 &submoas_struct.subprefix,
                                 asn_buffer, MAX_BUFFER_LEN)) == -1) {
          fprintf(stderr, "ERROR: Could not write %s file\n", state->filename);
          return;
        }
        /* Changing the superprefix and putting them back */
        submoas_struct.number_of_subasns = 0;
        submoas_struct.end = timenow;
        kh_value(state->subprefix_map, m) = submoas_struct;
      }
    }
    // removing value of superprefix */
    kh_destroy(subprefixes_in_superprefix, super1_khash);
    // removing superprefix entry from superprefix map */
    kh_del(superprefix_map, state->superprefix_map, k);
  }

  bgpstream_patricia_tree_result_set_destroy(&res);
}

/* This function is called for each node in patricia.
   Removed stale prefixes and origin asns */
static bgpstream_patricia_walk_cb_result_t
rem_patricia(bgpstream_patricia_tree_t *pt,
             bgpstream_patricia_node_t *node, void *data)
{
  bvc_t *consumer = data;
  bvc_submoas_state_t *state = STATE;
  uint32_t time_now = state->time_now;
  char pfx2_str[INET6_ADDRSTRLEN + 3];
  int j;
  pref_info_t *this_prefix_info = bgpstream_patricia_tree_get_user(node);
  const bgpstream_pfx_t *pfx = bgpstream_patricia_tree_get_pfx(node);
  bgpstream_pfx_snprintf(pfx2_str, INET6_ADDRSTRLEN + 3, pfx);
  int num_orig_asns = this_prefix_info->number_of_asns;
  for (j = 0; j < num_orig_asns; j++) {
    if (this_prefix_info->origin_asns[j].last_seen + state->window_size <
        time_now) {

      /* Origin ASN is removed, check whether its prefix belongs to some prefix
       * involved in a submoas */
      check_remove_submoas_asn(consumer, pfx,
                               this_prefix_info->origin_asns[j].asn);
      if (num_orig_asns == 1) {
        num_orig_asns--;
        break;
      }
      this_prefix_info->origin_asns[j] =
        this_prefix_info->origin_asns[num_orig_asns - 1];
      num_orig_asns--;
      j--;
    }
  }
  /* Remove prefix from patricia tree if all of the ASNs are no longer visible
   */
  if (num_orig_asns == 0) {
    /* That prefix might be a superprefix
      Remove/update its subprefixes */
    check_remove_superprefix(consumer, pfx);
    bgpstream_patricia_tree_remove_node(pt, node);

  } else {
    /* Some origin ASNs are still visible. Update patricia tree */
    this_prefix_info->number_of_asns = num_orig_asns;
    bgpstream_patricia_tree_set_user(pt, node, this_prefix_info);
  }

  return BGPSTREAM_PATRICIA_WALK_CONTINUE;
}

/* Main driver function
   Processes views, computes submoas information and generates output files and
   metrics */
int bvc_submoas_process_view(bvc_t *consumer, bgpview_t *view)
{
  bvc_submoas_state_t *state = STATE;
  state->time_now = bgpview_get_time(view);
  state->arrival_delay = epoch_sec() - bgpview_get_time(view);
  uint32_t last_valid_ts = bgpview_get_time(view) - state->window_size;

  if (state->first_ts == 0) {
    state->first_ts = bgpview_get_time(view);
  }

  /* compute current window size*/
  if (last_valid_ts < state->first_ts) {
    state->current_window_size = bgpview_get_time(view) - state->first_ts;
  } else {
    state->current_window_size = state->window_size;
  }
  if (!(state->file = bvcu_open_outfile(state->filename, OUTPUT_FILE_FORMAT,
        state->output_folder, state->time_now, state->current_window_size))) {
    return -1;
  }

  uint32_t time_now = state->time_now;
  int new_prefix;
  int atleast_one = 0;
  bgpview_iter_t *it;
  bgpstream_pfx_t *pfx;

  bgpstream_peer_id_t peerid;
  int ipv_idx;
  bgpstream_as_path_seg_t *origin_seg;
  uint32_t origin_asn;
  // int i;
  //  uint32_t visibility;
  uint32_t last_origin_asn;
  // uint32_t last_origin_ind;
  uint32_t peers_cnt;
  bgpstream_patricia_node_t *pfx_node;
  // Initializing timeseries variables */
  state->ongoing_submoas_pfxs_count = 0;
  state->finished_submoas_pfxs_count = 0;
  state->new_submoas_pfxs_count = 0;
  state->newrec_submoas_pfxs_count = 0;
  int count;
  count = 0;

  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  } /* iterate through all prefixes */
  state->iter = it;
  for (bgpview_iter_first_pfx(it, 0 /* all versions */, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {
    count++;
    if (count == 5) {
      // break;
    }

    pfx = bgpview_iter_pfx_get_pfx(it);

    /* ignore prefixes in blacklist */
    if (bgpstream_pfx_set_exists(state->blacklist_pfxs, pfx)) {
      continue;
    }

    pref_info_t *this_prefix_info;
    int new_asn_seen;
    new_asn_seen = 0;
    new_prefix = 0;
    atleast_one = 0;

    ipv_idx = bgpstream_ipv2idx(pfx->address.version);
    peers_cnt = 0;
    last_origin_asn = -1;
    // last_origin_ind= -1;
    // visibility=0;
    char pfx_str[INET6_ADDRSTRLEN + 3];
    bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx);
    pfx_node = bgpstream_patricia_tree_search_exact(state->pt, pfx);
    // printf("pref %s \n",pfx_str);

    /*Checking whether prefix exists in patricia tree */
    if (pfx_node != NULL) {
      this_prefix_info = bgpstream_patricia_tree_get_user(pfx_node);
    } else {
      /* New prefix found. Allocate memory */
      new_prefix = 1;
      this_prefix_info = malloc(sizeof *this_prefix_info);
      this_prefix_info->number_of_asns = 0;
      this_prefix_info->start = time_now;
    }

    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(it); bgpview_iter_pfx_next_peer(it)) {
      /* only consider peers that are full-feed */
      peerid = bgpview_iter_peer_get_peer_id(it);
      if (bgpstream_id_set_exists(
            BVC_GET_CHAIN_STATE(consumer)->full_feed_peer_ids[ipv_idx],
            peerid)) {
        /* get origin asn */
        if ((origin_seg = bgpview_iter_pfx_peer_get_origin_seg(it)) == NULL) {
          return -1;
        }
        /* we do not consider sets and confederations for the moment */
        /* TODO (extend the code to deal with segments */
        if (origin_seg->type != BGPSTREAM_AS_PATH_SEG_ASN) {

          continue;
        }
        // printf("%p",(void)*buf2);

        // printf ("already present \n");

        atleast_one = 1;
        origin_asn = ((bgpstream_as_path_seg_asn_t *)origin_seg)->asn;
        /* first entry in the row */
        if (peers_cnt == 0) {
          last_origin_asn = origin_asn;
          // origin_asn[peers_cnt]=origin_asn;
          int j;
          bool found;
          found = 0;
          /* Loop through all asns and update timestamps */
          for (j = 0; j < this_prefix_info->number_of_asns; j++) {
            if (this_prefix_info->origin_asns[j].asn == origin_asn) {
              found = 1;
              this_prefix_info->origin_asns[j].last_seen = time_now;
              // last_origin_ind=j;
              break;
            }
          }
          /* New ASN found for a prefix */
          if (found == 0) {
            new_asn_seen = 1;
            this_prefix_info->origin_asns[this_prefix_info->number_of_asns]
              .asn = origin_asn;
            this_prefix_info->origin_asns[this_prefix_info->number_of_asns]
              .last_seen = time_now;
            // last_origin_ind=this_prefix_info->number_of_asns;
            this_prefix_info->number_of_asns++;
          }
          peers_cnt = 1;
        } else {
          if (origin_asn != last_origin_asn) {
            int j;
            bool found;
            found = 0;

            for (j = 0; j < this_prefix_info->number_of_asns; j++) {
              if (this_prefix_info->origin_asns[j].asn == origin_asn) {
                found = 1;
                this_prefix_info->origin_asns[j].last_seen = time_now;

                break;
              }
            }
            if (found == 0) {
              new_asn_seen = 1;
              this_prefix_info->origin_asns[this_prefix_info->number_of_asns]
                .asn = origin_asn;
              this_prefix_info->origin_asns[this_prefix_info->number_of_asns]
                .last_seen = time_now;
              this_prefix_info->number_of_asns++;
            }
            // peers_cnt+=1;
          }
        }

      } // full_feed if
    }   // peers if
    if (atleast_one == 1) {
      /* At least one ASN is seen */
      /* Check whether prefix was seen before */
      if (new_prefix == 0) {
        /* Check if a new ASN is observed. if yes, Add it to patricia tree */
        /* Otherwise, update last_seens of existing origin asns */
        pfx_node = bgpstream_patricia_tree_search_exact(state->pt, pfx);
        bgpstream_patricia_tree_set_user(state->pt, pfx_node, this_prefix_info);
        if (new_asn_seen) {
          /* Update/create submoas hashtable*/
          update_patricia(consumer, pfx_node, this_prefix_info, it, time_now);
          // update_patricia(consumer,pfx_node,this_prefix_info, time_now);
        }
      }
      /* New prefix is observed */
      if (new_prefix) {
        if ((pfx_node = bgpstream_patricia_tree_insert(state->pt, pfx)) ==
            NULL) {
          printf("Failed to insert\n");
          printf("err returning\n");
          return -1;
        }
        bgpstream_patricia_tree_set_user(state->pt, pfx_node, this_prefix_info);
        update_patricia(consumer, pfx_node, this_prefix_info, it, time_now);
        // update_patricia(consumer,pfx_node,this_prefix_info, time_now);
      }
    } else {
      /* Struct was created but not put into patricia. Free memory */
      if (new_prefix) {
        free(this_prefix_info);
      }
    }

  } // prefix if

  /* Check for each node in patricia and remove stale asns, prefixes */
  bgpstream_patricia_tree_walk(state->pt, rem_patricia, consumer);

  print_ongoing(consumer);

  /* Write all ongoing moas to file */

  /* Close outuput file */
  wandio_wdestroy(state->file);
  /* generate the .done file */
  bvcu_create_donefile(state->filename);
  /* compute processed delay */
  state->processed_delay = epoch_sec() - bgpview_get_time(view);
  state->processing_time = state->processed_delay - state->arrival_delay;

  /* Output timeseries meterics */
  if (output_timeseries(consumer, bgpview_get_time(view)) != 0) {
    return -1;
  }

  bgpview_iter_destroy(it);
  return 0;
}
