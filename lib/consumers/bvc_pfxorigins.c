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

#include "bvc_pfxorigins.h"
#include "bgpview_consumer_interface.h"
#include "bgpview_consumer_utils.h"
#include "bgpstream_utils_pfx_set.h"
#include "khash.h"
#include "utils.h"
#include <wandio.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define NAME "pfx-origins"
#define CONSUMER_METRIC_PREFIX "pfx-origins"

#define BUFFER_LEN 1024
#define METRIC_PREFIX_FORMAT "%s." CONSUMER_METRIC_PREFIX ".%s"
#define META_METRIC_PREFIX_FORMAT "%s.meta.bgpview.consumer." NAME ".%s"

/** Default output folder */
#define DEFAULT_OUTPUT_FOLDER "./"

/* IPv4 default route */
#define IPV4_DEFAULT_ROUTE "0.0.0.0/0"

/* IPv6 default route */
#define IPV6_DEFAULT_ROUTE "0::/0"

/** Default maximum number of unique origins */
#define MAX_ORIGIN_AS_CNT 65535

#define ARRAY_SIZE_INCR 64

/** MAX LEN of AS PATH SEGMENT STR  */
#define MAX_ASPATH_SEGMENT_STR 255 * 16

#define STATE (BVC_GET_STATE(consumer, pfxorigins))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_pfxorigins = {BVC_ID_PFXORIGINS, NAME,
                               BVC_GENERATE_PTRS(pfxorigins)};

typedef struct struct_origin_info_t {

  /* number of unique origins */
  uint16_t num_asns;

  uint16_t array_size;

  /* array of origins */
  bgpstream_as_path_seg_t **origin_asns;

} origin_info_t;

typedef struct struct_origin_status_t {

  /* previous origins announcing the prefix */
  origin_info_t previous;

  /* current origins announcing the prefix */
  origin_info_t current;

} origin_status_t;

/** Map <prefix,origin_status>
 */
KHASH_INIT(bwv_pfx_origin, bgpstream_pfx_t, origin_status_t, 1,
           bgpstream_pfx_hash_val, bgpstream_pfx_equal_val)
typedef khash_t(bwv_pfx_origin) bwv_pfx_origin_t;

/* our 'instance' */
typedef struct bvc_pfxorigins_state {

  /** ts when the view arrived */
  uint32_t arrival_delay;
  /** ts when the view processing finished */
  uint32_t processed_delay;
  /** processing time */
  uint32_t processing_time;

  /** blacklist prefixes */
  bgpstream_pfx_set_t *blacklist_pfxs;

  /** output folder */
  char output_folder[PATH_MAX];

  /* maintain the origin ASns for each observed prefix */
  bwv_pfx_origin_t *pfx_origins;

  /** Timeseries Key Package */
  timeseries_kp_t *kp;

  /** Metrics indices */
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;

  int new_routed_pfxs_idx;
  int removed_pfxs_idx;
  int changing_pfxs_idx;
  int stable_pfxs_idx;

} bvc_pfxorigins_state_t;

/* ==================== CONSUMER INTERNAL FUNCTIONS ==================== */

/** Update the pfx_origins map and print the status to file  */

static int process_origin_state(bvc_t *consumer, uint32_t current_view_ts)
{
  bvc_pfxorigins_state_t *state = STATE;

  iow_t *f = NULL;
  char filename[BVCU_PATH_MAX];
  // char buffer_str[BUFFER_LEN];
  char origin_str[MAX_ASPATH_SEGMENT_STR];
  char pfx_str[INET6_ADDRSTRLEN + 3];

  /* open file for writing */
  if (!(f = bvcu_open_outfile(filename, "%s/" NAME ".%" PRIu32 ".gz",
      state->output_folder, current_view_ts))) {
    return -1;
  }

  khiter_t k;
  int i, j;
  origin_status_t *os;
  bgpstream_pfx_t *pfx;
  uint8_t differ;
  uint8_t found;

  int changing_pfxs = 0;
  int new_routed_pfxs = 0;
  int removed_pfxs = 0;
  int stable_pfxs = 0;

  /* for each prefix */
  for (k = kh_begin(state->pfx_origins); k != kh_end(state->pfx_origins); k++) {
    if (kh_exist(state->pfx_origins, k)) {
      pfx = &kh_key(state->pfx_origins, k);
      bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx);

      os = &kh_val(state->pfx_origins, k);
      differ = 0;

      /* check if current and previous differ */
      if (os->current.num_asns == os->previous.num_asns) {
        for (i = 0; i < os->current.num_asns; i++) {
          found = 0;
          for (j = 0; j < os->previous.num_asns; j++) {
            if (bgpstream_as_path_seg_equal(os->current.origin_asns[i],
                                            os->previous.origin_asns[j]) != 0) {
              found = 1;
              break;
            }
          }
          /* if at least one asn doesn't match, we exit */
          if (!found) {
            differ = 1;
            break;
          }
        }
      } else {
        differ = 1;
      }

      /* ts | prefix | origin before | origin after | category */
      wandio_printf(f, "%" PRIu32 "|%s|", current_view_ts, pfx_str);
      for (i = 0; i < os->previous.num_asns; i++) {
        // printing origin segments
        if (bgpstream_as_path_seg_snprintf(origin_str, MAX_ASPATH_SEGMENT_STR,
                                           os->previous.origin_asns[i]) >=
            MAX_ASPATH_SEGMENT_STR) {
          fprintf(stderr, "Could not write segment string correctly\n");
          return -1;
        }
        wandio_printf(f, "%s", origin_str);
        if (i < os->previous.num_asns - 1) {
          wandio_printf(f, " ");
        }
      }
      wandio_printf(f, "|");
      for (i = 0; i < os->current.num_asns; i++) {
        if (bgpstream_as_path_seg_snprintf(origin_str, MAX_ASPATH_SEGMENT_STR,
                                           os->current.origin_asns[i]) >=
            MAX_ASPATH_SEGMENT_STR) {
          fprintf(stderr, "Could not write segment string correctly\n");
          return -1;
        }
        wandio_printf(f, "%s", origin_str);
        if (i < os->current.num_asns - 1) {
          wandio_printf(f, " ");
        }
      }

      wandio_printf(f, "|");
      /* if differ == 0, the origin set remained the same, therefore we do not
       * need to
       * update the previous set */
      if (differ == 0) {
        wandio_printf(f, "STABLE\n");
        stable_pfxs++;

        /* if things did not change, we just reset the current */
        for (i = 0; i < os->current.num_asns; i++) {
          bgpstream_as_path_seg_destroy(os->current.origin_asns[i]);
        }
	free(os->current.origin_asns);
      } else {

        /* if the prefix disappeared, we remove it from
         * the structure */
        if (os->current.num_asns == 0) {
          wandio_printf(f, "REMOVED\n");
          kh_del(bwv_pfx_origin, state->pfx_origins, k);
          removed_pfxs++;
        } else {
          if (os->previous.num_asns == 0) {
            wandio_printf(f, "NEWROUTED\n");
            new_routed_pfxs++;
          } else {
            wandio_printf(f, "CHANGED\n");
            changing_pfxs++;
          }
        }

        for (i = os->current.num_asns; i < os->previous.num_asns; i++) {
          if (os->previous.origin_asns[i] != NULL) {
            bgpstream_as_path_seg_destroy(os->previous.origin_asns[i]);
            os->previous.origin_asns[i] = NULL;
          }
        }
	free(os->previous.origin_asns);
	os->previous.origin_asns = os->current.origin_asns;

        /* update previous counter */
        os->previous.num_asns = os->current.num_asns;
	os->previous.array_size = os->current.array_size;

      }
      // reset current (no destroy is called, objects are in previous now)
      os->current.origin_asns = calloc(ARRAY_SIZE_INCR,
			  sizeof(bgpstream_as_path_seg_t *));
      os->current.array_size = ARRAY_SIZE_INCR;
      os->current.num_asns = 0;
    }
  }

  /* Close file and generate .done if new information was printed */
  wandio_wdestroy(f);

  /* generate the .done file */
  bvcu_create_donefile(filename);

  /* fprintf(stdout, "%"PRIu32" same: %d changed: %d appeared: %d disappeared:
   * %d\n", current_view_ts, same, changed, appeared, disappeared);   */

  timeseries_kp_set(state->kp, state->new_routed_pfxs_idx, new_routed_pfxs);
  timeseries_kp_set(state->kp, state->removed_pfxs_idx, removed_pfxs);
  timeseries_kp_set(state->kp, state->changing_pfxs_idx, changing_pfxs);
  timeseries_kp_set(state->kp, state->stable_pfxs_idx, stable_pfxs);

  return 0;
}

/** Create timeseries metrics */

static int create_ts_metrics(bvc_t *consumer)
{

  char buffer[BUFFER_LEN];
  bvc_pfxorigins_state_t *state = STATE;

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "arrival_delay");
  if ((state->arrival_delay_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "processed_delay");
  if ((state->processed_delay_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "processing_time");
  if ((state->processing_time_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT, CHAIN_STATE->metric_prefix,
           "new_routed_pfxs_cnt");
  if ((state->new_routed_pfxs_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT, CHAIN_STATE->metric_prefix,
           "removed_pfxs_cnt");
  if ((state->removed_pfxs_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT, CHAIN_STATE->metric_prefix,
           "changed_origin_pfxs_cnt");
  if ((state->changing_pfxs_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  snprintf(buffer, BUFFER_LEN, METRIC_PREFIX_FORMAT, CHAIN_STATE->metric_prefix,
           "stable_origin_pfxs_cnt");
  if ((state->stable_pfxs_idx = timeseries_kp_add_key(STATE->kp, buffer)) ==
      -1) {
    return -1;
  }

  return 0;
}

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr, "consumer usage: %s\n"
                  "       -o <path>             output folder (default: %s)\n",
          consumer->name, DEFAULT_OUTPUT_FOLDER);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  assert(argc > 0 && argv != NULL);

  bvc_pfxorigins_state_t *state = STATE;

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, ":o:?")) >= 0) {
    switch (opt) {
    case 'o':
      strncpy(state->output_folder, optarg, BUFFER_LEN - 1);
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

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_pfxorigins_alloc()
{
  return &bvc_pfxorigins;
}

int bvc_pfxorigins_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_pfxorigins_state_t *state = NULL;
  bgpstream_pfx_t pfx;

  if ((state = malloc_zero(sizeof(bvc_pfxorigins_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  /* defaults here */
  strncpy(state->output_folder, DEFAULT_OUTPUT_FOLDER, BUFFER_LEN);

  state->pfx_origins = NULL;

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  /* react to args here */
  fprintf(stderr, "INFO: output folder: %s\n", state->output_folder);

  /* init */
  if ((state->pfx_origins = kh_init(bwv_pfx_origin)) == NULL) {
    goto err;
  }

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

  if ((state->kp = timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) ==
      NULL) {
    fprintf(stderr, "Error: Could not create timeseries key package\n");
    goto err;
  }

  if (create_ts_metrics(consumer) != 0) {
    goto err;
  }

  if (BVC_GET_CHAIN_STATE(consumer)->visibility_computed == 0) {
    fprintf(stderr, "ERROR: pfx-origins requires the Visibility consumer "
                    "to be run first\n");
    goto err;
  }

  return 0;

err:
  bvc_pfxorigins_destroy(consumer);
  return -1;
}

static void origins_free(origin_status_t os)
{
  int i;
  for (i = 0; i < os.previous.num_asns; i++) {
    if (os.previous.origin_asns[i] != NULL) {
      bgpstream_as_path_seg_destroy(os.previous.origin_asns[i]);
    }
  }
  for (i = 0; i < os.current.num_asns; i++) {
    if (os.current.origin_asns[i] != NULL) {
      bgpstream_as_path_seg_destroy(os.current.origin_asns[i]);
    }
  }
  free(os.previous.origin_asns);
  free(os.current.origin_asns);
}

void bvc_pfxorigins_destroy(bvc_t *consumer)
{
  bvc_pfxorigins_state_t *state = STATE;

  if (state != NULL) {
    if (state->pfx_origins != NULL) {
      kh_free_vals(bwv_pfx_origin, state->pfx_origins, origins_free);
      kh_destroy(bwv_pfx_origin, state->pfx_origins);
    }

    if (state->blacklist_pfxs != NULL) {
      bgpstream_pfx_set_destroy(state->blacklist_pfxs);
    }

    timeseries_kp_free(&state->kp);
    free(state);
    BVC_SET_STATE(consumer, NULL);
  }
}

int bvc_pfxorigins_process_view(bvc_t *consumer, bgpview_t *view)
{
  bvc_pfxorigins_state_t *state = STATE;

  bgpstream_pfx_t *pfx;

  bgpstream_peer_id_t peerid;
  int ipv_idx;
  bgpstream_as_path_seg_t *origin_seg;

  /* iterate through the pfxorigins map*/
  khiter_t k;
  int khret;

  origin_status_t *os;
  uint8_t found;
  int i;
  uint32_t current_view_ts = bgpview_get_time(view);

  bgpview_iter_t *it;

  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  /* compute arrival delay */
  state->arrival_delay = epoch_sec() - bgpview_get_time(view);

  /* iterate through all prefixes */
  for (bgpview_iter_first_pfx(it, 0 /* all versions */, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {
    pfx = bgpview_iter_pfx_get_pfx(it);

    /* ignore prefixes in blacklist */
    if (bgpstream_pfx_set_exists(state->blacklist_pfxs, pfx)) {
      continue;
    }

    ipv_idx = bgpstream_ipv2idx(pfx->address.version);
    os = NULL;

    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(it); bgpview_iter_pfx_next_peer(it)) {
      /* only consider peers that are full-feed */
      peerid = bgpview_iter_peer_get_peer_id(it);

      if (bgpstream_id_set_exists(
            BVC_GET_CHAIN_STATE(consumer)->full_feed_peer_ids[ipv_idx],
            peerid)) {
        /* get origin segment  */
        if ((origin_seg = bgpview_iter_pfx_peer_get_origin_seg(it)) == NULL) {
          return -1;
        }

        /* the first time we find an origin to account, we insert the prefix in
         * the map
         * (if it is not there yet) and get the corresponding origin status */
        if (os == NULL) {

          /* check if prefix is already in the map */
          if ((k = kh_get(bwv_pfx_origin, state->pfx_origins, *pfx)) ==
              kh_end(state->pfx_origins)) {
            /* if not insert the prefix and init the origin arrays */
            k = kh_put(bwv_pfx_origin, state->pfx_origins, *pfx, &khret);
            os = &kh_value(state->pfx_origins, k);
            os->previous.num_asns = 0;
            os->previous.array_size = ARRAY_SIZE_INCR;
            os->current.num_asns = 0;
            os->current.array_size = ARRAY_SIZE_INCR;
	    os->current.origin_asns = calloc(ARRAY_SIZE_INCR,
			   sizeof(bgpstream_as_path_seg_t *));
	    os->previous.origin_asns = calloc(ARRAY_SIZE_INCR,
			   sizeof(bgpstream_as_path_seg_t *));
          }
          /* get os pointer */
          os = &kh_value(state->pfx_origins, k);
        }

        /* insert the AS origin in the data structure */
        found = 0;
        for (i = 0; i < os->current.num_asns; i++) {

          if (bgpstream_as_path_seg_equal(os->current.origin_asns[i],
                                          origin_seg) != 0) {
            found = 1;
            break;
          }
        }

        if (!found) {
	  if (os->current.num_asns >= os->current.array_size - 1) {
	      char pfxstr[1024];
              os->current.origin_asns = realloc(os->current.origin_asns,
			      (os->current.array_size + ARRAY_SIZE_INCR) *
			       sizeof(bgpstream_as_path_seg_t *));

	      os->current.array_size += ARRAY_SIZE_INCR;
	      bgpstream_pfx_snprintf(pfxstr, 1024, pfx);
	      fprintf(stderr, "Resizing %s origin ASN array to %d\n",
			      pfxstr, os->current.array_size);
	  }
          os->current.origin_asns[os->current.num_asns] =
            bgpstream_as_path_seg_dup(origin_seg);
          if (os->current.origin_asns[os->current.num_asns] == NULL) {
            fprintf(stderr, "Could not allocate memory for AS segment\n");
            return -1;
          }
          os->current.num_asns++;
          assert(os->current.num_asns != MAX_ORIGIN_AS_CNT);
        }
      }
    }
  }

  if (process_origin_state(consumer, current_view_ts) == -1) {
    return -1;
  }

  /* compute processed delay */
  state->processed_delay = epoch_sec() - bgpview_get_time(view);
  state->processing_time = STATE->processed_delay - STATE->arrival_delay;

  /* set remaining timeseries metrics */
  timeseries_kp_set(state->kp, state->arrival_delay_idx, state->arrival_delay);

  timeseries_kp_set(state->kp, state->processed_delay_idx,
                    state->processed_delay);

  timeseries_kp_set(state->kp, state->processing_time_idx,
                    state->processing_time);

  /* flush */
  if (timeseries_kp_flush(STATE->kp, current_view_ts) != 0) {
    fprintf(stderr, "Warning: could not flush %s %" PRIu32 "\n", NAME,
            bgpview_get_time(view));
  }

  return 0;
}
