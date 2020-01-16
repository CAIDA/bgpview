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

#include "bvc_moas.h"
#include "bgpview_consumer_interface.h"
#include "bgpstream_utils_pfx_set.h"
#include "khash.h"
#include "utils.h"
#include <wandio.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define NAME "moas"
#define CONSUMER_METRIC_PREFIX "moas"

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

/** Default compression level of output file */
#define DEFAULT_COMPRESS_LEVEL 6

/* IPv4 default route */
#define IPV4_DEFAULT_ROUTE "0.0.0.0/0"

/* IPv6 default route */
#define IPV6_DEFAULT_ROUTE "0::/0"

#define STATE (BVC_GET_STATE(consumer, moas))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_moas = {BVC_ID_MOAS, NAME, BVC_GENERATE_PTRS(moas)};

/* MOAS categories */
typedef enum moas_category {
  NEW = 0,
  NEWREC = 1,
  ONGOING = 2,
  FINISHED = 3,
} moas_category_t;

/** First time the moas was observed, start and end time */
typedef struct moas_properties {
  uint32_t first_seen;
  uint32_t start;
  uint32_t end;
} moas_properties_t;

/** List of origin ASns in a MOAS */
typedef struct moas_signature {
  uint32_t origins[MAX_UNIQUE_ORIGINS];
  uint8_t n;
} moas_signature_t;

/** MOAS signature hash function */
#if ULONG_MAX == ULLONG_MAX
static unsigned long
#else
static unsigned long long
#endif
moasinfo_map_hash(moas_signature_t ms)
{
  uint8_t i;
  uint32_t h = 0;
  for (i = 0; i < ms.n; i++) {
    h += ms.origins[i];
  }
  return h;
}

/** uint32 comparison function for qsort */
static int uint32_cmp(const void *a, const void *b)
{
  uint32_t *x = (uint32_t *)a;
  uint32_t *y = (uint32_t *)b;
  if (*x < *y) {
    return -1;
  }
  if (*x > *y) {
    return 1;
  }
  return 0;
}

/** MOAS signature equal function */
static int moasinfo_map_equal(moas_signature_t ms1, moas_signature_t ms2)
{
  if (ms1.n == ms2.n) {
    qsort(&ms1.origins, ms1.n, sizeof(uint32_t), uint32_cmp);
    qsort(&ms2.origins, ms2.n, sizeof(uint32_t), uint32_cmp);
    int i;
    for (i = 0; i < ms1.n; i++) {
      if (ms1.origins[i] != ms2.origins[i]) {
        return 0;
      }
    }
    return 1;
  }
  return 0;
}

/** Map <moas_sig,moas_properties>: store the timestamps
 *  for each MOAS in the current window */
KHASH_INIT(moasinfo_map, moas_signature_t, moas_properties_t, 1,
           moasinfo_map_hash, moasinfo_map_equal);
typedef khash_t(moasinfo_map) moasinfo_map_t;

/** Map <pfx,moas_info>: store information for each
 *  MOAS prefix in the current window */
KHASH_INIT(pfx_moasinfo_map, bgpstream_pfx_t, moasinfo_map_t *, 1,
           bgpstream_pfx_hash_val, bgpstream_pfx_equal_val);
typedef khash_t(pfx_moasinfo_map) pfx_moasinfo_map_t;

/* our 'instance' */
typedef struct bvc_moas_state {

  /** first processed timestamp */
  uint32_t first_ts;

  /** window size requested by user */
  uint32_t window_size;

  /** current window size (always <= window size) */
  uint32_t current_window_size;

  /** blacklist prefixes */
  bgpstream_pfx_set_t *blacklist_pfxs;

  /** wandio file handler */
  iow_t *wandio_fh;

  /** output folder */
  char output_folder[MAX_BUFFER_LEN];

  /** MOASes observed in the current window */
  pfx_moasinfo_map_t *current_moases;

  /** New/recurring/ongoing/finished MOAS prefixes count */
  uint32_t new_moas_pfxs_count;
  uint32_t new_recurring_moas_pfxs_count;
  uint32_t ongoing_moas_pfxs_count;
  uint32_t finished_moas_pfxs_count;

  /** diff ts when the view arrived */
  uint32_t arrival_delay;
  /** diff ts when the view processing finished */
  uint32_t processed_delay;
  /** processing time */
  uint32_t processing_time;

  /** Timeseries Key Package */
  timeseries_kp_t *kp;

  /** Metrics indices */
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;
  int current_window_size_idx;

  int new_moas_pfxs_count_idx;
  int new_recurring_moas_pfxs_count_idx;
  int ongoing_moas_pfxs_count_idx;
  int finished_moas_pfxs_count_idx;

} bvc_moas_state_t;

/** Print and update current moases */
static int output_timeseries(bvc_t *consumer, uint32_t ts)
{
  bvc_moas_state_t *state = STATE;
  timeseries_kp_set(state->kp, state->arrival_delay_idx, state->arrival_delay);
  timeseries_kp_set(state->kp, state->processed_delay_idx,
                    state->processed_delay);
  timeseries_kp_set(state->kp, state->processing_time_idx,
                    state->processing_time);

  timeseries_kp_set(state->kp, state->new_moas_pfxs_count_idx,
                    state->new_moas_pfxs_count);
  timeseries_kp_set(state->kp, state->new_recurring_moas_pfxs_count_idx,
                    state->new_recurring_moas_pfxs_count);
  timeseries_kp_set(state->kp, state->ongoing_moas_pfxs_count_idx,
                    state->ongoing_moas_pfxs_count);
  timeseries_kp_set(state->kp, state->finished_moas_pfxs_count_idx,
                    state->finished_moas_pfxs_count);

  timeseries_kp_set(state->kp, state->current_window_size_idx,
                    state->current_window_size);

  if (timeseries_kp_flush(state->kp, ts) != 0) {
    fprintf(stderr, "Warning: could not flush %s %" PRIu32 "\n", NAME, ts);
  }

  return 0;
}

/* init log */
static int init_output_log(bvc_t *consumer, uint32_t ts)
{

  bvc_moas_state_t *state = STATE;
  char filename[MAX_BUFFER_LEN];
  state->wandio_fh = NULL;

  /* OUTPUT_FILE_FORMAT "%s/"NAME".%"PRIu32".%"PRIu32"s-window.events.gz" */
  snprintf(filename, MAX_BUFFER_LEN, OUTPUT_FILE_FORMAT, state->output_folder,
           ts, state->current_window_size);

  /* open file for writing */
  if ((state->wandio_fh =
         wandio_wcreate(filename, wandio_detect_compression_type(filename),
                        DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n", filename);
    return -1;
  }

  state->new_moas_pfxs_count = 0;
  state->new_recurring_moas_pfxs_count = 0;
  state->ongoing_moas_pfxs_count = 0;
  state->finished_moas_pfxs_count = 0;

  return 0;
}

/* close log */
static int close_output_log(bvc_t *consumer, uint32_t ts)
{
  bvc_moas_state_t *state = STATE;
  char filename[MAX_BUFFER_LEN];

  if (state->wandio_fh) {
    /* Close file and generate .done if new information was printed */
    wandio_wdestroy(state->wandio_fh);

    /* generate the .done file */
    snprintf(filename, MAX_BUFFER_LEN, OUTPUT_FILE_FORMAT ".done",
             state->output_folder, ts, state->current_window_size);
    if ((state->wandio_fh =
           wandio_wcreate(filename, wandio_detect_compression_type(filename),
                          DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
      fprintf(stderr, "ERROR: Could not open %s for writing\n", filename);
      return -1;
    }
    wandio_wdestroy(state->wandio_fh);
  }
  state->wandio_fh = NULL;
  return 0;
}

static const char *get_category_str(moas_category_t mc)
{
  switch (mc) {
  case NEW:
    return "NEW";
  case NEWREC:
    return "NEWREC";
  case ONGOING:
    return "ONGOING";
  case FINISHED:
    return "FINISHED";
  }
  return "ERROR";
}

static void update_moas_counters(bvc_t *consumer, moas_category_t mc)
{
  bvc_moas_state_t *state = STATE;
  switch (mc) {
  case NEW:
    state->new_moas_pfxs_count++;
    break;
  case NEWREC:
    state->new_recurring_moas_pfxs_count++;
    break;
  case ONGOING:
    state->ongoing_moas_pfxs_count++;
    break;
  case FINISHED:
    state->finished_moas_pfxs_count++;
    break;
  }
}

static int log_moas(bvc_t *consumer, bgpview_t *view, bgpview_iter_t *it,
                    bgpstream_pfx_t *pfx, moas_signature_t *ms,
                    moas_properties_t *mp, moas_category_t mc, uint32_t ts)
{
  bvc_moas_state_t *state = STATE;
  char pfx_str[INET6_ADDRSTRLEN + 3];
  char asn_buffer[MAX_BUFFER_LEN];
  bgpstream_peer_id_t peerid;
  bgpstream_as_path_seg_t *seg;
  int ipv_idx;
  int first_path = 1;

  // avoid printing out onging events
  if (mc == ONGOING){
    update_moas_counters(consumer, mc);
    return 0;
  }

  bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx);

  /* NEW FILE FORMAT:
   * TIME|PFX|NEW/NEWREC/FINISHED|PFX_PATHS 
   * NB: in FINISHED events, the PATHS fields will be empty 
   * since AS path strings can contain commas, the AS paths with be
   * colon-separated, e.g.:
   * AS1 AS2 {AS3,AS4}:AS1 AS2 AS5
   * 
   *
   * deprecated:
   *  view-timestamp|prefix|CATEGORY|first_seen_ts|start_ts|end_ts|announcing
   * ASns[|ASPATHS]
   *
   * the list of aspaths observed for the prefix are outputted only if the moas
   * is new or
   * new-recurring
   */
  /*
  old version:
  if (wandio_printf(state->wandio_fh,
                    "%" PRIu32 "|%s|%s|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|",
                    ts, pfx_str, get_category_str(mc), mp->first_seen,
                    mp->start, mp->end) == -1) {
  */

  if (wandio_printf(state->wandio_fh,
                    "%" PRIu32 "|%s|%s|",
                    ts, pfx_str, get_category_str(mc)) == -1) {
    fprintf(stderr, "ERROR: Could not write data to file\n");
    return -1;
  }
  /*
  int i;
  int ret;
  for (i = 0; i < ms->n; i++) {
    // last origin
    if (i == ms->n - 1) {
      ret = wandio_printf(state->wandio_fh, "%" PRIu32, ms->origins[i]);
    } else {
      ret = wandio_printf(state->wandio_fh, "%" PRIu32 " ", ms->origins[i]);
    }
    if (ret == -1) {
      fprintf(stderr, "ERROR: Could not write data to file\n");
      return -1;
    }
  }
  */
  if (mc == NEW || mc == NEWREC) {
    /* mc is either NEW or NEWREC, hence we print the set of AS paths
     * as observed by all full feed peers */
    ipv_idx = bgpstream_ipv2idx(pfx->address.version);

    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(it); bgpview_iter_pfx_next_peer(it)) {

      // printing a path for each peer
      peerid = bgpview_iter_peer_get_peer_id(it);
      if (bgpstream_id_set_exists(
            BVC_GET_CHAIN_STATE(consumer)->full_feed_peer_ids[ipv_idx],
            peerid)) {
        bgpview_iter_pfx_peer_as_path_seg_iter_reset(it);

        // if it's not the first path, print ":" at the beginning of the path
        if (first_path != 1 && wandio_printf(state->wandio_fh, ":") == -1) {
          fprintf(stderr, "ERROR: Could not write data to file\n");
          return -1;
        }

        // first ASn
        seg = bgpview_iter_pfx_peer_as_path_seg_next(it);
        if (seg != NULL) {
          if (bgpstream_as_path_seg_snprintf(asn_buffer, MAX_BUFFER_LEN, seg) >=
              MAX_BUFFER_LEN) {
            fprintf(stderr, "ERROR: ASn print truncated output\n");
            return -1;
          }
          if (wandio_printf(state->wandio_fh, "%s", asn_buffer) == -1) {
            fprintf(stderr, "ERROR: Could not write data to file\n");
            return -1;
          }
        }
        // from the second ASN to the last one (origin ASn)
        while ((seg = bgpview_iter_pfx_peer_as_path_seg_next(it)) != NULL) {
          // printing each segment
          if (bgpstream_as_path_seg_snprintf(asn_buffer, MAX_BUFFER_LEN, seg) >=
              MAX_BUFFER_LEN) {
            fprintf(stderr, "ERROR: ASn print truncated output\n");
            return -1;
          }
          if (wandio_printf(state->wandio_fh, " %s", asn_buffer) == -1) {
            fprintf(stderr, "ERROR: Could not write data to file\n");
            return -1;
          }
        }

        // not first path anymore, start printing ":" at the beginning of each path
        first_path = 0;
      }
    }
  }

  // print end of line
  if (wandio_printf(state->wandio_fh, "\n") == -1) {
    fprintf(stderr, "ERROR: Could not write data to file\n");
    return -1;
  }

  update_moas_counters(consumer, mc);

  return 0;
}

/** Update the moas structure (and log finished moases) */
static int clean_moas(bvc_t *consumer, uint32_t ts, uint32_t last_valid_ts)
{

  bvc_moas_state_t *state = STATE;
  khiter_t p, m;
  bgpstream_pfx_t *pfx;
  moasinfo_map_t *per_pfx_moases;
  moas_signature_t *ms;
  moas_properties_t *mpro;

  /* for each prefix */
  for (p = kh_begin(state->current_moases); p != kh_end(state->current_moases);
       p++) {
    if (kh_exist(state->current_moases, p)) {
      pfx = &kh_key(state->current_moases, p);
      per_pfx_moases = kh_val(state->current_moases, p);

      /* for each moas */
      for (m = kh_begin(per_pfx_moases); m != kh_end(per_pfx_moases); m++) {
        if (kh_exist(per_pfx_moases, m)) {
          ms = &kh_key(per_pfx_moases, m);
          mpro = &kh_val(per_pfx_moases, m);

          /* outdated moas, remove it */
          if (mpro->end < last_valid_ts) {
            kh_del(moasinfo_map, per_pfx_moases, m);
          } else {
            if (mpro->end < ts) {
              // report finished moases
              if (mpro->start > 0) {
                if (log_moas(consumer, NULL, NULL, pfx, ms,
                             mpro, FINISHED, ts) != 0) {
                  return -1;
                }
                // signal that the moas has finished
                kh_val(per_pfx_moases, m).start = 0;
              }
            }
          }
        }
      }
    }
  }
  return 0;
}

/** Add moas to current moases */
static int add_moas(bvc_t *consumer, bgpview_t *view, bgpview_iter_t *it,
                    moas_signature_t *ms, uint32_t ts, uint32_t last_valid_ts)
{
  bvc_moas_state_t *state = STATE;
  bgpstream_pfx_t *pfx;

  moasinfo_map_t *per_pfx_moases;
  moas_properties_t *moas_properties = NULL;

  moas_category_t mc = FINISHED;
  khiter_t k;
  int khret;

  /* convert pfx_ptr in storage */
  pfx = bgpview_iter_pfx_get_pfx(it);

  /* check if prefix is in MOAS already, otherwise create it */
  if ((k = kh_get(pfx_moasinfo_map, state->current_moases, *pfx)) ==
      kh_end(state->current_moases)) {
    /* if not insert the prefix */
    k = kh_put(pfx_moasinfo_map, state->current_moases, *pfx, &khret);
    /* moasinfo_map is an empty map */
    if ((kh_value(state->current_moases, k) = kh_init(moasinfo_map)) == NULL) {
      fprintf(stderr, "Error: could not create moas_info map\n");
      return -1;
    }
  }

  /* get the moasinfo map*/
  per_pfx_moases = kh_value(state->current_moases, k);

  /* check if it is a new moas */
  if ((k = kh_get(moasinfo_map, per_pfx_moases, *ms)) ==
      kh_end(per_pfx_moases)) {
    mc = NEW;
    k = kh_put(moasinfo_map, per_pfx_moases, *ms, &khret);
    kh_value(per_pfx_moases, k).start = ts;
    kh_value(per_pfx_moases, k).end = ts;
    kh_value(per_pfx_moases, k).first_seen = ts;
  } else {
    /* if start is 0 it means the the moas finished
     * so this is a new occurence */
    if (kh_value(per_pfx_moases, k).start == 0) {
      if (kh_value(per_pfx_moases, k).end < last_valid_ts) {
        mc = NEW;
      } else {
        mc = NEWREC;
      }
      kh_value(per_pfx_moases, k).start = ts;
      kh_value(per_pfx_moases, k).end = ts;
    } 
    /* DANILO: removed print ongoing MOAS
       MINGWEI: uncommented out the following block. it causes bug that generates duplicated NEW events.
    */
    else { // otherwise is a moas which is continuing
      mc = ONGOING;
      kh_value(per_pfx_moases, k).end = ts;
    }
  }

  moas_properties = &kh_value(per_pfx_moases, k);

  return log_moas(consumer, view, it, pfx, ms, moas_properties, mc, ts);
}

/** Create timeseries metrics */

static int create_ts_metrics(bvc_t *consumer)
{

  char buffer[MAX_BUFFER_LEN];
  bvc_moas_state_t *state = STATE;

  /* regular metrics */
  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "current_window_size");
  if ((state->current_window_size_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "new_moas_count");
  if ((state->new_moas_pfxs_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "new_recurring_moas_count");
  if ((state->new_recurring_moas_pfxs_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ongoing_moas_count");
  if ((state->ongoing_moas_pfxs_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "finished_moas_count");
  if ((state->finished_moas_pfxs_count_idx =
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

  bvc_moas_state_t *state = STATE;

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
  struct stat st;
  errno = 0;
  if (stat(state->output_folder, &st) == -1) {
    fprintf(stderr, "Error: %s does not exist\n", state->output_folder);
    usage(consumer);
    return -1;
  } else {
    if (!S_ISDIR(st.st_mode)) {
      fprintf(stderr, "Error: %s is not a directory \n", state->output_folder);
      usage(consumer);
      return -1;
    }
  }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_moas_alloc()
{
  return &bvc_moas;
}

int bvc_moas_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_moas_state_t *state = NULL;
  bgpstream_pfx_t pfx;

  if ((state = malloc_zero(sizeof(bvc_moas_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  /* defaults here */
  state->window_size = DEFAULT_WINDOW_SIZE;
  strncpy(state->output_folder, DEFAULT_OUTPUT_FOLDER, MAX_BUFFER_LEN);
  state->output_folder[MAX_BUFFER_LEN - 1] = '\0';
  state->current_moases = NULL;

  if ((state->blacklist_pfxs = bgpstream_pfx_set_create()) == NULL) {
    fprintf(stderr, "Error: Could not create blacklist pfx set\n");
    goto err;
  }

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  /* react to args here */
  fprintf(stderr, "INFO: window size: %" PRIu32 "\n", state->window_size);
  fprintf(stderr, "INFO: output folder: %s\n", state->output_folder);

  /* init */

  if ((state->current_moases = kh_init(pfx_moasinfo_map)) == NULL) {
    fprintf(stderr, "Error: Could not create current_moases\n");
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

  return 0;

err:
  bvc_moas_destroy(consumer);
  return -1;
}

void bvc_moas_destroy(bvc_t *consumer)
{
  bvc_moas_state_t *state = STATE;

  if (state != NULL) {

    if (state->current_moases != NULL) {
      khiter_t p;
      for (p = kh_begin(state->current_moases);
           p != kh_end(state->current_moases); p++) {
        if (kh_exist(state->current_moases, p)) {
          kh_destroy(moasinfo_map, kh_val(state->current_moases, p));
        }
      }
      kh_destroy(pfx_moasinfo_map, state->current_moases);
    }

    if (state->blacklist_pfxs != NULL) {
      bgpstream_pfx_set_destroy(state->blacklist_pfxs);
    }

    if (state->kp != NULL) {
      timeseries_kp_free(&state->kp);
    }
    free(state);
    BVC_SET_STATE(consumer, NULL);
  }
}

int bvc_moas_process_view(bvc_t *consumer, bgpview_t *view)
{

  bvc_moas_state_t *state = STATE;
  bgpview_iter_t *it;
  bgpstream_pfx_t *pfx;

  int ipv_idx; // ip version index
  bgpstream_peer_id_t peerid;
  bgpstream_as_path_seg_t *origin_seg;
  moas_signature_t ms;
  int i;
  uint32_t origin_asn;
  uint32_t last_valid_ts = bgpview_get_time(view) - state->window_size;

  /* check visibility has been computed */
  if (BVC_GET_CHAIN_STATE(consumer)->visibility_computed == 0) {
    fprintf(stderr, "ERROR: moas requires the Visibility consumer "
                    "to be run first\n");
    return -1;
  }

  /* compute arrival delay */
  state->arrival_delay = epoch_sec() - bgpview_get_time(view);

  /* init the first timestamp */
  if (state->first_ts == 0) {
    state->first_ts = bgpview_get_time(view);
  }

  /* compute current window size*/
  if (last_valid_ts < state->first_ts) {
    state->current_window_size = bgpview_get_time(view) - state->first_ts;
  } else {
    state->current_window_size = state->window_size;
  }

  /* create view iterator */
  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  /* init output log file */
  if (init_output_log(consumer, bgpview_get_time(view)) != 0) {
    return -1;
  }

  /* iterate through all prefixes */
  for (bgpview_iter_first_pfx(it, 0 /* all versions */, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {

    pfx = bgpview_iter_pfx_get_pfx(it);

    /* ignore prefixes in blacklist */
    if (bgpstream_pfx_set_exists(state->blacklist_pfxs, pfx)) {
      continue;
    }

    ipv_idx = bgpstream_ipv2idx(pfx->address.version);

    ms.n = 0;

    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(it); bgpview_iter_pfx_next_peer(it)) {

      peerid = bgpview_iter_peer_get_peer_id(it);

      /* only consider peers that are full-feed */

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
        origin_asn = ((bgpstream_as_path_seg_asn_t *)origin_seg)->asn;

        /* check if origin is already in the moas signature struct
         * this check works when the signature is empty too */
        for (i = 0; i < ms.n; i++) {
          if (ms.origins[i] == origin_asn) {
            break;
          }
        }
        /* if the origin was not found, add it */
        if (i == ms.n) {
          ms.origins[ms.n] = origin_asn;
          ms.n++;
        }
      }
    }

    /* check if a moas has been detected */
    if (ms.n > 1) {
      /* add moas to state and log it on file */
      if (add_moas(consumer, view, it, &ms, bgpview_get_time(view),
                   last_valid_ts) != 0) {
        return -1;
      }
    }
  }

  bgpview_iter_destroy(it);

  /* remove stale moases and report these that are finished */
  if (clean_moas(consumer, bgpview_get_time(view), last_valid_ts) != 0) {
    return -1;
  }

  if (close_output_log(consumer, bgpview_get_time(view)) != 0) {
    return -1;
  }

  /* compute processed delay */
  state->processed_delay = epoch_sec() - bgpview_get_time(view);
  state->processing_time = state->processed_delay - state->arrival_delay;

  if (output_timeseries(consumer, bgpview_get_time(view)) != 0) {
    return -1;
  }

  return 0;
}
