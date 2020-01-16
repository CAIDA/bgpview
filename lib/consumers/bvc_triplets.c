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

#include "bvc_triplets.h"
#include "bgpview_consumer_interface.h"
#include "bgpstream_utils_pfx_set.h"
#include "khash.h"
#include "utils.h"
#include <wandio.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define NAME "triplets"
#define CONSUMER_METRIC_PREFIX "triplets"

#define OUTPUT_FILE_FORMAT                                                     \
  "%s/" NAME ".%" PRIu32 ".%" PRIu32 "s-window.events.gz"

#define OUTPUT_FILE_FORMAT_NEWEDGES                                            \
  "%s/edges.%" PRIu32 ".%" PRIu32 "s-window.events.gz"

#define OUTPUT_FILE_FORMAT_TRIPLETS                                            \
  "%s/triplets.%" PRIu32 ".%" PRIu32 "s-window.events.gz"

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

#define NEW 1

#define NEWREC 2

#define FINISHED 3
#define ONGOING 4

#define STATE (BVC_GET_STATE(consumer, triplets))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_triplets = {BVC_ID_TRIPLETS, NAME,
                             BVC_GENERATE_PTRS(triplets)};

typedef struct triplet_info {
  // time when triplet was seen first
  uint32_t first_seen;
  // time when triplet was seen last
  uint32_t last_seen;
  // time when finished
  uint32_t end;
  // time when seen for current occurrence
  uint32_t start;
  // current status
  bool ongoing;

} triplet_info_t;

KHASH_INIT(triplets_map, char *, triplet_info_t, 1, kh_str_hash_func,
           kh_str_hash_equal);
typedef khash_t(triplets_map) triplets_map_t;

/* our 'instance' */
// main struct
typedef struct bvc_triplets_state {
  // window size given by user
  uint32_t window_size;
  // current time stamp
  uint32_t time_now;
  /** output folder */
  char output_folder[MAX_BUFFER_LEN];
  // Khash holding triplets
  triplets_map_t *triplets_map;
  // Output files for edges and triplets
  char filename_triplets[MAX_BUFFER_LEN];
  iow_t *file_triplets;

  /** diff ts when the view arrived */
  uint32_t arrival_delay;
  /** diff ts when the view processing finished */
  uint32_t processed_delay;
  /** processing time */
  uint32_t processing_time;

  /** Timeseries Key Package */
  timeseries_kp_t *kp;

  // Timeseries variables
  // Meta
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;
  int new_triplets_count;
  uint32_t new_triplets_count_idx;
  int ongoing_triplets_count_idx;
  uint32_t ongoing_triplets_count;
  int finished_triplets_count_idx;
  uint32_t finished_triplets_count;
  int newrec_triplets_count_idx;
  uint32_t newrec_triplets_count;

} bvc_triplets_state_t;

static int output_timeseries(bvc_t *consumer, uint32_t ts)
{
  bvc_triplets_state_t *state = STATE;
  timeseries_kp_set(state->kp, state->arrival_delay_idx, state->arrival_delay);
  timeseries_kp_set(state->kp, state->processed_delay_idx,
                    state->processed_delay);
  timeseries_kp_set(state->kp, state->processing_time_idx,
                    state->processing_time);

  timeseries_kp_set(state->kp, state->new_triplets_count_idx,
                    state->new_triplets_count);
  timeseries_kp_set(state->kp, state->ongoing_triplets_count_idx,
                    state->ongoing_triplets_count);
  timeseries_kp_set(state->kp, state->finished_triplets_count_idx,
                    state->finished_triplets_count);
  timeseries_kp_set(state->kp, state->newrec_triplets_count_idx,
                    state->newrec_triplets_count);

  if (timeseries_kp_flush(state->kp, ts) != 0) {
    fprintf(stderr, "Warning: could not flush %s %" PRIu32 "\n", NAME, ts);
  }

  return 0;
}

static int create_ts_metrics(bvc_t *consumer)
{

  char buffer[MAX_BUFFER_LEN];
  bvc_triplets_state_t *state = STATE;

  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "new_triplets_count");
  if ((state->new_triplets_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }
  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "finished_triplets_count");
  if ((state->finished_triplets_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }
  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "newrec_triplets_count");
  if ((state->newrec_triplets_count_idx =
         timeseries_kp_add_key(state->kp, buffer)) == -1) {
    return -1;
  }
  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size,
           "ongoing_triplets_count");
  if ((state->ongoing_triplets_count_idx =
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

  bvc_triplets_state_t *state = STATE;

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

bvc_t *bvc_triplets_alloc()
{
  return &bvc_triplets;
}

int bvc_triplets_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_triplets_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_triplets_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  /* defaults here */
  state->window_size = DEFAULT_WINDOW_SIZE;
  strncpy(state->output_folder, DEFAULT_OUTPUT_FOLDER, MAX_BUFFER_LEN);
  state->output_folder[MAX_BUFFER_LEN - 1] = '\0';
  state->file_triplets = NULL;

  if ((state->triplets_map = kh_init(triplets_map)) == NULL) {
    fprintf(stderr, "Error: could not create triplets map\n");
    return -1;
  }
  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  /* react to args here */
  fprintf(stderr, "INFO: window size: %" PRIu32 "\n", state->window_size);
  fprintf(stderr, "INFO: output folder: %s\n", state->output_folder);
  if ((state->kp = timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) ==
      NULL) {
    fprintf(stderr, "Error: Could not create timeseries key package\n");
    goto err;
  }
  if (create_ts_metrics(consumer) != 0) {
    goto err;
  }

  /* init */
  return 0;

err:
  bvc_triplets_destroy(consumer);
  return -1;
}

void bvc_triplets_destroy(bvc_t *consumer)
{
  bvc_triplets_state_t *state = STATE;
  if (state != NULL) {
    khint_t k;
    for (k = kh_begin(state->triplets_map); k != kh_end(state->triplets_map);
         k++) {
      if (kh_exist(state->triplets_map, k)) {
        free(kh_key(state->triplets_map, k));
      }
    }
    kh_destroy(triplets_map, state->triplets_map);
    if (state->kp != NULL) {
      timeseries_kp_free(&state->kp);
    }
    free(state);
    BVC_SET_STATE(consumer, NULL);
  }
}

// Diagnostic function. Prints all triplets
static void print_triplets(bvc_t *consumer)
{
  bvc_triplets_state_t *state = STATE;
  khint_t k;
  printf("PRINGTIN \n");
  for (k = kh_begin(state->triplets_map); k != kh_end(state->triplets_map);
       k++) {
    if (kh_exist(state->triplets_map, k)) {
      //      printf("for tripet %s : ", kh_key(state->triplets_map,k));
      printf("new triplet from khash %s \n", kh_key(state->triplets_map, k));
      triplet_info_t triplet_info = kh_value(state->triplets_map, k);
      printf(" last seen was %d \n ", triplet_info.last_seen);
    }
  }
}

// Writes in output file for triplets
static void print_to_file_triplets(bvc_t *consumer, int status, char *triplet,
  triplet_info_t triplet_info, bgpstream_pfx_t *pfx)
{
  bvc_triplets_state_t *state = STATE;
  char pfx_str[MAX_BUFFER_LEN];

  if (status == NEW) {
    // printf("printing \n");
    if (wandio_printf(
          state->file_triplets,
          "%" PRIu32 "|%s|NEW|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|%s\n",
          state->time_now, triplet, triplet_info.first_seen, triplet_info.start,
          triplet_info.end,
          bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx)) == -1) {
      fprintf(stderr, "ERROR: Could not write %s file\n",
              state->filename_triplets);
      return;
    }
  }

  if (status == NEWREC) {
    if (wandio_printf(
          state->file_triplets,
          "%" PRIu32 "|%s|NEWREC|%" PRIu32 "|%" PRIu32 "|%" PRIu32 "|%s\n",
          state->time_now, triplet, triplet_info.first_seen, triplet_info.start,
          triplet_info.end,
          bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx)) == -1) {
      fprintf(stderr, "ERROR: Could not write %s file\n",
              state->filename_triplets);
      return;
    }
  }

  if (status == FINISHED) {
    if (wandio_printf(state->file_triplets, "%" PRIu32 "|%s|FINISHED|%" PRIu32
                                            "|%" PRIu32 "|%" PRIu32 "\n",
                      state->time_now, triplet, triplet_info.first_seen,
                      triplet_info.start, triplet_info.end) == -1) {
      fprintf(stderr, "ERROR: Could not write %s file\n",
              state->filename_triplets);
      return;
    }
  }
}

static void print_ongoing_triplets(bvc_t *consumer)
{
  bvc_triplets_state_t *state = STATE;
  khint_t k;
  for (k = kh_begin(state->triplets_map); k != kh_end(state->triplets_map);
       k++) {
    if (kh_exist(state->triplets_map, k)) {
      triplet_info_t triplet_info = kh_value(state->triplets_map, k);
      if (triplet_info.ongoing) {
        if (wandio_printf(state->file_triplets,
                          "%" PRIu32 "|%s|ONGOING|%" PRIu32 "|%" PRIu32
                          "|%" PRIu32 "\n",
                          state->time_now, kh_key(state->triplets_map, k),
                          triplet_info.first_seen, triplet_info.start,
                          triplet_info.end) == -1) {
          fprintf(stderr, "ERROR: Could not write %s file\n",
                  state->filename_triplets);
          return;
        }
        state->ongoing_triplets_count++;
      }
    }
  }
}

// Scans all ongoing triples and remove stale ones
static void remove_stale_triplet(bvc_t *consumer)
{
  bvc_triplets_state_t *state = STATE;
  khint_t k;
  for (k = kh_begin(state->triplets_map); k != kh_end(state->triplets_map);
       k++) {
    if (kh_exist(state->triplets_map, k)) {

      triplet_info_t triplet_info = kh_value(state->triplets_map, k);
      // if last_seen is different from current time then declare it dead
      if (triplet_info.last_seen < state->time_now && triplet_info.ongoing) {
        triplet_info.end = state->time_now;
        triplet_info.ongoing = 0;
        kh_value(state->triplets_map, k) = triplet_info;
        print_to_file_triplets(consumer, FINISHED,
                               kh_key(state->triplets_map, k), triplet_info,
                               NULL);
        state->finished_triplets_count++;
      }
    }
  }
}

// Updates khash and stores new and newrec triplets
static void insert_update_triplet(bvc_t *consumer, char *triplet,
    bgpstream_pfx_t *pfx)
{
  bvc_triplets_state_t *state = STATE;
  khint_t k, j;
  int ret;
  k = kh_get(triplets_map, state->triplets_map, triplet);
  triplet_info_t triplet_info;
  char *copy_triplet;
  ;
  if (k == kh_end(state->triplets_map)) {
    // NEW triplet

    if ((copy_triplet = strdup(triplet)) == NULL) {
      fprintf(stderr, "error copying triplet \n");
      return;
    }
    // putting in khash and initializing variables
    j = kh_put(triplets_map, state->triplets_map, copy_triplet, &ret);
    triplet_info.first_seen = state->time_now;
    triplet_info.last_seen = state->time_now;
    triplet_info.start = state->time_now;
    triplet_info.ongoing = 1;
    triplet_info.end = 0;
    kh_value(state->triplets_map, j) = triplet_info;
    print_to_file_triplets(consumer, NEW, triplet, triplet_info, pfx);
    state->new_triplets_count++;
  } else {
    // Triplet seen before
    triplet_info = kh_value(state->triplets_map, k);
    triplet_info.last_seen = state->time_now;
    // if ongoing, update last_seen. otherwise check for new or newrec
    if (triplet_info.ongoing == 0) {
      triplet_info.ongoing = 1;
      // Checking when was the last time it started. Make it newrec if last end
      // is within the current window
      if (triplet_info.start + state->window_size > state->time_now) {
        print_to_file_triplets(consumer, NEWREC, kh_key(state->triplets_map, k),
                               triplet_info, pfx);
        state->newrec_triplets_count++;
      } else {
        print_to_file_triplets(consumer, NEW, kh_key(state->triplets_map, k),
                               triplet_info, pfx);
        state->new_triplets_count++;
      }
      triplet_info.start = state->time_now;
    }
    kh_value(state->triplets_map, k) = triplet_info;
  }
}

int bvc_triplets_process_view(bvc_t *consumer, bgpview_t *view)
{
  bvc_triplets_state_t *state = STATE;
  bgpview_iter_t *it;
  bgpstream_pfx_t *pfx;
  bgpstream_peer_id_t peerid;
  uint32_t time_now = bgpview_get_time(view);
  state->time_now = time_now;
  /* compute arrival delay */
  state->arrival_delay = epoch_sec() - bgpview_get_time(view);

  // Initializing counter for libtimeseries
  state->new_triplets_count = 0;
  state->ongoing_triplets_count = 0;
  state->finished_triplets_count = 0;
  state->newrec_triplets_count = 0;

  // Opening file for triplets
  snprintf(state->filename_triplets, MAX_BUFFER_LEN,
           OUTPUT_FILE_FORMAT_TRIPLETS, state->output_folder, time_now,
           state->window_size);
  /* open file for writing */
  if ((state->file_triplets = wandio_wcreate(
         state->filename_triplets,
         wandio_detect_compression_type(state->filename_triplets),
         DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n",
            state->filename_triplets);
    return -1;
  }

  int ipv_idx, ret, i;

  /* borrowed pointer to a path segment */
  bgpstream_as_path_seg_t *seg;
  //  uint32_t asn1;
  //  uint32_t asn2;
  uint32_t asn;
  uint32_t normal_asn;
  uint32_t prev_asn;
  uint32_t prev_prev_asn;
  // print ongoing triplets
  print_ongoing_triplets(consumer);

  /* check visibility has been computed */
  if (BVC_GET_CHAIN_STATE(consumer)->visibility_computed == 0) {
    fprintf(stderr, "ERROR: edges requires the Visibility consumer "
                    "to be run first\n");
    return -1;
  }

  /* create view iterator */
  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  /* iterate through all prefixes */
  for (bgpview_iter_first_pfx(it, 0 /* all versions */, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {
    pfx = bgpview_iter_pfx_get_pfx(it);
    ipv_idx = bgpstream_ipv2idx(pfx->address.version);
    // peers_cnt = 0;
    // last_origin_asn = 0;

    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(it); bgpview_iter_pfx_next_peer(it)) {
      /* only consider peers that are full-feed */
      peerid = bgpview_iter_peer_get_peer_id(it);
      if (bgpstream_id_set_exists(
            BVC_GET_CHAIN_STATE(consumer)->full_feed_peer_ids[ipv_idx],
            peerid)) {

        // initializing asns for each view
        i = 0;
        // asn1=0;
        // asn2=0;
        normal_asn = 0;
        prev_asn = 0;
        prev_prev_asn = 0;

        bgpview_iter_pfx_peer_as_path_seg_iter_reset(it);

        while ((seg = bgpview_iter_pfx_peer_as_path_seg_next(it)) != NULL) {
          i++;
          /* checking if a segment is a regular asn */
          if (seg->type == BGPSTREAM_AS_PATH_SEG_ASN) {
            asn = ((bgpstream_as_path_seg_asn_t *)seg)->asn;

            normal_asn = 1;
          }
          // Check whether asn is a regular asn
          else {
            normal_asn = 0;
            prev_asn = 0;
            continue;
          }

          if (normal_asn) {
            // Previous asn was a regular one
            if (prev_asn != 0) {
              // asn1=asn;
              // asn2=prev_asn;
              // Continue if ASN perpending is observed
              if (asn == prev_asn) {
                continue;
              }
              // Getting the greater than two ASNs. Assuming edges are not
              // directional
              if (asn < prev_asn) {
                // asn1=prev_asn;
                // asn2=asn;
              }

              // Check whether we have seen this edge before or not. Update
              // respective khashes
            }
            // Reading atleast three different ASNs to create first triplet
            if (prev_prev_asn != 0) {
              int buffer_len = 25;
              char triplet[buffer_len];
              triplet[0] = '\0';
              int written = 0;
              // Making a char array from three ASNs

              ret = snprintf(triplet, buffer_len - 1,
                             "%" PRIu32 "-%" PRIu32 "-%" PRIu32 " ",
                             prev_prev_asn, prev_asn, asn);
              // ret =snprintf(triplet, buffer_len - 1, "%"PRIu32"-%"PRIu32"",
              // prev_asn, asn);
              if (ret < 0 || ret >= buffer_len - written - 1) {
                fprintf(stderr, "ERROR: cannot write ASN tiplet.\n");
                return -1;
              }
              written += ret;
              triplet[written - 1] = '\0';
              // Check whether we have seen this edge before or not. Update
              // respective khashes
              insert_update_triplet(consumer, triplet, pfx);
            }
            // updating ASN variables for next ASNs in the aspath
            prev_prev_asn = prev_asn;
            prev_asn = asn;
          }
        }
        prev_asn = 0;
        prev_prev_asn = 0;
      }
    }
  }
  // Loops through all ongoing edges and checks for stale edges/triplets
  remove_stale_triplet(consumer);
  bgpview_iter_destroy(it);
  // Close file I/O
  wandio_wdestroy(state->file_triplets);
  char filename[MAX_BUFFER_LEN];

  iow_t *done_triplets = NULL;
  snprintf(filename, MAX_BUFFER_LEN, OUTPUT_FILE_FORMAT_TRIPLETS ".done",
           state->output_folder, time_now, state->window_size);
  if ((done_triplets =
         wandio_wcreate(filename, wandio_detect_compression_type(filename),
                        DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n", filename);
    return -1;
  }
  wandio_wdestroy(done_triplets);

  /* compute processed delay */
  state->processed_delay = epoch_sec() - bgpview_get_time(view);
  state->processing_time = state->processed_delay - state->arrival_delay;

  // Output timeseries
  if (output_timeseries(consumer, bgpview_get_time(view)) != 0) {
    return -1;
  }

  return 0;
}
