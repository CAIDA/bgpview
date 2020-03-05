/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini, Adriano Faggiani
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

#include "bvc_peerpfxorigins.h"
#include "bgpview_consumer_interface.h"
#include "bgpstream_utils_pfx_set.h"
#include "khash.h"
#include "utils.h"
#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <wandio.h>

#define NAME "peer-pfx-origins"
#define PEER_TABLE_NAME NAME"-peertable"

/** Default compression level of output file */
#define DEFAULT_COMPRESS_LEVEL 6

/** Default number of unique origins */
#define ORIGIN_CNT 512

/** Default number of peers per origin */
#define ORIGIN_PEER_CNT 1024

#define STATE (BVC_GET_STATE(consumer, peerpfxorigins))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_peerpfxorigins = { //
  BVC_ID_PEERPFXORIGINS, //
  NAME, //
  BVC_GENERATE_PTRS(peerpfxorigins) //
};

typedef struct origin_peers {

  bgpstream_as_path_seg_t *origin;

  bgpstream_peer_id_t peers[ORIGIN_PEER_CNT];

  int peers_cnt;

} origin_peers_t;

/* our 'instance' */
typedef struct bvc_peerpfxorigins_state {

  /** output directory */
  char *outdir;

  /** peer table output file */
  iow_t *peers_outfile;

  /** prefix origins output file */
  iow_t *pfx_outfile;

  /** only output peer counts */
  int peer_count_only;

  /* ----- working state ----- */

  origin_peers_t origins[ORIGIN_CNT];

  int origins_cnt;

} bvc_peerpfxorigins_state_t;

/* ==================== CONSUMER INTERNAL FUNCTIONS ==================== */

static int open_outfiles(bvc_t *consumer, uint32_t vtime)
{
  char filename[1024];

  if (STATE->peer_count_only == 0) {
    /* peers table */
    if (snprintf(filename, sizeof(filename),
                 "%s/" PEER_TABLE_NAME ".%" PRIu32 ".gz", STATE->outdir,
                 vtime) >= sizeof(filename)) {
      fprintf(stderr, "ERROR: File name too long for buffer\n");
      return -1;
    }
    if ((STATE->peers_outfile =
         wandio_wcreate(filename, wandio_detect_compression_type(filename),
                        DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
      fprintf(stderr, "ERROR: Could not open %s for writing\n", filename);
      return -1;
    }
  }

  /* pfx origins table */
  if (snprintf(filename, sizeof(filename), "%s/" NAME ".%" PRIu32 ".gz",
               STATE->outdir, vtime) >= sizeof(filename)) {
    fprintf(stderr, "ERROR: File name too long for buffer\n");
    return -1;
  }
  if ((STATE->pfx_outfile =
         wandio_wcreate(filename, wandio_detect_compression_type(filename),
                        DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n", filename);
    return -1;
  }

  return 0;
}

static int close_outfiles(bvc_t *consumer, uint32_t vtime)
{
  char filename[1024];

  if (STATE->peer_count_only == 0) {
    wandio_wdestroy(STATE->peers_outfile);
    STATE->peers_outfile = NULL;

    if (snprintf(filename, sizeof(filename),
                 "%s/" PEER_TABLE_NAME ".%" PRIu32 ".gz.done", STATE->outdir,
                 vtime) >= sizeof(filename)) {
      fprintf(stderr, "ERROR: File name too long for buffer\n");
      return -1;
    }
    fclose(fopen(filename, "w"));
  }

  wandio_wdestroy(STATE->pfx_outfile);
  STATE->pfx_outfile = NULL;

  if (snprintf(filename, sizeof(filename), "%s/" NAME ".%" PRIu32 ".gz.done",
               STATE->outdir, vtime) >= sizeof(filename)) {
    fprintf(stderr, "ERROR: File name too long for buffer\n");
    return -1;
  }
  fclose(fopen(filename, "w"));

  return 0;
}

static int output_peers(bvc_t *consumer, bgpview_t *view)
{
  bgpview_iter_t *it;
  bgpstream_peer_sig_t *peer_sig;
  char peer_ip[INET6_ADDRSTRLEN];

  if (STATE->peer_count_only != 0) {
    wandio_printf(STATE->pfx_outfile, "# peer_cnt: %d\n",
                  bgpview_peer_cnt(view, BGPVIEW_FIELD_ACTIVE));
    return 0;
  }

  it = bgpview_iter_create(view);

  wandio_printf(STATE->peers_outfile, "peer_id|collector|peer_asn|peer_ip\n");

  for (bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(it); //
       bgpview_iter_next_peer(it)) {
    peer_sig = bgpview_iter_peer_get_sig(it);
    bgpstream_addr_ntop(peer_ip, sizeof(peer_ip), &peer_sig->peer_ip_addr);
    wandio_printf(STATE->peers_outfile,
                  "%"PRIu16"|" // peer id
                  "%s|" // collector
                  "%"PRIu32"|" // peer asn
                  "%s\n", // peer ip
                  bgpview_iter_peer_get_peer_id(it),
                  peer_sig->collector_str,
                  peer_sig->peer_asnumber,
                  peer_ip);
  }

  bgpview_iter_destroy(it);
  return 0;
}

static int output_origins(bvc_t *consumer, bgpstream_pfx_t *pfx)
{
  char pfx_str[INET6_ADDRSTRLEN + 3];
  char orig_str[4096];
  int i, j;
  origin_peers_t *op;

  bgpstream_pfx_snprintf(pfx_str, sizeof(pfx_str), pfx);

#if 0
  /* DEBUG */
  if (STATE->origins_cnt > 10) {
    fprintf(stderr, "DEBUG: %s has %d unique origins\n", pfx_str,
            STATE->origins_cnt);
  }
#endif

  for (i = 0; i < STATE->origins_cnt; i++) {
    op = &STATE->origins[i];

    if (bgpstream_as_path_seg_snprintf(orig_str, sizeof(orig_str),
                                       op->origin) >= sizeof(orig_str)) {
      fprintf(stderr, "ERROR: Origin segment too long\n");
      return -1;
    }

    if (STATE->peer_count_only != 0) {
      wandio_printf(STATE->pfx_outfile, "%s|%s|%d\n", pfx_str, orig_str,
        op->peers_cnt);
    } else {
      wandio_printf(STATE->pfx_outfile, "%s|%s|", pfx_str, orig_str);
      for (j = 0; j < op->peers_cnt; j++) {
        wandio_printf(STATE->pfx_outfile, "%s%"PRIu16, (j > 0) ? "," : "",
                      op->peers[j]);
      }
      wandio_printf(STATE->pfx_outfile, "\n");
    }
  }

  return 0;
}

static int process_prefixes(bvc_t *consumer, bgpview_t *view)
{
  bgpview_iter_t *it = bgpview_iter_create(view);
  int i;
  origin_peers_t *op;
  bgpstream_as_path_seg_t *seg;

  if (STATE->peer_count_only != 0) {
    wandio_printf(STATE->pfx_outfile, "prefix|origin|peer_cnt\n");
  } else {
    wandio_printf(STATE->pfx_outfile, "prefix|origin|peer_id\n");
  }

  // for each prefix
  for (bgpview_iter_first_pfx(it, 0, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); //
       bgpview_iter_next_pfx(it)) {

    // reset working state
    STATE->origins_cnt = 0;

    // for each peer
    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(it); //
         bgpview_iter_pfx_next_peer(it)) {

      seg = bgpview_iter_pfx_peer_get_origin_seg(it);

      // linear search through array -- most prefixes should have one origin
      op = NULL;
      for (i = 0; i < STATE->origins_cnt; i++) {
        if (bgpstream_as_path_seg_equal(STATE->origins[i].origin, seg)) {
          op = & STATE->origins[i];
          break;
        }
      }
      if (op == NULL) {
        // new origin
        assert(STATE->origins_cnt < ORIGIN_CNT);
        op = &STATE->origins[STATE->origins_cnt];
        STATE->origins_cnt++;

        op->origin = seg;
        op->peers_cnt = 0;
      }

      assert(op->peers_cnt < ORIGIN_PEER_CNT);

      op->peers[op->peers_cnt] = bgpview_iter_peer_get_peer_id(it);
      op->peers_cnt++;
    }

    if (output_origins(consumer, bgpview_iter_pfx_get_pfx(it)) != 0) {
      goto err;
    }
  }

  bgpview_iter_destroy(it);
  return 0;

err:
  bgpview_iter_destroy(it);
  return -1;
}

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr, "consumer usage: %s\n"
                  "       -o <path>             output directory\n"
                  "       -c                    only output peer counts\n",
          consumer->name);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  assert(argc > 0 && argv != NULL);

  bvc_peerpfxorigins_state_t *state = STATE;

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
    fprintf(stderr, "ERROR: pfx-origins output directory required\n");
    usage(consumer);
    return -1;
  }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_peerpfxorigins_alloc()
{
  return &bvc_peerpfxorigins;
}

int bvc_peerpfxorigins_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_peerpfxorigins_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_peerpfxorigins_state_t))) == NULL) {
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
  bvc_peerpfxorigins_destroy(consumer);
  return -1;
}

void bvc_peerpfxorigins_destroy(bvc_t *consumer)
{
  bvc_peerpfxorigins_state_t *state = STATE;

  if (state == NULL) {
    return;
  }

  free(state->outdir);

  free(state);
  BVC_SET_STATE(consumer, NULL);
}

int bvc_peerpfxorigins_process_view(bvc_t *consumer, bgpview_t *view)
{
  uint32_t vtime = bgpview_get_time(view);

  /* prepare output files for writing */
  if (open_outfiles(consumer, vtime) != 0) {
    return -1;
  }

  /* write the peer table out */
  if (output_peers(consumer, view) != 0) {
    return -1;
  }

  /* spin through the view and output prefix origin info */
  if (process_prefixes(consumer, view) != 0) {
    return -1;
  }

  /* close the output files and create .done file */
  if (close_outfiles(consumer, vtime) != 0) {
    return -1;
  }

  return 0;
}
