/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King
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

#include "bvc_pathchange.h"
#include "bgpview_consumer_interface.h"
#include "bgpview_consumer_utils.h"
#include "utils.h"
#include <assert.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <wandio.h>

#define NAME "path-change"

#define BUFFER_LEN 1024
// bgp.meta.bgpview.consumer.path-change.{metric}
#define META_METRIC_PREFIX_FORMAT "%s.meta.bgpview.consumer." NAME ".%s"

/* macro to access the current consumer state */
#define STATE (BVC_GET_STATE(consumer, pathchange))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_pathchange = {
  BVC_ID_PATHCHANGE,            //
  NAME,                         //
  BVC_GENERATE_PTRS(pathchange) //
};

/* our 'instance' */
typedef struct bvc_pathchange_state {

  /** Output filename pattern */
  char *outfile_pattern;

  /** Current output filename */
  char *outfile_name;

  /** Output file compression level */
  int outfile_compress_level;

  /** Current output file */
  iow_t *outfile;

  /** File rotation interval */
  uint32_t rotation_interval;

  /** Align rotation times to multiples of the interval */
  int rotate_noalign;

  /** First view written to the current output file */
  uint32_t next_rotate_time;

  /** Our libtimeseries KP */
  timeseries_kp_t *kp;

  /** Copy of the previous view */
  bgpview_t *parent_view;

  /* timeseries indexes: */
  int proc_time_idx;

} bvc_pathchange_state_t;

#define SHOULD_ROTATE(state, time)                                             \
  (((state)->rotation_interval > 0) && ((time) >= (state)->next_rotate_time))

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(
    stderr,
    "consumer usage: %s\n"
    "       -f <filename> output file pattern for writing views\n"
    "                       accepts same format parameters as strftime(3)\n"
    "                       as well as '%%s' to write unix time\n"
    "       -r <seconds>  output file rotation period (default: no "
    "rotation)\n"
    "       -a            disable alignment of output file rotation to "
    "multiples of the rotation interval\n"
    "       -c <level>    output compression level to use (default: %d)\n",
    consumer->name, BVCU_DEFAULT_COMPRESS_LEVEL);
}

/* borrowed from bvc_archiver */
static int complete_file(bvc_t *consumer)
{
  bvc_pathchange_state_t *state = STATE;

  /* close the current output file */
  if (state->outfile == NULL) {
    return 0;
  }
  wandio_wdestroy(state->outfile);
  state->outfile = NULL;

  free(state->outfile_name);
  state->outfile_name = NULL;

  return 0;
}

#define stradd(str, bufp, buflim)                                              \
  do {                                                                         \
    char *strp = str;                                                          \
    while (bufp < buflim && (*bufp = *strp++) != '\0')                         \
      ++bufp;                                                                  \
  } while (0)

static char *generate_file_name(const char *template, uint32_t time)
{
  /* some of the structure of this code is borrowed from the
     FreeBSD implementation of strftime */

  /* the output buffer */
  /* @todo change the code to dynamically realloc this if we need more
     space */
  char buf[1024];
  char tbuf[1024];
  char *bufp = buf;
  char *buflim = buf + sizeof(buf);

  const char *tmpl = template;
  char secs[11]; /* length of UINT32_MAX +1 */
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 0;

  for (; *tmpl; ++tmpl) {
    if (*tmpl == '%') {
      switch (*++tmpl) {
      case '\0':
        --tmpl;
        break;

      case 's': /* unix timestamp */
        snprintf(secs, sizeof(secs), "%" PRIu32, time);
        stradd(secs, bufp, buflim);
        continue;

      default:
        /* we want to be generous and leave non-recognized formats
           intact - especially for strftime to use */
        --tmpl;
      }
    }
    if (bufp == buflim)
      break;
    *bufp++ = *tmpl;
  }

  *bufp = '\0';

  /* now let strftime have a go */
  tv.tv_sec = time;
  strftime(tbuf, sizeof(tbuf), buf, gmtime(&tv.tv_sec));
  return strdup(tbuf);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  bvc_pathchange_state_t *state = STATE;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, ":c:f:r:?a")) >= 0) {
    switch (opt) {
    case 'a':
      state->rotate_noalign = 1;
      break;

    case 'c':
      state->outfile_compress_level = atoi(optarg);
      break;

    case 'f':
      if ((state->outfile_pattern = strdup(optarg)) == NULL) {
        return -1;
      }
      break;

    case 'r':
      state->rotation_interval = atoi(optarg);
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

static int create_ts_metrics(bvc_t *consumer)
{
  char buffer[BUFFER_LEN];
  bvc_pathchange_state_t *state = STATE;

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "processing_time");
  if ((state->proc_time_idx = timeseries_kp_add_key(STATE->kp, buffer)) == -1) {
    return -1;
  }

  return 0;
}

/* returns 0 if they are the same */
static int diff_cells(bgpview_iter_t *parent_view_it, bgpview_iter_t *itC)
{
  bgpstream_as_path_store_path_id_t idxH =
    bgpview_iter_pfx_peer_get_as_path_store_path_id(parent_view_it);
  bgpstream_as_path_store_path_id_t idxC =
    bgpview_iter_pfx_peer_get_as_path_store_path_id(itC);

  return bcmp(&idxH, &idxC, sizeof(bgpstream_as_path_store_path_id_t)) != 0;
}

static int diff_paths(bvc_t *consumer, bgpview_t *view)
{
  if (STATE->parent_view == NULL) {
    /* nothing to compare with */
    return 0;
  }

  bgpview_iter_t *it;
  bgpview_iter_t *parent_view_it;
  bgpstream_pfx_t *pfx;
  bgpstream_peer_id_t peer_id;

  char pfx_str[INET6_ADDRSTRLEN + 3] = "";
  bgpstream_peer_sig_t *ps;
  char peer_str[INET6_ADDRSTRLEN] = "";
  char old_path_str[4096] = "";
  bgpstream_as_path_t *old_path = NULL;
  char new_path_str[4096] = "";
  bgpstream_as_path_t *new_path = NULL;

  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  if ((parent_view_it = bgpview_iter_create(STATE->parent_view)) == NULL) {
    return -1;
  }

  /* for each prefix in view */
  for (bgpview_iter_first_pfx(it, 0, BGPVIEW_FIELD_ACTIVE); //
       bgpview_iter_has_more_pfx(it);                       //
       bgpview_iter_next_pfx(it)) {

    pfx = bgpview_iter_pfx_get_pfx(it);

    if (bgpview_iter_seek_pfx(parent_view_it, pfx, BGPVIEW_FIELD_ACTIVE) == 0) {
      // this is a new prefix, skip it...
      continue;
    }

    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE); //
         bgpview_iter_pfx_has_more_peer(it);                    //
         bgpview_iter_pfx_next_peer(it)) {

      peer_id = bgpview_iter_peer_get_peer_id(it);

      /* is it in parent view? */
      if (bgpview_iter_pfx_seek_peer(parent_view_it, peer_id,
                                     BGPVIEW_FIELD_ACTIVE) == 0) {
        // new peer, skip it...
        continue;
      }

      /* is the path different? */
      if (diff_cells(parent_view_it, it) != 0) {
        /* there is currently a bug somewhere that causes us to use different
         * path store IDs for the same effective path, so we need to do a full
         * check of the paths */
        old_path = bgpview_iter_pfx_peer_get_as_path(parent_view_it);
        new_path = bgpview_iter_pfx_peer_get_as_path(it);
        if (bgpstream_as_path_equal(old_path, new_path) == 0) {
          bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN + 3, pfx);
          ps = bgpview_iter_peer_get_sig(it);
          bgpstream_addr_ntop(peer_str, INET6_ADDRSTRLEN, &ps->peer_ip_addr);
          bgpstream_as_path_snprintf(old_path_str, 4096, old_path);
          bgpstream_as_path_snprintf(new_path_str, 4096, new_path);

          wandio_printf(STATE->outfile, "%" PRIu32 "|" /* time */
                                        "%s|"          /* prefix */
                                        "%s|"          /* collector */
                                        "%" PRIu32 "|" /* peer ASN */
                                        "%s|"          /* peer IP */
                                        "%s|"          /* old-path */
                                        "%s"           /* new-path */
                                        "\n",
                        bgpview_get_time(view), pfx_str, ps->collector_str,
                        ps->peer_asnumber, peer_str, old_path_str,
                        new_path_str);
        }
        bgpstream_as_path_destroy(old_path);
        bgpstream_as_path_destroy(new_path);
      }
    }
  }

  bgpview_iter_destroy(it);
  bgpview_iter_destroy(parent_view_it);
  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_pathchange_alloc()
{
  return &bvc_pathchange;
}

int bvc_pathchange_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_pathchange_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_pathchange_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  /* allocate dynamic memory HERE */

  /* set defaults */

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  /* default to stdout */
  if (state->outfile_pattern == NULL) {
    state->outfile_pattern = strdup("-");
  }

  if (strcmp("-", state->outfile_pattern) == 0 &&
      (state->rotation_interval > 0)) {
    fprintf(stderr,
            "WARN: Cannot rotate output files when writing to stdout\n");
    state->rotation_interval = 0;
  }

  /* outfile is opened when first view is processed */

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
  return -1;
}

void bvc_pathchange_destroy(bvc_t *consumer)
{
  bvc_pathchange_state_t *state = STATE;

  if (state == NULL) {
    return;
  }

  /* frees outfile and outfile_name */
  if (complete_file(consumer) != 0) {
    fprintf(stderr, "WARN: Failed to cleanly close output files\n");
  }

  free(state->outfile_pattern);
  state->outfile_pattern = NULL;

  free(state->outfile_name);
  state->outfile_name = NULL;

  timeseries_kp_free(&state->kp);

  bgpview_destroy(state->parent_view);
  state->parent_view = NULL;

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_pathchange_process_view(bvc_t *consumer, bgpview_t *view)
{
  uint32_t time_begin = epoch_sec();
  uint32_t view_time = bgpview_get_time(view);
  uint32_t file_time = view_time;
  int compress_type;

  if (STATE->outfile == NULL || SHOULD_ROTATE(STATE, view_time)) {
    if (STATE->rotation_interval > 0) {
      if (STATE->outfile != NULL && complete_file(consumer) != 0) {
        fprintf(stderr, "ERROR: Failed to rotate output file\n");
        goto err;
      }

      /* align the time to a multiple of the interval */
      if (STATE->rotate_noalign == 0) {
        file_time =
          (view_time / STATE->rotation_interval) * STATE->rotation_interval;
      }
      STATE->next_rotate_time = file_time + STATE->rotation_interval;
    }

    /* compute the output filename */
    if ((STATE->outfile_name =
           generate_file_name(STATE->outfile_pattern, file_time)) == NULL) {
      goto err;
    }
    compress_type = wandio_detect_compression_type(STATE->outfile_name);
    if ((STATE->outfile =
           wandio_wcreate(STATE->outfile_name, compress_type,
                          STATE->outfile_compress_level, O_CREAT)) == NULL) {
      fprintf(stderr, "ERROR: Could not open %s for writing\n",
              STATE->outfile_name);
      goto err;
    }
  }

  if (diff_paths(consumer, view) != 0) {
    goto err;
  }

  /* NB: this code is borrowed from viewsender */
  if (STATE->parent_view == NULL) {
    /* this is our first */
    if ((STATE->parent_view = bgpview_dup(view)) == NULL) {
      goto err;
    }
  } else {
    /* we have a parent view, just copy into it */
    /* first, clear the destination */
    bgpview_clear(STATE->parent_view);
    if (bgpview_copy(STATE->parent_view, view) != 0) {
      goto err;
    }
  }
  assert(STATE->parent_view != NULL);
  assert(bgpview_get_time(view) == bgpview_get_time(STATE->parent_view));

  uint64_t proc_time = epoch_sec() - time_begin;
  timeseries_kp_set(STATE->kp, STATE->proc_time_idx, proc_time);

  // flush
  if (timeseries_kp_flush(STATE->kp, bgpview_get_time(view)) != 0) {
    fprintf(stderr, "Warning: could not flush %s %" PRIu32 "\n", NAME,
            bgpview_get_time(view));
  }

  return 0;

err:
  return -1;
}
