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

#include "config.h"
#include "bvc_viewsender.h"
#ifdef WITH_BGPVIEW_IO_KAFKA
#include "kafka/bgpview_io_kafka.h"
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
#include "zmq/bgpview_io_zmq.h"
#endif
#include "utils.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <czmq.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>

#define NAME "view-sender"

#define BUFFER_LEN 1024
// bgp.meta.bgpview.consumer.view-sender.{kafka|zmq}.{rrc00}.{metric}
#define META_METRIC_PREFIX_FORMAT  "%s.meta.bgpview.consumer."NAME".%s.%s.%s"

/** A Sync frame will be sent once per N views */
#define SYNC_FREQUENCY 12

#define FILTER_FF_V4CNT_DEFAULT 400000
#define FILTER_FF_V6CNT_DEFAULT 10000

/* macro to access the current consumer state */
#define STATE					\
  (BVC_GET_STATE(consumer, viewsender))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))


/* our 'class' */
static bvc_t bvc_viewsender = {
  BVC_ID_VIEWSENDER,
  NAME,
  BVC_GENERATE_PTRS(viewsender)
};

/* our 'instance' */
typedef struct bvc_viewsender_state {
#ifdef WITH_BGPVIEW_IO_KAFKA
  bgpview_io_kafka_t *kafka_client;
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
  bgpview_io_zmq_client_t *zmq_client;
#endif

  /* our IO type (kafka|zmq) */
  char *io_module;

  /* our instance name (is allowed to be different to instance name given to IO
     module) */
  char *instance;
  char *gr_instance;

  /* should we filter to only full-feed peers */
  int filter_ff_v4cnt;
  int filter_ff_v6cnt;

  /** Timeseries Key Package */
  timeseries_kp_t *kp;

#ifdef WITH_BGPVIEW_IO_KAFKA
  /** Sync frequency */
  int sync_freq;
  /** Number of diffs sent */
  int num_diffs;
  /** Parent view */
  bgpview_t *parent_view;
#endif

  /* Metric Indices */
  int send_time_idx;
  int proc_time_idx;
  int arr_delay_time_idx;

#ifdef WITH_BGPVIEW_IO_KAFKA
  int pfx_cnt_idx;
  int copy_time_idx;
  int common_pfx_idx;
  int added_pfx_idx;
  int removed_pfx_idx;
  int changed_pfx_idx;
  int added_pfx_peer_idx;
  int changed_pfx_peer_idx;
  int removed_pfx_peer_idx;
  int sync_cnt_idx;
#endif
} bvc_viewsender_state_t;

static char *graphite_safe(char *p)
{
  if(p == NULL)
    {
      return p;
    }

  char *r = p;
  while(*p != '\0')
    {
      if(*p == '.')
	{
	  *p = '_';
	}
      if(*p == '*')
	{
	  *p = '-';
	}
      p++;
    }
  return r;
}

/** Create timeseries metrics */
static int create_ts_metrics(bvc_t *consumer)
{

  char buffer[BUFFER_LEN];
  bvc_viewsender_state_t *state = STATE;

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->io_module,
           state->gr_instance, "timing.processing_time");
  if((state->proc_time_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->io_module,
           state->gr_instance, "timing.arrival_delay");
  if((state->arr_delay_time_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

#ifdef WITH_BGPVIEW_IO_KAFKA
  if (state->kafka_client != NULL) {
    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "timing.send_time");
    if((state->send_time_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "timing.copy_time");
    if((state->copy_time_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "diffs.common_pfx_cnt");
    if((state->common_pfx_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "diffs.added_pfx_cnt");
    if((state->added_pfx_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "diffs.removed_pfx_cnt");
    if((state->removed_pfx_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "diffs.changed_pfx_cnt");
    if((state->changed_pfx_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "diffs.added_pfx_peer_cnt");
    if((state->added_pfx_peer_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "diffs.changed_pfx_peer_cnt");
    if((state->changed_pfx_peer_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "diffs.removed_pfx_peer_cnt");
    if((state->removed_pfx_peer_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "sync.pfx_cnt");
    if((state->sync_cnt_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }

    snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
             CHAIN_STATE->metric_prefix, state->io_module,
             state->gr_instance, "pfx_cnt");
    if((state->pfx_cnt_idx =
        timeseries_kp_add_key(STATE->kp, buffer)) == -1)
      {
        return -1;
      }
  }
#endif

  return 0;
}

static int configure_io(bvc_t *consumer)
{
  char *io_options = NULL;

  /* the string at io_module will contain the name of the IO module
     optionally followed by a space and then the arguments to pass
     to the module */
  if((io_options = strchr(STATE->io_module, ' ')) != NULL)
    {
      /* set the space to a nul, which allows io_module to be used
         for the module name, and then increment io_options to
         point to the next character, which will be the start of the
         arg string (or at worst case, the terminating \0 */
      *io_options = '\0';
      io_options++;
    }

  if (0) { /* just to simplify the if/else with macros */ }
#ifdef WITH_BGPVIEW_IO_KAFKA
  else if (strcmp(STATE->io_module, "kafka") == 0)
    {
      fprintf(stderr, "INFO: Starting Kafka IO producer module...\n");
      if ((STATE->kafka_client =
           bgpview_io_kafka_init(BGPVIEW_IO_KAFKA_MODE_PRODUCER,
                                 io_options)) == NULL) {
        goto err;
      }
      if (bgpview_io_kafka_start(STATE->kafka_client) != 0) {
        fprintf(stderr, "ERROR: Could not start Kafka client\n");
        goto err;
      }
    }
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
  else if (strcmp(STATE->io_module, "zmq") == 0)
    {
      fprintf(stderr, "INFO: Starting ZMQ IO producer module...\n");
      if((STATE->zmq_client =
          bgpview_io_zmq_client_init(BGPVIEW_PRODUCER_INTENT_PREFIX)) == NULL)
        {
          fprintf(stderr, "ERROR: could not initialize ZMQ module\n");
          goto err;
        }
      if(bgpview_io_zmq_client_set_opts(STATE->zmq_client, io_options) != 0)
        {
          goto err;
        }
      if(bgpview_io_zmq_client_start(STATE->zmq_client) != 0)
        {
          goto err;
        }
    }
#endif
  else
    {
      fprintf(stderr, "ERROR: Unsupported IO module '%s'\n", STATE->io_module);
      goto err;
    }

  return 0;

 err:
  return -1;
}

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s [options] -n <instance-name> -i <io-module>\n"
          "       -i <module opts>      IO module to use for sending views.\n"
          "                               Available modules:\n",
          consumer->name);
#ifdef WITH_BGPVIEW_IO_KAFKA
  fprintf(stderr,
          "                                - kafka\n");
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
  fprintf(stderr,
          "                                - zmq\n");
#endif
  fprintf(stderr,
          "       -n <instance-name>    Unique name for this sender (required)\n");
#ifdef WITH_BGPVIEW_IO_KAFKA
  fprintf(stderr,
          "       -s <sync-frequency>   Sync frame freq. in # views (default: %d)\n"
          "                               (used only for Kafka)\n",
          SYNC_FREQUENCY);
#endif
  fprintf(stderr,
          "       -4 <pfx-cnt>          Only send peers with > N IPv4 pfxs (default: %d)\n"
          "       -6 <pfx-cnt>          Only send peers with > N IPv6 pfxs (default: %d)\n",
          FILTER_FF_V4CNT_DEFAULT,
          FILTER_FF_V6CNT_DEFAULT);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt, prevoptind;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while(prevoptind = optind,
        (opt = getopt(argc, argv, ":4:6:i:n:s:?")) >= 0)
    {
      if (optind == prevoptind + 2 && optarg && *optarg == '-' ) {
        opt = ':';
        -- optind;
      }
      switch(opt)
	{
        case '4':
          STATE->filter_ff_v4cnt = atoi(optarg);
          break;

        case '6':
          STATE->filter_ff_v6cnt = atoi(optarg);
          break;

        case 'i':
          if (STATE->io_module != NULL)
            {
              fprintf(stderr,
                      "WARN: Only one IO module may be used at a time\n");
              free(STATE->io_module);
            }
          STATE->io_module = strdup(optarg);
          break;

        case 'n':
          STATE->instance = strdup(optarg);
          STATE->gr_instance = strdup(optarg);
          graphite_safe(STATE->gr_instance);
          break;

#ifdef WITH_BGPVIEW_IO_KAFKA
        case 's':
          STATE->sync_freq = atoi(optarg);
          break;
#endif

	case '?':
	case ':':
	default:
	  usage(consumer);
	  return -1;
	}
    }

  if (STATE->io_module == NULL) {
    fprintf(stderr, "ERROR: IO module must be set using -i\n");
    usage(consumer);
    return -1;
  }

  if (STATE->instance == NULL) {
    fprintf(stderr, "ERROR: Producer name must be set using -n\n");
    usage(consumer);
    return -1;
  }

  return 0;
}

static int filter_ff(bgpview_iter_t *iter, bgpview_io_filter_type_t type,
                     void *user)
{
  bvc_t *consumer = (bvc_t *)user;
  return (type == BGPVIEW_IO_FILTER_PFX) ||
    (
     (bgpview_iter_peer_get_pfx_cnt(iter,
                                    BGPSTREAM_ADDR_VERSION_IPV4,
                                    BGPVIEW_FIELD_ACTIVE)
      >= STATE->filter_ff_v4cnt) ||
     (bgpview_iter_peer_get_pfx_cnt(iter,
                                    BGPSTREAM_ADDR_VERSION_IPV6,
                                    BGPVIEW_FIELD_ACTIVE)
      >= STATE->filter_ff_v6cnt)
     );
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_viewsender_alloc()
{
  return &bvc_viewsender;
}


int bvc_viewsender_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_viewsender_state_t *state = NULL;

  if((state = malloc_zero(sizeof(bvc_viewsender_state_t))) == NULL)
    {
      return -1;
    }

  BVC_SET_STATE(consumer, state);

#ifdef WITH_BGPVIEW_IO_KAFKA
  state->sync_freq = SYNC_FREQUENCY;
#endif

  STATE->filter_ff_v4cnt = FILTER_FF_V4CNT_DEFAULT;
  STATE->filter_ff_v6cnt = FILTER_FF_V6CNT_DEFAULT;

  /* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      goto err;
    }

  if (configure_io(consumer) != 0) {
    usage(consumer);
    goto err;
  }

  if((state->kp = timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) == NULL)
    {
      fprintf(stderr, "Error: Could not create timeseries key package\n");
      goto err;
    }

  if(create_ts_metrics(consumer) != 0)
    {
      goto err;
    }

  return 0;

 err:
  return -1;
}


void bvc_viewsender_destroy(bvc_t *consumer)
{
  bvc_viewsender_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  /* deallocate dynamic memory HERE */

  free(state->io_module);
  state->io_module = NULL;

  free(state->instance);
  state->instance = NULL;

  free(state->gr_instance);
  state->gr_instance = NULL;

  timeseries_kp_free(&state->kp);

#ifdef WITH_BGPVIEW_IO_KAFKA
  if (state->kafka_client != NULL) {
    bgpview_io_kafka_destroy(state->kafka_client);
    state->kafka_client = NULL;
    bgpview_destroy(state->parent_view);
    state->parent_view = NULL;
  }
#endif

#ifdef WITH_BGPVIEW_IO_ZMQ
  if (state->zmq_client != NULL) {
    bgpview_io_zmq_client_stop(state->zmq_client);
    bgpview_io_zmq_client_free(state->zmq_client);
    state->zmq_client = NULL;
  }
#endif

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_viewsender_process_view(bvc_t *consumer, bgpview_t *view)
{
  bvc_viewsender_state_t *state = STATE;

  uint64_t start_time = zclock_time()/1000;
  uint64_t arrival_delay = zclock_time()/1000 - bgpview_get_time(view);
  timeseries_kp_set(state->kp, state->arr_delay_time_idx, arrival_delay);

  if (0) { /* just to simplify the if/else with macros */ }
  #ifdef WITH_BGPVIEW_IO_KAFKA
  else if (STATE->kafka_client != NULL)
    {
      bgpview_t *pvp = NULL;

      // are we sending a sync frame or a diff frame?
      if (state->parent_view == NULL || state->num_diffs == state->sync_freq-1) {
        state->num_diffs = 0;
        pvp = NULL;
      } else {
        pvp = state->parent_view;
        state->num_diffs++;
      }

      // send the view
      if (bgpview_io_kafka_send_view(state->kafka_client, view, pvp,
                                     filter_ff, consumer)
          != 0)
        {
          return -1;
        }

      uint64_t send_end = zclock_time()/1000;
      uint64_t send_time = send_end - start_time;

      // do the create/copy
      if (state->parent_view == NULL) {
        if ((state->parent_view = bgpview_dup(view)) == NULL) {
          return -1;
        }
      } else {
        /* we have a parent view, just copy into it */
        /* first, clear the destination */
        bgpview_clear(state->parent_view);
        if (bgpview_copy(state->parent_view, view) != 0) {
          return -1;
        }
      }
      assert(state->parent_view != NULL);
      assert(bgpview_get_time(view) == bgpview_get_time(state->parent_view));

      uint64_t copy_end = zclock_time()/1000;
      uint64_t copy_time = copy_end - send_end;

      // set timeseries metrics
      bgpview_io_kafka_stats_t *stats =
        bgpview_io_kafka_get_stats(state->kafka_client);

      timeseries_kp_set(state->kp, state->send_time_idx, send_time);
      timeseries_kp_set(state->kp, state->copy_time_idx, copy_time);

      timeseries_kp_set(state->kp, state->common_pfx_idx, stats->common_pfxs_cnt);
      timeseries_kp_set(state->kp, state->added_pfx_idx, stats->added_pfxs_cnt);
      timeseries_kp_set(state->kp, state->removed_pfx_idx, stats->removed_pfxs_cnt);
      timeseries_kp_set(state->kp, state->changed_pfx_idx, stats->changed_pfxs_cnt);

      timeseries_kp_set(state->kp, state->added_pfx_peer_idx,
                        stats->added_pfx_peer_cnt);
      timeseries_kp_set(state->kp, state->changed_pfx_peer_idx,
                        stats->changed_pfx_peer_cnt);
      timeseries_kp_set(state->kp, state->removed_pfx_peer_idx,
                        stats->removed_pfx_peer_cnt);

      timeseries_kp_set(state->kp, state->sync_cnt_idx, stats->sync_pfx_cnt);
      timeseries_kp_set(state->kp, state->pfx_cnt_idx, stats->pfx_cnt);
    }
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
  else if (STATE->zmq_client != NULL)
    {
      if (bgpview_io_zmq_client_send_view(state->zmq_client, view,
                                          filter_ff, consumer)
          != 0)
        {
          return -1;
        }
    }
#endif
  else
    {
      assert(0);
      return -1;
    }

  uint64_t proc_time = zclock_time() - start_time;
  timeseries_kp_set(state->kp, state->proc_time_idx, proc_time);

  // flush
  if(timeseries_kp_flush(STATE->kp, bgpview_get_time(view)) != 0)
    {
      fprintf(stderr, "Warning: could not flush %s %"PRIu32"\n",
              NAME, bgpview_get_time(view));
    }

  return 0;
}
