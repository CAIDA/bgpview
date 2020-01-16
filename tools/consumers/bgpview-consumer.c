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
#ifdef WITH_BGPVIEW_IO_FILE
#include "file/bgpview_io_file.h"
#endif
#ifdef WITH_BGPVIEW_IO_KAFKA
#include "kafka/bgpview_io_kafka.h"
#endif
#ifdef WITH_BGPVIEW_IO_BSRT
#include "bsrt/bgpview_io_bsrt.h"
#endif
#ifdef WITH_BGPVIEW_IO_TEST
#include "test/bgpview_io_test.h"
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
#include "zmq/bgpview_io_zmq.h"
#endif
#include "bgpview.h"
#include "bgpview_io.h"
#include "bgpview_consumer_manager.h"
#include "config.h"
#include "utils.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static bgpview_consumer_manager_t *manager = NULL;
static timeseries_t *timeseries = NULL;

static bgpstream_patricia_tree_t *pfx_tree = NULL;
static bgpstream_pfx_set_t *pfx_set = NULL;
static bgpstream_id_set_t *asn_set = NULL;

typedef int(filter_parser_func)(char *value);

enum filter_type {
  FILTER_PFX = 0,
  FILTER_PFX_EXACT = 1,
  FILTER_ORIGIN = 2,
};
#define FILTER_CNT 3

static const char *filter_type_str[] = {
  "pfx", "pfx-exact", "origin",
};

static const char *filter_desc[] = {
  "match on prefix and sub-prefixes", "match on prefix", "match on origin ASN",
};

static int filter_cnt = 0;
static int filter_cnts[] = {
  0, 0, 0,
};

static int peer_filters_cnt = 0;
static int pfx_filters_cnt = 0;
static int pfx_peer_filters_cnt = 0;

static bgpview_t *view = NULL;

#ifdef WITH_BGPVIEW_IO_FILE
static io_t *file_handle = NULL;
#endif
#ifdef WITH_BGPVIEW_IO_KAFKA
static bgpview_io_kafka_t *kafka_client = NULL;
#endif
#ifdef WITH_BGPVIEW_IO_BSRT
static bgpview_io_bsrt_t *bsrt_handle = NULL;
#endif
#ifdef WITH_BGPVIEW_IO_TEST
static bgpview_io_test_t *test_generator = NULL;
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
static bgpview_io_zmq_client_t *zmq_client = NULL;
#endif

static int parse_pfx(char *value)
{
  bgpstream_pfx_t pfx;

  if (value == NULL) {
    fprintf(stderr, "ERROR: Missing value for prefix filter\n");
    return -1;
  }

  if (bgpstream_str2pfx(value, &pfx) == NULL) {
    fprintf(stderr, "ERROR: Malformed prefix filter value '%s'\n", value);
    return -1;
  }

  if (bgpstream_patricia_tree_insert(pfx_tree, &pfx) ==
      NULL) {
    fprintf(stderr, "ERROR: Failed to insert pfx filter into tree\n");
  }

  pfx_filters_cnt++;
  return 0;
}

static int parse_pfx_exact(char *value)
{
  bgpstream_pfx_t pfx;

  if (value == NULL) {
    fprintf(stderr, "ERROR: Missing value for prefix filter\n");
    return -1;
  }

  if (bgpstream_str2pfx(value, &pfx) == NULL) {
    fprintf(stderr, "ERROR: Malformed prefix filter value '%s'\n", value);
    return -1;
  }

  if (bgpstream_pfx_set_insert(pfx_set, &pfx) < 0) {
    fprintf(stderr, "ERROR: Failed to insert pfx filter into set\n");
  }

  pfx_filters_cnt++;
  return 0;
}

static int parse_origin(char *value)
{
  char *endptr = NULL;
  uint32_t asn = strtoul(value, &endptr, 10);
  if (*endptr != '\0') {
    fprintf(stderr, "ERROR: Invalid origin ASN value '%s'\n", value);
    return -1;
  }
  if (bgpstream_id_set_insert(asn_set, asn) < 0) {
    fprintf(stderr, "ERROR: Could not insert origin filter into set\n");
    return -1;
  }
  pfx_peer_filters_cnt++;
  return 0;
}

static filter_parser_func *filter_parsers[] = {
  parse_pfx, parse_pfx_exact, parse_origin,
};

static int match_pfx(bgpstream_pfx_t *pfx)
{
  return ((bgpstream_patricia_tree_get_pfx_overlap_info(pfx_tree, pfx) &
           (BGPSTREAM_PATRICIA_EXACT_MATCH |
            BGPSTREAM_PATRICIA_LESS_SPECIFICS)) != 0);
}

static int match_pfx_exact(bgpstream_pfx_t *pfx)
{
  return bgpstream_pfx_set_exists(pfx_set, pfx);
}

static bgpview_io_filter_pfx_cb_t *filter_pfx_matchers[] = {
  match_pfx, match_pfx_exact, NULL,
};

static int match_pfx_peer_origin(bgpstream_as_path_store_path_t *store_path)
{
  bgpstream_as_path_seg_t *seg;
  seg = bgpstream_as_path_store_path_get_origin_seg(store_path);
  return (seg->type == BGPSTREAM_AS_PATH_SEG_ASN &&
          bgpstream_id_set_exists(
            asn_set, ((bgpstream_as_path_seg_asn_t *)seg)->asn) != 0);
}

static bgpview_io_filter_pfx_peer_cb_t *filter_pfx_peer_matchers[] = {
  NULL, NULL, match_pfx_peer_origin,
};

static int filters_init(void)
{
  if ((pfx_tree = bgpstream_patricia_tree_create(NULL)) == NULL) {
    return -1;
  }

  if ((pfx_set = bgpstream_pfx_set_create()) == NULL) {
    return -1;
  }

  if ((asn_set = bgpstream_id_set_create()) == NULL) {
    return -1;
  }

  return 0;
}

static void filters_destroy(void)
{
  bgpstream_patricia_tree_destroy(pfx_tree);
  pfx_tree = NULL;
  bgpstream_pfx_set_destroy(pfx_set);
  pfx_set = NULL;
  bgpstream_id_set_destroy(asn_set);
  asn_set = NULL;
}

static int parse_filter(char *filter_str)
{
  char *val = NULL;
  int i;
  int found = 0;

  /* first, find the value (if any) */
  val = strchr(filter_str, ':');

  if (val != NULL) {
    *val = '\0';
    val++;
  }

  /* now find the type */
  for (i = 0; i < FILTER_CNT; i++) {
    if (strcmp(filter_type_str[i], filter_str) == 0) {
      if (filter_parsers[i](val) != 0) {
        return -1;
      }
      filter_cnts[i]++;
      filter_cnt++;
      found = 1;
      break;
    }
  }

  if (found == 0) {
    fprintf(stderr, "ERROR: Invalid filter type '%s'\n", filter_str);
    return -1;
  }

  return 0;
}

static int filter_pfx(bgpstream_pfx_t *pfx)
{
  int i, ret;

  /* if this func is called, at least one type of filter is enabled */
  for (i = 0; i < FILTER_CNT; i++) {
    if (filter_pfx_matchers[i] != NULL && filter_cnts[i] > 0 &&
        (ret = filter_pfx_matchers[i](pfx)) != 0) {
      return ret;
    }
  }

  return 0;
}

static int filter_peer(bgpstream_peer_sig_t *peersig)
{
  return 1;
}

static int filter_pfx_peer(bgpstream_as_path_store_path_t *store_path)
{
  int i, ret;

  /* if this func is called, at least one type of filter is enabled */
  for (i = 0; i < FILTER_CNT; i++) {
    if (filter_pfx_peer_matchers[i] != NULL && filter_cnts[i] > 0 &&
        (ret = filter_pfx_peer_matchers[i](store_path)) != 0) {
      return ret;
    }
  }

  return 0;
}

static void filter_usage(void)
{
  int i;
  for (i = 0; i < FILTER_CNT; i++) {
    fprintf(stderr, "                               - %s (%s)\n",
            filter_type_str[i], filter_desc[i]);
  }
}

static void timeseries_usage(void)
{
  assert(timeseries != NULL);
  timeseries_backend_t **backends = NULL;
  int i;

  backends = timeseries_get_all_backends(timeseries);

  fprintf(stderr, "                               available backends:\n");
  for (i = 0; i < TIMESERIES_BACKEND_ID_LAST; i++) {
    /* skip unavailable backends */
    if (backends[i] == NULL) {
      continue;
    }

    assert(timeseries_backend_get_name(backends[i]));
    fprintf(stderr, "                                - %s\n",
            timeseries_backend_get_name(backends[i]));
  }
}

static void consumer_usage(void)
{
  assert(manager != NULL);
  bvc_t **avail_consumers = NULL;
  int i;

  /* get the available consumers from the manager */
  avail_consumers = bgpview_consumer_manager_get_all_consumers(manager);

  fprintf(stderr, "                               available consumers:\n");
  for (i = 0; i < BVC_ID_LAST; i++) {
    /* skip unavailable consumers */
    if (avail_consumers[i] == NULL) {
      continue;
    }

    assert(bvc_get_name(avail_consumers[i]));
    fprintf(stderr, "                                - %s\n",
            bvc_get_name(avail_consumers[i]));
  }
}

static void usage(const char *name)
{
  /* top-level */
  fprintf(stderr, "usage: %s [<options>]\n", name);

  /* IO module config */
  fprintf(stderr,
          "       -i\"<module> <opts>\"     IO module to use for obtaining views.\n"
          "                               Available modules:\n");
#ifdef WITH_BGPVIEW_IO_FILE
  fprintf(stderr, "                                - file\n");
#endif
#ifdef WITH_BGPVIEW_IO_TEST
  fprintf(stderr, "                                - test\n");
#endif
#ifdef WITH_BGPVIEW_IO_KAFKA
  fprintf(stderr, "                                - kafka\n");
#endif
#ifdef WITH_BGPVIEW_IO_BSRT
  fprintf(stderr, "                                - bsrt\n");
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
  fprintf(stderr, "                                - zmq\n");
#endif

  /* Timeseries config */
  fprintf(stderr,
          "       -b <backend>          Enable the given timeseries backend,\n"
          "                               -b can be used multiple times\n");
  timeseries_usage();
  fprintf(stderr,
          "       -m <prefix>           Metric prefix (default: %s)\n"
          "       -N <num-views>        Maximum number of views to process\n"
          "                               (default: infinite)\n",
          BGPVIEW_METRIC_PREFIX_DEFAULT);

  /* Consumers config */
  fprintf(stderr, "       -c\"<consumer> <opts>\" Consumer to activate (can be "
                  "used multiple times)\n");
  consumer_usage();

  /* Filter config */
  fprintf(stderr,
          "       -f <type:value>       Add a filter. Supported types are:\n");
  filter_usage();
}

static int configure_io(char *io_module)
{
  char *io_options = NULL;

  /* the string at io_module will contain the name of the IO module
   optionally followed by a space and then the arguments to pass
   to the module */
  if ((io_options = strchr(io_module, ' ')) != NULL) {
    /* set the space to a nul, which allows io_module to be used
       for the module name, and then increment io_options to
       point to the next character, which will be the start of the
       arg string (or at worst case, the terminating \0 */
    *io_options = '\0';
    io_options++;
  }

  if (0) { /* just to simplify the if/else with macros */
  }
#ifdef WITH_BGPVIEW_IO_FILE
  else if (strcmp(io_module, "file") == 0) {
    if (io_options == NULL || strlen(io_options) == 0) {
      fprintf(stderr,
              "ERROR: filename must be provided when using the file module\n");
      goto err;
    }
    if ((file_handle = wandio_create(io_options)) == NULL) {
      fprintf(stderr, "ERROR: Could not open BGPView file '%s'\n", io_options);
      goto err;
    }
  }
#endif
#ifdef WITH_BGPVIEW_IO_KAFKA
  else if (strcmp(io_module, "kafka") == 0) {
    fprintf(stderr, "INFO: Starting Kakfa IO consumer module...\n");
    if ((kafka_client = bgpview_io_kafka_init(
           BGPVIEW_IO_KAFKA_MODE_AUTO_CONSUMER, io_options)) == NULL) {
      fprintf(stderr, "ERROR: could not initialize Kafka module\n");
      goto err;
    }
    if (bgpview_io_kafka_start(kafka_client) != 0) {
      goto err;
    }
  }
#endif
#ifdef WITH_BGPVIEW_IO_BSRT
  else if (strcmp(io_module, "bsrt") == 0) {
    fprintf(stderr, "INFO: Starting BSRT IO consumer module...\n");
    if ((bsrt_handle = bgpview_io_bsrt_init(io_options, timeseries)) == NULL) {
      fprintf(stderr, "ERROR: could not initialize BSRT module\n");
      goto err;
    }
    if (bgpview_io_bsrt_start(bsrt_handle) != 0) {
      goto err;
    }
  }
#endif
#ifdef WITH_BGPVIEW_IO_TEST
  else if (strcmp(io_module, "test") == 0) {
    fprintf(stderr, "INFO: Starting Test View Generator IO module...\n");
    if ((test_generator = bgpview_io_test_create(io_options)) == NULL) {
      fprintf(stderr, "ERROR: could not initialize Test module\n");
      goto err;
    }
  }
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
  else if (strcmp(io_module, "zmq") == 0) {
    fprintf(stderr, "INFO: Starting ZMQ consumer IO module...\n");
    if ((zmq_client = bgpview_io_zmq_client_init(0)) == NULL) {
      fprintf(stderr, "ERROR: could not initialize ZMQ module\n");
      goto err;
    }
    if (bgpview_io_zmq_client_set_opts(zmq_client, io_options) != 0) {
      goto err;
    }
    if (bgpview_io_zmq_client_start(zmq_client) != 0) {
      goto err;
    }
  }
#endif
  else {
    fprintf(stderr, "ERROR: Unsupported IO module '%s'\n", io_module);
    goto err;
  }

  return 0;

err:
  return -1;
}

static void shutdown_io(void)
{
#ifdef WITH_BGPVIEW_IO_FILE
  if (file_handle != NULL) {
    wandio_destroy(file_handle);
    file_handle = NULL;
  }
#endif
#ifdef WITH_BGPVIEW_IO_KAFKA
  if (kafka_client != NULL) {
    bgpview_io_kafka_destroy(kafka_client);
    kafka_client = NULL;
  }
#endif
#ifdef WITH_BGPVIEW_IO_BSRT
  if (bsrt_handle != NULL) {
    bgpview_io_bsrt_destroy(bsrt_handle);
    bsrt_handle = NULL;
  }
#endif
#ifdef WITH_BGPVIEW_IO_TEST
  if (test_generator != NULL) {
    bgpview_io_test_destroy(test_generator);
    test_generator = NULL;
  }
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
  if (zmq_client != NULL) {
    bgpview_io_zmq_client_stop(zmq_client);
    bgpview_io_zmq_client_free(zmq_client);
    zmq_client = NULL;
  }
#endif
}

static int recv_view(char *io_module)
{
  if (0) { /* just to simplify the if/else with macros */
  }
#ifdef WITH_BGPVIEW_IO_FILE
  else if (strcmp(io_module, "file") == 0) {
    bgpview_clear(view);
    return bgpview_io_file_read(
      file_handle, view, (peer_filters_cnt != 0) ? filter_peer : NULL,
      (pfx_filters_cnt != 0) ? filter_pfx : NULL,
      (pfx_peer_filters_cnt != 0) ? filter_pfx_peer : NULL);
  }
#endif
#ifdef WITH_BGPVIEW_IO_KAFKA
  else if (strcmp(io_module, "kafka") == 0) {
    return bgpview_io_kafka_recv_view(
      kafka_client, view, (peer_filters_cnt != 0) ? filter_peer : NULL,
      (pfx_filters_cnt != 0) ? filter_pfx : NULL,
      (pfx_peer_filters_cnt != 0) ? filter_pfx_peer : NULL);
  }
#endif
#ifdef WITH_BGPVIEW_IO_BSRT
  else if (strcmp(io_module, "bsrt") == 0) {
    return bgpview_io_bsrt_recv_view(bsrt_handle);
  }
#endif
#ifdef WITH_BGPVIEW_IO_TEST
  else if (strcmp(io_module, "test") == 0) {
    bgpview_clear(view);
    return bgpview_io_test_generate_view(test_generator, view);
  }
#endif
#ifdef WITH_BGPVIEW_IO_ZMQ
  else if (strcmp(io_module, "zmq") == 0) {
    bgpview_clear(view);
    return bgpview_io_zmq_client_recv_view(
      zmq_client, BGPVIEW_IO_ZMQ_CLIENT_RECV_MODE_BLOCK, view,
      (peer_filters_cnt != 0) ? filter_peer : NULL,
      (pfx_filters_cnt != 0) ? filter_pfx : NULL,
      (pfx_peer_filters_cnt != 0) ? filter_pfx_peer : NULL);
  }
#endif

  return -1;
}

int main(int argc, char **argv)
{
  /* for option parsing */
  int opt;
  int prevoptind;

  /* to store command line argument values */
  char *consumer_cmds[BVC_ID_LAST];
  int consumer_cmds_cnt = 0;
  int i;

  char *metric_prefix = NULL;

  char *backends[TIMESERIES_BACKEND_ID_LAST];
  int backends_cnt = 0;
  char *backend_arg_ptr = NULL;
  timeseries_backend_t *backend = NULL;

  int processed_view_limit = -1;
  int processed_view = 0;

  char *io_module = NULL;

  if (filters_init() != 0) {
    fprintf(stderr, "ERROR: Could not initialize filters\n");
    return -1;
  }

  if ((timeseries = timeseries_init()) == NULL) {
    fprintf(stderr, "ERROR: Could not initialize libtimeseries\n");
    return -1;
  }

  /* better just grab a pointer to the manager */
  if ((manager = bgpview_consumer_manager_create(timeseries)) == NULL) {
    fprintf(stderr, "ERROR: Could not initialize consumer manager\n");
    return -1;
  }

  while (prevoptind = optind,
         (opt = getopt(argc, argv, "f:i:m:N:b:c:v?")) >= 0) {
    if (optind == prevoptind + 2 && (optarg && *optarg == '-')) {
      fprintf(stderr, "ERROR: argument for %s looks like an option "
          "(remove the space after %s to force the argument)\n",
          argv[optind-2], argv[optind-2]);
      return -1;
    }
    switch (opt) {

    case 'f':
      if (parse_filter(optarg) != 0) {
        usage(argv[0]);
        return -1;
      }
      break;

    case 'i':
      if (io_module != NULL) {
        fprintf(stderr, "WARN: Only one IO module may be used at a time\n");
      }
      io_module = optarg;
      break;

    case 'm':
      metric_prefix = optarg;
      break;

    case 'N':
      processed_view_limit = atoi(optarg);
      break;

    case 'b':
      backends[backends_cnt++] = optarg;
      break;

    case 'c':
      if (consumer_cmds_cnt >= BVC_ID_LAST) {
        fprintf(stderr, "ERROR: At most %d consumers can be enabled\n",
                BVC_ID_LAST);
        usage(argv[0]);
        return -1;
      }
      consumer_cmds[consumer_cmds_cnt++] = optarg;
      break;

    case 'v':
      fprintf(stderr, "bgpview version %d.%d.%d\n", BGPVIEW_MAJOR_VERSION,
              BGPVIEW_MID_VERSION, BGPVIEW_MINOR_VERSION);
      return 0;

    default:
      usage(argv[0]);
      return -1;
    }
  }

  /* NB: once getopt completes, optind points to the first non-option
     argument */

  if (metric_prefix != NULL) {
    bgpview_consumer_manager_set_metric_prefix(manager, metric_prefix);
  }

  if (io_module == NULL) {
    fprintf(stderr, "ERROR: An IO module must be specified using -i\n");
    usage(argv[0]);
    return -1;
  }

  if (consumer_cmds_cnt == 0) {
    fprintf(stderr, "ERROR: Consumer(s) must be specified using -c\n");
    usage(argv[0]);
    return -1;
  }

  if (backends_cnt == 0) {
    fprintf(
      stderr,
      "ERROR: At least one timeseries backend must be specified using -b\n");
    usage(argv[0]);
    goto err;
  }

  /* enable the backends that were requested */
  for (i = 0; i < backends_cnt; i++) {
    /* the string at backends[i] will contain the name of the plugin,
       optionally followed by a space and then the arguments to pass
       to the plugin */
    if ((backend_arg_ptr = strchr(backends[i], ' ')) != NULL) {
      /* set the space to a nul, which allows backends[i] to be used
         for the backend name, and then increment plugin_arg_ptr to
         point to the next character, which will be the start of the
         arg string (or at worst case, the terminating \0 */
      *backend_arg_ptr = '\0';
      backend_arg_ptr++;
    }

    /* lookup the backend using the name given */
    if ((backend = timeseries_get_backend_by_name(timeseries, backends[i])) ==
        NULL) {
      fprintf(stderr, "ERROR: Invalid backend name (%s)\n", backends[i]);
      usage(argv[0]);
      goto err;
    }

    if (timeseries_enable_backend(backend, backend_arg_ptr) != 0) {
      fprintf(stderr, "ERROR: Failed to initialized backend (%s)", backends[i]);
      usage(argv[0]);
      goto err;
    }
  }

  for (i = 0; i < consumer_cmds_cnt; i++) {
    assert(consumer_cmds[i] != NULL);
    if (bgpview_consumer_manager_enable_consumer_from_str(
          manager, consumer_cmds[i]) == NULL) {
      usage(argv[0]);
      goto err;
    }
  }

  if (configure_io(io_module) != 0) {
    usage(argv[0]);
    goto err;
  }

  if (0) { /* just to simplify the if/else with macros */
  }
#ifdef WITH_BGPVIEW_IO_BSRT
  else if (strcmp(io_module, "bsrt") == 0) {
    // Borrow the view generated by bsrt
    view = bgpview_io_bsrt_get_view_ptr(bsrt_handle);
    if (filter_cnt > 0) {
      fprintf(stderr, "ERROR: -f filter option is not compatible with bsrt "
          "io module.  Use bsrt options instead.\n");
      // TODO: convert -f options to bgpstream_add_filter(bsrt->stream, ...)
      goto err;
    }
  }
#endif
  else {
    // Create our own view
    if ((view = bgpview_create(NULL, NULL, NULL, NULL)) == NULL) {
      fprintf(stderr, "ERROR: Could not create view\n");
      goto err;
    }
    /* disable per-pfx-per-peer user pointer */
    bgpview_disable_user_data(view);
  }

  while (recv_view(io_module) == 0) {
    if (bgpview_consumer_manager_process_view(manager, view) != 0) {
      fprintf(stderr, "ERROR: Failed to process view at %d\n",
              bgpview_get_time(view));
      goto err;
    }

    processed_view++;

    if (processed_view_limit > 0 && processed_view >= processed_view_limit) {
      fprintf(stderr, "Processed %d view(s).\n", processed_view);
      break;
    }
  }

  fprintf(stderr, "INFO: Shutting down...\n");
  shutdown_io();
  fprintf(stderr, "INFO: Destroying filters...\n");
  filters_destroy();
  fprintf(stderr, "INFO: Destroying BGPView...\n");
  bgpview_destroy(view);
  fprintf(stderr, "INFO: Destroying Consumer Manager...\n");
  bgpview_consumer_manager_destroy(&manager);
  fprintf(stderr, "INFO: Destroying libtimeseries...\n");
  timeseries_free(&timeseries);
  fprintf(stderr, "INFO: Shutdown complete\n");
  return 0;

err:
  shutdown_io();
  filters_destroy();
  bgpview_destroy(view);
  bgpview_consumer_manager_destroy(&manager);
  timeseries_free(&timeseries);
  return -1;
}
