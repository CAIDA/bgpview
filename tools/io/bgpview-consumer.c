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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>       /* time */

/* this must be all we include from bgpview */
#include "bgpview_io_client.h"
#include "bgpview.h"
#include "bgpview_io_common.h"
#include "bgpview_consumer_manager.h"

#include "config.h"
#include "utils.h"

static bgpview_consumer_manager_t *manager = NULL;
static timeseries_t *timeseries = NULL;

static bgpstream_patricia_tree_t *pfx_tree = NULL;
static bgpstream_pfx_storage_set_t *pfx_set = NULL;
static bgpstream_id_set_t *asn_set = NULL;

typedef int (filter_parser_func)(char *value);

enum filter_type {
  FILTER_PFX = 0,
  FILTER_PFX_EXACT = 1,
  FILTER_ORIGIN = 2,
};
#define FILTER_CNT 3

static char *filter_type_str[] = {
  "pfx",
  "pfx-exact",
  "origin",
};

static char *filter_desc[] = {
  "match on prefix and sub-prefixes",
  "match on prefix",
  "match on origin ASN",
};

static int filter_cnts[] = {
  0,
  0,
  0,
};

static int peer_filters_cnt = 0;
static int pfx_filters_cnt = 0;
static int pfx_peer_filters_cnt = 0;

static int parse_pfx(char *value)
{
  bgpstream_pfx_storage_t pfx;

  if(value == NULL)
    {
      fprintf(stderr, "ERROR: Missing value for prefix filter\n");
      return -1;
    }

  if(bgpstream_str2pfx(value, &pfx) == NULL)
    {
      fprintf(stderr, "ERROR: Malformed prefix filter value '%s'\n", value);
      return -1;
    }

  if(bgpstream_patricia_tree_insert(pfx_tree, (bgpstream_pfx_t*)&pfx) == NULL)
    {
      fprintf(stderr, "ERROR: Failed to insert pfx filter into tree\n");
    }

  pfx_filters_cnt++;
  return 0;
}

static int parse_pfx_exact(char *value)
{
  bgpstream_pfx_storage_t pfx;

  if(value == NULL)
    {
      fprintf(stderr, "ERROR: Missing value for prefix filter\n");
      return -1;
    }

  if(bgpstream_str2pfx(value, &pfx) == NULL)
    {
      fprintf(stderr, "ERROR: Malformed prefix filter value '%s'\n", value);
      return -1;
    }

  if(bgpstream_pfx_storage_set_insert(pfx_set, &pfx) < 0)
    {
      fprintf(stderr, "ERROR: Failed to insert pfx filter into set\n");
    }

  pfx_filters_cnt++;
  return 0;
}

static int parse_origin(char *value)
{
  char *endptr = NULL;
  int asn = strtoul(value, &endptr, 10);
  if(*endptr != '\0')
    {
      fprintf(stderr, "ERROR: Invalid origin ASN value '%s'\n", value);
      return -1;
    }
  if(bgpstream_id_set_insert(asn_set, asn) < 0)
    {
      fprintf(stderr, "ERROR: Could not insert origin filter into set\n");
      return -1;
    }
  pfx_peer_filters_cnt++;
  return 0;
}

static filter_parser_func *filter_parsers[] = {
  parse_pfx,
  parse_pfx_exact,
  parse_origin,
};

static int match_pfx(bgpstream_pfx_t *pfx)
{
  return ((bgpstream_patricia_tree_get_pfx_overlap_info(pfx_tree, pfx) &
           (BGPSTREAM_PATRICIA_EXACT_MATCH | BGPSTREAM_PATRICIA_LESS_SPECIFICS)) != 0);
}

static int match_pfx_exact(bgpstream_pfx_t *pfx)
{
  return bgpstream_pfx_storage_set_exists(pfx_set,
                                           (bgpstream_pfx_storage_t*)pfx);
}

static bgpview_io_filter_pfx_cb_t *filter_pfx_matchers[] = {
  match_pfx,
  match_pfx_exact,
  NULL,
};

static int match_pfx_peer_origin(bgpstream_as_path_store_path_t *store_path)
{
  bgpstream_as_path_seg_t *seg;
  seg = bgpstream_as_path_store_path_get_origin_seg(store_path);
  return (seg->type == BGPSTREAM_AS_PATH_SEG_ASN &&
          bgpstream_id_set_exists(asn_set,
                                  ((bgpstream_as_path_seg_asn_t*)seg)->asn) != 0);
}

static bgpview_io_filter_pfx_peer_cb_t *filter_pfx_peer_matchers[] = {
  NULL,
  NULL,
  match_pfx_peer_origin,
};

static int filters_init()
{
  if((pfx_tree = bgpstream_patricia_tree_create(NULL)) == NULL)
    {
      return -1;
    }

  if((pfx_set = bgpstream_pfx_storage_set_create()) == NULL)
    {
      return -1;
    }

  if((asn_set = bgpstream_id_set_create()) == NULL)
    {
      return -1;
    }

  return 0;
}

static void filters_destroy()
{
  bgpstream_patricia_tree_destroy(pfx_tree);
  pfx_tree = NULL;
  bgpstream_pfx_storage_set_destroy(pfx_set);
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

  if(val != NULL)
    {
      *val = '\0';
      val++;
    }

  /* now find the type */
  for(i=0; i<FILTER_CNT; i++)
    {
      if(strcmp(filter_type_str[i], filter_str) == 0)
        {
          if(filter_parsers[i](val) != 0)
            {
              return -1;
            }
          filter_cnts[i]++;
          found = 1;
          break;
        }
    }

  if(found == 0)
    {
      fprintf(stderr, "ERROR: Invalid filter type '%s'\n", filter_str);
      return -1;
    }

  return 0;
}

static int filter_pfx(bgpstream_pfx_t *pfx)
{
  int i, ret;

  /* if this func is called, at least one type of filter is enabled */
  for(i=0; i<FILTER_CNT; i++)
    {
      if(filter_pfx_matchers[i] != NULL &&
         filter_cnts[i] > 0 && (ret = filter_pfx_matchers[i](pfx)) != 0)
        {
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
  for(i=0; i<FILTER_CNT; i++)
    {
      if(filter_pfx_peer_matchers[i] != NULL &&
         filter_cnts[i] > 0 && (ret = filter_pfx_peer_matchers[i](store_path)) != 0)
        {
          return ret;
        }
    }

  return 0;
}

static void filter_usage()
{
  int i;
  for(i = 0; i < FILTER_CNT; i++)
    {
      fprintf(stderr, "                               - %s (%s)\n",
              filter_type_str[i], filter_desc[i]);
    }
}

static void timeseries_usage()
{
  assert(timeseries != NULL);
  timeseries_backend_t **backends = NULL;
  int i;

  backends = timeseries_get_all_backends(timeseries);

  fprintf(stderr,
	  "                               available backends:\n");
  for(i = 0; i < TIMESERIES_BACKEND_ID_LAST; i++)
    {
      /* skip unavailable backends */
      if(backends[i] == NULL)
	{
	  continue;
	}

      assert(timeseries_backend_get_name(backends[i]));
      fprintf(stderr, "                                - %s\n",
	      timeseries_backend_get_name(backends[i]));
    }
}

static void consumer_usage()
{
  assert(manager != NULL);
  bvc_t **avail_consumers = NULL;
  int i;

  /* get the available consumers from the manager */
  avail_consumers = bgpview_consumer_manager_get_all_consumers(manager);

  fprintf(stderr,
	  "                               available consumers:\n");
  for(i = 0; i < BVC_ID_LAST; i++)
    {
      /* skip unavailable consumers */
      if(avail_consumers[i] == NULL)
	{
	  continue;
	}

      assert(bvc_get_name(avail_consumers[i]));
      fprintf(stderr,
	      "                                - %s\n",
	      bvc_get_name(avail_consumers[i]));
    }
}

static void usage(const char *name)
{
  fprintf(stderr,
	  "usage: %s [<options>]\n"
	  "       -b <backend>          Enable the given timeseries backend,\n"
	  "                               -b can be used multiple times\n",
	  name);
  timeseries_usage();
  fprintf(stderr,
          "       -m <prefix>           Metric prefix (default: %s)\n"
          "       -N <num-views>        Maximum number of views to process before the consumer stops\n"
          "                               (default: infinite)\n",
          BGPVIEW_METRIC_PREFIX_DEFAULT
          );
  fprintf(stderr,
	  "       -c <consumer>         Consumer to active (can be used multiple times)\n");
  consumer_usage();
  fprintf(stderr,
          "       -f <type:value>       Add a filter. Supported types are:\n");
  filter_usage();
  fprintf(stderr,
	  "       -i <interval-ms>      Time in ms between heartbeats to server\n"
	  "                               (default: %d)\n"
          "       -I <interest>         Advertise the given interest. May be used multiple times\n"
          "                               One of: first-full, full, partial\n"
	  "       -l <beats>            Number of heartbeats that can go by before the\n"
	  "                               server is declared dead (default: %d)\n"
	  "       -n <identity>         Globally unique client name (default: random)\n"
	  "       -r <retry-min>        Min wait time (in msec) before reconnecting server\n"

	  "                               (default: %d)\n"
	  "       -R <retry-max>        Max wait time (in msec) before reconnecting server\n"
	  "                               (default: %d)\n"
	  "       -s <server-uri>       0MQ-style URI to connect to server on\n"
	  "                               (default: %s)\n"
          "       -S <server-sub-uri>   0MQ-style URI to subscribe to tables on\n"
          "                               (default: %s)\n",
	  BGPVIEW_IO_HEARTBEAT_INTERVAL_DEFAULT,
	  BGPVIEW_IO_HEARTBEAT_LIVENESS_DEFAULT,
	  BGPVIEW_IO_RECONNECT_INTERVAL_MIN,
	  BGPVIEW_IO_RECONNECT_INTERVAL_MAX,
	  BGPVIEW_IO_CLIENT_SERVER_URI_DEFAULT,
	  BGPVIEW_IO_CLIENT_SERVER_SUB_URI_DEFAULT);
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

  const char *server_uri = NULL;
  const char *server_sub_uri = NULL;
  const char *identity = NULL;

  uint64_t heartbeat_interval = BGPVIEW_IO_HEARTBEAT_INTERVAL_DEFAULT;
  int heartbeat_liveness      = BGPVIEW_IO_HEARTBEAT_LIVENESS_DEFAULT;
  uint64_t reconnect_interval_min = BGPVIEW_IO_RECONNECT_INTERVAL_MIN;
  uint64_t reconnect_interval_max = BGPVIEW_IO_RECONNECT_INTERVAL_MAX;

  uint8_t interests = 0;
  uint8_t intents = 0;
  bgpview_io_client_t *client = NULL;

  int rx_interests;
  bgpview_t *view = NULL;
  int processed_view_limit = -1;
  int processed_view = 0;

  if(filters_init() != 0)
    {
      fprintf(stderr, "ERROR: Could not initialize filters\n");
      return -1;
    }

  if((timeseries = timeseries_init()) == NULL)
    {
      fprintf(stderr, "ERROR: Could not initialize libtimeseries\n");
      return -1;
    }

  /* better just grab a pointer to the manager */
  if((manager = bgpview_consumer_manager_create(timeseries)) == NULL)
    {
      fprintf(stderr, "ERROR: Could not initialize consumer manager\n");
      return -1;
    }

  while(prevoptind = optind,
	(opt = getopt(argc, argv, ":f:m:N:b:c:i:I:l:n:r:R:s:S:v?")) >= 0)
    {
      if (optind == prevoptind + 2 && *optarg == '-' ) {
        opt = ':';
        -- optind;
      }
      switch(opt)
	{
	case ':':
	  fprintf(stderr, "ERROR: Missing option argument for -%c\n", optopt);
	  usage(argv[0]);
	  return -1;
	  break;

        case 'f':
          if (parse_filter(optarg) != 0)
            {
              usage(argv[0]);
              return -1;
            }

	case 'm':
	  metric_prefix = strdup(optarg);
	  break;

        case 'N':
          processed_view_limit = atoi(optarg);
          break;

	case 'b':
	  backends[backends_cnt++] = strdup(optarg);
	  break;

	case 'c':
	  if(consumer_cmds_cnt >= BVC_ID_LAST)
	    {
	      fprintf(stderr, "ERROR: At most %d consumers can be enabled\n",
		      BVC_ID_LAST);
	      usage(argv[0]);
	      return -1;
	    }
	  consumer_cmds[consumer_cmds_cnt++] = optarg;
	  break;

	case 'i':
	  heartbeat_interval = atoi(optarg);
	  break;

        case 'I':
          if(strcmp(optarg, "first-full") == 0)
            {
              interests |= BGPVIEW_CONSUMER_INTEREST_FIRSTFULL;
            }
          else if(strcmp(optarg, "full") == 0)
            {
              interests |= BGPVIEW_CONSUMER_INTEREST_FULL;
            }
          else if(strcmp(optarg, "partial") == 0)
            {
              interests |= BGPVIEW_CONSUMER_INTEREST_PARTIAL;
            }
          else
            {
              fprintf(stderr,
                      "ERROR: Invalid interest (%s)."
                      "Interest must be one of "
                      "'first-full', 'full', or 'partial'\n", optarg);
              usage(argv[0]);
              return -1;
            }
          break;

	case 'l':
	  heartbeat_liveness = atoi(optarg);
	  break;

	case 'n':
	  identity = optarg;
	  break;

	case 'r':
	  reconnect_interval_min = atoi(optarg);
	  break;

	case 'R':
	  reconnect_interval_max = atoi(optarg);
	  break;

	case 's':
	  server_uri = optarg;
	  break;

	case 'S':
	  server_sub_uri = optarg;
	  break;

	case '?':
	case 'v':
	  fprintf(stderr, "bgpview version %d.%d.%d\n",
		  BGPSTREAM_MAJOR_VERSION,
		  BGPSTREAM_MID_VERSION,
		  BGPSTREAM_MINOR_VERSION);
	  usage(argv[0]);
	  return 0;
	  break;

	default:
	  usage(argv[0]);
	  return -1;
	  break;
	}
    }

  /* NB: once getopt completes, optind points to the first non-option
     argument */

  if(metric_prefix != NULL)
    {
      bgpview_consumer_manager_set_metric_prefix(manager, metric_prefix);
    }

  if(consumer_cmds_cnt == 0)
    {
      fprintf(stderr,
	      "ERROR: Consumer(s) must be specified using -c\n");
      usage(argv[0]);
      return -1;
    }

  if(backends_cnt == 0)
    {
      fprintf(stderr,
	      "ERROR: At least one timeseries backend must be specified using -b\n");
      usage(argv[0]);
      goto err;
    }

  /* enable the backends that were requested */
  for(i=0; i<backends_cnt; i++)
    {
      /* the string at backends[i] will contain the name of the plugin,
	 optionally followed by a space and then the arguments to pass
	 to the plugin */
      if((backend_arg_ptr = strchr(backends[i], ' ')) != NULL)
	{
	  /* set the space to a nul, which allows backends[i] to be used
	     for the backend name, and then increment plugin_arg_ptr to
	     point to the next character, which will be the start of the
	     arg string (or at worst case, the terminating \0 */
	  *backend_arg_ptr = '\0';
	  backend_arg_ptr++;
	}

      /* lookup the backend using the name given */
      if((backend = timeseries_get_backend_by_name(timeseries,
						   backends[i])) == NULL)
	{
	  fprintf(stderr, "ERROR: Invalid backend name (%s)\n",
		  backends[i]);
	  usage(argv[0]);
	  goto err;
	}

      if(timeseries_enable_backend(backend, backend_arg_ptr) != 0)
	{
	  fprintf(stderr, "ERROR: Failed to initialized backend (%s)",
		  backends[i]);
	  usage(argv[0]);
	  goto err;
	}

      /* free the string we dup'd */
      free(backends[i]);
      backends[i] = NULL;
    }

  for(i=0; i<consumer_cmds_cnt; i++)
    {
      assert(consumer_cmds[i] != NULL);
      if(bgpview_consumer_manager_enable_consumer_from_str(manager,
						      consumer_cmds[i]) == NULL)
        {
          usage(argv[0]);
          goto err;
        }
    }

  if(interests == 0)
    {
      fprintf(stderr, "WARN: Defaulting to FIRST-FULL interest\n");
      fprintf(stderr, "WARN: Specify interests using -p <interest>\n");
      interests = BGPVIEW_CONSUMER_INTEREST_FIRSTFULL;
    }

  if((client =
      bgpview_io_client_init(interests, intents)) == NULL)
    {
      fprintf(stderr, "ERROR: could not initialize bgpview client\n");
      usage(argv[0]);
      goto err;
    }

  if(server_uri != NULL &&
     bgpview_io_client_set_server_uri(client, server_uri) != 0)
    {
      bgpview_io_client_perr(client);
      goto err;
    }

  if(server_sub_uri != NULL &&
     bgpview_io_client_set_server_sub_uri(client, server_sub_uri) != 0)
    {
      bgpview_io_client_perr(client);
      goto err;
    }

  if(identity != NULL &&
     bgpview_io_client_set_identity(client, identity) != 0)
    {
      bgpview_io_client_perr(client);
      goto err;
    }

  bgpview_io_client_set_heartbeat_interval(client, heartbeat_interval);

  bgpview_io_client_set_heartbeat_liveness(client, heartbeat_liveness);

  bgpview_io_client_set_reconnect_interval_min(client, reconnect_interval_min);

  bgpview_io_client_set_reconnect_interval_max(client, reconnect_interval_max);

  fprintf(stderr, "INFO: Starting client... ");
  if(bgpview_io_client_start(client) != 0)
    {
      bgpview_io_client_perr(client);
      goto err;
    }
  fprintf(stderr, "done\n");

  if((view = bgpview_create(NULL, NULL, NULL, NULL)) == NULL)
    {
      fprintf(stderr, "ERROR: Could not create view\n");
      goto err;
    }
  /* disable per-pfx-per-peer user pointer */
  bgpview_disable_user_data(view);

  while((rx_interests =
         bgpview_io_client_recv_view(client,
                                     BGPVIEW_IO_CLIENT_RECV_MODE_BLOCK,
                                     view,
                                     (peer_filters_cnt != 0) ? filter_peer : NULL,
                                     (pfx_filters_cnt != 0) ? filter_pfx : NULL,
                                     (pfx_peer_filters_cnt != 0) ? filter_pfx_peer : NULL
                                     )) > 0)
    {
      if(bgpview_consumer_manager_process_view(manager, rx_interests, view) != 0)
	{
	  fprintf(stderr, "ERROR: Failed to process view at %d\n",
		  bgpview_get_time(view));
	  goto err;
	}

      bgpview_clear(view);
      processed_view++;

      if(processed_view_limit > 0 && processed_view >= processed_view_limit)
        {
          fprintf(stderr, "Processed %d view(s).\n", processed_view);
          break;
        }
    }

  fprintf(stderr, "INFO: Shutting down...\n");

  bgpview_io_client_stop(client);
  bgpview_io_client_perr(client);

  /* cleanup */
  filters_destroy();
  bgpview_io_client_free(client);
  bgpview_destroy(view);
  bgpview_consumer_manager_destroy(&manager);
  timeseries_free(&timeseries);
  if(metric_prefix !=NULL)
    {
      free(metric_prefix);
    }
  fprintf(stderr, "INFO: Shutdown complete\n");

  /* complete successfully */
  return 0;

 err:
  filters_destroy();
  if(client != NULL) {
    bgpview_io_client_perr(client);
    bgpview_io_client_free(client);
  }
  if(metric_prefix !=NULL)
    {
      free(metric_prefix);
    }
  bgpview_destroy(view);
  bgpview_consumer_manager_destroy(&manager);
  timeseries_free(&timeseries);
  return -1;
}
