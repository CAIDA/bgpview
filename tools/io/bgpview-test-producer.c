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

#include "config.h"
#include "utils.h"

#define VIEW_INTERVAL 300

#define TEST_TABLE_NUM_DEFAULT 1
#define TEST_TABLE_SIZE_DEFAULT 50
#define TEST_PEER_NUM_DEFAULT 1

#define ORIG_ASN_MAX 50000
#define CORE_ASN_MAX 4000

#define MAX_PEER_CNT 1024

static int full_feed_size = -1;

/* pfx table */
static char                     *test_collector_name;
static uint32_t                  test_time;
static uint32_t                  test_peer_first_ip;
static bgpstream_addr_storage_t  test_peer_ip;
static uint32_t                  test_peer_asns[MAX_PEER_CNT];
static uint8_t                   test_peer_status;

/* pfx row */
static bgpstream_pfx_storage_t   test_prefix;
static uint32_t                  test_prefix_first_addr;
static bgpstream_as_path_t      *test_as_path;
static bgpstream_as_path_seg_asn_t test_as_path_segs[100];

static int filter_ff_peers(bgpview_iter_t *iter,
                           bgpview_io_filter_type_t type)
{
  return (type == BGPVIEW_IO_FILTER_PFX) ||
    bgpview_iter_peer_get_pfx_cnt(iter,
                                  BGPSTREAM_ADDR_VERSION_IPV4,
                                  BGPVIEW_FIELD_ACTIVE) >= full_feed_size;
}

static void create_test_data()
{
  int i;

  /* PREFIX TABLE */

  /* TIME */
  test_time = 1320969600;

  /* COLLECTOR NAME */
  test_collector_name = "TEST-COLLECTOR";

  /* FIRST PEER IP */
  test_peer_ip.ipv4.s_addr = test_peer_first_ip = 0x00FAD982; /* add one each time */
  test_peer_ip.version = BGPSTREAM_ADDR_VERSION_IPV4;

  /* PEER ASNS */
  for(i=0; i<MAX_PEER_CNT; i++)
    {
      test_peer_asns[i] = rand() % ORIG_ASN_MAX;
    }

  /* FIRST PEER STATUS */
  test_peer_status = 0x01;

  /* FIRST PREFIX */
  test_prefix_first_addr = test_prefix.address.ipv4.s_addr = 0x00000000;
  test_prefix.address.version = BGPSTREAM_ADDR_VERSION_IPV4;
  test_prefix.mask_len = 24;

  /* AS PATH */
  test_as_path = bgpstream_as_path_create();
  assert(test_as_path != NULL);
}

static void build_as_path(uint32_t peer_asn)
{
  /* pick how many segments will be in this path (2-5) */
  int seg_cnt = (rand() % 5) + 2;
  int i;
  uint32_t origin_asn = rand() % ORIG_ASN_MAX;

  /* semi-randomly build the AS path, starting from the peer_asn */
  test_as_path_segs[0].type = BGPSTREAM_AS_PATH_SEG_ASN;
  test_as_path_segs[0].asn = peer_asn;
  for(i=1; i<seg_cnt-1; i++)
    {
      test_as_path_segs[i].type = BGPSTREAM_AS_PATH_SEG_ASN;
      test_as_path_segs[i].asn = (peer_asn + origin_asn + i) % ORIG_ASN_MAX;
    }
  test_as_path_segs[seg_cnt-1].type = BGPSTREAM_AS_PATH_SEG_ASN;
  test_as_path_segs[seg_cnt-1].asn = origin_asn;

  /* now populate the path */
  bgpstream_as_path_populate_from_data_zc(test_as_path,
                                          (uint8_t*)test_as_path_segs,
                                          sizeof(bgpstream_as_path_seg_asn_t)
                                          * seg_cnt);
}

static void usage(const char *name)
{
  fprintf(stderr,
	  "usage: %s [<options>]\n"
          "       -c                    Randomly decide if peers are up or down\n"
          "       -C                    Initial test time (default: %d)\n"
          "       -f <full-feed-size>   Only send full-feed peers\n"
	  "       -i <interval-ms>      Time in ms between heartbeats to server\n"
	  "                               (default: %d)\n"
	  "       -l <beats>            Number of heartbeats that can go by before the\n"
	  "                               server is declared dead (default: %d)\n"
	  "       -m <msg-timeout>      Time to wait before re-sending message to server\n"
	  "                               (default: %d)\n"
	  "       -M <msg-retries>      Number of times to retry a request before giving up\n"
	  "                               (default: %d)\n"
	  "       -n <identity>         Globally unique client name (default: random)\n"
          "       -N <table-cnt>        Number of tables (default: %d)\n"
          "       -p                    Randomly decide if a peer observes each prefix\n"
          "       -P <peer-cnt>         Number of peers (default: %d)\n"
	  "       -r <retry-min>        Min wait time (in msec) before reconnecting server\n"

	  "                               (default: %d)\n"
	  "       -R <retry-max>        Max wait time (in msec) before reconnecting server\n"
	  "                               (default: %d)\n"
	  "       -s <server-uri>       0MQ-style URI to connect to server on\n"
	  "                               (default: %s)\n"
          "       -S <server-sub-uri>   0MQ-style URI to subscribe to tables on\n"
          "                               (default: %s)\n"
	  "       -t <shutdown-timeout> Time to wait for requests on shutdown\n"
	  "                               (default: %d)\n"
	  "       -T <table-size>       Size of prefix tables (default: %d)\n",
	  name,
          test_time,
	  BGPVIEW_IO_HEARTBEAT_INTERVAL_DEFAULT,
	  BGPVIEW_IO_HEARTBEAT_LIVENESS_DEFAULT,
	  BGPVIEW_IO_CLIENT_REQUEST_TIMEOUT_DEFAULT,
	  BGPVIEW_IO_CLIENT_REQUEST_RETRIES_DEFAULT,
          TEST_TABLE_NUM_DEFAULT,
          TEST_PEER_NUM_DEFAULT,
	  BGPVIEW_IO_RECONNECT_INTERVAL_MIN,
	  BGPVIEW_IO_RECONNECT_INTERVAL_MAX,
	  BGPVIEW_IO_CLIENT_SERVER_URI_DEFAULT,
	  BGPVIEW_IO_CLIENT_SERVER_SUB_URI_DEFAULT,
	  BGPVIEW_IO_CLIENT_SHUTDOWN_LINGER_DEFAULT,
	  TEST_TABLE_SIZE_DEFAULT);
}

int main(int argc, char **argv)
{
  int i, tbl, peer, peer_id;
  /* for option parsing */
  int opt;
  int prevoptind;

  /* to store command line argument values */
  const char *server_uri = NULL;
  const char *server_sub_uri = NULL;
  const char *identity = NULL;

  int use_random_peers = 0;
  int use_random_pfxs = 0;

  uint64_t heartbeat_interval = BGPVIEW_IO_HEARTBEAT_INTERVAL_DEFAULT;
  int heartbeat_liveness      = BGPVIEW_IO_HEARTBEAT_LIVENESS_DEFAULT;
  uint64_t reconnect_interval_min = BGPVIEW_IO_RECONNECT_INTERVAL_MIN;
  uint64_t reconnect_interval_max = BGPVIEW_IO_RECONNECT_INTERVAL_MAX;
  uint64_t shutdown_linger = BGPVIEW_IO_CLIENT_SHUTDOWN_LINGER_DEFAULT;
  uint64_t request_timeout = BGPVIEW_IO_CLIENT_REQUEST_TIMEOUT_DEFAULT;
  int request_retries = BGPVIEW_IO_CLIENT_REQUEST_RETRIES_DEFAULT;

  uint8_t interests = 0;
  uint8_t intents = 0;
  bgpview_io_client_t *client = NULL;

  bgpview_t *view = NULL;
  bgpview_iter_t *iter = NULL;

  /* initialize test data */
  create_test_data();

  uint32_t test_table_size = TEST_TABLE_SIZE_DEFAULT;
  uint32_t test_table_num = TEST_TABLE_NUM_DEFAULT;
  uint32_t test_peer_num = TEST_PEER_NUM_DEFAULT;

  uint32_t pfx_cnt;

  while(prevoptind = optind,
	(opt = getopt(argc, argv, ":cC:f:i:l:m:M:n:N:pP:r:R:s:S:t:T:v?")) >= 0)
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

        case 'c':
          use_random_peers = 1;
          break;

        case 'C':
          test_time = atoi(optarg);
          break;

        case 'f':
          full_feed_size = atoi(optarg);
          break;

	case 'i':
	  heartbeat_interval = atoi(optarg);
	  break;

	case 'l':
	  heartbeat_liveness = atoi(optarg);
	  break;

	case 'm':
	  request_timeout = atoi(optarg);
	  break;

	case 'M':
	  request_retries = atoi(optarg);
	  break;

	case 'n':
	  identity = optarg;
	  break;

	case 'N':
	  test_table_num = atoi(optarg);
	  break;

        case 'p':
          use_random_pfxs = 1;
          break;

	case 'P':
	  test_peer_num = atoi(optarg);
          assert(test_peer_num <= MAX_PEER_CNT);
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

	case 't':
	  shutdown_linger = atoi(optarg);
	  break;

	case 'T':
	  test_table_size = atoi(optarg);
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

  interests = 0;
  intents = BGPVIEW_PRODUCER_INTENT_PREFIX;

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

  bgpview_io_client_set_shutdown_linger(client, shutdown_linger);

  bgpview_io_client_set_request_timeout(client, request_timeout);

  bgpview_io_client_set_request_retries(client, request_retries);

  fprintf(stderr, "TEST: Starting client... ");
  if(bgpview_io_client_start(client) != 0)
    {
      bgpview_io_client_perr(client);
      goto err;
    }
  fprintf(stderr, "done\n");

  /* issue a bunch of requests */
  
  /* initialize random seed: */  
  srand(1);

  if((view = bgpview_create(NULL, NULL, NULL, NULL)) == NULL)
    {
      fprintf(stderr, "Could not create view\n");
      goto err;
    }
  if((iter = bgpview_iter_create(view)) == NULL)
    {
      fprintf(stderr, "Could not create view iterator\n");
      goto err;
    }

  for(tbl = 0; tbl < test_table_num; tbl++)
    {
      fprintf(stderr,
              "--------------------[ PREFIX START %03d ]--------------------\n",
              tbl);

      bgpview_set_time(view, test_time+(tbl*VIEW_INTERVAL));

      /* reset peer ip */
      test_peer_ip.ipv4.s_addr = test_peer_first_ip;

      fprintf(stderr, "TEST: Simulating %d peer(s)\n", test_peer_num);
      for(peer = 0; peer < test_peer_num; peer++)
        {
          test_peer_ip.ipv4.s_addr = htonl(ntohl(test_peer_ip.ipv4.s_addr) + 1);

          // returns number from 0 to 2
	  test_peer_status = (use_random_peers) ? rand() % 3 : 2;
          if((peer_id = bgpview_iter_add_peer(iter,
                                                      test_collector_name,
                                                      (bgpstream_ip_addr_t *)&test_peer_ip,
                                                      test_peer_asns[peer])) == 0)
            {
              fprintf(stderr, "Could not add peer to table\n");
              goto err;
            }
          if(bgpview_iter_activate_peer(iter) != 1)
            {
              fprintf(stderr, "Failed to activate peer\n");
              goto err;
            }
          fprintf(stderr, "TEST: Added peer %d (asn: %"PRIu32") ", peer_id, test_peer_asns[peer]);

	  if(test_peer_status != 2)
            {
              fprintf(stderr, "(down)\n");
              continue;
            }
          else
            {
              fprintf(stderr, "(up)\n");
            }

          test_prefix.address.ipv4.s_addr = test_prefix_first_addr;
          pfx_cnt = 0;
          for(i=0; i<test_table_size; i++)
            {
              test_prefix.address.ipv4.s_addr =
                htonl(ntohl(test_prefix.address.ipv4.s_addr) + 256);

              build_as_path(test_peer_asns[peer]);

             /* there is a 1/10 chance that we don't observe this prefix */
              if(use_random_pfxs && (rand() % 10) == 0)
                {
                  /* randomly we don't see this prefix */
                  continue;
                }
              if(bgpview_iter_add_pfx_peer(iter,
                                                   (bgpstream_pfx_t *)&test_prefix,
                                                   peer_id,
                                                   test_as_path) != 0)
                {
                  fprintf(stderr, "Could not add pfx info to table\n");
                  goto err;
                }
              if(bgpview_iter_pfx_activate_peer(iter) != 1)
                {
                  fprintf(stderr, "Failed to activate pfx-peer\n");
                  goto err;
                }
              pfx_cnt++;
            }
          fprintf(stderr, "TEST: Added %d prefixes...\n", pfx_cnt);
        }

      if(bgpview_io_client_send_view(client, view,
                                     (full_feed_size >= 0) ? filter_ff_peers : NULL) != 0)
        {
          fprintf(stderr, "Could not end table\n");
          goto err;
        }

      bgpview_clear(view);

      fprintf(stderr,
              "--------------------[ PREFIX DONE %03d ]--------------------\n\n",
              tbl);
    }

  fprintf(stderr, "TEST: Shutting down...\n");

  bgpview_io_client_stop(client);
  bgpview_io_client_perr(client);

  /* cleanup */
  bgpview_io_client_free(client);
  bgpview_iter_destroy(iter);
  bgpview_destroy(view);
  bgpstream_as_path_destroy(test_as_path);
  fprintf(stderr, "TEST: Shutdown complete\n");

  /* complete successfully */
  return 0;

 err:
  bgpview_io_client_perr(client);
  if(client != NULL) {
    bgpview_io_client_free(client);
  }
  if(view != NULL) {
    bgpview_destroy(view);
  }
  bgpstream_as_path_destroy(test_as_path);

  return -1;
}
