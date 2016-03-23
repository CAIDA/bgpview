/*
 * This file is part of bgpstream
 *
 * Copyright (C) 2015 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
 *
 * All rights reserved.
 *
 * This code has been developed by CAIDA at UC San Diego.
 * For more information, contact bgpstream-info@caida.org
 *
 * This source code is proprietary to the CAIDA group at UC San Diego and may
 * not be redistributed, published or disclosed without prior permission from
 * CAIDA.
 *
 * Report any bugs, questions or comments to bgpstream-info@caida.org
 *
 */

#include "config.h"
#include "bgpview_io_file.h"
#include "bgpview_io_zmq.h"
#include "bgpview.h"
#include "config.h"
#include "utils.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <wandio.h>

#define TABLE_NUM_DEFAULT 1

static void usage(const char *name)
{
  fprintf(stderr,
	  "usage: %s [<options>]\n"
          "       -f <view-file>        File to read BGPViews from\n"
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
	  "       -r <retry-min>        Min wait time (in msec) before reconnecting server\n"

	  "                               (default: %d)\n"
	  "       -R <retry-max>        Max wait time (in msec) before reconnecting server\n"
	  "                               (default: %d)\n"
	  "       -s <server-uri>       0MQ-style URI to connect to server on\n"
	  "                               (default: %s)\n"
          "       -S <server-sub-uri>   0MQ-style URI to subscribe to tables on\n"
          "                               (default: %s)\n"
	  "       -t <shutdown-timeout> Time to wait for requests on shutdown\n"
	  "                               (default: %d)\n",
	  name,
	  BGPVIEW_IO_ZMQ_HEARTBEAT_INTERVAL_DEFAULT,
	  BGPVIEW_IO_ZMQ_HEARTBEAT_LIVENESS_DEFAULT,
	  BGPVIEW_IO_ZMQ_CLIENT_REQUEST_TIMEOUT_DEFAULT,
	  BGPVIEW_IO_ZMQ_CLIENT_REQUEST_RETRIES_DEFAULT,
          TABLE_NUM_DEFAULT,
	  BGPVIEW_IO_ZMQ_RECONNECT_INTERVAL_MIN,
	  BGPVIEW_IO_ZMQ_RECONNECT_INTERVAL_MAX,
	  BGPVIEW_IO_ZMQ_CLIENT_SERVER_URI_DEFAULT,
	  BGPVIEW_IO_ZMQ_CLIENT_SERVER_SUB_URI_DEFAULT,
	  BGPVIEW_IO_ZMQ_CLIENT_SHUTDOWN_LINGER_DEFAULT);
}

int main(int argc, char **argv)
{
  int tbl, ret;
  /* for option parsing */
  int opt;
  int prevoptind;

  const char *view_filename = NULL;
  io_t *infile = NULL;

  /* to store command line argument values */
  const char *server_uri = NULL;
  const char *server_sub_uri = NULL;
  const char *identity = NULL;

  uint64_t heartbeat_interval = BGPVIEW_IO_ZMQ_HEARTBEAT_INTERVAL_DEFAULT;
  int heartbeat_liveness      = BGPVIEW_IO_ZMQ_HEARTBEAT_LIVENESS_DEFAULT;
  uint64_t reconnect_interval_min = BGPVIEW_IO_ZMQ_RECONNECT_INTERVAL_MIN;
  uint64_t reconnect_interval_max = BGPVIEW_IO_ZMQ_RECONNECT_INTERVAL_MAX;
  uint64_t shutdown_linger = BGPVIEW_IO_ZMQ_CLIENT_SHUTDOWN_LINGER_DEFAULT;
  uint64_t request_timeout = BGPVIEW_IO_ZMQ_CLIENT_REQUEST_TIMEOUT_DEFAULT;
  int request_retries = BGPVIEW_IO_ZMQ_CLIENT_REQUEST_RETRIES_DEFAULT;

  uint8_t intents = 0;
  bgpview_io_zmq_client_t *client = NULL;

  bgpview_t *view = NULL;

  uint32_t table_num = TABLE_NUM_DEFAULT;

  while(prevoptind = optind,
	(opt = getopt(argc, argv, ":f:i:l:m:M:n:N:r:R:s:S:t:v?")) >= 0)
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
          view_filename = optarg;
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
	  table_num = atoi(optarg);
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

  if(view_filename == NULL)
    {
      fprintf(stderr, "ERROR: BGPView file must be specified using -f\n");
      usage(argv[0]);
      goto err;
    }

  if((infile = wandio_create(view_filename)) == NULL)
    {
      fprintf(stderr, "ERROR: Could not open %s for reading\n",
              view_filename);
      goto err;
    }

  intents = BGPVIEW_PRODUCER_INTENT_PREFIX;

  if((client =
      bgpview_io_zmq_client_init(intents)) == NULL)
    {
      fprintf(stderr, "ERROR: could not initialize bgpview client\n");
      usage(argv[0]);
      goto err;
    }

  if(server_uri != NULL &&
     bgpview_io_zmq_client_set_server_uri(client, server_uri) != 0)
    {
      goto err;
    }

  if(server_sub_uri != NULL &&
     bgpview_io_zmq_client_set_server_sub_uri(client, server_sub_uri) != 0)
    {
      goto err;
    }

  if(identity != NULL &&
     bgpview_io_zmq_client_set_identity(client, identity) != 0)
    {
      goto err;
    }

  bgpview_io_zmq_client_set_heartbeat_interval(client, heartbeat_interval);

  bgpview_io_zmq_client_set_heartbeat_liveness(client, heartbeat_liveness);

  bgpview_io_zmq_client_set_reconnect_interval_min(client, reconnect_interval_min);

  bgpview_io_zmq_client_set_reconnect_interval_max(client, reconnect_interval_max);

  bgpview_io_zmq_client_set_shutdown_linger(client, shutdown_linger);

  bgpview_io_zmq_client_set_request_timeout(client, request_timeout);

  bgpview_io_zmq_client_set_request_retries(client, request_retries);

  fprintf(stderr, "TEST: Starting client... ");
  if(bgpview_io_zmq_client_start(client) != 0)
    {
      goto err;
    }
  fprintf(stderr, "done\n");

  if((view = bgpview_create(NULL, NULL, NULL, NULL)) == NULL)
    {
      fprintf(stderr, "Could not create view\n");
      goto err;
    }

  for(tbl = 0; tbl < table_num; tbl++)
    {
      if((ret = bgpview_io_file_read(infile, view, NULL, NULL, NULL)) == 0)
        {
          /* eof */
          break;
        }
      else if(ret < 0)
        {
          fprintf(stderr, "ERROR: Failed to read view from file\n");
          goto err;
        }
      fprintf(stderr, "INFO: Read view #%d\n", tbl+1);

      if(bgpview_io_zmq_client_send_view(client, view, NULL) != 0)
        {
          fprintf(stderr, "ERROR: Could not send view\n");
          goto err;
        }
      fprintf(stderr, "INFO: Sent view #%d\n", tbl+1);

      bgpview_clear(view);
    }

  fprintf(stderr, "INFO: Shutting down...\n");

  bgpview_io_zmq_client_stop(client);

  /* cleanup */
  bgpview_io_zmq_client_free(client);
  bgpview_destroy(view);
  wandio_destroy(infile);
  fprintf(stderr, "INFO: Shutdown complete\n");

  /* complete successfully */
  return 0;

 err:
  if(client != NULL) {
    bgpview_io_zmq_client_free(client);
  }
  if(view != NULL) {
    bgpview_destroy(view);
  }
  if(infile != NULL) {
    wandio_destroy(infile);
  }

  return -1;
}
