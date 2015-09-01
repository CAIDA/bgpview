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
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* include bgpview server's public interface */
/* @@ never include the _int.h file from tools. */
#include "bgpview_io_server.h"
#include "bgpview_io_common.h"

/** Indicates that bgpview is waiting to shutdown */
volatile sig_atomic_t bgpview_shutdown = 0;

/** The number of SIGINTs to catch before aborting */
#define HARD_SHUTDOWN 3

static bgpview_io_server_t *server = NULL;

/** Handles SIGINT gracefully and shuts down */
static void catch_sigint(int sig)
{
  bgpview_shutdown++;
  if(bgpview_shutdown == HARD_SHUTDOWN)
    {
      fprintf(stderr, "caught %d SIGINT's. shutting down NOW\n",
	      HARD_SHUTDOWN);
      exit(-1);
    }

  fprintf(stderr, "caught SIGINT, shutting down at the next opportunity\n");

  if(server != NULL)
    {
      bgpview_io_server_stop(server);
    }

  signal(sig, catch_sigint);
}

static void usage(const char *name)
{
  fprintf(stderr,
	  "usage: %s [<options>]\n"
	  "       -c <client-uri>    0MQ-style URI to listen for clients on\n"
	  "                          (default: %s)\n"
          "       -C <client-pub-uri> 0MQ-style URI to publish tables on\n"
          "                          (default: %s)\n"
	  "       -i <interval-ms>   Time in ms between heartbeats to clients\n"
	  "                          (default: %d)\n"
	  "       -l <beats>         Number of heartbeats that can go by before \n"
	  "                          a client is declared dead (default: %d)\n"
	  "       -w <window-len>    Number of views in the window (default: %d)\n"
          "       -m <prefix>        Metric prefix (default: %s)\n",
	  name,
	  BGPVIEW_IO_CLIENT_URI_DEFAULT,
	  BGPVIEW_IO_CLIENT_PUB_URI_DEFAULT,
	  BGPVIEW_IO_HEARTBEAT_INTERVAL_DEFAULT,
	  BGPVIEW_IO_HEARTBEAT_LIVENESS_DEFAULT,
	  BGPVIEW_IO_SERVER_WINDOW_LEN,
          BGPVIEW_IO_SERVER_METRIC_PREFIX_DEFAULT);
}

int main(int argc, char **argv)
{
  /* for option parsing */
  int opt;
  int prevoptind;

  /* to store command line argument values */
  const char *client_uri = NULL;
  const char *client_pub_uri = NULL;

  uint64_t heartbeat_interval = BGPVIEW_IO_HEARTBEAT_INTERVAL_DEFAULT;
  int heartbeat_liveness      = BGPVIEW_IO_HEARTBEAT_LIVENESS_DEFAULT;
  char metric_prefix[BGPVIEW_IO_SERVER_METRIC_PREFIX_LEN];

  strcpy(metric_prefix, BGPVIEW_IO_SERVER_METRIC_PREFIX_DEFAULT);

  int window_len = BGPVIEW_IO_SERVER_WINDOW_LEN;

  signal(SIGINT, catch_sigint);

  while(prevoptind = optind,
	(opt = getopt(argc, argv, ":c:C:i:l:w:m:v?")) >= 0)
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
	  client_uri = optarg;
	  break;

	case 'C':
	  client_pub_uri = optarg;
	  break;

	case 'i':
	  heartbeat_interval = atoi(optarg);
	  break;

	case 'l':
	  heartbeat_liveness = atoi(optarg);
	  break;

	case 'w':
	  window_len = atoi(optarg);
	  break;

        case 'm':
            strcpy(metric_prefix, optarg);
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

  if((server = bgpview_io_server_init()) == NULL)
    {
      fprintf(stderr, "ERROR: could not initialize bgpview server\n");
      goto err;
    }

  bgpview_io_server_set_metric_prefix(server, metric_prefix);

  if(client_uri != NULL)
    {
      bgpview_io_server_set_client_uri(server, client_uri);
    }

  if(client_pub_uri != NULL)
    {
      bgpview_io_server_set_client_pub_uri(server, client_pub_uri);
    }

  bgpview_io_server_set_heartbeat_interval(server, heartbeat_interval);

  bgpview_io_server_set_heartbeat_liveness(server, heartbeat_liveness);

  bgpview_io_server_set_window_len(server, window_len);

  /* do work */
  /* this function will block until the server shuts down */
  bgpview_io_server_start(server);

  /* this will always be set, normally to a SIGINT-caught message */
  bgpview_io_server_perr(server);

  /* cleanup */
  bgpview_io_server_free(server);

  /* complete successfully */
  return 0;

 err:
  if(server != NULL) {
    bgpview_io_server_free(server);
  }
  return -1;
}
