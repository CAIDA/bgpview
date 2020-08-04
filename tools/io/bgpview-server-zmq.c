/*
 * Copyright (C) 2014 The Regents of the University of California.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "config.h"

#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bgpview_io_zmq.h"

/** Indicates that bgpview is waiting to shutdown */
volatile sig_atomic_t bgpview_shutdown = 0;

/** The number of SIGINTs to catch before aborting */
#define HARD_SHUTDOWN 3

static bgpview_io_zmq_server_t *server = NULL;

/** Handles SIGINT gracefully and shuts down */
static void catch_sigint(int sig)
{
  bgpview_shutdown++;
  if (bgpview_shutdown == HARD_SHUTDOWN) {
    fprintf(stderr, "caught %d SIGINT's. shutting down NOW\n", HARD_SHUTDOWN);
    exit(-1);
  }

  fprintf(stderr, "caught SIGINT, shutting down at the next opportunity\n");

  if (server != NULL) {
    bgpview_io_zmq_server_stop(server);
  }

  signal(sig, catch_sigint);
}

static void usage(const char *name)
{
  fprintf(
    stderr,
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
    name, BGPVIEW_IO_ZMQ_CLIENT_URI_DEFAULT,
    BGPVIEW_IO_ZMQ_CLIENT_PUB_URI_DEFAULT,
    BGPVIEW_IO_ZMQ_HEARTBEAT_INTERVAL_DEFAULT,
    BGPVIEW_IO_ZMQ_HEARTBEAT_LIVENESS_DEFAULT, BGPVIEW_IO_ZMQ_SERVER_WINDOW_LEN,
    BGPVIEW_IO_ZMQ_SERVER_METRIC_PREFIX_DEFAULT);
}

int main(int argc, char **argv)
{
  /* for option parsing */
  int opt;
  int prevoptind;

  /* to store command line argument values */
  const char *client_uri = NULL;
  const char *client_pub_uri = NULL;

  uint64_t heartbeat_interval = BGPVIEW_IO_ZMQ_HEARTBEAT_INTERVAL_DEFAULT;
  int heartbeat_liveness = BGPVIEW_IO_ZMQ_HEARTBEAT_LIVENESS_DEFAULT;
  char metric_prefix[BGPVIEW_IO_ZMQ_SERVER_METRIC_PREFIX_LEN];

  strcpy(metric_prefix, BGPVIEW_IO_ZMQ_SERVER_METRIC_PREFIX_DEFAULT);

  int window_len = BGPVIEW_IO_ZMQ_SERVER_WINDOW_LEN;

  signal(SIGINT, catch_sigint);

  while (prevoptind = optind,
         (opt = getopt(argc, argv, ":c:C:i:l:w:m:v?")) >= 0) {
    if (optind == prevoptind + 2 && *optarg == '-') {
      opt = ':';
      --optind;
    }
    switch (opt) {
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
      fprintf(stderr, "bgpview version %d.%d.%d\n", BGPVIEW_MAJOR_VERSION,
              BGPSTREAM_MID_VERSION, BGPSTREAM_MINOR_VERSION);
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

  if ((server = bgpview_io_zmq_server_init()) == NULL) {
    fprintf(stderr, "ERROR: could not initialize bgpview server\n");
    goto err;
  }

  bgpview_io_zmq_server_set_metric_prefix(server, metric_prefix);

  if (client_uri != NULL) {
    bgpview_io_zmq_server_set_client_uri(server, client_uri);
  }

  if (client_pub_uri != NULL) {
    bgpview_io_zmq_server_set_client_pub_uri(server, client_pub_uri);
  }

  bgpview_io_zmq_server_set_heartbeat_interval(server, heartbeat_interval);

  bgpview_io_zmq_server_set_heartbeat_liveness(server, heartbeat_liveness);

  bgpview_io_zmq_server_set_window_len(server, window_len);

  /* do work */
  /* this function will block until the server shuts down */
  bgpview_io_zmq_server_start(server);

  /* cleanup */
  bgpview_io_zmq_server_free(server);

  /* complete successfully */
  return 0;

err:
  if (server != NULL) {
    bgpview_io_zmq_server_free(server);
  }
  return -1;
}
