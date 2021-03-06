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

#include "bgpview_io_zmq_int.h"
#include "bgpview_io_zmq_server_int.h"
#include "config.h"
#include "khash.h"
#include "utils.h"
#include <stdint.h>
#include <stdio.h>

enum {
  POLL_ITEM_CLIENT = 0,
  POLL_ITEM_CNT = 1,
};

#define SERVER_METRIC_FORMAT "%s.meta.bgpview.server"

#define DUMP_METRIC(metric_prefix, value, time, fmt, ...)                      \
  do {                                                                         \
    fprintf(stdout, SERVER_METRIC_FORMAT "." fmt " %" PRIu64 " %" PRIu32 "\n", \
            metric_prefix, __VA_ARGS__, value, time);                          \
  } while (0)

/* after how many heartbeats should we ask the store to check timeouts */
#define STORE_HEARTBEATS_PER_TIMEOUT 60

/** Number of zmq I/O threads */
#define SERVER_ZMQ_IO_THREADS 3

static void client_free(bgpview_io_zmq_server_client_t **client_p)
{
  bgpview_io_zmq_server_client_t *client = *client_p;

  if (client == NULL) {
    return;
  }

  zmq_msg_close(&client->identity);

  free(client->id);
  client->id = NULL;
  free(client->hexid);
  client->hexid = NULL;

  free(client);

  *client_p = NULL;
  return;
}

/* because the hash calls with only the pointer, not the local ref */
static void client_free_wrap(bgpview_io_zmq_server_client_t *client)
{
  client_free(&client);
}

static char *msg_strhex(zmq_msg_t *msg)
{
  assert(msg != NULL);

  static const char hex_char[] = "0123456789ABCDEF";

  size_t size = zmq_msg_size(msg);
  byte *data = zmq_msg_data(msg);
  char *hex_str = (char *)malloc(size * 2 + 1);
  if (hex_str == NULL) {
    return NULL;
  }

  uint byte_nbr;
  for (byte_nbr = 0; byte_nbr < size; byte_nbr++) {
    hex_str[byte_nbr * 2 + 0] = hex_char[data[byte_nbr] >> 4];
    hex_str[byte_nbr * 2 + 1] = hex_char[data[byte_nbr] & 15];
  }
  hex_str[size * 2] = 0;
  return hex_str;
}

static char *msg_str(zmq_msg_t *msg)
{
  char *str;

  byte *data = zmq_msg_data(msg);
  size_t size = zmq_msg_size(msg);

  if ((str = malloc(sizeof(char) * (size + 1))) == NULL) {
    return NULL;
  }

  memcpy(str, data, size);
  str[size] = '\0';

  return str;
}

static int msg_isbinary(zmq_msg_t *msg)
{
  size_t size = zmq_msg_size(msg);
  byte *data = zmq_msg_data(msg);
  size_t i;

  for (i = 0; i < size; i++) {
    if (data[i] < 9 || data[i] > 127) {
      return 1;
    }
  }
  return 0;
}

static bgpview_io_zmq_server_client_t *
client_init(bgpview_io_zmq_server_t *server, zmq_msg_t *id_msg)
{
  bgpview_io_zmq_server_client_t *client;
  int khret;
  khiter_t khiter;

  if ((client = malloc_zero(sizeof(bgpview_io_zmq_server_client_t))) == NULL) {
    return NULL;
  }

  if (zmq_msg_init(&client->identity) == -1 ||
      zmq_msg_copy(&client->identity, id_msg) == -1) {
    goto err;
  }
  zmq_msg_close(id_msg);

  client->hexid = msg_strhex(&client->identity);
  assert(client->hexid);

  if (msg_isbinary(&client->identity) != 0) {
    client->id = msg_strhex(&client->identity);
  } else {
    client->id = msg_str(&client->identity);
  }
  if (client->id == NULL) {
    return NULL;
  }
  client->expiry =
    epoch_msec() + (server->heartbeat_interval * server->heartbeat_liveness);

  client->info.name = client->id;

  /* insert client into the hash */
  khiter = kh_put(strclient, server->clients, client->hexid, &khret);
  if (khret == -1) {
    goto err;
  }
  kh_val(server->clients, khiter) = client;

  return client;

err:
  client_free(&client);
  return NULL;
}

/** @todo consider using something other than the hex id as the key */
static bgpview_io_zmq_server_client_t *
client_get(bgpview_io_zmq_server_t *server, zmq_msg_t *id_msg)
{
  bgpview_io_zmq_server_client_t *client;
  khiter_t khiter;
  char *id;

  if ((id = msg_strhex(id_msg)) == NULL) {
    return NULL;
  }

  if ((khiter = kh_get(strclient, server->clients, id)) ==
      kh_end(server->clients)) {
    free(id);
    return NULL;
  }

  client = kh_val(server->clients, khiter);
  /* we are already tracking this client, treat the msg as a heartbeat */
  /* touch the timeout */
  client->expiry =
    epoch_msec() + (server->heartbeat_interval * server->heartbeat_liveness);
  free(id);
  return client;
}

static void clients_remove(bgpview_io_zmq_server_t *server,
                           bgpview_io_zmq_server_client_t *client)
{
  khiter_t khiter;
  if ((khiter = kh_get(strclient, server->clients, client->hexid)) ==
      kh_end(server->clients)) {
    /* already removed? */
    fprintf(stderr, "WARN: Removing non-existent client\n");
    return;
  }

  kh_del(strclient, server->clients, khiter);
}

static int clients_purge(bgpview_io_zmq_server_t *server)
{
  khiter_t k;
  bgpview_io_zmq_server_client_t *client;

  for (k = kh_begin(server->clients); k != kh_end(server->clients); ++k) {
    if (kh_exist(server->clients, k) != 0) {
      client = kh_val(server->clients, k);

      if (epoch_msec() < client->expiry) {
        break; /* client is alive, we're done here */
      }

      fprintf(stderr, "INFO: Removing dead client (%s)\n", client->id);
      fprintf(stderr, "INFO: Expiry: %" PRIu64 " Time: %" PRIu64 "\n",
              client->expiry, epoch_msec());
      if (bgpview_io_zmq_store_client_disconnect(server->store,
                                                 &client->info) != 0) {
        fprintf(stderr, "Store failed to handle client disconnect\n");
        return -1;
      }
      /* the key string is actually owned by the client, dont free */
      client_free(&client);
      kh_del(strclient, server->clients, k);
    }
  }

  return 0;
}

static void clients_free(bgpview_io_zmq_server_t *server)
{
  assert(server != NULL);
  assert(server->clients != NULL);

  kh_free_vals(strclient, server->clients, client_free_wrap);
  kh_destroy(strclient, server->clients);
  server->clients = NULL;
}

static int send_reply(bgpview_io_zmq_server_t *server,
                      bgpview_io_zmq_server_client_t *client,
                      zmq_msg_t *seq_msg)
{
  uint8_t reply_t_p = BGPVIEW_IO_ZMQ_MSG_TYPE_REPLY;
  zmq_msg_t id_cpy;

#ifdef DEBUG
  fprintf(stderr, "======================================\n");
  fprintf(stderr, "DEBUG: Sending reply\n");
#endif

  /* add the client id */
  /** @todo pass received client id thru, save copying */
  if (zmq_msg_init(&id_cpy) == -1 ||
      zmq_msg_copy(&id_cpy, &client->identity) == -1) {
    fprintf(stderr, "Failed to duplicate client id\n");
    goto err;
  }
  if (zmq_msg_send(&id_cpy, server->client_socket, ZMQ_SNDMORE) == -1) {
    zmq_msg_close(&id_cpy);
    fprintf(stderr, "Failed to send reply client id for %s - %d\n", client->id,
            errno);
    goto err;
  }

  /* add the reply type */
  if (zmq_send(server->client_socket, &reply_t_p,
               bgpview_io_zmq_msg_type_size_t,
               ZMQ_SNDMORE) != bgpview_io_zmq_msg_type_size_t) {
    fprintf(stderr, "Failed to send reply message type\n");
    goto err;
  }

  /* add the seq num */
  if (zmq_msg_send(seq_msg, server->client_socket, 0) == -1) {
    fprintf(stderr, "Could not send reply seq frame\n");
    goto err;
  }

#ifdef DEBUG
  fprintf(stderr, "======================================\n\n");
#endif

  return 0;

err:
  return -1;
}

static int handle_recv_view(bgpview_io_zmq_server_t *server,
                            bgpview_io_zmq_server_client_t *client)
{
  uint32_t view_time;
  bgpview_t *view;

  /* first receive the time of the view */
  if (zmq_recv(server->client_socket, &view_time, sizeof(view_time), 0) !=
      sizeof(view_time)) {
    fprintf(stderr, "Could not recieve view time header\n");
    goto err;
  }
  view_time = ntohl(view_time);

  DUMP_METRIC(server->metric_prefix, (uint64_t)(epoch_sec() - view_time),
              view_time, "view_receive.%s.begin_delay", client->id);

#ifdef DEBUG
  fprintf(stderr, "**************************************\n");
  fprintf(stderr, "DEBUG: Getting view from client (%" PRIu32 "):\n",
          view_time);
  fprintf(stderr, "**************************************\n\n");
#endif

  /* ask the store for a pointer to the view to recieve into */
  view = bgpview_io_zmq_store_get_view(server->store, view_time);

  /* temporarily store the truncated time so that we can fix the view after it
     has been rx'd */
  if (view != NULL) {
    view_time = bgpview_get_time(view);
  }

  /* receive the view */
  if (bgpview_io_zmq_recv(server->client_socket, view, NULL, NULL, NULL) != 0) {
    goto err;
  }

  if (view != NULL) {
    /* now reset the time to what the store wanted it to be */
    bgpview_set_time(view, view_time);
  }

  DUMP_METRIC(server->metric_prefix, (uint64_t)(epoch_sec() - view_time),
              view_time, "view_receive.%s.receive_delay", client->id);

  /* tell the store that the view has been updated */
  if (bgpview_io_zmq_store_view_updated(server->store, view, &client->info) !=
      0) {
    goto err;
  }

  return 0;

err:
  return -1;
}

/*
 * | SEQ NUM       |
 * | DATA MSG TYPE |
 * | Payload       |
 */
static int handle_view_message(bgpview_io_zmq_server_t *server,
                               bgpview_io_zmq_server_client_t *client)
{
  zmq_msg_t seq_msg;

  /* grab the seq num and save it for later */
  if (zmq_msg_init(&seq_msg) == -1) {
    fprintf(stderr, "Could not init seq num msg\n");
    goto err;
  }

  if (zmq_msg_recv(&seq_msg, server->client_socket, 0) == -1) {
    fprintf(stderr, "Could not extract seq number\n");
    goto err;
  }
  /* just to be safe */
  if (zmq_msg_size(&seq_msg) != sizeof(seq_num_t)) {
    fprintf(stderr, "Invalid seq number frame\n");
    goto err;
  }

  if (zsocket_rcvmore(server->client_socket) == 0) {
    goto err;
  }

  /* regardless of what they asked for, let them know that we got the request */
  if (send_reply(server, client, &seq_msg) != 0) {
    goto err;
  }

  if (handle_recv_view(server, client) != 0) {
    goto err;
  }

  return 0;

err:
  zmq_msg_close(&seq_msg);
  return -1;
}

static int handle_ready_message(bgpview_io_zmq_server_t *server,
                                bgpview_io_zmq_server_client_t *client)
{
#ifdef DEBUG
  fprintf(stderr, "DEBUG: Creating new client %s\n", client->id);
#endif

  uint8_t new_intents;

  /* next is the intents */
  if (zsocket_rcvmore(server->client_socket) == 0) {
    fprintf(stderr, "Message missing intents\n");
    goto err;
  }
  if (zmq_recv(server->client_socket, &new_intents, sizeof(new_intents), 0) !=
      sizeof(new_intents)) {
    fprintf(stderr, "Could not extract client intents\n");
    goto err;
  }

  /* we already knew about this client, don't re-add */
  if (client->info.intents == new_intents) {
    return 0;
  }

  client->info.intents = new_intents;

  /* call the "client connect" callback */
  if (bgpview_io_zmq_store_client_connect(server->store, &client->info) != 0) {
    fprintf(stderr, "Store failed to handle client connect\n");
    goto err;
  }

  return 0;

err:
  return -1;
}

static int handle_message(bgpview_io_zmq_server_t *server,
                          bgpview_io_zmq_server_client_t **client_p,
                          bgpview_io_zmq_msg_type_t msg_type)
{
  uint64_t begin_time;
  assert(client_p != NULL);
  bgpview_io_zmq_server_client_t *client = *client_p;
  assert(client != NULL);
  zmq_msg_t msg;

  /* check each type we support (in descending order of frequency) */
  switch (msg_type) {
  case BGPVIEW_IO_ZMQ_MSG_TYPE_VIEW:
    begin_time = epoch_msec();

    /* every data now begins with intents */
    if (handle_ready_message(server, client) != 0) {
      goto err;
    }

    /* parse the request, and then call the appropriate callback */
    if (handle_view_message(server, client) != 0) {
      /* err no will already be set */
      goto err;
    }

    fprintf(stderr, "DEBUG: handle_view_message from %s %" PRIu64 "\n",
            client->id, epoch_msec() - begin_time);

    break;

  case BGPVIEW_IO_ZMQ_MSG_TYPE_HEARTBEAT:
    /* safe to ignore these */
    break;

  case BGPVIEW_IO_ZMQ_MSG_TYPE_READY:
    if (handle_ready_message(server, client) != 0) {
      goto err;
    }
    break;

  case BGPVIEW_IO_ZMQ_MSG_TYPE_TERM:
/* if we get an explicit term, we want to remove the client from our
   hash, and also fire the appropriate callback */

#ifdef DEBUG
    fprintf(stderr, "**************************************\n");
    fprintf(stderr, "DEBUG: Got disconnect from client:\n");
#endif

    /* call the "client disconnect" callback */
    if (bgpview_io_zmq_store_client_disconnect(server->store, &client->info) !=
        0) {
      fprintf(stderr, "Store failed to handle client disconnect\n");
      goto err;
    }

    clients_remove(server, client);
    client_free(&client);
    break;

  default:
    fprintf(stderr, "Invalid message type (%d) rx'd from client, ignoring\n",
            msg_type);
    /* need to recv remainder of message */
    while (zsocket_rcvmore(server->client_socket) != 0) {
      if (zmq_msg_init(&msg) == -1) {
        fprintf(stderr, "Could not init proxy message\n");
        goto err;
      }
      if (zmq_msg_recv(&msg, server->client_socket, 0) == -1) {
        fprintf(stderr, "Failed to clear message from socket\n");
        goto err;
      }
      zmq_msg_close(&msg);
    }
    goto err;
    break;
  }

  *client_p = NULL;
  return 0;

err:
  *client_p = NULL;
  return -1;
}

static int run_server(bgpview_io_zmq_server_t *server)
{
  bgpview_io_zmq_msg_type_t msg_type;
  bgpview_io_zmq_server_client_t *client = NULL;
  khiter_t k;

  uint8_t msg_type_p;

  zmq_msg_t client_id;
  zmq_msg_t id_cpy;

  uint64_t begin_time = epoch_msec();

  /* get the client id frame */
  if (zmq_msg_init(&client_id) == -1) {
    fprintf(stderr, "Failed to init msg\n");
    goto err;
  }

  if (zmq_msg_recv(&client_id, server->client_socket, 0) == -1) {
    switch (errno) {
    case EAGAIN:
      goto timeout;
      break;

    case ETERM:
    case EINTR:
      goto interrupt;
      break;

    default:
      fprintf(stderr, "Could not recv from client\n");
      goto err;
      break;
    }
  }

  /* any kind of message from a client means that it is alive */
  /* treat the first frame as an identity frame */

  if (zsocket_rcvmore(server->client_socket) == 0) {
    fprintf(stderr, "Invalid message received from client "
                    "(missing seq num)\n");
    goto err;
  }

  /* now grab the message type */
  msg_type = bgpview_io_zmq_recv_type(server->client_socket, 0);

  /* check if this client is already registered */
  if ((client = client_get(server, &client_id)) == NULL) {
    /* create state for this client */
    if ((client = client_init(server, &client_id)) == NULL) {
      goto err;
    }
  }

  /* by here we have a client object and it is time to handle whatever
     message we were sent */
  if (handle_message(server, &client, msg_type) != 0) {
    goto err;
  }

timeout:
  /* time for heartbeats */
  assert(server->heartbeat_next > 0);
  if (epoch_msec() >= server->heartbeat_next) {
    for (k = kh_begin(server->clients); k != kh_end(server->clients); ++k) {
      if (kh_exist(server->clients, k) == 0) {
        continue;
      }

      client = kh_val(server->clients, k);

      if (zmq_msg_init(&id_cpy) == -1 ||
          zmq_msg_copy(&id_cpy, &client->identity) == -1) {
        fprintf(stderr, "Failed to duplicate client id\n");
        goto err;
      }
      if (zmq_msg_send(&id_cpy, server->client_socket, ZMQ_SNDMORE) == -1) {
        zmq_msg_close(&id_cpy);
        fprintf(stderr, "Could not send client id to client %s\n", client->id);
        goto err;
      }

      msg_type_p = BGPVIEW_IO_ZMQ_MSG_TYPE_HEARTBEAT;
      if (zmq_send(server->client_socket, &msg_type_p,
                   bgpview_io_zmq_msg_type_size_t,
                   0) != bgpview_io_zmq_msg_type_size_t) {
        fprintf(stderr, "Could not send heartbeat msg to client %s\n",
                client->id);
        goto err;
      }
    }
    server->heartbeat_next = epoch_msec() + server->heartbeat_interval;

    /* should we ask the store to check its timeouts? */
    if (server->store_timeout_cnt == STORE_HEARTBEATS_PER_TIMEOUT) {
      fprintf(stderr, "DEBUG: Checking store timeouts\n");
      if (bgpview_io_zmq_store_check_timeouts(server->store) != 0) {
        fprintf(stderr, "Failed to check store timeouts\n");
        goto err;
      }
      server->store_timeout_cnt = 0;
    } else {
      server->store_timeout_cnt++;
    }
  }

  if (clients_purge(server) != 0) {
    goto err;
  }

  fprintf(stderr, "DEBUG: run_server in %" PRIu64 "\n",
          epoch_msec() - begin_time);

  return 0;

err:
  return -1;

interrupt:
  /* we were interrupted */
  fprintf(stderr, "Caught SIGINT\n");
  return -1;
}

bgpview_io_zmq_server_t *bgpview_io_zmq_server_init()
{
  bgpview_io_zmq_server_t *server = NULL;

  if ((server = malloc_zero(sizeof(bgpview_io_zmq_server_t))) == NULL) {
    fprintf(stderr, "ERROR: Could not allocate server structure\n");
    return NULL;
  }

  /* init czmq */
  if ((server->ctx = zctx_new()) == NULL) {
    fprintf(stderr, "Failed to create 0MQ context\n");
    goto err;
  }

  zsys_set_io_threads(SERVER_ZMQ_IO_THREADS);

  /* set default config */

  if ((server->client_uri = strdup(BGPVIEW_IO_ZMQ_CLIENT_URI_DEFAULT)) ==
      NULL) {
    fprintf(stderr, "Failed to duplicate client uri string\n");
    goto err;
  }

  if ((server->client_pub_uri =
         strdup(BGPVIEW_IO_ZMQ_CLIENT_PUB_URI_DEFAULT)) == NULL) {
    fprintf(stderr, "Failed to duplicate client pub uri string\n");
    goto err;
  }

  server->heartbeat_interval = BGPVIEW_IO_ZMQ_HEARTBEAT_INTERVAL_DEFAULT;

  server->heartbeat_liveness = BGPVIEW_IO_ZMQ_HEARTBEAT_LIVENESS_DEFAULT;

  server->store_window_len = BGPVIEW_IO_ZMQ_SERVER_WINDOW_LEN;

  /* create an empty client list */
  if ((server->clients = kh_init(strclient)) == NULL) {
    fprintf(stderr, "Could not create client list\n");
    goto err;
  }

  strcpy(server->metric_prefix, BGPVIEW_IO_ZMQ_SERVER_METRIC_PREFIX_DEFAULT);

  return server;

err:
  if (server != NULL) {
    bgpview_io_zmq_server_free(server);
  }
  return NULL;
}

void bgpview_io_zmq_server_set_metric_prefix(bgpview_io_zmq_server_t *server,
                                             char *metric_prefix)
{
  if (metric_prefix != NULL &&
      strlen(metric_prefix) < BGPVIEW_IO_ZMQ_SERVER_METRIC_PREFIX_LEN - 1) {
    strcpy(server->metric_prefix, metric_prefix);
  }
}

int bgpview_io_zmq_server_start(bgpview_io_zmq_server_t *server)
{
  if ((server->store = bgpview_io_zmq_store_create(
         server, server->store_window_len)) == NULL) {
    fprintf(stderr, "Could not create store\n");
    return -1;
  }

  /* bind to client socket */
  if ((server->client_socket = zsocket_new(server->ctx, ZMQ_ROUTER)) == NULL) {
    fprintf(stderr, "Failed to create client socket\n");
    return -1;
  }
  /*zsocket_set_router_mandatory(server->client_socket, 1);*/
  zsocket_set_rcvtimeo(server->client_socket, server->heartbeat_interval);
  zsocket_set_sndhwm(server->client_socket, 0);
  zsocket_set_rcvhwm(server->client_socket, 0);
  if (zsocket_bind(server->client_socket, "%s", server->client_uri) < 0) {
    fprintf(stderr, "Could not bind to client socket\n");
    return -1;
  }

  /* bind to the pub socket */
  if ((server->client_pub_socket = zsocket_new(server->ctx, ZMQ_PUB)) == NULL) {
    fprintf(stderr, "Failed to create client PUB socket\n");
    return -1;
  }
  zsocket_set_sndhwm(server->client_pub_socket, 2);
  if (zsocket_bind(server->client_pub_socket, "%s", server->client_pub_uri) <
      0) {
    fprintf(stderr, "Could not bind to client PUB socket (%s)\n",
            server->client_pub_uri);
    return -1;
  }

  /* seed the time for the next heartbeat sent to servers */
  server->heartbeat_next = epoch_msec() + server->heartbeat_interval;

  /* start processing requests */
  while ((server->shutdown == 0) && (run_server(server) == 0)) {
    /* nothing here */
  }

  return -1;
}

void bgpview_io_zmq_server_stop(bgpview_io_zmq_server_t *server)
{
  assert(server != NULL);
  server->shutdown = 1;
}

void bgpview_io_zmq_server_free(bgpview_io_zmq_server_t *server)
{
  assert(server != NULL);

  free(server->client_uri);
  server->client_uri = NULL;

  free(server->client_pub_uri);
  server->client_pub_uri = NULL;

  clients_free(server);
  server->clients = NULL;

  bgpview_io_zmq_store_destroy(server->store);
  server->store = NULL;

  /* free'd by zctx_destroy */
  server->client_socket = NULL;

  zctx_destroy(&server->ctx);

  free(server);

  return;
}

void bgpview_io_zmq_server_set_window_len(bgpview_io_zmq_server_t *server,
                                          int window_len)
{
  assert(server != NULL);
  server->store_window_len = window_len;
}

int bgpview_io_zmq_server_set_client_uri(bgpview_io_zmq_server_t *server,
                                         const char *uri)
{
  assert(server != NULL);

  /* remember, we set one by default */
  assert(server->client_uri != NULL);
  free(server->client_uri);

  if ((server->client_uri = strdup(uri)) == NULL) {
    fprintf(stderr, "Could not malloc client uri string\n");
    return -1;
  }

  return 0;
}

int bgpview_io_zmq_server_set_client_pub_uri(bgpview_io_zmq_server_t *server,
                                             const char *uri)
{
  assert(server != NULL);

  /* remember, we set one by default */
  assert(server->client_pub_uri != NULL);
  free(server->client_pub_uri);

  if ((server->client_pub_uri = strdup(uri)) == NULL) {
    fprintf(stderr, "Could not malloc client pub uri string\n");
    return -1;
  }

  return 0;
}

void bgpview_io_zmq_server_set_heartbeat_interval(
  bgpview_io_zmq_server_t *server, uint64_t interval_ms)
{
  assert(server != NULL);

  server->heartbeat_interval = interval_ms;
}

void bgpview_io_zmq_server_set_heartbeat_liveness(
  bgpview_io_zmq_server_t *server, int beats)
{
  assert(server != NULL);

  server->heartbeat_liveness = beats;
}

/* ========== PUBLISH FUNCTIONS ========== */

int bgpview_io_zmq_server_publish_view(bgpview_io_zmq_server_t *server,
                                       bgpview_t *view)
{
  uint32_t time = bgpview_get_time(view);

#ifdef DEBUG
  fprintf(stderr, "DEBUG: Publishing view:\n");
  if (bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE) < 100) {
    bgpview_io_zmq_dump(view);
  }
#endif

  /* NULL -> no peer filtering */
  if (bgpview_io_zmq_send(server->client_pub_socket, view, NULL, NULL) != 0) {
    return -1;
  }

  DUMP_METRIC(server->metric_prefix, (uint64_t)(epoch_sec() - time), time, "%s",
              "publication.delay");

  return 0;
}
