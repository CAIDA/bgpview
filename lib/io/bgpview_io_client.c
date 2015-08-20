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

#include <stdint.h>

#include "bgpview_io_client_int.h"

#include "bgpview_io_client_broker.h"
#include "bgpview_io.h"

#include "khash.h"
#include "utils.h"

#define ERR (&client->err)
#define BCFG (client->broker_config)
#define TBL (client->pfx_table)

/* allow the table hash to be reused for 1 day */
#define TABLE_MAX_REUSE_CNT 1440

#define METRIC_PREFIX "bgp.meta.bgpview.client"

#define DUMP_METRIC(value, time, fmt, ...)                      \
do {                                                            \
  fprintf(stdout, METRIC_PREFIX"."fmt" %"PRIu64" %"PRIu32"\n",  \
          __VA_ARGS__, value, time);                            \
 } while(0)                                                     \

/* create and send headers for a data message */
int send_view_hdrs(bgpview_io_client_t *client, bgpview_t *view)
{
  uint8_t   type_b = BGPVIEW_MSG_TYPE_VIEW;
  seq_num_t seq_num = client->seq_num++;
  uint32_t u32;

  /* message type */
  if(zmq_send(client->broker_zocket, &type_b,
              bgpview_msg_type_size_t, ZMQ_SNDMORE)
     != bgpview_msg_type_size_t)
    {
      bgpview_io_err_set_err(ERR, errno,
			     "Could not add request type to message");
      goto err;
    }

  /* sequence number */
  if(zmq_send(client->broker_zocket, &seq_num, sizeof(seq_num_t), ZMQ_SNDMORE)
     != sizeof(seq_num_t))
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Could not add sequence number to message");
      goto err;
    }

  /* view time */
  u32 = htonl(bgpview_get_time(view));
  if(zmq_send(client->broker_zocket, &u32, sizeof(u32), ZMQ_SNDMORE)
     != sizeof(u32))
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Could not send view time header");
      goto err;
    }

  return 0;

 err:
  return -1;
}

/* ========== PUBLIC FUNCS BELOW HERE ========== */

bgpview_io_client_t *bgpview_io_client_init(uint8_t interests, uint8_t intents)
{
  bgpview_io_client_t *client;
  if((client = malloc_zero(sizeof(bgpview_io_client_t))) == NULL)
    {
      /* cannot set an err at this point */
      return NULL;
    }
  /* now we are ready to set errors... */

  /* now init the shared state for our broker */

  BCFG.master = client;

  BCFG.interests = interests;
  BCFG.intents = intents;

  /* init czmq */
  if((BCFG.ctx = zctx_new()) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Failed to create 0MQ context");
      goto err;
    }

  if((BCFG.server_uri =
      strdup(BGPVIEW_IO_CLIENT_SERVER_URI_DEFAULT)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Failed to duplicate server uri string");
      goto err;
    }

  if((BCFG.server_sub_uri =
      strdup(BGPVIEW_IO_CLIENT_SERVER_SUB_URI_DEFAULT)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Failed to duplicate server SUB uri string");
      goto err;
    }

  BCFG.heartbeat_interval = BGPVIEW_IO_HEARTBEAT_INTERVAL_DEFAULT;

  BCFG.heartbeat_liveness = BGPVIEW_IO_HEARTBEAT_LIVENESS_DEFAULT;

  BCFG.reconnect_interval_min = BGPVIEW_IO_RECONNECT_INTERVAL_MIN;

  BCFG.reconnect_interval_max = BGPVIEW_IO_RECONNECT_INTERVAL_MAX;

  BCFG.shutdown_linger = BGPVIEW_IO_CLIENT_SHUTDOWN_LINGER_DEFAULT;

  BCFG.request_timeout = BGPVIEW_IO_CLIENT_REQUEST_TIMEOUT_DEFAULT;
  BCFG.request_retries = BGPVIEW_IO_CLIENT_REQUEST_RETRIES_DEFAULT;

  /* establish a pipe between us and the broker */
  if((client->broker_sock = zsock_new(ZMQ_PAIR)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Failed to create socket end");
      goto err;
    }
  if((BCFG.master_pipe = zsock_new(ZMQ_PAIR)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Failed to create socket end");
      goto err;
    }
  /* bind and connect pipe ends */
  if(zsock_bind(client->broker_sock, "inproc://client-broker") != 0)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Failed to bind broker socket");
      goto err;
    }
  if(zsock_connect(BCFG.master_pipe, "inproc://client-broker") != 0)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Failed to connect broker socket");
      goto err;
    }

  return client;

 err:
  if(client != NULL)
    {
      bgpview_io_client_free(client);
    }
  return NULL;
}

void bgpview_io_client_set_cb_userdata(bgpview_io_client_t *client,
				       void *user)
{
  assert(client != NULL);
  BCFG.callbacks.user = user;
}

int bgpview_io_client_start(bgpview_io_client_t *client)
{
  /* crank up the broker */
  if((client->broker =
      zactor_new(bgpview_io_client_broker_run, &BCFG)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Failed to start broker");
      return -1;
    }

  /* by the time the zactor_new function returns, the broker has been
     initialized, so lets check for any error messages that it has signaled */
  if(bgpview_io_err_is_err(&BCFG.err) != 0)
    {
      client->err = BCFG.err;
      client->shutdown = 1;
      return -1;
    }

  /* store a pointer to the socket we will use to talk with the broker */
  client->broker_zocket = zsock_resolve(client->broker_sock);
  assert(client->broker_zocket != NULL);

  return 0;
}

void bgpview_io_client_perr(bgpview_io_client_t *client)
{
  assert(client != NULL);
  bgpview_io_err_perr(ERR);
}

#define ASSERT_INTENT(intent) assert((BCFG.intents & intent) != 0);

int bgpview_io_client_send_view(bgpview_io_client_t *client,
                                bgpview_t *view,
                                bgpview_filter_peer_cb_t *cb)
{
  if(send_view_hdrs(client, view) != 0)
    {
      goto err;
    }

  /* now just transmit the view */
  if(bgpview_io_send(client->broker_zocket, view, cb) != 0)
    {
      goto err;
    }

  return 0;

 err:
  return -1;
}

int bgpview_io_client_recv_view(bgpview_io_client_t *client,
				bgpview_io_client_recv_mode_t blocking,
				bgpview_t *view)

{
  uint8_t interests = 0;

  assert(view != NULL);

  /* attempt to get the set of interests */
  if(zmq_recv(client->broker_zocket,
	      &interests, sizeof(interests),
	      (blocking == BGPVIEW_IO_CLIENT_RECV_MODE_NONBLOCK) ?
	        ZMQ_DONTWAIT : 0
	      ) != sizeof(interests))
        {
	  /* likely this means that we have shut the broker down */
	  return -1;
        }

  if(bgpview_io_recv(client->broker_zocket, view) != 0)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_PROTOCOL,
			     "Failed to receive view");
      return -1;
    }

  return interests;
}

void bgpview_io_client_stop(bgpview_io_client_t *client)
{
  /* shuts the broker down */
  zactor_destroy(&client->broker);

  /* grab the error message from the broker */
  if(bgpview_io_err_is_err(&BCFG.err) != 0)
    {
      client->err = BCFG.err;
    }

  client->shutdown = 1;
  return;
}

void bgpview_io_client_free(bgpview_io_client_t *client)
{
  assert(client != NULL);

  /* @todo figure out a more elegant way to deal with this */
  if(client->shutdown == 0)
    {
      bgpview_io_client_stop(client);
    }

  free(BCFG.server_uri);
  BCFG.server_uri = NULL;

  free(BCFG.server_sub_uri);
  BCFG.server_sub_uri = NULL;

  free(BCFG.identity);
  BCFG.identity = NULL;

  zsock_destroy(&client->broker_sock);
  zsock_destroy(&BCFG.master_pipe);

  zctx_destroy(&BCFG.ctx);

  free(client);

  return;
}

int bgpview_io_client_set_server_uri(bgpview_io_client_t *client,
				     const char *uri)
{
  assert(client != NULL);

  if(client->broker != NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Could not set server uri (broker started)");
      return -1;
    }

  free(BCFG.server_uri);

  if((BCFG.server_uri = strdup(uri)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Could not set server uri");
      return -1;
    }

  return 0;
}

int bgpview_io_client_set_server_sub_uri(bgpview_io_client_t *client,
                                         const char *uri)
{
  assert(client != NULL);

  if(client->broker != NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Could not set server SUB uri (broker started)");
      return -1;
    }

  free(BCFG.server_sub_uri);

  if((BCFG.server_sub_uri = strdup(uri)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Could not set server SUB uri");
      return -1;
    }

  return 0;
}

void bgpview_io_client_set_heartbeat_interval(bgpview_io_client_t *client,
					      uint64_t interval_ms)
{
  assert(client != NULL);

  if(client->broker != NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Could not set heartbeat interval (broker started)");
      return;
    }

  BCFG.heartbeat_interval = interval_ms;
}

void bgpview_io_client_set_heartbeat_liveness(bgpview_io_client_t *client,
					      int beats)
{
  assert(client != NULL);

  if(client->broker != NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Could not set heartbeat liveness (broker started)");
      return;
    }

  BCFG.heartbeat_liveness = beats;
}

void bgpview_io_client_set_reconnect_interval_min(bgpview_io_client_t *client,
						  uint64_t reconnect_interval_min)
{
  assert(client != NULL);

  if(client->broker != NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Could not set min reconnect interval "
			     "(broker started)");
      return;
    }

  BCFG.reconnect_interval_min = reconnect_interval_min;
}

void bgpview_io_client_set_reconnect_interval_max(bgpview_io_client_t *client,
						  uint64_t reconnect_interval_max)
{
  assert(client != NULL);

  if(client->broker != NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Could not set max reconnect interval "
			     "(broker started)");
      return;
    }

  BCFG.reconnect_interval_max = reconnect_interval_max;
}

void bgpview_io_client_set_shutdown_linger(bgpview_io_client_t *client,
					   uint64_t linger)
{
  assert(client != NULL);

  BCFG.shutdown_linger = linger;
}

void bgpview_io_client_set_request_timeout(bgpview_io_client_t *client,
					   uint64_t timeout_ms)
{
  assert(client != NULL);

  BCFG.request_timeout = timeout_ms;
}

void bgpview_io_client_set_request_retries(bgpview_io_client_t *client,
					   int retry_cnt)
{
  assert(client != NULL);

  BCFG.request_retries = retry_cnt;
}

int bgpview_io_client_set_identity(bgpview_io_client_t *client,
				   const char *identity)
{
  assert(client != NULL);

  if(client->broker != NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_INIT_FAILED,
			     "Could not set identity (broker started)");
      return -1;
    }

  free(BCFG.identity);

  if((BCFG.identity = strdup(identity)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Could not set client identity");
      return -1;
    }

  return 0;
}
