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

#include "bgpview_io_zmq_client_int.h"

#include "khash.h"
#include "utils.h"

static int handle_server_msg(zloop_t *loop, zsock_t *reader, void *arg);
static int handle_server_sub_msg(zloop_t *loop, zsock_t *reader, void *arg);
static int handle_master_msg(zloop_t *loop, zsock_t *reader, void *arg);

#define CFG (broker->cfg)

#define DO_CALLBACK(cbfunc, args...)                    \
  do {                                                  \
    if(CFG->callbacks.cbfunc != NULL)                   \
      {                                                 \
	CFG->callbacks.cbfunc(CFG->master, args,        \
                              CFG->callbacks.user);     \
      }                                                 \
  } while(0)

#define ISERR                                   \
  if(errno == EINTR || errno == ETERM)          \
    {                                           \
      goto interrupt;                           \
    }                                           \
  else

static int req_list_find_empty(bgpview_io_zmq_client_broker_t *broker)
{
  int i;

  for(i=0; i<MAX_OUTSTANDING_REQ; i++)
    {
      if(broker->req_list[i].in_use == 0)
        {
          return i;
        }
    }

  return -1;
}

static int req_list_find(bgpview_io_zmq_client_broker_t *broker, seq_num_t seq_num)
{
  int i;

  for(i=0; i<MAX_OUTSTANDING_REQ; i++)
    {
      if(broker->req_list[i].seq_num == seq_num &&
         broker->req_list[i].in_use != 0)
        {
          return i;
        }
    }

  return -1;
}

static void req_mark_unused(bgpview_io_zmq_client_broker_t *broker,
                            bgpview_io_zmq_client_broker_req_t *req)
{
  int i;

  req->in_use = 0;
  broker->req_count--;

  for(i=0; i<req->msg_frames_cnt; i++)
    {
      zmq_msg_close(&req->msg_frames[i]);
    }
  req->msg_frames_cnt = 0;
}

static void reset_heartbeat_timer(bgpview_io_zmq_client_broker_t *broker,
				  uint64_t clock)
{
  broker->heartbeat_next = clock + CFG->heartbeat_interval;
}

static void reset_heartbeat_liveness(bgpview_io_zmq_client_broker_t *broker)
{
  broker->heartbeat_liveness_remaining = CFG->heartbeat_liveness;
}

static int server_subscribe(bgpview_io_zmq_client_broker_t *broker)
{
  if((broker->server_sub_socket = zsocket_new(CFG->ctx, ZMQ_SUB)) == NULL)
    {
      fprintf(stderr, "Failed to create server SUB connection\n");
      return -1;
    }
  zsocket_set_rcvhwm(broker->server_sub_socket, 3);
  zsocket_set_subscribe(broker->server_sub_socket, "");

  if(zsocket_connect(broker->server_sub_socket, "%s", CFG->server_sub_uri) < 0)
    {
      fprintf(stderr, "Could not connect to server");
      return -1;
    }

  /* create a new reader for this server sub socket */
  if(zloop_reader(broker->loop, broker->server_sub_socket,
                  handle_server_sub_msg, broker) != 0)
    {
      fprintf(stderr, "Could not add server sub socket to reactor\n");
      return -1;
    }

  return 0;
}

static int server_send_intents(bgpview_io_zmq_client_broker_t *broker,
                               int sndmore)
{
  /* send our intents */
  if(zmq_send(broker->server_socket, &CFG->intents, 1, sndmore) == -1)
    {
      fprintf(stderr, "Could not send ready msg to server\n");
      return -1;
    }

  return 0;
}

static int server_connect(bgpview_io_zmq_client_broker_t *broker)
{
  uint8_t msg_type_p;

  /* connect to server socket */
  if((broker->server_socket = zsocket_new(CFG->ctx, ZMQ_DEALER)) == NULL)
    {
      fprintf(stderr, "Failed to create server connection\n");
      return -1;
    }

  /* up the hwm */
  /*
    zsocket_set_sndhwm(broker->server_socket, 0);
    zsocket_set_rcvhwm(broker->server_socket, MAX_OUTSTANDING_REQ*2);
  */

  if(CFG->identity != NULL && strlen(CFG->identity) > 0)
    {
      zsock_set_identity(broker->server_socket, CFG->identity);
    }
  else
    {
      CFG->identity = zsock_identity(broker->server_socket);
    }

  if(zsocket_connect(broker->server_socket, "%s", CFG->server_uri) < 0)
    {
      fprintf(stderr, "Could not connect to server");
      return -1;
    }

  msg_type_p = BGPVIEW_IO_ZMQ_MSG_TYPE_READY;
  if(zmq_send(broker->server_socket, &msg_type_p, 1, ZMQ_SNDMORE) == -1)
    {
      fprintf(stderr, "Could not send ready msg to server\n");
      return -1;
    }

  if(server_send_intents(broker, 0) != 0)
    {
      return -1;
    }

  /* reset the time for the next heartbeat sent to the server */
  reset_heartbeat_timer(broker, zclock_time());

  /* create a new reader for this server socket */
  if(zloop_reader(broker->loop, broker->server_socket,
                  handle_server_msg, broker) != 0)
    {
      fprintf(stderr, "Could not add server socket to reactor\n");
      return -1;
    }

  assert(broker->server_socket != NULL);

  /* subscribe for server table messages (if we are a consumer) */
  return server_subscribe(broker);
}

static void server_disconnect(bgpview_io_zmq_client_broker_t *broker)
{
  /* remove the server reader from the reactor */
  zloop_reader_end(broker->loop, broker->server_socket);
  /* destroy the server socket */
  zsocket_destroy(CFG->ctx, broker->server_socket);

  /* if we are a consumer, then remove the sub socket too */
  if(broker->server_sub_socket != NULL)
    {
      /* remove the server sub reader from the reactor */
      zloop_reader_end(broker->loop, broker->server_sub_socket);
      /* destroy the server sub socket */
      zsocket_destroy(CFG->ctx, broker->server_sub_socket);
    }
}

static int server_send_term(bgpview_io_zmq_client_broker_t *broker)
{
  uint8_t msg_type_p = BGPVIEW_IO_ZMQ_MSG_TYPE_TERM;

  fprintf(stderr, "DEBUG: broker sending TERM\n");

  if(zmq_send(broker->server_socket, &msg_type_p, 1, 0) == -1)
    {
      fprintf(stderr, "Could not send ready msg to server\n");
      return -1;
    }

  return 0;
}

static int handle_reply(bgpview_io_zmq_client_broker_t *broker)
{
  seq_num_t seq_num;
  bgpview_io_zmq_client_broker_req_t *req;

  int idx;

  /* there must be more frames for us */
  if(zsocket_rcvmore(broker->server_socket) == 0)
    {
      fprintf(stderr,
              "Invalid message received from server (missing seq num)\n");
      goto err;
    }

  if(zmq_recv(broker->server_socket, &seq_num, sizeof(seq_num_t), 0)
     != sizeof(seq_num_t))
    {
      fprintf(stderr, "Invalid message received from server "
              "(malformed sequence number)\n");
    }

  /* find the corresponding record from the outstanding req set */
  if((idx = req_list_find(broker, seq_num)) == -1)
    {
      fprintf(stderr,
	      "WARN: No outstanding request info for seq num %"PRIu32"\n",
	      seq_num);
      return 0;
    }

  fprintf(stderr, "DEBUG: Got reply for seq num %"PRIu32" (%d)\n",
          seq_num, idx);

  req = &broker->req_list[idx];

  /* mark this request as unused */
  req_mark_unused(broker, req);

  /** @todo consider how/if we should tell the client about a reply */

  return 0;

 err:
  return -1;
}

static int send_request(bgpview_io_zmq_client_broker_t *broker,
			bgpview_io_zmq_client_broker_req_t *req,
			uint64_t clock)
{
  int i = 1;
  zmq_msg_t llm_cpy;
  int mask;

  req->retry_at = clock + CFG->request_timeout;

  /* send the type */
  if(zmq_send(broker->server_socket, &req->msg_type,
              bgpview_io_zmq_msg_type_size_t, ZMQ_SNDMORE)
     != bgpview_io_zmq_msg_type_size_t)
    {
      return -1;
    }

  /* send our intents in case the server gave up on us */
  if(server_send_intents(broker, ZMQ_SNDMORE) != 0)
    {
      return -1;
    }

  /* send the seq num */
  if(zmq_send(broker->server_socket,
              &req->seq_num, sizeof(seq_num_t),
              ZMQ_SNDMORE)
     != sizeof(seq_num_t))
    {
      return -1;
    }

  for(i=0; i<req->msg_frames_cnt; i++)
    {
      mask = (i < (req->msg_frames_cnt-1)) ? ZMQ_SNDMORE : 0;
      zmq_msg_init(&llm_cpy);
      if(zmq_msg_copy(&llm_cpy, &req->msg_frames[i]) == -1)
        {
	  fprintf(stderr, "Could not copy message\n");
	  return -1;
        }
      if(zmq_sendmsg(broker->server_socket, &llm_cpy, mask) == -1)
	{
          zmq_msg_close(&llm_cpy);
	  fprintf(stderr, "Could not pass message to server\n");
	  return -1;
	}
    }

  return 0;
}

static int is_shutdown_time(bgpview_io_zmq_client_broker_t *broker, uint64_t clock)
{
  if(broker->shutdown_time > 0 &&
     ((broker->req_count == 0) ||
      (broker->shutdown_time <= clock)))
    {
      /* time to end */
      return 1;
    }
  return 0;
}

static int handle_timeouts(bgpview_io_zmq_client_broker_t *broker, uint64_t clock)
{
  int idx;
  bgpview_io_zmq_client_broker_req_t *req;

  /* nothing to time out */
  if(broker->req_count == 0)
    {
      return 0;
    }

  /* re-tx any requests that have timed out */

  for(idx=0; idx<MAX_OUTSTANDING_REQ; idx++)
    {
      if(broker->req_list[idx].in_use == 0)
        {
          continue;
        }

      req = &broker->req_list[idx];

      if(clock < req->retry_at)
	{
	  /*fprintf(stderr, "DEBUG: at %"PRIu64", waiting for %"PRIu64"\n",
	    zclock_time(), req->retry_at);*/
          continue;
	}

      /* we are either going to discard this request, or re-tx it */
      if(--req->retries_remaining == 0)
	{
	  /* time to abandon this request */
	  /** @todo send notice to client */
	  fprintf(stderr,
		  "DEBUG: Request %"PRIu32" expired without reply, "
		  "abandoning\n",
		  req->seq_num);

          req_mark_unused(broker, req);
	  continue;
	}

      fprintf(stderr, "DEBUG: Retrying request %"PRIu32"\n", req->seq_num);

      if(send_request(broker, req, clock) != 0)
	{
	  goto err;
	}
    }

  return 0;

 err:
  return -1;
}

static int handle_heartbeat_timer(zloop_t *loop, int timer_id, void *arg)
{
  bgpview_io_zmq_client_broker_t *broker = (bgpview_io_zmq_client_broker_t*)arg;

  uint8_t msg_type_p;

  uint64_t clock = zclock_time();

  if(is_shutdown_time(broker, clock) != 0)
    {
      return -1;
    }

  if(--broker->heartbeat_liveness_remaining == 0)
    {
      /* the server has been flat-lining for too long, get the paddles! */
      fprintf(stderr, "WARN: heartbeat failure, can't reach server\n");
      fprintf(stderr, "WARN: reconnecting in %"PRIu64" msec...\n",
              broker->reconnect_interval_next);

      zclock_sleep(broker->reconnect_interval_next);

      if(broker->reconnect_interval_next < CFG->reconnect_interval_max)
        {
          broker->reconnect_interval_next *= 2;
        }

      /* shut down our sockets */
      server_disconnect(broker);
      /* reconnect */
      server_connect(broker);
      assert(broker->server_socket != NULL);

      reset_heartbeat_liveness(broker);
    }

  /* send heartbeat to server if it is time */
  if(clock > broker->heartbeat_next)
    {
      msg_type_p = BGPVIEW_IO_ZMQ_MSG_TYPE_HEARTBEAT;
      if(zmq_send(broker->server_socket, &msg_type_p, 1, 0) == -1)
	{
	  fprintf(stderr, "Could not send heartbeat msg to server\n");
	  goto err;
	}

      reset_heartbeat_timer(broker, clock);
    }

  if(handle_timeouts(broker, clock) != 0)
    {
      return -1;
    }

  return 0;

 err:
  return -1;
}

static int handle_server_msg(zloop_t *loop, zsock_t *reader, void *arg)
{
  bgpview_io_zmq_client_broker_t *broker = (bgpview_io_zmq_client_broker_t*)arg;
  bgpview_io_zmq_msg_type_t msg_type;
  uint64_t clock;
  int retries = 0;

  while(retries < BGPVIEW_IO_ZMQ_CLIENT_BROKER_GREEDY_MAX_MSG)
    {
      clock = zclock_time();

      if(is_shutdown_time(broker, clock) != 0)
        {
          return -1;
        }

      msg_type = bgpview_io_zmq_recv_type(broker->server_socket, ZMQ_DONTWAIT);

      if(zctx_interrupted != 0)
        {
          goto interrupt;
        }

      switch(msg_type)
        {
        case BGPVIEW_IO_ZMQ_MSG_TYPE_REPLY:
          reset_heartbeat_liveness(broker);
          if(handle_reply(broker) != 0)
            {
              goto err;
            }

          if(zctx_interrupted != 0)
            {
              goto interrupt;
            }
          break;

        case BGPVIEW_IO_ZMQ_MSG_TYPE_HEARTBEAT:
          reset_heartbeat_liveness(broker);
          break;

        case BGPVIEW_IO_ZMQ_MSG_TYPE_UNKNOWN:
          /* nothing more to receive at the moment */
          if(errno == EAGAIN)
            {
              return 0;
            }
          /* fall through */

        default:
          fprintf(stderr, "Invalid message type received from "
                  "server (%d)\n", msg_type);
          goto err;
        }

      broker->reconnect_interval_next =
        CFG->reconnect_interval_min;

      /* have we just processed the last reply? */
      if(is_shutdown_time(broker, clock) != 0)
        {
          return -1;
        }
      if(handle_timeouts(broker, clock) != 0)
        {
          return -1;
        }

      /* check if the number of outstanding requests has dropped enough to start
         accepting more from our master */
      if(broker->master_removed != 0 &&
         broker->req_count < MAX_OUTSTANDING_REQ)
        {
          fprintf(stderr, "INFO: Accepting requests\n");
          if(zloop_reader(broker->loop, broker->master_pipe,
                          handle_master_msg, broker) != 0)
            {
              fprintf(stderr, "Could not re-add master pipe to reactor\n");
              return -1;
            }
          broker->master_removed = 0;
        }

      retries++;
    }

  return 0;

 interrupt:
  fprintf(stderr, "Caught interrupt");
  return -1;

 err:
  return -1;
}

static int handle_server_sub_msg(zloop_t *loop, zsock_t *reader, void *arg)
{
  bgpview_io_zmq_client_broker_t *broker = (bgpview_io_zmq_client_broker_t*)arg;

  zmq_msg_t msg;
  int flags;
  int first_frame = 1;

  /* send empty start-of-view message to master (this is a leftover from when we
     used to prefix the view with interests) */
  if(zmq_send(broker->master_zocket, "", 0, ZMQ_SNDMORE) == -1)
    {
      fprintf(stderr, "Could not send start-of-view to master\n");
      return -1;
    }

  /* now relay the view to master */
  /* for some ridiculous reason, rcvmore is false for the first frame... */
  while(first_frame != 0 || zsocket_rcvmore(broker->server_sub_socket) != 0)
    {
      first_frame = 0;

      /* suck the next message from the server */
      if(zmq_msg_init(&msg) == -1)
	{
	  fprintf(stderr, "Could not init proxy message\n");
	  goto err;
	}
      if(zmq_msg_recv(&msg, broker->server_sub_socket, 0) == -1)
	{
	  switch(errno)
	    {
	    case EINTR:
	      goto interrupt;
	      break;

	    default:
	      fprintf(stderr, "Failed to receive view\n");
	      goto err;
	      break;
	    }
	}

      /* is this the last part of the message? */
      flags = (zsocket_rcvmore(broker->server_sub_socket) != 0) ? ZMQ_SNDMORE : 0;
      /* send this on to the master */
      if(zmq_msg_send(&msg, broker->master_zocket, flags) == -1)
	{
          zmq_msg_close(&msg);
	  fprintf(stderr, "Could not pass message to master\n");
	  return -1;
	}
    }

  return 0;

 interrupt:
  fprintf(stderr, "Caught interrupt");
  zmq_msg_close(&msg);
  return -1;

 err:
  zmq_msg_close(&msg);
  return -1;
}

static int handle_master_msg(zloop_t *loop, zsock_t *reader, void *arg)
{
  bgpview_io_zmq_client_broker_t *broker = (bgpview_io_zmq_client_broker_t*)arg;
  bgpview_io_zmq_msg_type_t msg_type;
  bgpview_io_zmq_client_broker_req_t *req = NULL;

  uint64_t clock = zclock_time();

  int idx;

  if(is_shutdown_time(broker, clock) != 0)
    {
      return -1;
    }

  /* peek at the first frame (msg type) */
  if((msg_type = bgpview_io_zmq_recv_type(broker->master_zocket, 0))
     != BGPVIEW_IO_ZMQ_MSG_TYPE_UNKNOWN)
    {
      if(msg_type != BGPVIEW_IO_ZMQ_MSG_TYPE_VIEW)
        {
          fprintf(stderr, "Invalid message type received from master\n");
          goto err;
        }
      /* there must be more frames for us */
      if(zsocket_rcvmore(broker->master_zocket) == 0)
        {
          fprintf(stderr, "Invalid message received from master "
                  "(missing seq num)\n");
          goto err;
        }

      /* there is guaranteed to be an empty request, find it */
      idx = req_list_find_empty(broker);
      fprintf(stderr, "DEBUG: Storing request at index %d\n", idx);
      assert(idx != -1);
      req = &broker->req_list[idx];

      /* count this req */
      broker->req_count++;

      req->msg_type = msg_type;
      req->in_use = 1;

      /* now we need the seq number */
      if(zmq_recv(broker->master_zocket, &req->seq_num, sizeof(seq_num_t), 0)
         != sizeof(seq_num_t))
        {
          ISERR
            {
              fprintf(stderr, "Invalid message received from master "
                      "(malformed sequence number)\n");
              goto err;
            }
        }

      /* read the payload of the message into a list to send to the server */
      if(zsocket_rcvmore(broker->master_zocket) == 0)
        {
          fprintf(stderr, "Invalid message received from master "
                  "(missing payload)\n");
          goto err;
        }

      /* recv messages into the req list until rcvmore is false */
      while(1)
        {
          /* expand the frames array if we need more */
          if(req->msg_frames_alloc == req->msg_frames_cnt)
            {
              req->msg_frames_alloc +=
                BGPVIEW_IO_ZMQ_CLIENT_BROKER_REQ_MSG_FRAME_CHUNK;
              if((req->msg_frames =
                  realloc(req->msg_frames,
                          sizeof(zmq_msg_t) * req->msg_frames_alloc)) == NULL)
                {
                  fprintf(stderr, "Could not allocate message frames\n");
                  goto err;
                }

              fprintf(stderr, "DEBUG: %d frames allocated for req %d\n",
                      req->msg_frames_alloc, idx);
            }

          if(zmq_msg_init(&req->msg_frames[req->msg_frames_cnt]) != 0)
            {
              fprintf(stderr, "Could not create llm\n");
              goto err;
            }
          if(zmq_msg_recv(&req->msg_frames[req->msg_frames_cnt],
			  broker->master_zocket, 0) == -1)
            {
              goto interrupt;
            }
	  req->msg_frames_cnt++;
          if(zsocket_rcvmore(broker->master_zocket) == 0)
            {
              break;
            }
        }

      /* init the re-transmit state */
      req->retries_remaining = CFG->request_retries;
      /* retry_at is set by send_request */


      /*fprintf(stderr, "DEBUG: tx.seq: %"PRIu32", tx.msg_type: %d\n",
        req.seq_num, req.msg_type);*/

      /* now send on to the server */
      if(send_request(broker, req, zclock_time()) != 0)
        {
          goto err;
        }

      req = NULL;
    }
  else
    {
      fprintf(stderr, "Invalid msg from master\n");
    }

  if(handle_timeouts(broker, clock) != 0)
    {
      return -1;
    }

  /* check if we have too many outstanding requests */
  if(broker->req_count == MAX_OUTSTANDING_REQ)
    {
      fprintf(stderr, "INFO: Rate limiting\n");
      zloop_reader_end(broker->loop, broker->master_pipe);
      broker->master_removed = 1;
    }

  return 0;

 interrupt:
  fprintf(stderr, "Caught interrupt");
  return -1;

 err:
  return -1;
}

static int handle_signal_msg(zloop_t *loop, zsock_t *reader, void *arg)
{
  bgpview_io_zmq_client_broker_t *broker = (bgpview_io_zmq_client_broker_t*)arg;
  uint64_t clock = zclock_time();

  zmsg_t *msg = NULL;
  char *command = NULL;

  if(is_shutdown_time(broker, clock) != 0)
    {
      return -1;
    }

  if((msg = zmsg_recv(broker->signal_pipe)) == NULL)
    {
      goto interrupt;
    }
  if((command = zmsg_popstr(msg)) == NULL)
    {
      goto quit;
    }
  if(strcmp(command, "$TERM") == 0)
    {
      /* this is a message for us, just shut down */
      if(broker->shutdown_time == 0)
        {
          fprintf(stderr,
                  "INFO: Got $TERM, shutting down client broker on next "
                  "cycle\n");
          broker->shutdown_time = clock + CFG->shutdown_linger;
        }
      if(is_shutdown_time(broker, clock) != 0)
        {
          goto quit;
        }
    }
  else
    {
      fprintf(stderr, "Invalid signal from master\n");
      goto quit;
    }

  if(handle_timeouts(broker, clock) != 0)
    {
      goto quit;
    }

  zmsg_destroy(&msg);
  free(command);
  return 0;

 interrupt:
  fprintf(stderr, "Caught interrupt");
  zmsg_destroy(&msg);
  free(command);
  return -1;

 quit:
  zmsg_destroy(&msg);
  free(command);
  return -1;
}

static void broker_free(bgpview_io_zmq_client_broker_t **broker_p)
{
  assert(broker_p != NULL);
  bgpview_io_zmq_client_broker_t *broker = *broker_p;
  int i;

  /* free our reactor */
  zloop_destroy(&broker->loop);

  if(broker->req_count > 0)
    {
      fprintf(stderr,
	      "WARNING: At shutdown there were %d outstanding requests\n",
	      broker->req_count);
    }
  for(i=0; i<MAX_OUTSTANDING_REQ; i++)
    {
      free(broker->req_list[i].msg_frames);
      broker->req_list[i].msg_frames = NULL;
    }

  /* free'd by zctx_destroy in master */
  broker->server_socket = NULL;

  free(broker);

  *broker_p = NULL;
  return;
}

static int init_reactor(bgpview_io_zmq_client_broker_t *broker)
{
  /* set up the reactor */
  if((broker->loop = zloop_new()) == NULL)
    {
      fprintf(stderr, "Could not initialize reactor\n");
      return -1;
    }
  /* DEBUG */
  //zloop_set_verbose(broker->loop, true);

  /* add a heartbeat timer */
  if((broker->timer_id = zloop_timer(broker->loop,
                                     CFG->heartbeat_interval, 0,
                                     handle_heartbeat_timer, broker)) < 0)
    {
      fprintf(stderr, "Could not add heartbeat timer reactor\n");
      return -1;
    }

  /* add master pipe to reactor */
  if(zloop_reader(broker->loop, broker->master_pipe,
                  handle_master_msg, broker) != 0)
    {
      fprintf(stderr, "Could not add master pipe to reactor\n");
      return -1;
    }

  /* add signal pipe to reactor */
  if(zloop_reader(broker->loop, broker->signal_pipe,
                  handle_signal_msg, broker) != 0)
    {
      fprintf(stderr, "Could not add signal pipe to reactor\n");
      return -1;
    }

  return 0;
}

static bgpview_io_zmq_client_broker_t *broker_init(zsock_t *signal_pipe,
                                                   bgpview_io_zmq_client_broker_config_t *cfg)
{
  bgpview_io_zmq_client_broker_t *broker;

  if((broker = malloc_zero(sizeof(bgpview_io_zmq_client_broker_t))) == NULL)
    {
      fprintf(stderr, "Could not initialize broker state\n");
      return NULL;
    }

  broker->cfg = cfg;

  broker->signal_pipe = signal_pipe;
  broker->master_pipe = CFG->master_pipe;
  broker->master_zocket = zsock_resolve(broker->master_pipe);
  assert(broker->master_zocket != NULL);

  /* init counters from options */
  reset_heartbeat_liveness(broker);
  broker->reconnect_interval_next = CFG->reconnect_interval_min;

  if(init_reactor(broker) != 0)
    {
      goto err;
    }

  return broker;

 err:
  broker_free(&broker);
  return NULL;
}

/* ========== PUBLIC FUNCS BELOW HERE ========== */

/* broker owns none of the memory passed to it. only responsible for what it
   mallocs itself (e.g. poller) */
void bgpview_io_zmq_client_broker_run(zsock_t *pipe, void *args)
{
  bgpview_io_zmq_client_broker_t *broker;

  assert(pipe != NULL);
  assert(args != NULL);

  if((broker =
      broker_init(pipe, (bgpview_io_zmq_client_broker_config_t*)args)) == NULL)
    {
      return;
    }

  /* connect to the server */
  if(server_connect(broker) != 0)
    {
      return;
    }

  /* signal to our master that we are ready */
  if(zsock_signal(pipe, 0) != 0)
    {
      fprintf(stderr, "Could not send ready signal to master\n");
      return;
    }

  /* blocks until broker exits */
  zloop_start(broker->loop);

  if(server_send_term(broker) != 0)
    {
      // err will be set
      broker_free(&broker);
      return;
    }

  /* send a term on the master socket in case someone is listening */
  zsock_set_sndtimeo(broker->master_pipe, 0);
  zstr_send(broker->master_pipe, "$TERM");

  broker_free(&broker);
  return;
}
