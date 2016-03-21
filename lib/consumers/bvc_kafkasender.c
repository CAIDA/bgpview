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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <errno.h>
#include <time.h>

#include "utils.h"
#include "bgpview_io_kafka.h"

#include "bgpview_io_kafka_peer.pb.h"
#include "bgpview_io_kafka_client.h"
#include "bgpview_consumer_interface.h"
#include "bvc_kafkasender.h"

#define NAME "kafka-sender"
#define CONSUMER_METRIC_PREFIX       "view.consumer.kafka-sender"

#define BUFFER_LEN 1024
#define METRIC_SENT_PREFIX_FORMAT    "%s."CONSUMER_METRIC_PREFIX".pfx.sent.%s"
#define METRIC_LOCAL_PREFIX_FORMAT    "%s."CONSUMER_METRIC_PREFIX".pfx.local.%s"
#define META_METRIC_PREFIX_FORMAT  "%s."CONSUMER_METRIC_PREFIX".meta.%s"


/* macro to access the current consumer state */
#define STATE					\
  (BVC_GET_STATE(consumer, kafkasender))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))


/* our 'class' */
static bvc_t bvc_kafkasender = {
  BVC_ID_KAFKASENDER,
  NAME,
  BVC_GENERATE_PTRS(kafkasender)
};



/* our 'instance' */
typedef struct bvc_kafkasender_state {

	bgpview_io_kafka_client_t *client;
	int cs;

	/** Timeseries Key Package */
	timeseries_kp_t *kp;

	/** Metrics indices */
	kafka_performance_t *metrics;

	int send_time_idx;
	int clone_time_idx;
	int total_time_idx;
	int arrival_time_idx;
	int processed_time_idx;

	int add_idx;
	int remove_idx;
	int common_idx;
	int change_idx;
	int current_pfx_cnt_idx;
	int historical_pfx_cnt_idx;

	int sync_cnt_idx;

} bvc_kafkasender_state_t;





/** Create timeseries metrics */
static int create_ts_metrics(bvc_t *consumer)
{

  char buffer[BUFFER_LEN];
  bvc_kafkasender_state_t *state = STATE;


  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "arrival_time");
  if((state->arrival_time_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }


  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "processed_time");
  if((state->processed_time_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "total_time");
  if((state->total_time_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "send_time");
  if((state->send_time_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "cloning_time");
  if((state->clone_time_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, METRIC_SENT_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "add.pfx_cnt");
  if((state->add_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, METRIC_SENT_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "remove.pfx_cnt");
  if((state->remove_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, METRIC_SENT_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "common.pfx_cnt");
  if((state->common_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, METRIC_SENT_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "change.pfx_cnt");
  if((state->change_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, BUFFER_LEN, METRIC_SENT_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "sync.pfx_cnt");
  if((state->sync_cnt_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }


  snprintf(buffer, BUFFER_LEN, METRIC_LOCAL_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "historical_view.pfx_cnt");
  if((state->historical_pfx_cnt_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }


  snprintf(buffer, BUFFER_LEN, METRIC_LOCAL_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, "current_view.pfx_cnt");
  if((state->current_pfx_cnt_idx =
      timeseries_kp_add_key(STATE->kp, buffer)) == -1)
    {
      return -1;
    }







  return 0;
}


/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s\n",
	  consumer->name);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while((opt = getopt(argc, argv, ":?")) >= 0)
    {
      switch(opt)
	{
	case '?':
	case ':':
	default:
	  usage(consumer);
	  return -1;
	}
    }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_kafkasender_alloc()
{
  return &bvc_kafkasender;
}


int bvc_kafkasender_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_kafkasender_state_t *state = NULL;

  if((state = malloc_zero(sizeof(bvc_kafkasender_state_t))) == NULL)
    {
      return -1;
    }

  if((state->metrics = malloc_zero(sizeof(kafka_performance_t))) == NULL)
    {
      return -1;
    }

  BVC_SET_STATE(consumer, state);

  /* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      goto err;
    }

  state->client=bgpview_io_kafka_client_init();
  state->cs=0;

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


void bvc_kafkasender_destroy(bvc_t *consumer)
{
  bvc_kafkasender_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  /* deallocate dynamic memory HERE */

  timeseries_kp_free(&state->kp);
  bgpview_io_kafka_client_free(state->client);
  free(state->metrics);

  free(state);

  BVC_SET_STATE(consumer, NULL);
}


int bvc_kafkasender_process_view(bvc_t *consumer, uint8_t interests, bgpview_t *view)
{
	bvc_kafkasender_state_t *state = STATE;
	uint32_t current_view_ts = bgpview_get_time(view);

	//bgpview_io_kafka_client_t *client=bgpview_io_kafka_client_init();

	//TODO: CONFIGURE


	if(state->cs==0)
		bgpview_io_kafka_client_start_producer(state->client);
	state->cs=1;


	time_t rawtime;
	struct tm * timeinfo;
	time(&rawtime);
	timeinfo = localtime(&rawtime);

    time_t start, end;
    int length;

    time(&start);

	bgpview_io_kafka_client_send_view(state->client,view,state->metrics,NULL);

	time(&rawtime);
	timeinfo = localtime(&rawtime);
	time(&end);
	length = difftime(end,start);


	// set remaining timeseries metrics

	timeseries_kp_set(state->kp, state->arrival_time_idx,
					state->metrics->arrival_time);

	timeseries_kp_set(state->kp, state->processed_time_idx,
			state->metrics->processed_time);


	timeseries_kp_set(state->kp, state->total_time_idx,
				state->metrics->total_time);

	timeseries_kp_set(state->kp, state->send_time_idx,
				state->metrics->send_time);



	timeseries_kp_set(state->kp, state->clone_time_idx,
						state->metrics->clone_time);


	timeseries_kp_set(state->kp, state->add_idx,
			state->metrics->add);

	timeseries_kp_set(state->kp, state->remove_idx,
			state->metrics->remove);
	timeseries_kp_set(state->kp, state->common_idx,
			state->metrics->common);

	timeseries_kp_set(state->kp, state->change_idx,
			state->metrics->change);


	timeseries_kp_set(state->kp, state->current_pfx_cnt_idx,
			state->metrics->current_pfx_cnt);

	timeseries_kp_set(state->kp, state->historical_pfx_cnt_idx,
			state->metrics->historical_pfx_cnt);

	timeseries_kp_set(state->kp, state->sync_cnt_idx,
			state->metrics->sync_cnt);

	// flush
	if(timeseries_kp_flush(STATE->kp, current_view_ts) != 0)
	{
	  fprintf(stderr, "Warning: could not flush %s %"PRIu32"\n",
			  NAME, bgpview_get_time(view));
	}



	return 0;
}
