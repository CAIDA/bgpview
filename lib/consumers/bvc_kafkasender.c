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
#include "bgpview_consumer_interface.h"
#include "bvc_kafkasender.h"

#define NAME "kafka-sender"


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

  /* count how many view have
   * been processed */
  int view_counter;

  /* count the number of elements (i.e.
   * <pfx,peer> information in the matrix)
   * are present in the current view */
  int current_view_elements;

} bvc_kafkasender_state_t;

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
  BVC_SET_STATE(consumer, state);


  /* allocate dynamic memory HERE */


  /* set defaults */

  state->view_counter = 0;

  state->current_view_elements = 0;


  /* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      goto err;
    }

  /* react to args HERE */


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

  free(state);

  BVC_SET_STATE(consumer, NULL);
}


int bvc_kafkasender_process_view(bvc_t *consumer, uint8_t interests, bgpview_t *view)
{

	kafka_data_t dest;
	// DEFAULT TMP VALUES

	dest.brokers = "192.172.226.44:9092,192.172.226.46:9092";
	dest.pfxs_paths_topic="views";
	dest.peers_topic="peers";
	dest.metadata_topic="metadata";
	dest.pfxs_paths_partition=0;
	dest.peers_partition=0;
	dest.metadata_partition=0;
	dest.pfxs_paths_offset=0;
	dest.peers_offset=0;
	dest.metadata_offset=0;


	bgpview_io_kafka_send(dest,view,NULL);
	exit(0);
  return 0;
}
