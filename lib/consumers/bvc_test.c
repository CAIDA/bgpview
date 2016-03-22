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
#include <unistd.h>

#include "utils.h"

#include "bgpview.h"
#include "bgpview_debug.h" /*< for bgpview_debug_dump */

#include "bgpview_consumer_interface.h"
#include "bgpstream_utils_pfx_set.h"

#include "bvc_test.h"

#define NAME "test"

#define MAX_DUMP_SIZE 100

#define STATE					\
  (BVC_GET_STATE(consumer, test))

static bvc_t bvc_test = {
  BVC_ID_TEST,
  NAME,
  BVC_GENERATE_PTRS(test)
};

typedef struct bvc_test_state {

  /** The number of views we have processed */
  int view_cnt;


} bvc_test_state_t;

#if 0
/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s\n",
	  /*	  "       -c <level>    output compression level to use (default: %d)\n" */
	  consumer->name);
}
#endif

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  /*int opt;*/

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
#if 0
  while((opt = getopt(argc, argv, ":c:f:?")) >= 0)
    {
      switch(opt)
	{
	case 'c':
	  state->compress_level = atoi(optarg);
	  break;

	case 'f':
	  state->ascii_file = strdup(optarg);
	  break;

	case '?':
	case ':':
	default:
	  usage(consumer);
	  return -1;
	}
    }
#endif

  return 0;
}

bvc_t *bvc_test_alloc()
{
  return &bvc_test;
}

int bvc_test_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_test_state_t *state = NULL;

  if((state = malloc_zero(sizeof(bvc_test_state_t))) == NULL)
    {
      return -1;
    }
  BVC_SET_STATE(consumer, state);

  /* set defaults here */

  /* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      return -1;
    }

  /* react to args here */

  return 0;
}

void bvc_test_destroy(bvc_t *consumer)
{
  bvc_test_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  fprintf(stdout, "BVC-TEST: %d views processed\n",
	  STATE->view_cnt);

  /* destroy things here */
  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_test_process_view(bvc_t *consumer, uint8_t interests,
			  bgpview_t *view)
{
  bvc_test_state_t *state = STATE;

  fprintf(stdout, "BVC-TEST: Interests: ");
  bgpview_consumer_interest_dump(interests);
  fprintf(stdout, "\n");

  /* only dump 'small' views, otherwise it is just obnoxious */
  if(bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE)
     < MAX_DUMP_SIZE)
    {
      bgpview_debug_dump(view);
    }
  else
    {
      fprintf(stdout, "BVC-TEST: Time:      %"PRIu32"\n",
	      bgpview_get_time(view));
      fprintf(stdout, "BVC-TEST: IPv4-Pfxs: %"PRIu32"\n",
	      bgpview_v4pfx_cnt(view, BGPVIEW_FIELD_ACTIVE));
      fprintf(stdout, "BVC-TEST: IPv6-Pfxs: %"PRIu32"\n",
	      bgpview_v6pfx_cnt(view, BGPVIEW_FIELD_ACTIVE));
      fprintf(stdout, "--------------------\n");
    }

  timeseries_set_single(BVC_GET_TIMESERIES(consumer),
			"bvc-test.v4pfxs_cnt",
			bgpview_v4pfx_cnt(view,
						  BGPVIEW_FIELD_ACTIVE),
			bgpview_get_time(view));

  state->view_cnt++;

  return 0;
  
  /** End of old test consumer **/

  // TEST: add some memory to the users pointers
  void *my_memory = NULL;
  bgpview_iter_t *it;
  it = bgpview_iter_create(view);

  
  // set destructors
  bgpview_set_user_destructor(view, free);
  bgpview_set_pfx_user_destructor(view, free);
  bgpview_set_peer_user_destructor(view, free);
  bgpview_set_pfx_peer_user_destructor(view, free);

  // view user memory allocation
  my_memory = malloc(sizeof(int));
  bgpview_set_user(view, my_memory);
  my_memory = NULL;

  // per-peer user memory allocation
  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
      my_memory = malloc(sizeof(int));
      *(int *)my_memory = bgpview_iter_peer_get_peer_id(it) + 100;
      bgpview_iter_peer_set_user(it, my_memory);
      
      my_memory = NULL;            
    }

  // per-prefix user memory allocation 
  for(bgpview_iter_first_pfx(it, BGPSTREAM_ADDR_VERSION_IPV4, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(it);
      bgpview_iter_next_pfx(it))
    {
      my_memory = malloc(sizeof(int));
      bgpview_iter_pfx_set_user(it, my_memory);
      my_memory = NULL;
      
      // per-prefix per-peer user memory allocation
      for(bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
          bgpview_iter_pfx_has_more_peer(it);
          bgpview_iter_pfx_next_peer(it))
        {
          my_memory = malloc(sizeof(int));
          *(int *)my_memory = bgpview_iter_peer_get_peer_id(it);
          bgpview_iter_pfx_peer_set_user(it, my_memory);
          my_memory = NULL;          
        }
    }

  // TEST: use seek in peers
  /* for(int i = 1; i <= 3 ; i++) */
  /*   { */
  /*     if(!bgpview_iter_seek_peer(it, i, BGPVIEW_FIELD_ALL_VALID)) */
  /*       { */
  /*         fprintf(stderr,"Peer %d not found\n",i); */
  /*       } */
  /*     else */
  /*       { */
  /*         if(bgpview_iter_seek_peer(it, i, BGPVIEW_FIELD_ACTIVE)) */
  /*           { */
  /*             fprintf(stderr,"Peer %d found - [ACTIVE]\n",i); */
  /*           } */
  /*         if(bgpview_iter_seek_peer(it, i, BGPVIEW_FIELD_INACTIVE)) */
  /*           { */
  /*             fprintf(stderr,"Peer %d found - [ACTIVE]\n",i); */
  /*           } */
  /*       } */
  /*   } */

  int d = 0;
  
  // TEST: check pfx-peers iterator and deactivate
  d = 0;
  for(bgpview_iter_first_pfx_peer(it, BGPSTREAM_ADDR_VERSION_IPV4,
                                          BGPVIEW_FIELD_ACTIVE,
                                          BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx_peer(it);
      bgpview_iter_next_pfx_peer(it))
    {
      // TEST: memory is correct
      /* fprintf(stderr, "Peer id: %d, %d\n", */
      /*         *(int *)bgpview_iter_peer_get_user(it), */
      /*         *(int *)bgpview_iter_pfx_peer_get_user(it)); */
      if(rand()%10 > 5)
        {
          bgpview_iter_pfx_deactivate_peer(it);
          d++;
        }
    }

  // dump view after random pfx-peer deactivation
  fprintf(stderr,"Deactivated %d pfx-peers\n", d);
  bgpview_debug_dump(view);

  // TEST: check pfx iterator and deactivate
  /* d = 0; */
  /* for(bgpview_iter_first_pfx(it, BGPSTREAM_ADDR_VERSION_IPV4, */
  /*                                    BGPVIEW_FIELD_ACTIVE); */
  /*     bgpview_iter_has_more_pfx(it); */
  /*     bgpview_iter_next_pfx(it)) */
  /*   { */
  /*     if(rand()%10 > 5) */
  /*       { */
  /*         bgpview_iter_deactivate_pfx(it); */
  /*         d++; */
  /*       } */
  /*   } */

  /* // dump view after random pfx deactivation */
  /* fprintf(stderr,"Deactivated %d pfxs\n", d); */
  /* bgpview_debug_dump(view); */

  // TEST: check peer iterator and deactivate
  d = 0;
  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
      if(rand()%10 > 5)
        {
          bgpview_iter_deactivate_peer(it);
          d++;
        }
    }

  // dump view after random peer deactivation
  fprintf(stderr,"Deactivated %d peers\n", d);
  bgpview_debug_dump(view);



  // TEST REMOVE
    // TEST: check pfx-peers iterator and remove
  d = 0;
  for(bgpview_iter_first_pfx_peer(it, BGPSTREAM_ADDR_VERSION_IPV4,
                                          BGPVIEW_FIELD_ACTIVE,
                                          BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx_peer(it);
      bgpview_iter_next_pfx_peer(it))
    {
      // TEST: memory is correct
      /* fprintf(stderr, "Peer id: %d, %d\n", */
      /*         *(int *)bgpview_iter_peer_get_user(it), */
      /*         *(int *)bgpview_iter_pfx_peer_get_user(it)); */
      if(rand()%10 > 5)
        {
          bgpview_iter_pfx_remove_peer(it);
          d++;
        }
    }

  // dump view after random pfx-peer deactivation
  fprintf(stderr,"Removed %d pfx-peers\n", d);
  bgpview_debug_dump(view);

  // TEST: check pfx iterator and remove
  /* d = 0; */
  /* for(bgpview_iter_first_pfx(it, BGPSTREAM_ADDR_VERSION_IPV4, */
  /*                                    BGPVIEW_FIELD_ALL_VALID); */
  /*     bgpview_iter_has_more_pfx(it); */
  /*     bgpview_iter_next_pfx(it)) */
  /*   { */
  /*     if(rand()%10 > 5) */
  /*       { */
  /*         bgpview_iter_remove_pfx(it); */
  /*         d++; */
  /*       } */
  /*   } */

  /* // dump view after random pfx deactivation */
  /* fprintf(stderr,"Removed %d pfxs\n", d); */
  /* bgpview_debug_dump(view); */

  // TEST: check peer iterator and remove
  d = 0;
  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ALL_VALID);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
      if(rand()%10 > 5)
        {
          bgpview_iter_remove_peer(it);
          d++;
        }
    }

  // dump view after random peer deactivation
  fprintf(stderr,"Removed %d peers\n", d);
  bgpview_debug_dump(view);

  // garbage collect the view
  fprintf(stderr, "Running garbage collector\n");
  bgpview_gc(view);
  bgpview_debug_dump(view);
  
  // TEST: bgpview clear
  /* bgpview_clear(view); */
  /* fprintf(stderr,"Cleared view \n"); */
  /* bgpview_debug_dump(view); */

  fprintf(stderr,"End of test\n");
  
  bgpview_iter_destroy(it);

    return 0;
}
