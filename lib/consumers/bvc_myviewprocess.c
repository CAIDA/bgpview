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

#include "utils.h"
#include "bgpview_consumer_interface.h"

#include "bvc_myviewprocess.h"


#define NAME "my-view-process"


/* macro to access the current consumer state */
#define STATE					\
  (BVC_GET_STATE(consumer, myviewprocess))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))


/* our 'class' */
static bvc_t bvc_myviewprocess = {
  BVC_ID_MYVIEWPROCESS,
  NAME,
  BVC_GENERATE_PTRS(myviewprocess)
};



/* our 'instance' */
typedef struct bvc_myviewprocess_state {

  /* count how many view have 
   * been processed */
  int view_counter;

  /* count the number of elements (i.e.
   * <pfx,peer> information in the matrix)
   * are present in the current view */
  int current_view_elements;

} bvc_myviewprocess_state_t;


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

bvc_t *bvc_myviewprocess_alloc()
{
  return &bvc_myviewprocess;
}


int bvc_myviewprocess_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_myviewprocess_state_t *state = NULL;
  int i;

  if((state = malloc_zero(sizeof(bvc_myviewprocess_state_t))) == NULL)
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


void bvc_myviewprocess_destroy(bvc_t *consumer)
{
  bvc_myviewprocess_state_t *state = STATE;
  int i;
  
  if(state == NULL)
    {
      return;
    }

  /* deallocate dynamic memory HERE */

  free(state);

  BVC_SET_STATE(consumer, NULL);
}


int bvc_myviewprocess_process_view(bvc_t *consumer, uint8_t interests,
                                    bgpview_t *view)
{
  bvc_myviewprocess_state_t *state = STATE;
  bgpview_iter_t *it;
  int i;
  
  /* create a new iterator */
  if((it = bgpview_iter_create(view)) == NULL)
    {
      return -1;
    }

  /* increment the number of views processed */
  state->view_counter++;

  /* reset the elements counter */
  state->current_view_elements = 0;

  
  /* iterate through all peer of the current view 
   *  - active peers only
   */
  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_peer(it);
      bgpview_iter_next_peer(it))
    {
      /* Information that can be retrieved for the current peer:
       *
       * PEER NUMERIC ID:
       * bgpstream_peer_id_t id = bgpview_iter_peer_get_peer_id(it);
       *
       * PEER SIGNATURE (i.e. collector, peer ASn, peer IP):
       * bgpstream_peer_sig_t *s = bgpview_iter_peer_get_sig(it);       
       *
       * NUMBER OF CURRENTLY ANNOUNCED THE PFX
       * int announced_pfxs = bgpview_iter_peer_get_pfx_cnt(it, 0, BGPVIEW_FIELD_ACTIVE);
       * *0 -> ipv4 + ipv6
       * 
       */
    }

  
  /* iterate through all prefixes of the current view 
   *  - both ipv4 and ipv6 prefixes are considered
   *  - active prefixes only (i.e. do not consider prefixes that have
   *    been withdrawn) 
   */
  for(bgpview_iter_first_pfx(it, 0 /* all ip versions*/, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(it);
      bgpview_iter_next_pfx(it))
    {

      /* Information that can be retrieved for the current prefix:
       *
       * PREFIX:
       * bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(it)
       *
       * NUMBER OF PEERS CURRENTLY ANNOUNCING THE PFX
       * int peers_cnt = bgpview_iter_pfx_get_peer_cnt(it, BGPVIEW_FIELD_ACTIVE);
       */
      
      /* iterate over all the peers that currently observe the current pfx */
      for(bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
          bgpview_iter_pfx_has_more_peer(it);
          bgpview_iter_pfx_next_peer(it))
        {
          /* Information that can be retrieved for the current element:
           *
           * ORIGIN ASN:
           * int origin_asn = bgpview_iter_pfx_peer_get_orig_asn(it);
           */
          
          state->current_view_elements++;
        }
      
    }


  /* print the number of views processed so far 
   * FORMAT: <ts> num-views: <num-views> */
  printf("%"PRIu32" num-views: %d\n", bgpview_get_time(view), state->view_counter);

  /* print the number of elements in the current view
   * FORMAT: <ts> num-elements: <num-elements> */
  printf("%"PRIu32" num-elements: %d\n", bgpview_get_time(view), state->current_view_elements);


  /* destroy the view iterator */
  bgpview_iter_destroy(it);

  return 0;
}
