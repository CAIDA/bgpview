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

#include <assert.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "bgpview_consumer_interface.h"
#include "utils.h"

#include "bvc_myviewprocess.h"

#define NAME "my-view-process"

/* macro to access the current consumer state */
#define STATE (BVC_GET_STATE(consumer, myviewprocess))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_myviewprocess = {BVC_ID_MYVIEWPROCESS, NAME,
                                  BVC_GENERATE_PTRS(myviewprocess)};

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
  fprintf(stderr, "consumer usage: %s\n", consumer->name);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, ":?")) >= 0) {
    switch (opt) {
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

  if ((state = malloc_zero(sizeof(bvc_myviewprocess_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  /* allocate dynamic memory HERE */

  /* set defaults */

  state->view_counter = 0;

  state->current_view_elements = 0;

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
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

  if (state == NULL) {
    return;
  }

  /* deallocate dynamic memory HERE */

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_myviewprocess_process_view(bvc_t *consumer, bgpview_t *view)
{
  bvc_myviewprocess_state_t *state = STATE;
  bgpview_iter_t *it;

  /* create a new iterator */
  if ((it = bgpview_iter_create(view)) == NULL) {
    return -1;
  }

  /* increment the number of views processed */
  state->view_counter++;

  /* reset the elements counter */
  state->current_view_elements = 0;

  /* iterate through all peer of the current view
   *  - active peers only
   */
  for (bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(it); bgpview_iter_next_peer(it)) {
    /* Information that can be retrieved for the current peer:
     *
     * PEER NUMERIC ID:
     * bgpstream_peer_id_t id = bgpview_iter_peer_get_peer_id(it);
     *
     * PEER SIGNATURE (i.e. collector, peer ASn, peer IP):
     * bgpstream_peer_sig_t *s = bgpview_iter_peer_get_sig(it);
     *
     * NUMBER OF CURRENTLY ANNOUNCED THE PFX
     * int announced_pfxs = bgpview_iter_peer_get_pfx_cnt(it, 0,
     * BGPVIEW_FIELD_ACTIVE);
     * *0 -> ipv4 + ipv6
     *
     */
  }

  /* iterate through all prefixes of the current view
   *  - both ipv4 and ipv6 prefixes are considered
   *  - active prefixes only (i.e. do not consider prefixes that have
   *    been withdrawn)
   */
  for (bgpview_iter_first_pfx(it, 0 /* all ip versions*/, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(it); bgpview_iter_next_pfx(it)) {

    /* Information that can be retrieved for the current prefix:
     *
     * PREFIX:
     * bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(it)
     *
     * NUMBER OF PEERS CURRENTLY ANNOUNCING THE PFX
     * int peers_cnt = bgpview_iter_pfx_get_peer_cnt(it, BGPVIEW_FIELD_ACTIVE);
     */

    /* iterate over all the peers that currently observe the current pfx */
    for (bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(it); bgpview_iter_pfx_next_peer(it)) {
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
  printf("%" PRIu32 " num-views: %d\n", bgpview_get_time(view),
         state->view_counter);

  /* print the number of elements in the current view
   * FORMAT: <ts> num-elements: <num-elements> */
  printf("%" PRIu32 " num-elements: %d\n", bgpview_get_time(view),
         state->current_view_elements);

  /* destroy the view iterator */
  bgpview_iter_destroy(it);

  return 0;
}
