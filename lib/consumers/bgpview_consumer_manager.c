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
#include <stdio.h>
#include <string.h>

#include "utils.h"
#include "parse_cmd.h"

#include "bgpview_consumer_interface.h"
#include "bgpview_consumer_manager.h"

/* include all consumers here */

/* test consumer */
#include "bvc_test.h"

/* test consumer */
#include "bvc_perfmonitor.h"

/* Visibility consumer */
#include "bvc_visibility.h"

/* Per-AS Visibility consumer */
#include "bvc_perasvisibility.h"

/* Per-Geo Visibility consumer */
#include "bvc_pergeovisibility.h"

/* Announced Prefixes consumer */
#include "bvc_announcedpfxs.h"

/* Moas consumer */
#include "bvc_moas.h"

#ifdef WITH_BGPVIEW_IO_FILE
/* Archiver consumer */
#include "bvc_archiver.h"
#endif

/* Edges consumer */
#include "bvc_edges.h"

/* Triplet consumer */
#include "bvc_triplets.h"

/* Prefix origins consumer */
#include "bvc_pfxorigins.h"

/* Routed Space monitor consumer */
#include "bvc_routedspace.h"

/* My View Process consumer */
#include "bvc_myviewprocess.h"

#if defined(WITH_BGPVIEW_IO_KAFKA) || defined(WITH_BGPVIEW_IO_ZMQ)
/* View Sender consumer */
#include "bvc_viewsender.h"
#endif

/* Path Change consumer */
#include "bvc_pathchange.h"

/* Sub-prefix consumer */
#include "bvc_subpfx.h"

/* Per-peer prefix origins consumer */
#include "bvc_peerpfxorigins.h"

/* Prefix to AS consumer */
#include "bvc_pfx2as.h"

/* ==================== PRIVATE DATA STRUCTURES ==================== */

#define MAXOPTS 1024

struct bgpview_consumer_manager {

  /** Array of consumers
   * @note index of consumer is given by (bvc_id_t - 1)
   */
  bvc_t *consumers[BVC_ID_LAST];

  /** Borrowed pointer to a libtimeseries instance */
  timeseries_t *timeseries;

  /** State structure that is passed along with each view */
  bvc_chain_state_t chain_state;
};

/** Convenience typedef for the backend alloc function type */
typedef bvc_t *(*consumer_alloc_func_t)(void);

/** Array of backend allocation functions. */
static const consumer_alloc_func_t consumer_alloc_functions[] = {

  /** Pointer to test backend alloc function */
  bvc_test_alloc,

  /** Pointer to performance monitor function */
  bvc_perfmonitor_alloc,

  /** Pointer to visibility alloc function */
  bvc_visibility_alloc,

  /** Pointer to per-as vis alloc function */
  bvc_perasvisibility_alloc,

  /** Pointer to per-geo vis alloc function */
  bvc_pergeovisibility_alloc,

  /** Pointer to announcedpfxs alloc function */
  bvc_announcedpfxs_alloc,

  /** Pointer to moas alloc function */
  bvc_moas_alloc,

#ifdef WITH_BGPVIEW_IO_FILE
  /** Pointer to archiver alloc function */
  bvc_archiver_alloc,
#else
  NULL,
#endif

  /** DEPRECATED: submoas consumer */
  NULL,

  /** Pointer to edge alloc function */
  bvc_edges_alloc,

  /** Pointer to triplet alloc function */
  bvc_triplets_alloc,

  /** Pointer to pfxorigins alloc function */
  bvc_pfxorigins_alloc,

  /** Pointer to routedspace alloc function */
  bvc_routedspace_alloc,

  /** Pointer to myviewprocess alloc function */
  bvc_myviewprocess_alloc,

#if defined(WITH_BGPVIEW_IO_KAFKA) || defined(WITH_BGPVIEW_IO_ZMQ)
  /** Pointer to viewsender alloc function */
  bvc_viewsender_alloc,
#else
  NULL,
#endif

  /** Pointer to pathchange alloc function */
  bvc_pathchange_alloc,

  /** Pointer to subpfx alloc function */
  bvc_subpfx_alloc,

  /** Pointer to peerpfxorigins alloc function */
  bvc_peerpfxorigins_alloc,

  /** Pointer to pfx2as alloc function */
  bvc_pfx2as_alloc,

  /** Sample conditional consumer. If enabled, point to the alloc function,
      otherwise a NULL pointer to indicate the consumer is unavailable */
  /*
    #ifdef WITH_<NAME>
    bvc_<name>_alloc,
    #else
    NULL,
    #endif
  */

};

/* ==================== PRIVATE FUNCTIONS ==================== */

static bvc_t *consumer_alloc(timeseries_t *timeseries,
                             bvc_chain_state_t *chain_state, bvc_id_t id)
{
  bvc_t *consumer;
  assert(ARR_CNT(consumer_alloc_functions) == BVC_ID_LAST);

  if (consumer_alloc_functions[id - 1] == NULL) {
    return NULL;
  }

  /* first, create the struct */
  if ((consumer = malloc_zero(sizeof(bvc_t))) == NULL) {
    return NULL;
  }

  /* get the core consumer details (id, name, func ptrs) from the plugin */
  memcpy(consumer, consumer_alloc_functions[id - 1](), sizeof(bvc_t));

  consumer->timeseries = timeseries;

  consumer->chain_state = chain_state;

  return consumer;
}

static int consumer_init(bvc_t *consumer, int argc, char **argv)
{
  assert(consumer != NULL);

  /* if it has already been initialized, then we simply return */
  if (bvc_is_enabled(consumer)) {
    return 0;
  }

  /* otherwise, we need to init this plugin */

  /* ask the consumer to initialize. */
  if (consumer->init(consumer, argc, argv) != 0) {
    return -1;
  }

  consumer->enabled = 1;

  return 0;
}

static void consumer_destroy(bvc_t **consumer_p)
{
  assert(consumer_p != NULL);
  bvc_t *consumer = *consumer_p;
  *consumer_p = NULL;

  if (consumer == NULL) {
    return;
  }

  /* only free everything if we were enabled */
  if (bvc_is_enabled(consumer)) {
    /* ask the backend to free it's own state */
    consumer->destroy(consumer);
  }

  /* finally, free the actual backend structure */
  free(consumer);

  return;
}

static int init_bvc_chain_state(bgpview_consumer_manager_t *mgr)
{
  int i;
  strcpy(mgr->chain_state.metric_prefix, BGPVIEW_METRIC_PREFIX_DEFAULT);

  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    mgr->chain_state.full_feed_peer_ids[i] = bgpstream_id_set_create();
    mgr->chain_state.peer_ids_cnt[i] = 0;
    mgr->chain_state.full_feed_peer_asns_cnt[i] = 0;
    mgr->chain_state.usable_table_flag[i] = 0;
  }
  return 0;
}

static void destroy_bvc_chain_state(bgpview_consumer_manager_t *mgr)
{
  int i;
  for (i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; i++) {
    if (mgr->chain_state.full_feed_peer_ids[i] != NULL) {
      bgpstream_id_set_destroy(mgr->chain_state.full_feed_peer_ids[i]);
      mgr->chain_state.full_feed_peer_ids[i] = NULL;
    }
  }
}

/* ==================== PUBLIC MANAGER FUNCTIONS ==================== */

bgpview_consumer_manager_t *
bgpview_consumer_manager_create(timeseries_t *timeseries)
{
  bgpview_consumer_manager_t *mgr;
  int id;

  /* allocate some memory for our state */
  if ((mgr = malloc_zero(sizeof(bgpview_consumer_manager_t))) == NULL) {
    goto err;
  }

  mgr->timeseries = timeseries;

  if (init_bvc_chain_state(mgr) < 0) {
    goto err;
  }

  /* allocate the consumers (some may/will be NULL) */
  for (id = BVC_ID_FIRST; id <= BVC_ID_LAST; id++) {
    mgr->consumers[id - 1] = consumer_alloc(timeseries, &mgr->chain_state, id);
  }

  return mgr;
err:
  bgpview_consumer_manager_destroy(&mgr);
  return NULL;
}

void bgpview_consumer_manager_set_metric_prefix(bgpview_consumer_manager_t *mgr,
                                                char *metric_prefix)
{
  if (metric_prefix == NULL ||
      strlen(metric_prefix) >= BGPVIEW_METRIC_PREFIX_LEN) {
    return;
  }
  strcpy(mgr->chain_state.metric_prefix, metric_prefix);
}

void bgpview_consumer_manager_destroy(bgpview_consumer_manager_t **mgr_p)
{
  assert(mgr_p != NULL);
  bgpview_consumer_manager_t *mgr = *mgr_p;
  *mgr_p = NULL;
  int id;

  /* loop across all backends and free each one */
  for (id = BVC_ID_FIRST; id <= BVC_ID_LAST; id++) {
    consumer_destroy(&mgr->consumers[id - 1]);
  }

  destroy_bvc_chain_state(mgr);

  free(mgr);
  return;
}

int bgpview_consumer_manager_enable_consumer(bvc_t *consumer,
                                             const char *options)
{
  char *local_args = NULL;
  char *process_argv[MAXOPTS];
  int len;
  int process_argc = 0;
  int rc;

  fprintf(stderr, "INFO: Enabling consumer '%s'\n", consumer->name);

  /* first we need to parse the options */
  if (options != NULL && (len = strlen(options)) > 0) {
    local_args = strdup(options);
    parse_cmd(local_args, &process_argc, process_argv, MAXOPTS, consumer->name);
  } else {
    process_argv[process_argc++] = (char *)consumer->name;
  }

  /* we just need to pass this along to the consumer framework */
  rc = consumer_init(consumer, process_argc, process_argv);

  if (local_args != NULL) {
    free(local_args);
  }

  return rc;
}

bvc_t *bgpview_consumer_manager_enable_consumer_from_str(
  bgpview_consumer_manager_t *mgr, const char *cmd)
{
  char *cmdcpy = NULL;
  char *args = NULL;

  bvc_t *consumer;

  if ((cmdcpy = strdup(cmd)) == NULL) {
    goto err;
  }

  if ((args = strchr(cmdcpy, ' ')) != NULL) {
    /* set the space to a nul, which allows cmd to be used for the backend
       name, and then increment args ptr to point to the next character, which
       will be the start of the arg string (or at worst case, the terminating
       \0 */
    *args = '\0';
    args++;
  }

  if ((consumer = bgpview_consumer_manager_get_consumer_by_name(mgr, cmdcpy)) ==
      NULL) {
    fprintf(stderr, "ERROR: Invalid consumer name (%s)\n", cmdcpy);
    goto err;
  }

  if (bgpview_consumer_manager_enable_consumer(consumer, args) != 0) {
    fprintf(stderr, "ERROR: Failed to initialize consumer (%s)\n", cmd);
    goto err;
  }

  free(cmdcpy);

  return consumer;

err:
  if (cmdcpy != NULL) {
    free(cmdcpy);
  }
  return NULL;
}

bvc_t *
bgpview_consumer_manager_get_consumer_by_id(bgpview_consumer_manager_t *mgr,
                                            bvc_id_t id)
{
  assert(mgr != NULL);
  if (id < BVC_ID_FIRST || id > BVC_ID_LAST) {
    return NULL;
  }
  return mgr->consumers[id - 1];
}

bvc_t *
bgpview_consumer_manager_get_consumer_by_name(bgpview_consumer_manager_t *mgr,
                                              const char *name)
{
  bvc_t *consumer;
  int id;

  for (id = BVC_ID_FIRST; id <= BVC_ID_LAST; id++) {
    if ((consumer = bgpview_consumer_manager_get_consumer_by_id(mgr, id)) !=
          NULL &&
        strcasecmp(consumer->name, name) == 0) {
      return consumer;
    }
  }

  return NULL;
}

bvc_t **
bgpview_consumer_manager_get_all_consumers(bgpview_consumer_manager_t *mgr)
{
  return mgr->consumers;
}

int bgpview_consumer_manager_process_view(bgpview_consumer_manager_t *mgr,
                                          bgpview_t *view)
{
  int id;
  bvc_t *consumer;
  assert(mgr != NULL);

  for (id = BVC_ID_FIRST; id <= BVC_ID_LAST; id++) {
    if ((consumer = bgpview_consumer_manager_get_consumer_by_id(mgr, id)) ==
          NULL ||
        bvc_is_enabled(consumer) == 0) {
      continue;
    }
    if (consumer->process_view(consumer, view) != 0) {
      return -1;
    }
  }

  return 0;
}

/* ==================== CONSUMER ACCESSOR FUNCTIONS ==================== */

int bvc_is_enabled(bvc_t *consumer)
{
  return consumer->enabled;
}

bvc_id_t bvc_get_id(bvc_t *consumer)
{
  return consumer->id;
}

const char *bvc_get_name(bvc_t *consumer)
{
  return consumer->name;
}
