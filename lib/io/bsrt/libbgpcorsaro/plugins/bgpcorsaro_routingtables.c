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
#include "bgpcorsaro_int.h"
#include "config.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "bgpstream.h"

#include "utils.h"

#include "bgpcorsaro_io.h"
#include "bgpcorsaro_log.h"
#include "bgpcorsaro_plugin.h"

#include "bgpcorsaro_routingtables.h"
#include "routingtables.h"

/** @file
 *
 * @brief Bgpcorsaro RoutingTables plugin implementation
 *
 * @author Chiara Orsini, Ken Keys
 *
 */

/** The number of output file pointers to support non-blocking close at the end
    of an interval. If the wandio buffers are large enough that it takes more
    than 1 interval to drain the buffers, consider increasing this number */
#define OUTFILE_POINTERS 2

/** Common plugin information across all instances */
bgpcorsaro_plugin_t bgpcorsaro_routingtables_plugin;

/** Holds the state for an instance of this plugin */
struct bgpcorsaro_routingtables_state_t {

  /** The outfile for the plugin */
  iow_t *outfile;

  /** A set of pointers to outfiles to support non-blocking close */
  iow_t *outfile_p[OUTFILE_POINTERS];

  /** The current outfile */
  int outfile_n;

  /* plugin custom variables */

  /** routing tables instance */
  routingtables_t *routing_tables;

  /** decides whether metrics should be
   *  printed or not */
  uint8_t metrics_output_on;

  /** prefix used for outputed metrics */
  char *metric_prefix;
};

static struct bgpcorsaro_routingtables_state_t *bgpcorsaro_routingtables_state = NULL;

/** Extends the generic plugin state convenience macro in bgpcorsaro_plugin.h */
#define STATE(bgpcorsaro)                                                      \
  (bgpcorsaro_routingtables_state)

/** Extends the generic plugin plugin convenience macro in bgpcorsaro_plugin.h
 */
#define PLUGIN(bgpcorsaro)                                                     \
  (&bgpcorsaro_routingtables_plugin)

/* == PUBLIC PLUGIN FUNCS BELOW HERE == */

/** Implements the init_output function of the plugin API */
int bgpcorsaro_routingtables_init_output(bgpcorsaro_t *bgpcorsaro)
{
  struct bgpcorsaro_routingtables_state_t *state;
  bgpcorsaro_plugin_t *plugin = PLUGIN(bgpcorsaro);
  assert(plugin != NULL);

  if ((state = malloc_zero(sizeof(struct bgpcorsaro_routingtables_state_t))) ==
      NULL) {
    bgpcorsaro_log(__func__, bgpcorsaro,
                   "could not malloc bgpcorsaro_routingtables_state_t");
    goto err;
  }

  /** initialize plugin custom variables */
  if ((state->routing_tables = routingtables_create(PLUGIN_NAME,
      bgpcorsaro->timeseries)) == NULL) {
    bgpcorsaro_log(__func__, bgpcorsaro,
                   "could not create routingtables in routingtables plugin");
    goto err;
  }
  state->metric_prefix = NULL;
  state->metrics_output_on = 1; // default: on

  bgpcorsaro_routingtables_state = state;

  // update state with parsed args
  if (state->metric_prefix != NULL) {
    routingtables_set_metric_prefix(state->routing_tables,
                                    state->metric_prefix);
  }

  if (!state->metrics_output_on) {
    routingtables_turn_metric_output_off(state->routing_tables);
  }

  bgpcorsaro->shared_view = routingtables_get_view_ptr(state->routing_tables);

  /* defer opening the output file until we start the first interval */

  return 0;

err:
  bgpcorsaro_routingtables_close_output(bgpcorsaro);
  return -1;
}

/** Implements the close_output function of the plugin API */
int bgpcorsaro_routingtables_close_output(bgpcorsaro_t *bgpcorsaro)
{
  int i;
  struct bgpcorsaro_routingtables_state_t *state = STATE(bgpcorsaro);

  if (state != NULL) {
    /* close all the outfile pointers */
    for (i = 0; i < OUTFILE_POINTERS; i++) {
      if (state->outfile_p[i] != NULL) {
        wandio_wdestroy(state->outfile_p[i]);
        state->outfile_p[i] = NULL;
      }
    }
    state->outfile = NULL;

    /** destroy plugin custom variables */
    routingtables_destroy(state->routing_tables);
    state->routing_tables = NULL;
    if (state->metric_prefix != NULL) {
      free(state->metric_prefix);
    }
    state->metric_prefix = NULL;

    free(state);
    bgpcorsaro_routingtables_state = NULL;
  }
  return 0;
}

/** Implements the start_interval function of the plugin API */
int bgpcorsaro_routingtables_start_interval(bgpcorsaro_t *bgpcorsaro,
                                            bgpcorsaro_interval_t *int_start)
{
  struct bgpcorsaro_routingtables_state_t *state = STATE(bgpcorsaro);

  if (state->outfile == NULL) {
    if ((state->outfile_p[state->outfile_n] = bgpcorsaro_io_prepare_file(
           bgpcorsaro, PLUGIN_NAME, int_start)) == NULL) {
      bgpcorsaro_log(__func__, bgpcorsaro, "could not open %s output file",
                     PLUGIN_NAME);
      return -1;
    }
    state->outfile = state->outfile_p[state->outfile_n];
  }

  /** plugin interval start operations */
  if (routingtables_interval_start(state->routing_tables, int_start->time) <
      0) {
    // an error occurred during the interval_end operations
    bgpcorsaro_log(__func__, bgpcorsaro,
                   "could not start interval for %s plugin",
                   PLUGIN_NAME);
    return -1;
  }

  bgpcorsaro_io_write_interval_start(bgpcorsaro, state->outfile, int_start);

  return 0;
}

/** Implements the end_interval function of the plugin API */
int bgpcorsaro_routingtables_end_interval(bgpcorsaro_t *bgpcorsaro,
                                          bgpcorsaro_interval_t *int_end)
{
  struct bgpcorsaro_routingtables_state_t *state = STATE(bgpcorsaro);

  bgpcorsaro_log(__func__, bgpcorsaro, "Dumping stats for interval %d",
                 int_end->number);

  /** plugin end of interval operations */
  if (routingtables_interval_end(state->routing_tables, int_end->time) < 0) {
    // an error occurred during the interval_end operations
    bgpcorsaro_log(__func__, bgpcorsaro, "could not end interval for %s plugin",
                   PLUGIN_NAME);
    return -1;
  }

  bgpcorsaro_io_write_interval_end(bgpcorsaro, state->outfile, int_end);

  /* if we are rotating, now is when we should do it */
  if (bgpcorsaro_is_rotate_interval(bgpcorsaro)) {
    /* leave the current file to finish draining buffers */
    assert(state->outfile != NULL);

    /* move on to the next output pointer */
    state->outfile_n = (state->outfile_n + 1) % OUTFILE_POINTERS;

    if (state->outfile_p[state->outfile_n] != NULL) {
      /* we're gonna have to wait for this to close */
      wandio_wdestroy(state->outfile_p[state->outfile_n]);
      state->outfile_p[state->outfile_n] = NULL;
    }

    state->outfile = NULL;
  }

  return 0;
}

/** Implements the process_record function of the plugin API */
int bgpcorsaro_routingtables_process_record(bgpcorsaro_t *bgpcorsaro,
                                            bgpstream_record_t *bs_record)
{
  struct bgpcorsaro_routingtables_state_t *state = STATE(bgpcorsaro);

  bgpcorsaro->shared_view = routingtables_get_view_ptr(state->routing_tables);

  return routingtables_process_record(state->routing_tables, bs_record);
}
