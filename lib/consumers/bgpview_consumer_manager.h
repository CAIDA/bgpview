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

#ifndef __BGPVIEW_CONSUMER_MANAGER_H
#define __BGPVIEW_CONSUMER_MANAGER_H

#include "bgpstream_utils_id_set.h"
#include "bgpview.h"
#include <stdint.h>
#include <stdlib.h>
#include <timeseries.h>

/** @file
 *
 * @brief Header file that exposes the public interface of the bgpview
 * consumer manager
 *
 * @author Alistair King
 *
 */

/** Maximum length of the metric prefix string */
#define BGPVIEW_METRIC_PREFIX_LEN 1024

/** Default value of the metric prefix string */
#define BGPVIEW_METRIC_PREFIX_DEFAULT "bgp"

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

/** Opaque struct holding state for the bgpview consumer manager */
typedef struct bgpview_consumer_manager bgpview_consumer_manager_t;

/** Opaque struct holding state for a bgpview consumer */
typedef struct bvc bvc_t;

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

/** Per-view state that allows consumers to make use of the results of
 * processing carried out by previous consumers.
 *
 * When implementing a consumer that wants to share it's state with other
 * consumers (e.g. a consumer that determines the list of full-feed peers), you
 * should add a variable to this structure.
 */
typedef struct bvc_chain_state {

  /* Common metric prefix */
  char metric_prefix[BGPVIEW_METRIC_PREFIX_LEN];

  /* Visibility state */

  /** Has the visibility consumer run? */
  int visibility_computed;

  /** Total number of peers in the view */
  uint32_t peer_ids_cnt[BGPSTREAM_MAX_IP_VERSION_IDX];

  /* Set of full feed peers */
  bgpstream_id_set_t *full_feed_peer_ids[BGPSTREAM_MAX_IP_VERSION_IDX];

  /** Total number of full feed peer ASns in the view */
  uint32_t full_feed_peer_asns_cnt[BGPSTREAM_MAX_IP_VERSION_IDX];

  /** Is the table usable? I.e. has enough full-feed peers */
  int usable_table_flag[BGPSTREAM_MAX_IP_VERSION_IDX];

  /** @todo the next variables will be replaced with percentages */

  /** What is the minimum number of peers before a pfx is considered visible */
  int pfx_vis_peers_threshold;

  /** What is the minimum mask length for a prefix to be considered visible */
  int pfx_vis_mask_len_threshold;

} bvc_chain_state_t;

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** A unique identifier for each bgpview consumer that bgpview supports
 */
typedef enum bvc_id {
  /** Dumps debugging information about the views to stdout */
  BVC_ID_TEST = 1,

  BVC_ID_PERFMONITOR = 2,

  /** Computes visibility information for each view (used by per-as and
      per-geo consumers) */
  BVC_ID_VISIBILITY = 3,

  /** Writes information about per-AS visibility information to Charthouse */
  BVC_ID_PERASVISIBILITY = 4,

  /** Writes information about per-Geo visibility information to Charthouse */
  BVC_ID_PERGEOVISIBILITY = 5,

  /** Writes information about prefixes that have been visible in a given time
   *  window */
  BVC_ID_ANNOUNCEDPFXS = 6,

  /** Writes information about prefixes that have been announced by multiple
   *  origin ASns, i.e. MOAS */
  BVC_ID_MOAS = 7,

  /** Writes views to files */
  BVC_ID_ARCHIVER = 8,

  /** Writes information about prefixes whose more specific prefixes are
      announced by different ASNs */
  BVC_ID_SUBMOAS = 9,

  /** Writes information about prefixes that are reachable through new
      edges */
  BVC_ID_EDGES = 10,

  /** Writes information about prefixes that are reachable through triplets */
  BVC_ID_TRIPLETS = 11,

  /** Outputs the current origin ASns observed for each routed prefix */
  BVC_ID_PFXORIGINS = 12,

  /** Generates information about routed prefixes in a given time window  */
  BVC_ID_ROUTEDSPACE = 13,

  /** Sends views to Kafka or ZMQ-Server */
  BVC_ID_VIEWSENDER = 14,

  /** It can be used as a template, it writes generic information about the
   *  number of view processed and the number of elements in the current
   *  view */
  BVC_ID_MYVIEWPROCESS = 15,

  /** Extracts information about path changes between successive views */
  BVC_ID_PATHCHANGE = 16,

  /** Identifies prefixes that are nested in other prefixes. Replaces the
      "submoas" consumer and adds the "defcon" consumer features */
  BVC_ID_SUBPFX = 17,

  /** @todo add more consumers here */

  /** Lowest numbered bgpview consumer ID */
  BVC_ID_FIRST = BVC_ID_TEST,

  /** Highest numbered bgpview consumer ID */
  BVC_ID_LAST = BVC_ID_SUBPFX,

} bvc_id_t;

/** @} */

/** Create a new consumer manager instance
 *
 * @param timeseries    pointer to an initialized timeseries instance
 * @param metric_prefix pointer to an initialized string containing the
 *                      metric prefix common among all consumers
 *
 * @return the consumer manager instance created, NULL if an error occurs
 */
bgpview_consumer_manager_t *
bgpview_consumer_manager_create(timeseries_t *timeseries);

/** Set the metric prefix to prepend to all consumers' output
 *
 * @param  mgr            pointer to consumer manager instance
 * @param  metric_prefix  metric prefix string
 */
void bgpview_consumer_manager_set_metric_prefix(bgpview_consumer_manager_t *mgr,
                                                char *metric_prefix);

/** Free a consumer manager instance
 *
 * @param  mgr_p        Double-pointer to consumer manager instance to free
 */
void bgpview_consumer_manager_destroy(bgpview_consumer_manager_t **mgr_p);

/** Enable the given consumer unless it is already enabled
 *
 * @param consumer      Pointer to the consumer to be enabled
 * @param options       A string of options to configure the consumer
 * @return 0 if the consumer was initialized, -1 if an error occurred
 *
 * Once bgpview_consumer_manager_create is called,
 * bgpview_consumer_manager_enable_consumer should be called once for each
 * consumer that is to be used.
 *
 * To obtain a pointer to a consumer, use the
 * bgpview_consumer_manager_get_consumer_by_name or
 * bgpview_consumer_manager_get_consumer_by_id functions. To enumerate a list
 * of available consumers, the bgpview_consumer_manager_get_all_consumers
 * function can be used to get a list of all consumers and then bvc_get_name can
 * be used on each to get their name.
 *
 * If configuring a plugin from command line arguments, the helper function
 * bgpview_consumer_manager_enable_consumer_from_str can be used which takes a
 * single string where the first token (space-separated) is the name of the
 * consumer, and the remainder of the string is taken to be the options.
 */
int bgpview_consumer_manager_enable_consumer(bvc_t *consumer,
                                             const char *options);

/** Attempt to enable a consumer based on the given command string
 *
 * @param mgr           The manager object to enable the consumer for
 * @param cmd           The command string to parse for consumer name and
 * options
 * @return an enabled consumer if successful, NULL otherwise
 *
 * The `cmd` string is separated at the first space. The first token is taken to
 * be the consumer name, and the remainder is taken to be the options. For
 * example, the command: `test -a all` will attempt to enable the `test`
 * consumer and will pass `-a all` as options.
 */
bvc_t *bgpview_consumer_manager_enable_consumer_from_str(
  bgpview_consumer_manager_t *mgr, const char *cmd);

/** Retrieve the consumer object for the given consumer ID
 *
 * @param mgr           The manager object to retrieve the consumer object from
 * @param id            The ID of the consumer to retrieve
 * @return the consumer object for the given ID, NULL if there are no matches
 */
bvc_t *
bgpview_consumer_manager_get_consumer_by_id(bgpview_consumer_manager_t *mgr,
                                            bvc_id_t id);

/** Retrieve the consumer object for the given consumer name
 *
 * @param mgr           Manager object to retrieve the consumer from
 * @param name          The consumer name to retrieve
 * @return the consumer object for the given name, NULL if there are no matches
 */
bvc_t *
bgpview_consumer_manager_get_consumer_by_name(bgpview_consumer_manager_t *mgr,
                                              const char *name);

/** Get an array of available consumers
 *
 * @param mgr           The manager object to get all the consumers for
 * @return an array of consumer objects
 *
 * @note the number of elements in the array will be exactly BVC_ID_LAST.
 *
 * @note not all consumers in the list may be present (i.e. there may be NULL
 * pointers), or some may not be enabled. use bvc_is_enabled to check.
 */
bvc_t **
bgpview_consumer_manager_get_all_consumers(bgpview_consumer_manager_t *mgr);

/** Process the given view using each enabled consumer
 *
 * @param mgr           The manager object
 * @param view          Borrowed reference to the BGPView to process
 * @param return 0 if the view was processed successfully, -1 otherwise
 */
int bgpview_consumer_manager_process_view(bgpview_consumer_manager_t *mgr,
                                          bgpview_t *view);

/** Check if the given consumer is enabled already
 *
 * @param consumer       The consumer to check the status of
 * @return 1 if the consumer is enabled, 0 otherwise
 */
int bvc_is_enabled(bvc_t *consumer);

/** Get the ID for the given consumer
 *
 * @param consumer      The consumer object to retrieve the ID from
 * @return the ID of the given consumer
 */
bvc_id_t bvc_get_id(bvc_t *consumer);

/** Get the consumer name for the given ID
 *
 * @param id            The consumer ID to retrieve the name for
 * @return the name of the consumer, NULL if an invalid consumer was provided
 */
const char *bvc_get_name(bvc_t *consumer);

#endif /* __BGPVIEW_CONSUMER_H */
