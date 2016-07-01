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


#ifndef __BGPVIEW_CONSUMER_INTERFACE_H
#define __BGPVIEW_CONSUMER_INTERFACE_H

#include <inttypes.h>
#include "bgpview.h"
#include "bgpview_consumer_manager.h" /* for bvc_t */

/** @file
 *
 * @brief Header file that exposes the protected interface of the bgpview consumer API
 *
 * @author Alistair King
 *
 */

/** Convenience macro to allow consumer implementations to retrieve their state
 *  object
 */
#define BVC_GET_STATE(consumer, type)		\
  ((bvc_##type##_state_t*)(consumer)->state)

/** Convenience macro to allow consumer implementations to store a state
    pointer */
#define BVC_SET_STATE(consumer, ptr)		\
  do {						\
    (consumer)->state = ptr;			\
  } while(0)

#define BVC_GET_TIMESERIES(consumer)		\
  ((consumer)->timeseries)

#define BVC_GET_CHAIN_STATE(consumer)		\
  ((consumer)->chain_state)

/** Convenience macro that defines all the function prototypes for the timeseries
 * consumer API
 */
#define BVC_GENERATE_PROTOS(consname)					\
  bvc_t * bvc_##consname##_alloc();					\
  int bvc_##consname##_init(bvc_t *ds, int argc, char **argv);		\
  void bvc_##consname##_destroy(bvc_t *ds);				\
  int bvc_##consname##_process_view(bvc_t *ds, bgpview_t *view);

/** Convenience macro that defines all the function pointers for the timeseries
 * consumer API
 */
#define BVC_GENERATE_PTRS(consname)	\
  bvc_##consname##_init,		\
    bvc_##consname##_destroy,		\
    bvc_##consname##_process_view,	\
    0, NULL

/** Structure which represents a metadata consumer */
struct bvc
{
  /**
   * @name Consumer information fields
   *
   * These fields are always filled, even if a consumer is not enabled.
   *
   * @{ */

  /** The ID of the consumer */
  bvc_id_t id;

  /** The name of the consumer */
  const char *name;

  /** }@ */

  /**
   * @name Consumer function pointers
   *
   * These pointers are always filled, even if a consumer is not enabled.
   * Until the consumer is enabled, only the init function can be called.
   *
   * @{ */

  /** Initialize and enable this consumer
   *
   * @param consumer    The consumer object to allocate
   * @param argc        The number of tokens in argv
   * @param argv        An array of strings parsed from the command line
   * @return 0 if the consumer is successfully initialized, -1 otherwise
   *
   * @note the most common reason for returning -1 will likely be incorrect
   * command line arguments.
   *
   * @warning the strings contained in argv will be free'd once this function
   * returns. Ensure you make appropriate copies as needed.
   */
  int (*init)(struct bvc *consumer, int argc, char **argv);

  /** Shutdown and free consumer-specific state for this consumer
   *
   * @param consumer    The consumer object to free
   *
   * @note consumers should *only* free consumer-specific state. All other state
   * will be free'd for them by the consumer manager.
   */
  void (*destroy)(struct bvc *consumer);

  /** Process a new BGPView table
   *
   * @param consumer    The consumer object
   * @param view        The view to process
   * @return 0 if the view was processed successfully, -1 otherwise.
   *
   * This is the core of the consumer API
   */
  int (*process_view)(struct bvc *consumer, bgpview_t *view);

  /** }@ */

  /**
   * @name Consumer state fields
   *
   * These fields are only set if the consumer is enabled (and initialized)
   * @note These fields should *not* be directly manipulated by
   * consumers. Instead they should use accessor functions provided by the
   * consumer manager.
   *
   * @{ */

  int enabled;

  /** An opaque pointer to consumer-specific state if needed by the consumer */
  void *state;

  /** A borrowed pointer to a configured and operational timeseries instance */
  timeseries_t *timeseries;

  /** A borrowed pointer to the shared consumer state object */
  bvc_chain_state_t *chain_state;

  /** }@ */
};

#endif /* __BGPVIEW_CONSUMER_INT_H */
