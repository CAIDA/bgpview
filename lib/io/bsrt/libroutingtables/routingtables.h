/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini, Ken Keys
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
#ifndef __ROUTINGTABLES_H
#define __ROUTINGTABLES_H

#include "bgpstream.h"
#include "bgpview.h"
#include "timeseries.h"

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

/** Opaque handle that represents a routingtables instance */
typedef struct struct_routingtables_t routingtables_t;

/** @} */

/**
 * @name Public API Functions
 *
 * @{ */

/** Create a new routingtables instance
 *
 * @param plugin_name   string representing the plugin name
 * @param timeseries    pointer to an initialized timeseries instance
 * @return a pointer to a routingtables instance if successful, NULL otherwise
 */
routingtables_t *routingtables_create(char *plugin_name,
                                      timeseries_t *timeseries);

/** Return a pointer to the view used internally in the
 *  routingtables code
 *
 * @param rt               pointer to a routingtables instance to update
 * @return a pointer to the internal bgpview
 */
bgpview_t *routingtables_get_view_ptr(routingtables_t *rt);

/** Set the metric prefix to be used for when outpting the time series
 *  variables at the end of the interval
 *
 * @param rt               pointer to a routingtables instance to update
 * @param metric_prefix    metric prefix string
 */
void routingtables_set_metric_prefix(routingtables_t *rt,
    const char *metric_prefix);

/** Get the metric prefix to be used for when outpting the time series
 *  variables at the end of the interval
 *
 * @param rt               pointer to a routingtables instance to update
 * @return the current metric prefix string, NULL if an error occurred.
 */
char *routingtables_get_metric_prefix(routingtables_t *rt);

/** turn off metric output */
void routingtables_turn_metric_output_off(routingtables_t *rt);

/** Receive the beginning of interval signal
 *
 * @param rt            pointer to a routingtables instance to update
 * @param start_time    start of the interval in epoch time (bgp time)
 * @return 0 if the signal was processed correctly, <0 if an error occurred.
 */
int routingtables_interval_start(routingtables_t *rt, int start_time);

/** Receive the end of interval signal (and trigger the output of
 *  statistics as well as the transmission of bgp views to the bgpview
 *  if the transmission is activated)
 *
 * @param rt            pointer to a routingtables instance to update
 * @param end_time      end of the interval in epoch time (bgp time)
 * @return 0 if the signal was processed correctly, <0 if an error occurred.
 */
int routingtables_interval_end(routingtables_t *rt, int end_time);

/** Process the bgpstream record, i.e. use the information contained in the
 *  bgpstream record to update the current routing tables
 *
 * @param rt            pointer to a routingtables instance to update
 * @param record        pointer to a bgpstream record instance
 * @return 0 if the record was processed correctly, <0 if an error occurred.
 */
int routingtables_process_record(routingtables_t *rt,
                                 bgpstream_record_t *record);

/** Destroy the given routingtables instance
 *
 * @param rt pointer to a routingtables instance to destroy
 */
void routingtables_destroy(routingtables_t *rt);

/** @} */

#endif /* __ROUTINGTABLES_H */
