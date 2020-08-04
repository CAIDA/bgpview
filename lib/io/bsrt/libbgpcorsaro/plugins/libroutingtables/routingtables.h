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
