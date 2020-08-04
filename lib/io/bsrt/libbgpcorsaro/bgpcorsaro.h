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

#ifndef __BGPCORSARO_H
#define __BGPCORSARO_H

#include "bgpstream.h"
#include "timeseries.h"
#include "wandio.h"

/** @file
 *
 * @brief Header file which exports the public libbgpcorsaro API
 *
 * @author Alistair King
 *
 */

/**
 * @name Opaque Data Structures
 *
 * @{ */

/** Opaque struct holding bgpcorsaro output state */
typedef struct bgpcorsaro bgpcorsaro_t;
/** Opaque bgpcorsaro record wrapper */
typedef struct bgpcorsaro_record bgpcorsaro_record_t;
/** Struct holding bgpcorsaro record state */
typedef struct bgpcorsaro_record_state bgpcorsaro_record_state_t;
/** Opaque struct representing the start or end of an interval */
typedef struct bgpcorsaro_interval bgpcorsaro_interval_t;

/** @} */

/**
 * @name Enumerations
 *
 * @{ */

/** Settings for interval alignment */
#define BGPCORSARO_INTERVAL_ALIGN_DEFAULT 0

/** @} */

/**
 * @name Bgpcorsaro output API functions
 *
 * These functions are used to generate bgpcorsaro output from libbgpstream
 * records
 *
 * The basic process for using bgpcorsaro to generate output is:
 * -# init bgpcorsaro using bgpcorsaro_alloc_output
 * -# optionally call bgpcorsaro_set_interval to set the interval time
 * -# call bgpcorsaro_start_output to initialize the plugins (and create the
 * files)
 * -# call bgpcorsaro_per_record with each record to be processed
 * -# call bgpcorsaro_finalize when all records have been processed
 *
 * If an API function returns an error condition, it is your responsibility to
 * call bgpcorsaro_finalize to clean up any resources bgpcorsaro is using.  This
 * is so you can decide if halting execution is really what you want to do. For
 * example, if a record fails to process, you may decide to log it and attempt
 * to continue with the next record. Beware that this could get bgpcorsaro into
 * an unstable state if the error arose from something like malloc failing.
 *
 * @{ */

/** Allocate an bgpcorsaro object
 *
 * @param template     The string used to generate output files
 * @param timeseries    pointer to an initialized timeseries instance
 * @return a pointer to an opaque bgpcorsaro structure, or NULL if an error
 * occurs
 *
 * The template must contain a pattern to be replaced with the plugin
 * names (%P).
 *
 * The returned object can then be used to set options (bgpcorsaro_set_*) before
 * calling bgpcorsaro_start_output to write headers to the output files ready
 * to process records.
 */
bgpcorsaro_t *bgpcorsaro_alloc_output(char *template, timeseries_t *timeseries);

/** Initialize an bgpcorsaro object that has already been allocated
 *
 * @param bgpcorsaro       The bgpcorsaro object to start
 * @return 0 if bgpcorsaro is started successfully, -1 if an error occurs
 *
 * It is only when this function is called that the plugins will
 * initialize any state (open files etc).
 */
int bgpcorsaro_start_output(bgpcorsaro_t *bgpcorsaro);

/** Accessor function to enable/disable the alignment of the initial interval
 *
 * @param bgpcorsaro      The bgpcorsaro object to set the interval for
 * @param flag            Enable or disable the alignment of interval end times
 *
 * The end time of the first interval will be rounded down to the nearest
 * integer multiple of the interval length. Interval rounding makes the most
 * sense when the interval length is evenly divisible into 1 hour.
 * The default is no interval alignment.
 */
void bgpcorsaro_set_interval_alignment_flag(
  bgpcorsaro_t *bgpcorsaro, int flag);

/** Accessor function to set the interval length
 *
 * @param bgpcorsaro    The bgpcorsaro object to set the interval for
 * @param interval      The interval (in seconds)
 *
 * If this function is not called, the default interval,
 * BGPCORSARO_INTERVAL_DEFAULT, will be used.
 */
void bgpcorsaro_set_interval(bgpcorsaro_t *bgpcorsaro, unsigned int interval);

/** Accessor function to set the rotation frequency of output files
 *
 * @param bgpcorsaro    The bgpcorsaro object to set rotation for
 * @param intervals     The number of intervals after which the output files
 *                      will be rotated
 *
 * If this is set to > 0, all output files will be rotated at the end of
 * n intervals. The default is 0 (no rotation).
 */
void bgpcorsaro_set_output_rotation(bgpcorsaro_t *bgpcorsaro, int intervals);

/** Accessor function to set the rotation frequency of meta output files
 *
 * @param bgpcorsaro    The bgpcorsaro object to set the rotation for
 * @param intervals     The number of intervals after which the output files
 *                      will be rotated
 *
 * If this is set to > 0, bgpcorsaro meta output files (log) will be
 * rotated at the end of n intervals. The default is to follow the output
 * rotation interval specified by bgpcorsaro_set_output_rotation.
 */
void bgpcorsaro_set_meta_output_rotation(bgpcorsaro_t *bgpcorsaro,
                                         int intervals);

/** Convenience function to determine if the output files should be rotated
 *
 * @param bgpcorsaro     The bgpcorsaro object to check the rotation status of
 * @return 1 if output files should be rotated at the end of the current
 *         interval, 0 if not
 */
int bgpcorsaro_is_rotate_interval(bgpcorsaro_t *bgpcorsaro);

/** Accessor function to set the bgpstream pointer
 *
 * @param bgpcorsaro    The bgpcorsaro object to set the trace uri for
 * @param stream        A libbgpstream pointer for the current stream
 * @return 0 if the stream was successfully set, -1 if an error occurs
 */
int bgpcorsaro_set_stream(bgpcorsaro_t *bgpcorsaro, bgpstream_t *stream);

/** Accessor function to disable logging to a file
 *
 * @param bgpcorsaro    The bgpcorsaro to disable logging to a file for
 *
 * This function may be called at any time, but if a log file is already created
 * then it will continue to be used until a rotation interval is
 * encountered. Normally it should be called before calling
 * bgpcorsaro_start_output
 */
void bgpcorsaro_disable_logfile(bgpcorsaro_t *bgpcorsaro);

/** Accessor function to set the monitor name
 *
 * @param bgpcorsaro    The bgpcorsaro object to set the monitor name for
 * @param name          The string to set as the monitor name
 * @return 0 if the name was successfully set, -1 if an error occurs
 *
 * If it is not set, the value of gethostname() is used.
 */
int bgpcorsaro_set_monitorname(bgpcorsaro_t *bgpcorsaro, const char *name);

/** Accessor function to get the monitor name string
 *
 * @param bgpcorsaro    The bgpcorsaro object to set the monitor name for
 * @return a pointer to the monitor name string
 *
 */
const char *bgpcorsaro_get_monitorname(bgpcorsaro_t *bgpcorsaro);

/** Perform bgpcorsaro processing on a given bgpstream record
 *
 * @param bgpcorsaro    The bgpcorsaro object used to process the record
 * @param record        The bgpstream record to process
 * @return 0 if the record was successfully processed, -1 if an error occurs
 *
 * For each record, bgpcorsaro will determine whether it falls within the
 * current interval, if not, it will write out data for the previous interval.
 * The record is then handed to each plugin which processes it and updates
 * internal state.
 */
int bgpcorsaro_per_record(bgpcorsaro_t *bgpcorsaro, bgpstream_record_t *record);

/** Perform bgpcorsaro processing up to the end of the interval
 *
 * @param bgpcorsaro    The bgpcorsaro object used to process the record
 * @return 1 if an interval ended, 0 if EOF was reached, -1 if an error occurs
 *
 * ...
 */
int bgpcorsaro_process_interval(bgpcorsaro_t *bgpcorsaro);

/** Write the final interval and free resources allocated by bgpcorsaro
 *
 * @param bgpcorsaro    The bgpcorsaro object to finalize
 * @return 0 if bgpcorsaro finished properly, -1 if an error occurs.
 */
int bgpcorsaro_finalize_output(bgpcorsaro_t *bgpcorsaro);

/** @} */

#endif /* __BGPCORSARO_H */
