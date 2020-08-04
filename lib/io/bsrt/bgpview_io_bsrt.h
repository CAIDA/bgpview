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

#ifndef __BGPVIEW_IO_BSRT_H
#define __BGPVIEW_IO_BSRT_H

#include "bgpview.h"
#include "bgpview_io.h"
#include "timeseries.h"

/** @file
 *
 * @brief Header file that exposes the public interface of the bgpview BSRT
 * client
 *
 * @author Ken Keys
 *
 */

/**
 * @name Public Constants
 *
 * @{ */


/** @} */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

/** Opaque structure representing a BGPView BSRT IO instance */
typedef struct bgpview_io_bsrt bgpview_io_bsrt_t;

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** @} */

/** Initialize a new BGPView BSRT IO client
 *
 * @param opts          string containing options to be parsed with getopt
 * @param timeseries    pointer to shared timeseries
 * @return a pointer to a BGPView BSRT IO instance if successful, NULL if an
 * error occurred.
 */
bgpview_io_bsrt_t *bgpview_io_bsrt_init(const char *opts, timeseries_t *timeseries);

/** Destroy the given BGPView BSRT IO client
 *
 * @param client       pointer to the bgpview BSRT client instance to free
 */
void bgpview_io_bsrt_destroy(bgpview_io_bsrt_t *client);

/** Start the given bgpview BSRT client
 *
 * @param client       pointer to a BSRT client instance to start
 * @return 0 if the client started successfully, -1 otherwise.
 */
int bgpview_io_bsrt_start(bgpview_io_bsrt_t *client);

/** Attempt to receive a BGP View from BSRT
 *
 * @param client        pointer to the client instance to receive from
 * @return 0 or -1 if an error occurred.
 *
 * The view provided to this function must have been returned by
 * bgpview_io_bsrt_get_view_ptr().
 */
int bgpview_io_bsrt_recv_view(bgpview_io_bsrt_t *client);

/** Return a pointer to the view */
bgpview_t *bgpview_io_bsrt_get_view_ptr(bgpview_io_bsrt_t *client);

#endif /* __BGPVIEW_IO_BSRT_H */
