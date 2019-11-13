/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2019 The Regents of the University of California.
 * Authors: Ken Keys
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
 * @param view          pointer to the view to fill
 * @param peer_cb       callback function to use to filter peer entries
 *                      (may be NULL)
 * @param pfx_cb        callback function to use to filter prefix entries
 *                      (may be NULL)
 * @param pfx_peer_cb   callback function to use to filter prefix-peer entries
 *                      (may be NULL)
 * @return 0 or -1 if an error occurred.
 *
 * The view provided to this function must have been returned by
 * bgpview_io_bsrt_get_view_ptr().
 */
int bgpview_io_bsrt_recv_view(bgpview_io_bsrt_t *client, bgpview_t *view,
                              bgpview_io_filter_peer_cb_t *peer_cb,
                              bgpview_io_filter_pfx_cb_t *pfx_cb,
                              bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

/** Return a pointer to the view */
bgpview_t *bgpview_io_bsrt_get_view_ptr(bgpview_io_bsrt_t *client);

#endif /* __BGPVIEW_IO_BSRT_H */
