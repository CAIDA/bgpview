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

#ifndef __BGPVIEW_IO_ZMQ_STORE_H
#define __BGPVIEW_IO_ZMQ_STORE_H

#include "bgpstream_utils_pfx.h"
#include "bgpview.h"
#include "bgpview_io_zmq_server.h"

/** @file
 *
 * @brief Header file that exposes the protected interface of bgpview store.
 *
 * @author Alistair King
 *
 */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

typedef struct bgpview_io_zmq_store bgpview_io_zmq_store_t;

/** @} */

/** Create a new bgpview store instance
 *
 * @param server        pointer to the bgpview server instance
 * @param window_len    number of consecutive views in the store's window
 * @return a pointer to a bgpview store instance, or NULL if an error
 * occurred
 */
bgpview_io_zmq_store_t *
bgpview_io_zmq_store_create(bgpview_io_zmq_server_t *server, int window_len);

/** Destroy the given bgpview store instance
 *
 * @param store         pointer to the store instance to destroy
 */
void bgpview_io_zmq_store_destroy(bgpview_io_zmq_store_t *store);

/** Register a new bgpview client
 *
 * @param store         pointer to a store instance
 * @param name          string name of the client
 * @param intents       client intents
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_zmq_store_client_connect(
  bgpview_io_zmq_store_t *store, bgpview_io_zmq_server_client_info_t *client);

/** Deregister a bgpview client
 *
 * @param store         pointer to a store instance
 * @param name          string name of the client that disconnected
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_zmq_store_client_disconnect(
  bgpview_io_zmq_store_t *store, bgpview_io_zmq_server_client_info_t *client);

/** Retrieve a pointer to the view that represents the given time
 *
 * @param store         pointer to a store instance
 * @param time          time of the view to retrieve
 * @return borrowed pointer to a view if the given time is inside the current
 *         window, NULL if it is outside
 */
bgpview_t *bgpview_io_zmq_store_get_view(bgpview_io_zmq_store_t *store,
                                         uint32_t time);

/** Notify the store that a view it manages has been updated with new data
 *
 * @param store         pointer to a store instance
 * @param view          pointer to the view that has been updated
 * @param client        pointer to info about the client that sent the view
 * @return 0 if the view was processed successfully, -1 otherwise
 */
int bgpview_io_zmq_store_view_updated(
  bgpview_io_zmq_store_t *store, bgpview_t *view,
  bgpview_io_zmq_server_client_info_t *client);

/** Force a timeout check on the views currently in the store
 *
 * @param store         pointer to a store instance
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_zmq_store_check_timeouts(bgpview_io_zmq_store_t *store);

#endif /* __BGPSTORE_LIB_H */
