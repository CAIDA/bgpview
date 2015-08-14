/*
 * This file is part of bgpstream
 *
 * Copyright (C) 2015 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
 *
 * All rights reserved.
 *
 * This code has been developed by CAIDA at UC San Diego.
 * For more information, contact bgpstream-info@caida.org
 *
 * This source code is proprietary to the CAIDA group at UC San Diego and may
 * not be redistributed, published or disclosed without prior permission from
 * CAIDA.
 *
 * Report any bugs, questions or comments to bgpstream-info@caida.org
 *
 */

#ifndef __BGPVIEW_IO_STORE_H
#define __BGPVIEW_IO_STORE_H

#include "bgpview_io_server.h"
#include "bgpview.h"
#include "bgpstream_utils_pfx.h"

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

typedef struct bgpview_io_store bgpview_io_store_t;

/** @} */

/** Create a new bgpview store instance
 *
 * @param server        pointer to the bgpview server instance
 * @param window_len    number of consecutive views in the store's window
 * @return a pointer to a bgpview store instance, or NULL if an error
 * occurred
 */
bgpview_io_store_t *bgpview_io_store_create(bgpview_io_server_t *server,
					    int window_len);

/** Destroy the given bgpview store instance
 *
 * @param store         pointer to the store instance to destroy
 */
void bgpview_io_store_destroy(bgpview_io_store_t *store);

/** Register a new bgpview client
 *
 * @param store         pointer to a store instance
 * @param name          string name of the client
 * @param interests     client interests
 * @param intents       client intents
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_store_client_connect(bgpview_io_store_t *store,
                                    bgpview_io_server_client_info_t *client);

/** Deregister a bgpview client
 *
 * @param store         pointer to a store instance
 * @param name          string name of the client that disconnected
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_store_client_disconnect(bgpview_io_store_t *store,
                                       bgpview_io_server_client_info_t *client);

/** Retrieve a pointer to the view that represents the given time
 *
 * @param store         pointer to a store instance
 * @param time          time of the view to retrieve
 * @return borrowed pointer to a view if the given time is inside the current
 *         window, NULL if it is outside
 */
bgpview_t *bgpview_io_store_get_view(bgpview_io_store_t *store,
                                             uint32_t time);

/** Notify the store that a view it manages has been updated with new data
 *
 * @param store         pointer to a store instance
 * @param view          pointer to the view that has been updated
 * @param client        pointer to info about the client that sent the view
 * @return 0 if the view was processed successfully, -1 otherwise
 */
int bgpview_io_store_view_updated(bgpview_io_store_t *store,
                                  bgpview_t *view,
                                  bgpview_io_server_client_info_t *client);

/** Force a timeout check on the views currently in the store
 *
 * @param store         pointer to a store instance
 * @return 0 if successful, -1 otherwise
 */
int bgpview_io_store_check_timeouts(bgpview_io_store_t *store);

#endif /* __BGPSTORE_LIB_H */
