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

#ifndef __BGPVIEW_IO_H
#define __BGPVIEW_IO_H

#include "bgpview.h"

/** Possible entry types that can be passed to the filter callback */
typedef enum {

  /** The iterator refers to a peer */
  BGPVIEW_IO_FILTER_PEER = 0,

  /** The iterator refers to a prefix */
  BGPVIEW_IO_FILTER_PFX = 1,

  /** The iterator refers to a prefix-peer */
  BGPVIEW_IO_FILTER_PFX_PEER = 2,

} bgpview_io_filter_type_t;

/** Callback for filtering entries in a view when sending from
 * bgpview_io_client.
 *
 * @param iter          iterator to check
 * @param type          enum indicating the type of entry to filter
 * @return 1 to include the entry, 0 to exclude the entry, and -1 if an error
 * occured.
 *
 * @note This callback will be called for every prefix/peer combination, so it
 * should be efficient at determining if an entry is to be included.
 */
typedef int (bgpview_io_filter_cb_t)(bgpview_iter_t *iter,
                                     bgpview_io_filter_type_t type);

/** Callback for filtering peers when reading or receiving a view
 *
 * @param peersig       pointer to the signature of the peer
 * @return 1 to include the entry, 0 to exclude the entry, and -1 if an error
 * occurred.
 */
typedef int (bgpview_io_filter_peer_cb_t)(bgpstream_peer_sig_t *peersig);

/** Callback for filtering prefixes when reading or receiving a view
 *
 * @param pfx           pointer to the prefix
 * @return 1 to include the entry, 0 to exclude the entry, and -1 if an error
 * occurred.
 */
typedef int (bgpview_io_filter_pfx_cb_t)(bgpstream_pfx_t *pfx);

/** Callback for filtering prefix-peers when reading or receiving a view
 *
 * @param pfx           pointer to the prefix
 * @param path          pointer to the path
 * @return 1 to include the entry, 0 to exclude the entry, and -1 if an error
 * occurred.
 */
typedef int (bgpview_io_filter_pfx_peer_cb_t)(bgpstream_as_path_store_path_t *store_path);

#endif /* __BGPVIEW_IO_H */
