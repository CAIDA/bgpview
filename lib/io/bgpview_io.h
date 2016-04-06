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

#include "bgpstream_utils.h"
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

/** Magic number that denotes the end of the peers array */
#define BGPVIEW_IO_END_OF_PEERS 0xffff

/** Convenience macro to serialize a simple variable into a byte array.
 *
 * @param buf           pointer to the buffer (will be updated)
 * @param len           total length of the buffer
 * @param written       the number of bytes used in the buffer (will be updated)
 * @param from          the variable to serialize
 */
#define BGPVIEW_IO_SERIALIZE_VAL(buf, len, written, from)           \
  do {                                                              \
    size_t s;                                                       \
    assert(((len)-(written)) >= sizeof((from)));                    \
    memcpy((buf), &(from), sizeof(from));                           \
    s = sizeof(from);                                               \
    written += s;                                                   \
    buf += s;                                                       \
  } while(0)

/** Convenience macro to deserialize a simple variable from a byte array.
 *
 * @param buf           pointer to the buffer (will be updated)
 * @param len           total length of the buffer
 * @param read          the number of bytes already read from the buffer
 *                      (will be updated)
 * @param to            the variable to deserialize
 */
#define BGPVIEW_IO_DESERIALIZE_VAL(buf, len, read, to)  \
  do {							\
    size_t s;                                           \
    assert(((len)-(read)) >= sizeof(to));               \
    memcpy(&(to), (buf), sizeof(to));			\
    s = sizeof(to);					\
    read += s;						\
    buf += s;						\
  } while(0)

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

/** Serialize the given IP address into the given buffer
 *
 * @param buf           pointer to the buffer to serialize into
 * @param len           length of the buffer
 * @param ip            pointer to the IP address to serialize
 * @return the number of bytes written to the buffer if successful, -1 indicates
 * that an error occurred.
 */
int bgpview_io_serialize_ip(uint8_t *buf, size_t len, bgpstream_ip_addr_t *ip);

/** Deserialize an IP address from the given buffer
 *
 * @param buf           pointer to the buffer to deserialize from
 * @param len           length of the buffer
 * @param ip            pointer to the IP address to deserialize into
 * @return the number of bytes read from the buffer, <= 0 indicates an error
 * occurred.
 */
int bgpview_io_deserialize_ip(uint8_t *buf, size_t len,
                              bgpstream_addr_storage_t *ip);

/** Serialize the given prefix address into the given buffer
 *
 * @param buf           pointer to the buffer to serialize into
 * @param len           length of the buffer
 * @param pfx           pointer to the prefix to serialize
 * @return the number of bytes written to the buffer if successful, -1 indicates
 * that an error occurred.
 */
int bgpview_io_serialize_pfx(uint8_t *buf, size_t len, bgpstream_pfx_t *pfx);

/** Deserialize the given prefix address from the given buffer
 *
 * @param buf           pointer to the buffer to deserialize from
 * @param len           length of the buffer
 * @param pfx           pointer to the prefix to fill
 * @return the number of bytes read from the buffer if successful, -1 indicates
 * that an error occurred.
 */
int bgpview_io_deserialize_pfx(uint8_t *buf, size_t len,
                               bgpstream_pfx_storage_t *pfx);

/** Serialize peer ID and signature into the given buffer
 *
 * @param buf           pointer to the buffer to serialize into
 * @param len           length of the buffer
 * @param id            ID of the peer
 * @param sig           pointer to the signature of the peer
 * @return the number of bytes written, or -1 on an error
 *
 * @note this function **does not** write bytes in network byte order.
 */
int bgpview_io_serialize_peer(uint8_t *buf, size_t len,
                              bgpstream_peer_id_t id,
                              bgpstream_peer_sig_t *sig);

/** Deserialize peer ID and signature from the given buffer
 *
 * @param buf           pointer to the buffer to deserialize from
 * @param len           length of the buffer
 * @param id            pointer to update with the ID of the peer
 * @param sig           pointer to the signature of the peer
 * @return the number of bytes read, or -1 on an error
 *
 * @note this function **does not** flip bytes from network byte order.
 */
int bgpview_io_deserialize_peer(uint8_t *buf, size_t len,
                                bgpstream_peer_id_t *id,
                                bgpstream_peer_sig_t *sig);

/** Serialize the given AS Path Store Path
 *
 * @param buf           pointer to the buffer to serialize to
 * @param len           length of the buffer
 * @param spath         pointer to the Store Path to serialize
 * @return the number of bytes written, or -1 on an error
 *
 * @note this function is not strictly platform independent as it leaves values
 * in host byte order.
 */
int bgpview_io_serialize_as_path_store_path(uint8_t *buf, size_t len,
                                         bgpstream_as_path_store_path_t *spath);

/** Deserialize the given AS Path Store Path
 *
 * @param buf           pointer to the buffer to deserialize from
 * @param len           length of the buffer
 * @param spath         pointer to the Store Path to deserialize
 * @return the number of bytes read, or -1 on an error
 */
int
bgpview_io_deserialize_as_path_store_path(uint8_t *buf, size_t len,
                                     bgpstream_as_path_store_t *store,
                                     bgpstream_as_path_store_path_id_t *pathid);

/** Serialize the current pfx-peer
 *
 * @param buf           pointer to the buffer to serialize into
 * @param len           length of the buffer
 * @param it            pointer to a valid BGPView iterator
 * @param use_pathid    if 1, only path IDs will be serialized, not the
 *                      actual paths, if -1, then no path information will be
 *                      included
 * @return the number of bytes written, or -1 on error
 */
int bgpview_io_serialize_pfx_peer(uint8_t *buf, size_t len,
                                  bgpview_iter_t *it,
                                  int use_pathid);

/** Serialize the pfx-peers of the current prefix
 *
 * @param buf           pointer to the buffer to serialize into
 * @param len           length of the buffer
 * @param it            pointer to a valid BGPView iterator
 * @param peers_cnt[out] will be set to the number of pfx-peers serialized
 * @param cb            pointer to a filter callback
 * @param use_pathid    if 1, only path IDs will be serialized, not the
 *                      actual paths, if -1, then no path information will be
 *                      included
 * @return the number of bytes written, or -1 on error
 */
int bgpview_io_serialize_pfx_peers(uint8_t *buf, size_t len,
                                   bgpview_iter_t *it,
                                   int *peers_cnt,
                                   bgpview_io_filter_cb_t *cb,
                                   int use_pathid);

/** Serialize the full 'prefix row' that the iterator currently points at
 *
 * @param buf           pointer to the buffer to serialize into
 * @param len           length of the buffer
 * @param it            pointer to a valid BGPView iterator
 * @param cb            pointer to a filter callback
 * @param use_pathid    if 1, only path IDs will be serialized, not the
 *                      actual paths, if -1, then no path information will be
 *                      included
 * @return the number of bytes written, 0 if there were no peers to write, or -1
 * on error
 */
int bgpview_io_serialize_pfx_row(uint8_t *buf, size_t len,
                                 bgpview_iter_t *it,
                                 bgpview_io_filter_cb_t *cb,
                                 int use_pathid);

/** Deserialize a full 'prefix row' from the given buffer
 *
 * @param buf           pointer to the buffer to deserialize from
 * @param len           length of the buffer
 * @param it            pointer to a valid BGPView iterator
 * @param pfx_cb        pointer to a prefix filter callback
 * @param pfx_peer_cb   pointer to a prefix-peer filter callback
 * @param peerid_map    pointer to a mapping from serialized peerid to those in
 *                      the view
 * @param peerid_map_cnt number of elements in the peerid_map
 * @param pathid_map    pointer to a mapping from serialized path index to IDs
 *                      in the path store
 * @param state         indicates if the deserialized cells should be activated
 *                      or deactivated
 * @return the number of bytes read, or -1 on error
 *
 * If the pathid_map_cnt is < 0, then it is assumed that the full path is
 * serialized directly into the buffer. **Note:** An empty pathid_map is valid
 * iff the view is also NULL (i.e., a no-op read).
 */
int bgpview_io_deserialize_pfx_row(uint8_t *buf, size_t len,
                                   bgpview_iter_t *it,
                                   bgpview_io_filter_pfx_cb_t *pfx_cb,
                                   bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
                                   bgpstream_peer_id_t *peerid_map,
                                   int peerid_map_cnt,
                                   bgpstream_as_path_store_path_id_t *pathid_map,
                                   int pathid_map_cnt,
                                   bgpview_field_state_t state);

#endif /* __BGPVIEW_IO_H */
