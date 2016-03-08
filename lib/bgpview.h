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

#ifndef __BGPVIEW_H
#define __BGPVIEW_H

#include "bgpstream_utils_as_path.h"
#include "bgpstream_utils_peer_sig_map.h"

/** @file
 *
 * @brief Header file that exposes the public interface of bgpview view.
 *
 * @author Alistair King
 *
 */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

/** Opaque handle to an instance of a BGP View table.
 *
 * All interaction with a view must be done through the view API exposed in
 * bgpview.h
 */
typedef struct bgpview bgpview_t;

/** Opaque handle for iterating over fields of a BGP View table. */
typedef struct bgpview_iter bgpview_iter_t;

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

typedef enum {

  /** The current field is invalid (it could
   *  be destroyed and the status of the view
   *  would still be consistent). The number
   *  associated with the enumerator is such
   *  that no mask will ever iterate/seek
   *  over this field (it is exactly equivalent
   *  to a non existent field). */
  BGPVIEW_FIELD_INVALID   = 0b000,

  /** The current field is active */
  BGPVIEW_FIELD_ACTIVE    = 0b001,

  /** The current field is inactive */
  BGPVIEW_FIELD_INACTIVE  = 0b010,

} bgpview_field_state_t;

/** @} */

/**
 * @name Public Constants
 *
 * @{ */

/** BGPVIEW_FIELD_ALL_VALID is the expression to use
 *  when we do not need to specify ACTIVE or INACTIVE states,
 *  we are looking for any VALID state */
#define BGPVIEW_FIELD_ALL_VALID                 \
  BGPVIEW_FIELD_ACTIVE | BGPVIEW_FIELD_INACTIVE


/** @todo figure out how to signal no-export pfx-peer infos */
# if 0
/** if an origin AS number is within this range:
 *  [BGPVIEW_ASN_NOEXPORT_START,BGPVIEW_ASN_NOEXPORT_END]
 *  the pfx-peer info will not be exported (i.e. sent through the io channel) */
#define BGPVIEW_ASN_NOEXPORT_START BGPVIEW_ASN_NOEXPORT_END - 255
#define BGPVIEW_ASN_NOEXPORT_END   0xffffffff

#endif

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

/** Callback for destroying a custom user structure associated with bgpview
 *  view or one of its substructures
 * @param user    user pointer to destroy
 */
typedef void (bgpview_destroy_user_t) (void* user);

/** @} */

/** Create a new BGP View
 *
 * A BGP View holds a snapshot of the aggregated prefix information.
 * Basically, it maps from prefix -> peers -> prefix info
 *
 * @param bwv_user_destructor           a function that destroys the user structure
 *                                      in the bgpview_t structure
 * @param bwv_peer_user_destructor      a function that destroys the user structure
 *                                      used in each bwv_peerinfo_t structure
 * @param bwv_pfx_user_destructor       a function that destroys the user structure
 *                                      used in each bwv_peerid_pfxinfo_t structure
 * @param bwv_pfx_peer_user_destructor  a function that destroys the user structure
 *                                      used in each bgpview_pfx_peer_info_t structure
 *
 * @return a pointer to the view if successful, NULL otherwise
 *
 * The destroy functions passed are called everytime the associated user pointer
 * is set to a new value or when the corresponding structure are destroyed.
 * If a NULL parameter is passed, then when a the user pointer is set to a new
 * value (or when the structure containing such a user pointer is deallocated)
 * no destructor is called. In other words, when the destroy callback is set to
 * NULL the programmer is responsible for deallocating the user memory outside
 * the bgpview API.
 */
bgpview_t *
bgpview_create(bgpview_destroy_user_t *bwv_user_destructor,
               bgpview_destroy_user_t *bwv_peer_user_destructor,
               bgpview_destroy_user_t *bwv_pfx_user_destructor,
               bgpview_destroy_user_t *bwv_pfx_peer_user_destructor);

/** Create a new BGP View, reusing an existing peersigns table
 *
 * @param peersigns     pointer to a peersigns map to share
 *
 * A BGP View holds a snapshot of the aggregated prefix information.
 * Basically, it maps from prefix -> peers -> prefix info
 *
 * @param peersigns     pointer to a peersigns table that the view should use
 * @param pathstore     pointer to an AS path store that the view should use
 * @param bwv_user_destructor           a function that destroys the user structure
 *                                      in the bgpview_t structure
 * @param bwv_peer_user_destructor      a function that destroys the user structure
 *                                      used in each bwv_peerinfo_t structure
 * @param bwv_pfx_user_destructor       a function that destroys the user structure
 *                                      used in each bwv_peerid_pfxinfo_t structure
 * @param bwv_pfx_peer_user_destructor  a function that destroys the user structure
 *                                      used in each bgpview_pfx_peer_info_t structure
 *
 * @return a pointer to the view if successful, NULL otherwise
 */
bgpview_t *
bgpview_create_shared(bgpstream_peer_sig_map_t *peersigns,
                      bgpstream_as_path_store_t *pathstore,
                      bgpview_destroy_user_t *bwv_user_destructor,
                      bgpview_destroy_user_t *bwv_peer_user_destructor,
                      bgpview_destroy_user_t *bwv_pfx_user_destructor,
                      bgpview_destroy_user_t *bwv_pfx_peer_user_destructor);

/** @todo create a nice high-level api for accessing information in the view */

/** Destroy the given BGPView
 *
 * @param view          pointer to the view to destroy
 */
void
bgpview_destroy(bgpview_t *view);

/** Empty a view
 *
 * @param view          view to clear
 *
 * This does not actually free any memory, it just marks prefix and peers as
 * dirty so that future inserts can re-use the memory allocation. It does *not*
 * clear the peersigns table.
 */
void
bgpview_clear(bgpview_t *view);

/** Garbage collect a view
 *
 * @param view          view to garbage collect on
 *
 * This function frees memory marked as unused either by the
 * bgpview_clear or the various *_remove_* functions.
 *
 * @note at this point, any user data stored in unused portions of the view will
 * be freed using the appropriate destructor.
 */
void
bgpview_gc(bgpview_t *view);

/** Disable user data for a view
 *
 * @param view          view to disable user data for
 *
 * Disables the user pointer for per-prefix peer information. This can reduce
 * memory consumption for applications that do not need the pfx-peer user
 * pointer. Be careful with use of this mode.
 */
void
bgpview_disable_user_data(bgpview_t *view);


/**
 * @name Simple Accessor Functions
 *
 * @{ */

/** Get the total number of active IPv4 prefixes in the view
 *
 * @param view          pointer to a view structure
 * @param state_mask    mask of pfx states to include in the count
 *                      (i.e. active and/or inactive pfxs)
 * @return the number of IPv4 prefixes in the view
 */
uint32_t
bgpview_v4pfx_cnt(bgpview_t *view, uint8_t state_mask);

/** Get the total number of active IPv6 prefixes in the view
 *
 * @param view          pointer to a view structure
 * @param state_mask    mask of pfx states to include in the count
 *                      (i.e. active and/or inactive pfxs)
 * @return the number of IPv6 prefixes in the view
 */
uint32_t
bgpview_v6pfx_cnt(bgpview_t *view, uint8_t state_mask);

/** Get the total number of active prefixes (v4+v6) in the view
 *
 * @param view          pointer to a view structure
 * @param state_mask    mask of pfx states to include in the count
 *                      (i.e. active and/or inactive pfxs)
 * @return the number of prefixes in the view
 */
uint32_t
bgpview_pfx_cnt(bgpview_t *view, uint8_t state_mask);

/** Get the number of active peers in the view
 *
 * @param view          pointer to a view structure
 * @param state_mask    mask of peer states to include in the count
 *                      (i.e. active and/or inactive peers)
 * @return the number of peers in the view
 */
uint32_t
bgpview_peer_cnt(bgpview_t *view, uint8_t state_mask);

/** Get the BGP time that the view represents
 *
 * @param view          pointer to a view structure
 * @return the time that the view represents
 */
uint32_t
bgpview_get_time(bgpview_t *view);

/** Set the BGP time that the view represents
 *
 * @param view          pointer to a view structure
 * @param time          time to set
 */
void
bgpview_set_time(bgpview_t *view, uint32_t time);

/** Get the wall time that this view was created
 *
 * @param view          pointer to a view structure
 * @return the time that the view represents
 */
uint32_t
bgpview_get_time_created(bgpview_t *view);

/** Get the user pointer associated with the view
 *
 * @param view          pointer to a view structure
 * @return the user pointer associated with the view
 */
void *
bgpview_get_user(bgpview_t *view);

/** Set the user pointer associated with the view
 *
 * @param view          pointer to a view structure
 * @param user          user pointer to associate with the view structure
 * @return 1 if a new user pointer is set, 0 if the user pointer was already
 *         set to the address provided.
 */
int
bgpview_set_user(bgpview_t *view, void *user);

/** Set the user destructor function. If the function is set to NULL,
 *  then no the user pointer will not be destroyed by the bgpview
 *  functions, the programmer is responsible for that in its own program.
 *
 * @param view                  pointer to a view structure
 * @param bwv_user_destructor   function that destroys the view user memory
 */
void
bgpview_set_user_destructor(bgpview_t *view,
                            bgpview_destroy_user_t *bwv_user_destructor);

/** Set the pfx user destructor function. If the function is set to NULL,
 *  then no the user pointer will not be destroyed by the bgpview
 *  functions, the programmer is responsible for that in its own program.
 *
 * @param view                       pointer to a view structure
 * @param bwv_pfx_user_destructor    function that destroys the per-pfx user memory
 */
void
bgpview_set_pfx_user_destructor(bgpview_t *view,
                                bgpview_destroy_user_t *bwv_pfx_user_destructor);

/** Set the peer user destructor function. If the function is set to NULL,
 *  then no the user pointer will not be destroyed by the bgpview
 *  functions, the programmer is responsible for that in its own program.
 *
 * @param view                       pointer to a view structure
 * @param bwv_peer_user_destructor   function that destroys the per-peer view user memory
 */
void
bgpview_set_peer_user_destructor(bgpview_t *view,
                                 bgpview_destroy_user_t *bwv_peer_user_destructor);

/** Set the pfx-peer user destructor function. If the function is set to NULL,
 *  then no the user pointer will not be destroyed by the bgpview
 *  functions, the programmer is responsible for that in its own program.
 *
 * @param view                            pointer to a view structure
 * @param bwv_pfx_peer_user_destructor    function that destroys the per-pfx per-peer user memory
 */
void
bgpview_set_pfx_peer_user_destructor(bgpview_t *view,
                                     bgpview_destroy_user_t *bwv_pfx_peer_user_destructor);

/** Get the AS Path Store associated with this view
 *
 * @param view          pointer to the view to retrieve the AS Path Store for
 * @return pointer to the AS Path Store used by the view
 */
bgpstream_as_path_store_t *
bgpview_get_as_path_store(bgpview_t *view);

/** @} */

/**
 * @name View Iterator Functions
 *
 * @{ */

/** Create a new view iterator
 *
 * @param               Pointer to the view to create iterator for
 * @return pointer to an iterator if successful, NULL otherwise
 */
bgpview_iter_t *
bgpview_iter_create(bgpview_t *view);

/** Destroy the given iterator
 *
 * @param               Pointer to the iterator to destroy
 */
void
bgpview_iter_destroy(bgpview_iter_t *iter);

/** Reset the peer iterator to the first peer that matches
 *  the mask
 *
 * @param iter          Pointer to an iterator structure
 * @param state_mask    A mask that indicates the state of the
 *                      fields we iterate through
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_first_peer(bgpview_iter_t *iter,
                        uint8_t state_mask);

/** Advance the provided iterator to the next peer in the given view
 *
 * @param iter          Pointer to an iterator structure
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_next_peer(bgpview_iter_t *iter);

/** Check if the provided iterator point at an existing peer
 *  or the end has been reached
 *
 * @param iter          Pointer to an iterator structure
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_has_more_peer(bgpview_iter_t *iter);

/** Check if the provided peer exists in the current view
 *  and its state matches the mask provided; set the provided
 *  iterator to point at the peer (if it exists) or set it
 *  to the end of the peer table (if it doesn't exist)
 *
 * @param iter          Pointer to an iterator structure
 * @param peerid        The peer id
 * @param state_mask    A mask that indicates the state of the
 *                      fields we iterate through
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_seek_peer(bgpview_iter_t *iter,
                       bgpstream_peer_id_t peerid,
                       uint8_t state_mask);

/** Reset the prefix iterator to the first item for the given
 *  IP version that also matches the mask
 *
 * @param iter          Pointer to an iterator structure
 * @param version       0 if the intention is to iterate over
 *                      all IP versions, BGPSTREAM_ADDR_VERSION_IPV4 or
 *                      BGPSTREAM_ADDR_VERSION_IPV6 to iterate over a
 *                      single version
 * @param state_mask    A mask that indicates the state of the pfx
 *                      fields we iterate through
 * @return 1 if the iterator points at an existing prefix,
 *         0 if the end has been reached
 *
 * @note 1: the mask provided is permanent until a new first or
 *          a new seek function is called
 */
int
bgpview_iter_first_pfx(bgpview_iter_t *iter,
                       int version,
                       uint8_t state_mask);

/** Advance the provided iterator to the next prefix in the given view
 *
 * @param iter          Pointer to an iterator structure
 * @return 1 if the iterator points at an existing prefix,
 *         0 if the end has been reached
 */
int
bgpview_iter_next_pfx(bgpview_iter_t *iter);

/** Check if the provided iterator point at an existing prefix
 *  or the end has been reached
 *
 * @param iter          Pointer to an iterator structure
 * @return 1 if the iterator points at an existing prefix,
 *         0 if the end has been reached
 */
int
bgpview_iter_has_more_pfx(bgpview_iter_t *iter);

/** Check if the provided prefix exists in the current view
 *  and its state matches the mask provided; set the provided
 *  iterator to point at the prefix (if it exists) or set it
 *  to the end of the prefix table (if it doesn't exist)
 *
 * @param iter          Pointer to an iterator structure
 * @param state_mask    A mask that indicates the state of the pfx
 *                      fields we iterate through
 * @return 1 if the iterator points at an existing prefix,
 *         0 if the end has been reached
 *
 * @note: the seek function sets the version to pfx->version and
 *        sets the state mask for pfx iteration
 */
int
bgpview_iter_seek_pfx(bgpview_iter_t *iter,
                      bgpstream_pfx_t *pfx,
                      uint8_t state_mask);

/** Reset the peer iterator to the first peer (of the current
 *  prefix) that matches the mask
 *
 * @param iter          Pointer to an iterator structure
 * @param state_mask    A mask that indicates the state of the
 *                      fields we iterate through
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 *
 * @note: everytime the iterator is moved to a new peer
 * the peer iterator updated accordingly (e.g. if pfx_peer
 * points at pfx1 peer1, then the pfx iterator will point
 * at pfx1, the peer iterator will point at peer 1, and the
 * pfx_point iterator at the peer1 info associated with pfx1
 */
int
bgpview_iter_pfx_first_peer(bgpview_iter_t *iter,
                            uint8_t state_mask);

/** Advance the provided iterator to the next peer that
 * matches the mask for the current prefix
 *
 * @param iter          Pointer to an iterator structure
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_pfx_next_peer(bgpview_iter_t *iter);

/** Check if the provided iterator point at an existing peer
 *  for the current prefix or the end has been reached
 *
 * @param iter          Pointer to an iterator structure
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_pfx_has_more_peer(bgpview_iter_t *iter);

/** Check if the provided peer exists for the current prefix
 *  and its state matches the mask provided; set the provided
 *  iterator to point at the peer (if it exists) or set it
 *  to the end of the prefix-peer table (if it doesn't exist)
 *
 * @param iter          Pointer to an iterator structure
 * @param peerid        The peer id
 * @param state_mask    A mask that indicates the state of the
 *                      fields we iterate through
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_pfx_seek_peer(bgpview_iter_t *iter,
                           bgpstream_peer_id_t peerid,
                           uint8_t state_mask);

/** Reset the peer iterator to the first peer that matches the
 *  the peer mask for the first pfx that matches the IP version
 *  and the pfx_mask
 *
 * @param iter          Pointer to an iterator structure
 * @param version       0 if the intention is to iterate over
 *                      all IP versions, BGPSTREAM_ADDR_VERSION_IPV4 or
 *                      BGPSTREAM_ADDR_VERSION_IPV6 to iterate over a
 *                      single version
 * @param pfx_mask      A mask that indicates the state of the
 *                      prefixes we iterate through
 * @param peer_mask     A mask that indicates the state of the
 *                      peers we iterate through
 * @return 1 if the iterator points at an existing prefix,
 *         0 if the end has been reached
 *
 * @note: everytime the iterator is moved to a new peer
 * the peer iterator updated accordingly (e.g. if pfx_peer
 * points at pfx1 peer1, then the pfx iterator will point
 * at pfx1, the peer iterator will point at peer 1, and the
 * pfx_point iterator at the peer1 info associated with pfx1
 */
int
bgpview_iter_first_pfx_peer(bgpview_iter_t *iter,
                            int version,
                            uint8_t pfx_mask,
                            uint8_t peer_mask);

/** Advance the provided iterator to the next peer that matches the
 *  the peer mask for the next pfx that matches the pfx_mask
 * @param iter          Pointer to an iterator structure
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_next_pfx_peer(bgpview_iter_t *iter);

/** Check if the provided iterator point at an existing peer/prefix
 *  or the end has been reached
 *
 * @param iter          Pointer to an iterator structure
 * @return 1 if the iterator points at an existing peer,
 *         0 if the end has been reached
 */
int
bgpview_iter_has_more_pfx_peer(bgpview_iter_t *iter);


/** Check if the provided peer exists for the given prefix
 *  and their states match the masks provided; set the provided
 *  iterator to point at the peer (if it exists) or set it
 *  to the end of the prefix/peer tables (if it doesn't exist)
 *
 * @param iter          Pointer to an iterator structure
 * @param pfx           A pointer to a prefix, the prefix version
 *                      will be extracted from this structure
 * @param pfx_mask      A mask that indicates the state of the
 *                      prefixes we iterate through
 * @param peer_mask     A mask that indicates the state of the
 *                      peers we iterate through
 * @return 1 if the iterator points at an existing prefix,
 *         0 if the end has been reached
 * @note: the seek function sets the version to pfx->version and
 *        sets the state mask for pfx iteration
 */
int
bgpview_iter_seek_pfx_peer(bgpview_iter_t *iter,
                           bgpstream_pfx_t *pfx,
                           bgpstream_peer_id_t peerid,
                           uint8_t pfx_mask,
                           uint8_t peer_mask);

/** @} */

/**
 * @name View Iterator Add/Remove Functions
 *
 * @{ */

/** Insert a new peer in the BGPView
 *
 * @param iter             pointer to a view iterator
 * @param collector_str    pointer to the peer's collector name
 * @param peer_address     pointer to a peer's ip address
 * @param peer_asnumber    peer's AS number
 * @return the peer_id that is associated with the inserted peer if successful,
 *         0 otherwise
 *
 * When a new peer is created its state is set to inactive. if the peer is
 * already present, it will not be modified.
 *
 * When this function returns successfully, the provided iterator will be
 * pointing to the inserted peer (even if it already existed).
 *
 */
bgpstream_peer_id_t
bgpview_iter_add_peer(bgpview_iter_t *iter,
                      char *collector_str,
                      bgpstream_ip_addr_t *peer_address,
                      uint32_t peer_asnumber);

/** Remove the current peer from the BGPView
 *
 * @param iter             pointer to a view iterator
 * @return 0 if the peer was removed successfully, -1 otherwise
 *
 * @note for efficiency, this function may not actually free the memory
 * associated with the peer. If memory should be reclaimed, run the
 * bgpview_gc function after removals are complete.
 *
 * After removing a peer, the provided iterator will be advanced to the next
 * peer that matches the current iterator configuration, or will be invalidated
 * if there are no more peers.
 *
 * If the peer is currently active, it will be deactivated prior to removal.
 */
int
bgpview_iter_remove_peer(bgpview_iter_t *iter);

/** Insert a new pfx-peer information in the BGPView
 *
 * @param iter          pointer to a view iterator
 * @param pfx           pointer to the prefix
 * @param peer_id       peer identifier
 * @param as_path       pointer to the AS path for the prefix as observed
 *                      by peer peer_id (NULL for no path)
 * @return 0 if the insertion was successful, <0 otherwise
 *
 * In order for the function to succeed the peer must exist (it can be either
 * active or inactive).
 * When a new pfx-peer is created its state is set to inactive.
 * When this function returns successfully, the provided iterator will be
 * pointing to the inserted prefix-peer (even if it already existed).
 */
int
bgpview_iter_add_pfx_peer(bgpview_iter_t *iter,
                          bgpstream_pfx_t *pfx,
                          bgpstream_peer_id_t peer_id,
                          bgpstream_as_path_t *as_path);

/** Insert a new pfx-peer information in the BGP Watcher view (with an already
 * known existing AS Path ID)
 *
 * @param iter          pointer to a view iterator
 * @param pfx           pointer to the prefix
 * @param peer_id       peer identifier
 * @param path_id       AS Path Store ID of the corresponding AS Path
 * @return 0 if the insertion was successful, <0 otherwise
 *
 * In order for the function to succeed the peer must exist (it can be either
 * active or inactive).
 * When a new pfx-peer is created its state is set to inactive.
 * When this function returns successfully, the provided iterator will be
 * pointing to the inserted prefix-peer (even if it already existed).
 * The provided path ID must correspond to the appropriate AS Path in the store
 * used by this view.
 */
int
bgpview_iter_add_pfx_peer_by_id(bgpview_iter_t *iter,
                                bgpstream_pfx_t *pfx,
                                bgpstream_peer_id_t peer_id,
                                bgpstream_as_path_store_path_id_t path_id);

/** Remove the current pfx currently referenced by the given iterator
 *
 *
 *
 * @param iter             pointer to a view iterator
 * @return 0 if the pfx was removed successfully, -1 otherwise
 *
 * @note for efficiency, this function may not actually free the memory
 * associated with the pfx. If memory should be reclaimed, run the
 * bgpview_gc function after removals are complete.
 *
 * After removing a pfx, the provided iterator will be advanced to the next pfx
 * that matches the current iterator configuration, or will be invalidated if
 * there are no more pfxs.
 *
 * If the pfx is currently active, it will be deactivated prior to removal (thus
 * deactivating and removing all associated pfx-peers).
 */
int
bgpview_iter_remove_pfx(bgpview_iter_t *iter);

/** Insert a new peer info into the currently iterated pfx
 *
 * @param iter          pointer to a view iterator
 * @param peer_id       peer identifier
 * @param as_path       pointer to the AS path for the prefix as observed
 *                      by peer peer_id (NULL for no path)
 * @return 0 if the insertion was successful, <0 otherwise
 *
 * @note: in order for the function to succeed the peer must
 *        exist (it can be either active or inactive)
 * @note: when a new pfx-peer is created its state is set to
 *        inactive.
 */
int
bgpview_iter_pfx_add_peer(bgpview_iter_t *iter,
                          bgpstream_peer_id_t peer_id,
                          bgpstream_as_path_t *as_path);

/** Insert a new peer info into the currently iterated pfx (with an already
 * known existing AS Path ID)
 *
 * @param iter          pointer to a view iterator
 * @param peer_id       peer identifier
 * @param path_id       AS Path Store ID of the corresponding AS Path
 * @return 0 if the insertion was successful, <0 otherwise
 *
 * In order for the function to succeed the peer must exist (it can be either
 * active or inactive).
 * When a new pfx-peer is created its state is set to inactive.
 * The provided path ID must correspond to the appropriate AS Path in the store
 * used by this view.
 */
int
bgpview_iter_pfx_add_peer_by_id(bgpview_iter_t *iter,
                                bgpstream_peer_id_t peer_id,
                                bgpstream_as_path_store_path_id_t path_id);

/** Remove the current peer from the current prefix currently referenced by the
 * given iterator
 *
 *WARNING: IT DOES NOT ADVANCE THE ITERATOR ANYMORE
 *
 * @param iter             pointer to a view iterator
 * @return 0 if the pfx-peer was removed successfully, -1 otherwise
 *
 * @note for efficiency, this function may not actually free the memory
 * associated with the pfx-peer. If memory should be reclaimed, run the
 * bgpview_gc function after removals are complete.
 *
 * After removing a pfx-peer, the provided iterator will be advanced to the next
 * pfx-peer that matches the current iterator configuration, or will be
 * invalidated if there are no more pfx-peers.
 *
 * If the pfx-peer is currently active, it will be deactivated prior to
 * removal.
 *
 * If this is the last pfx-peer for the current pfx, the pfx will also be
 * removed.
 */
int
bgpview_iter_pfx_remove_peer(bgpview_iter_t *iter);

/** @} */

/**
 * @name View Iterator Getter and Setter Functions
 *
 * @{ */


/** Get the current view
 *
 * @param iter          Pointer to an iterator structure
 * @return a pointer to the current view,
 *         NULL if the iterator is not initialized.
 */
bgpview_t *
bgpview_iter_get_view(bgpview_iter_t *iter);

/** Get the current prefix for the given iterator
 *
 * @param iter          Pointer to an iterator structure
 * @return the prefix the pfx_iterator is currently pointing at,
 *         NULL if the iterator is not initialized, or has reached the end of
 *         the prefixes.
 */
bgpstream_pfx_t *
bgpview_iter_pfx_get_pfx(bgpview_iter_t *iter);

/** Get the number of peers providing information for the
 *  current prefix pointed by the given iterator
 *
 * @param iter          Pointer to an iterator structure
 * @param state_mask    mask of peer states to include in the count
 *                      (i.e. active and/or inactive peers)
 * @return the number of peers providing information for the prefix,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the prefixes.
 */
int
bgpview_iter_pfx_get_peer_cnt(bgpview_iter_t *iter,
                              uint8_t state_mask);

/** Get the state of the current prefix pointed by the given iterator
 *
 * @param iter          Pointer to an iterator structure
 * @return the state of the prefix (either ACTIVE or INACTIVE)
 *         INVALID if the iterator is not initialized, or has reached the end of
 *         the prefixes.
 */
bgpview_field_state_t
bgpview_iter_pfx_get_state(bgpview_iter_t *iter);

/** Get the current user ptr of the current prefix
 *
 * @param iter          Pointer to an iterator structure
 * @return the user ptr that the pfx_iterator is currently pointing at,
 *         NULL if the iterator is not initialized, or has reached the end of
 *         the prefixes.
 */
void *
bgpview_iter_pfx_get_user(bgpview_iter_t *iter);

/** Set the user ptr for the current prefix
 *
 * @param iter          Pointer to an iterator structure
 * @param user          Pointer to a memory to borrow to the prefix
 * @return  0 if the user pointer already pointed at the same memory location,
 *          1 if the internal user pointer has been updated,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the prefixes.
 */
int
bgpview_iter_pfx_set_user(bgpview_iter_t *iter, void *user);

/** Get the current peer id for the given iterator
 *
 * @param iter          Pointer to an iterator structure
 * @return the peer id that is currently pointing at,
 *         0 if the iterator is not initialized, or has reached the end of
 *         the peers.
 */
bgpstream_peer_id_t
bgpview_iter_peer_get_peer_id(bgpview_iter_t *iter);

/** Get the peer signature for the current peer id
 *
 * @param iter          Pointer to an iterator structure
 * @return the signature associated with the peer id,
 *         NULL if the iterator is not initialized, or has reached the end of
 *         the peers.
 */
bgpstream_peer_sig_t *
bgpview_iter_peer_get_sig(bgpview_iter_t *iter);


/** Get the number of prefixes (ipv4, ipv6, or all) that the current
 *  peer observed
 *
 * @param iter          Pointer to an iterator structure
 * @param version       0 if the intention is to consider over
 *                      all IP versions, BGPSTREAM_ADDR_VERSION_IPV4 or
 *                      BGPSTREAM_ADDR_VERSION_IPV6 to consider a
 *                      single version
 * @return number of observed prefixes,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the peers.
 */
int
bgpview_iter_peer_get_pfx_cnt(bgpview_iter_t *iter,
                              int version,
                              uint8_t state_mask);

/** Get the state of the current peer pointed by the given iterator
 *
 * @param iter          Pointer to an iterator structure
 * @return the state of the peer (either ACTIVE or INACTIVE)
 *         INVALID if the iterator is not initialized, or has reached the end of
 *         the peers.
 */
bgpview_field_state_t
bgpview_iter_peer_get_state(bgpview_iter_t *iter);

/** Get the current user ptr of the current peer
 *
 * @param iter          Pointer to an iterator structure
 * @return the user ptr that the peer_iterator is currently pointing at,
 *         NULL if the iterator is not initialized, or has reached the end of
 *         the peers.
 */
void *
bgpview_iter_peer_get_user(bgpview_iter_t *iter);

/** Set the user ptr for the current peer
 *
 * @param iter          Pointer to an iterator structure
 * @param user          Pointer to a memory to borrow to the peer
 * @return  0 if the user pointer already pointed at the same memory location,
 *          1 if the internal user pointer has been updated,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the peers.
 */
int
bgpview_iter_peer_set_user(bgpview_iter_t *iter, void *user);


/** Get the path for the current pfx-peer structure pointed by the given
 *  iterator
 *
 * @param iter          Pointer to an iterator structure
 * @return borrowed pointer to the AS path, NULL if the iterator is not
 *         initialized, or has reached the end of the peers for the given
 *         prefix.
 */
bgpstream_as_path_t *
bgpview_iter_pfx_peer_get_as_path(bgpview_iter_t *iter);

/** Get the origin segment for the current pfx-peer structure pointed by the
 *  given iterator
 *
 * @param iter          Pointer to an iterator structure
 * @return borrowed pointer to the origin AS segment, NULL if the iterator is
 *         not initialized, or has reached the end of the peers for the given
 *         prefix.
 */
bgpstream_as_path_seg_t *
bgpview_iter_pfx_peer_get_origin_seg(bgpview_iter_t *iter);

/** Get the AS Path Store Path for the current pfx-peer structure pointed at by
 * the given iterator
 *
 * @param iter          Pointer to an iterator structure
 * @return borrowed pointer to the AS Path Store Path, NULL if the iterator is
 *         not initialized, or has reached the end of the peers for the given
 *         prefix.
 */
bgpstream_as_path_store_path_t *
bgpview_iter_pfx_peer_get_as_path_store_path(bgpview_iter_t *iter);

/** Reset the AS Path Segment iterator for the current pfx-peer
 *
 * @param iter          Pointer to an iterator structure
 */
void
bgpview_iter_pfx_peer_as_path_seg_iter_reset(bgpview_iter_t *iter);

/** Get the next AS Path Segment for the current pfx-peer
 *
 * @param iter          Pointer to an iterator structure
 * @return borrowed pointer to an AS Path Segment structure or NULL if there are
 *         no more segments in the path
 */
bgpstream_as_path_seg_t *
bgpview_iter_pfx_peer_as_path_seg_next(bgpview_iter_t *iter);

/** Set the AS path for the current pfx-peer structure pointed by the given
 *  iterator
 *
 * @param iter          Pointer to an iterator structure
 * @param as_path       Pointer to an AS Path to set
 * @return 0 if the path was set successfully, -1 otherwise
 *
 * The AS Path is copied on insertion, so the caller maintains ownership of the
 * path.
 */
int
bgpview_iter_pfx_peer_set_as_path(bgpview_iter_t *iter,
                                  bgpstream_as_path_t *as_path);

/** Set the AS path for the current pfx-peer structure pointed by the given
 *  iterator
 *
 * @param iter          Pointer to an iterator structure
 * @param path_id       Pointer to an AS Path to set
 * @return 0 if the path was set successfully, -1 otherwise
 *
 * The given AS path ID must already exist in the path store used by the view.
 */
int
bgpview_iter_pfx_peer_set_as_path_by_id(bgpview_iter_t *iter,
                                bgpstream_as_path_store_path_id_t path_id);

/** Get the state of the current pfx-peer pointed by the given iterator
 *
 * @param iter          Pointer to an iterator structure
 * @return the state of the pfx-peer (either ACTIVE or INACTIVE)
 *         INVALID if the iterator is not initialized, or has reached the end of
 *         the peers for the given prefix.
 */
bgpview_field_state_t
bgpview_iter_pfx_peer_get_state(bgpview_iter_t *iter);

/** Get the current user ptr of the current pfx-peer
 *
 * @param iter          Pointer to an iterator structure
 * @return the user ptr that the pfx-peer iterator is currently pointing at,
 *         NULL if the iterator is not initialized, or has reached the end of
 *         the peers for the given prefix.
 */
void *
bgpview_iter_pfx_peer_get_user(bgpview_iter_t *iter);

/** Set the user ptr for the current pfx-peer
 *
 * @param iter          Pointer to an iterator structure
 * @param user          Pointer to a memory to borrow to the pfx-peer
 * @return  0 if the user pointer already pointed at the same memory location,
 *          1 if the internal user pointer has been updated,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the peers for the given prefix.
 */
int
bgpview_iter_pfx_peer_set_user(bgpview_iter_t *iter, void *user);

/** @} */

/**
 * @name View Iterator Activate Deactivate Functions
 *
 * @{ */

/** Activate the current peer
 *
 * @param iter          Pointer to an iterator structure
 * @return  0 if the peer was already active
 *          1 if the peer was inactive and it became active,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the prefixes.
 */
int
bgpview_iter_activate_peer(bgpview_iter_t *iter);

/** De-activate the current peer
 *
 * @param iter          Pointer to an iterator structure
 * @return  0 if the peer was already inactive
 *          1 if the peer was active and it became inactive,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the prefixes.
 * @note the function deactivates all the peer-pfxs of the bgpview
 *       associated with the same peer
 */
int
bgpview_iter_deactivate_peer(bgpview_iter_t *iter);

/** Activate the current prefix:
 *  a prefix is active only when there is at least one prefix
 *  peer info which is active. In order to have a coherent
 *  behavior the only way to activate a prefix is either to
 *  activate a peer-pfx or to insert/add a peer-pfx (that
 *  automatically causes the activation.
 */

/** De-activate the current prefix
 *
 * @param iter          Pointer to an iterator structure
 * @return  0 if the prefix was already inactive
 *          1 if the prefix was active and it became inactive,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the prefixes.
 * @note the function deactivates all the peer-pfxs associated with the
 *       same prefix
 */
int
bgpview_iter_deactivate_pfx(bgpview_iter_t *iter);

/** Activate the current pfx-peer
 *
 * @param iter          Pointer to an iterator structure
 * @return  0 if the pfx-peer was already active
 *          1 if the pfx-peer was inactive and it became active,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the peers for the given prefix.
 *
 * @note: this function will automatically activate the corresponding
 *        prefix and peer (if they are not active already)
 */
int
bgpview_iter_pfx_activate_peer(bgpview_iter_t *iter);

/** De-activate the current pfx-peer
 *
 * @param iter          Pointer to an iterator structure
 * @return  0 if the pfx-peer was already inactive
 *          1 if the pfx-peer was active and it became inactive,
 *         -1 if the iterator is not initialized, or has reached the end of
 *         the peers for the given prefix.
 * @note if this is the last peer active for the the given prefix, then it
 *       deactivates the prefix.
 */
int
bgpview_iter_pfx_deactivate_peer(bgpview_iter_t *iter);

/** Create a new BGP View, which is a clone of the desired view
 * The cloned view use the same peer map and as path store than the first view.
 *
 *
 * A BGP View holds a snapshot of the aggregated prefix information.
 * Basically, it maps from prefix -> peers -> prefix info
 *
 * @param view     pointer to a peersigns table that the view should use
 *
 * @return a pointer to the view if successful, NULL otherwise
 */

bgpview_t* bgpview_clone_view(bgpview_t* view);


#endif /* __BGPVIEW_H */
