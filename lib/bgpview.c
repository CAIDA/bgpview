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

#include <assert.h>
#include <stdio.h>

#include "config.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_TIME_H
#include <time.h>
#endif

#include "khash.h"
#include "utils.h"

#include "bgpstream_utils_pfx.h"
#include "bgpview.h"

/** Information about a prefix as seen from a peer */
typedef struct bwv_pfx_peerinfo {

  /** AS Path Store ID */
  bgpstream_as_path_store_path_id_t as_path_id;

  /** prefix-peer state */
  uint8_t state;

  /** @todo add other pfx info fields here (AS path, etc) */

} __attribute__((packed)) bwv_pfx_peerinfo_t;

/** Information about a prefix as seen from a peer */
typedef struct bwv_pfx_peerinfo_ext {

  /** AS Path Store ID */
  bgpstream_as_path_store_path_id_t as_path_id;

  /** prefix-peer state */
  uint8_t state;

  /** @todo add other pfx info fields here (AS path, etc) */

  /** Generic pointer to store per-pfx-per-peer information
   * This is ONLY usable if the view was created as extended
   */
  void *user;

} __attribute__((packed)) bwv_pfx_peerinfo_ext_t;

KHASH_INIT(bwv_peerid_pfx_peerinfo, uint16_t, bwv_pfx_peerinfo_t, 1,
	   kh_int_hash_func, kh_int_hash_equal)
typedef khash_t(bwv_peerid_pfx_peerinfo) bwv_peerid_pfx_peerinfo_t;

KHASH_INIT(bwv_peerid_pfx_peerinfo_ext, uint16_t, bwv_pfx_peerinfo_ext_t, 1,
	   kh_int_hash_func, kh_int_hash_equal)
typedef khash_t(bwv_peerid_pfx_peerinfo_ext) bwv_peerid_pfx_peerinfo_ext_t;

#define BWV_PFX_PEERINFO_SIZE(view)                                            \
  (((view)->disable_extended) ? sizeof(bwv_pfx_peerinfo_t)                     \
                              : sizeof(bwv_pfx_peerinfo_ext_t))

#define BWV_PFX_GET_PEER_PTR(view, pfxinfo, k)                                 \
  (((view)->disable_extended)                                                  \
     ? &BWV_PFX_GET_PEER(pfxinfo, k)                                           \
     : (bwv_pfx_peerinfo_t *)&BWV_PFX_GET_PEER_EXT(pfxinfo, k))

#define BWV_PFX_GET_PEER(pfxinfo, k)                                           \
  kh_val(pfxinfo->peers_min, k)

#define BWV_PFX_GET_PEER_EXT(pfxinfo, k)                                       \
  kh_val(pfxinfo->peers_ext, k)

#define BWV_PFX_GET_PEER_STATE(view, pfxinfo, k)                               \
  (BWV_PFX_GET_PEER_PTR(view, pfxinfo, k)->state)

#define BWV_PFX_SET_PEER_STATE(view, pfxinfo, k, state)                        \
  (BWV_PFX_GET_PEER_STATE(view, pfxinfo, k) = state)

#define ASSERT_BWV_PFX_PEERINFO_EXT(view) assert(view->disable_extended == 0)

/** Value for a prefix in the v4pfxs and v6pfxs tables */
typedef struct bwv_peerid_pfxinfo {

  /** Table of peers
   *
   * must select either peers_min or peers_ext
   * depending on view->disable_extended
   */
  union {
    void *peers_generic;
    bwv_peerid_pfx_peerinfo_t *peers_min;
    bwv_peerid_pfx_peerinfo_ext_t *peers_ext;
  };

  /** The number of peers in the peers list that currently observe this
      prefix */
  uint16_t peers_cnt[BGPVIEW_FIELD_ALL_VALID];

  /** State of the prefix, if ACTIVE the prefix is currently seen by at least
   *  one peer.  if active <==> peers_cnt >0
   *  (contains a bgpview_field_state_t value)
   */
  uint8_t state;

  /** Generic pointer to store per-pfx information on consumers */
  void *user;

} __attribute__((packed)) bwv_peerid_pfxinfo_t;

/** @todo: add documentation ? */

/************ map from prefix -> peers [-> prefix info] ************/

KHASH_INIT(bwv_v4pfx_peerid_pfxinfo, bgpstream_ipv4_pfx_t,
           bwv_peerid_pfxinfo_t *, 1, bgpstream_ipv4_pfx_hash_val,
           bgpstream_ipv4_pfx_equal_val)
typedef khash_t(bwv_v4pfx_peerid_pfxinfo) bwv_v4pfx_peerid_pfxinfo_t;

KHASH_INIT(bwv_v6pfx_peerid_pfxinfo, bgpstream_ipv6_pfx_t,
           bwv_peerid_pfxinfo_t *, 1, bgpstream_ipv6_pfx_hash_val,
           bgpstream_ipv6_pfx_equal_val)
typedef khash_t(bwv_v6pfx_peerid_pfxinfo) bwv_v6pfx_peerid_pfxinfo_t;

/***** map from peerid to peerinfo *****/

/** Additional per-peer info */
typedef struct bwv_peerinfo {

  /** The number of v4 prefixes that this peer observed */
  uint32_t v4_pfx_cnt[BGPVIEW_FIELD_ALL_VALID];

  /** The number of v6 prefixes that this peer observed */
  uint32_t v6_pfx_cnt[BGPVIEW_FIELD_ALL_VALID];

  /** State of the peer, if the peer is active */
  bgpview_field_state_t state;

  /** Generic pointer to store information related to the peer */
  void *user;

} bwv_peerinfo_t;

KHASH_INIT(bwv_peerid_peerinfo, bgpstream_peer_id_t, bwv_peerinfo_t, 1,
           kh_int_hash_func, kh_int_hash_equal)

/************ bgpview ************/

// TODO: documentation
struct bgpview {

  /** BGP Time that the view represents */
  uint32_t time;

  /** Wall time when the view was created */
  uint32_t time_created;

  /** Table of prefix info for v4 prefixes */
  bwv_v4pfx_peerid_pfxinfo_t *v4pfxs;

  /** The number of in-use v4pfxs */
  uint32_t v4pfxs_cnt[BGPVIEW_FIELD_ALL_VALID];

  /** Table of prefix info for v6 prefixes */
  bwv_v6pfx_peerid_pfxinfo_t *v6pfxs;

  /** The number of in-use v6pfxs */
  uint32_t v6pfxs_cnt[BGPVIEW_FIELD_ALL_VALID];

  /** Table of peerid -> peersign */
  bgpstream_peer_sig_map_t *peersigns;

  /** Is the peersigns table shared? */
  int peersigns_shared;

  /** Store of AS Paths */
  bgpstream_as_path_store_t *pathstore;

  /** Is the Path Store shared? */
  int pathstore_shared;

  /** Table of peerid -> peerinfo */
  /** todo*/
  kh_bwv_peerid_peerinfo_t *peerinfo;

  /** The number of active peers */
  uint32_t peerinfo_cnt[BGPVIEW_FIELD_ALL_VALID];

  /** Pointer to a function that destroys the user structure
   *  in the bgpview_t structure */
  bgpview_destroy_user_t *user_destructor;

  /** Pointer to a function that destroys the user structure
   *  in the bwv_peerinfo_t structure */
  bgpview_destroy_user_t *peer_user_destructor;

  /** Pointer to a function that destroys the user structure
   *  in the bwv_peerid_pfxinfo_t structure */
  bgpview_destroy_user_t *pfx_user_destructor;

  /** Pointer to a function that destroys the user structure
   *  in the bgpview_pfx_peer_info_t structure */
  bgpview_destroy_user_t *pfx_peer_user_destructor;

  /** State of the view */
  bgpview_field_state_t state;

  /** Generic pointer to store information related to the view */
  void *user;

  /** Is this an extended view?
   * I.e. is it possible to add a user pointer to a pfx-peer?
   */
  int disable_extended;

  uint8_t need_gc_v4pfxs;
  uint8_t need_gc_v6pfxs;
  uint8_t need_gc_peerinfo;
};

struct bgpview_iter {

  /** Pointer to the view instance we are iterating over */
  bgpview_t *view;

  /** The IP version that is currently iterated */
  bgpstream_addr_version_t version_ptr;

  /** 0 if all IP versions are iterated,
   *  BGPSTREAM_ADDR_VERSION_IPV4 if only IPv4 are iterated,
   *  BGPSTREAM_ADDR_VERSION_IPV6 if only IPv6 are iterated */
  int version_filter;

  /** Current pfx (the pfx it is valid if != kh_end of the appropriate version
      table */
  khiter_t pfx_it;
  /** State mask used for prefix iteration */
  uint8_t pfx_state_mask;

  /** Current pfx-peer */
  khiter_t pfx_peer_it;
  /** Is the pfx-peer iterator valid? */
  int pfx_peer_it_valid;
  /** State mask used for pfx-peer iteration */
  uint8_t pfx_peer_state_mask;
  /** AS Path Segment iterator */
  bgpstream_as_path_store_path_iter_t pfx_peer_path_it;

  /** Current peerinfo */
  khiter_t peer_it;
  /** State mask used for peer iteration */
  uint8_t peer_state_mask;
};

/* ========== PRIVATE FUNCTIONS ========== */

static void peerinfo_reset(bwv_peerinfo_t *v)
{
  v->state = BGPVIEW_FIELD_INVALID;
  v->v4_pfx_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
  v->v4_pfx_cnt[BGPVIEW_FIELD_ACTIVE] = 0;
  v->v6_pfx_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
  v->v6_pfx_cnt[BGPVIEW_FIELD_ACTIVE] = 0;
}

static void peerinfo_destroy_user(bgpview_t *view)
{
  khiter_t k;
  if (view->peer_user_destructor == NULL) {
    return;
  }
  for (k = kh_begin(view->peerinfo); k != kh_end(view->peerinfo); ++k) {
    if (!kh_exist(view->peerinfo, k) ||
        (kh_value(view->peerinfo, k).user == NULL)) {
      continue;
    }
    view->peer_user_destructor(kh_value(view->peerinfo, k).user);
    kh_value(view->peerinfo, k).user = NULL;
  }
}

static bwv_peerid_pfxinfo_t *peerid_pfxinfo_create(void)
{
  bwv_peerid_pfxinfo_t *v;

  if ((v = malloc_zero(sizeof(bwv_peerid_pfxinfo_t))) == NULL) {
    return NULL;
  }
  v->state = BGPVIEW_FIELD_INVALID;

  /* all other fields are memset to 0 */

  return v;
}

static int peerid_pfxinfo_insert(bgpview_iter_t *iter,
                                 bwv_peerid_pfxinfo_t *v,
                                 bgpstream_peer_id_t peerid,
                                 bgpstream_as_path_store_path_id_t path_id)
{
  bwv_pfx_peerinfo_t *peerinfo = NULL;
  int khret;
  khiter_t k;

  if (!v->peers_generic) {
    if (iter->view->disable_extended) {
      v->peers_min = kh_init(bwv_peerid_pfx_peerinfo);
    } else {
      v->peers_ext = kh_init(bwv_peerid_pfx_peerinfo_ext);
    }
  }

  if (iter->view->disable_extended) {
    k = kh_put(bwv_peerid_pfx_peerinfo, v->peers_min, peerid, &khret);
    if (khret > 0) {
      // peer didn't exist; initialize it
      kh_val(v->peers_min, k).state = BGPVIEW_FIELD_INVALID;
    }
    peerinfo = &kh_val(v->peers_min, k);
  } else {
    k = kh_put(bwv_peerid_pfx_peerinfo_ext, v->peers_ext, peerid, &khret);
    if (khret > 0) {
      // peer didn't exist; initialize it
      kh_val(v->peers_ext, k).state = BGPVIEW_FIELD_INVALID;
      kh_val(v->peers_ext, k).user = NULL;
    }
    peerinfo = (bwv_pfx_peerinfo_t*)&kh_val(v->peers_ext, k);
  }

  peerinfo->as_path_id = path_id;

  if (peerinfo->state == BGPVIEW_FIELD_INVALID) {
    // did not already exist or was invalid
    peerinfo->state = BGPVIEW_FIELD_INACTIVE;

    /** peerinfo->user remains untouched */

    /* and count this as a new inactive peer for this prefix */
    v->peers_cnt[BGPVIEW_FIELD_INACTIVE]++;

    /* also count this as an inactive pfx for the peer */
    switch (iter->version_ptr) {
    case BGPSTREAM_ADDR_VERSION_IPV4:
      kh_value(iter->view->peerinfo, iter->peer_it)
	.v4_pfx_cnt[BGPVIEW_FIELD_INACTIVE]++;
      break;
    case BGPSTREAM_ADDR_VERSION_IPV6:
      kh_value(iter->view->peerinfo, iter->peer_it)
	.v6_pfx_cnt[BGPVIEW_FIELD_INACTIVE]++;
      break;
    default:
      return -1;
    }
  }

  /* now seek the iterator to this pfx/peer */
  iter->pfx_peer_it = k;
  iter->pfx_peer_it_valid = 1;
  iter->pfx_peer_state_mask = BGPVIEW_FIELD_ALL_VALID;

  return 0;
}

static inline void pfx_peer_info_destroy(bgpview_t *view, bwv_pfx_peerinfo_t *v)
{
  return;
}

static inline void pfx_peer_info_ext_destroy(bgpview_t *view,
                                             bwv_pfx_peerinfo_ext_t *v)
{
  ASSERT_BWV_PFX_PEERINFO_EXT(view);
  if (v->user != NULL && view->pfx_peer_user_destructor != NULL) {
    view->pfx_peer_user_destructor(v->user);
  }
  v->user = NULL;
}

static void peerid_pfxinfo_destroy(bgpview_t *view, bwv_peerid_pfxinfo_t *v)
{
  if (v == NULL) {
    return;
  }
  khiter_t k;
  if (v->peers_generic != NULL) {
    if (view->disable_extended == 0) {
      for (k = kh_begin(v->peers_ext); k != kh_end(v->peers_ext); ++k) {
        if (!kh_exist(v->peers_ext, k)) continue;
        pfx_peer_info_ext_destroy(view, &kh_val(v->peers_ext, k));
      }
      kh_destroy(bwv_peerid_pfx_peerinfo_ext, v->peers_ext);
    } else {
      // loop calling empty inline function can be optimized away
      for (k = kh_begin(v->peers_min); k != kh_end(v->peers_min); ++k) {
        if (!kh_exist(v->peers_min, k)) continue;
        pfx_peer_info_destroy(view, &kh_val(v->peers_min, k));
      }
      kh_destroy(bwv_peerid_pfx_peerinfo, v->peers_min);
    }
  }
  v->peers_generic = NULL;
  v->state = BGPVIEW_FIELD_INVALID;
  if (view->pfx_user_destructor != NULL && v->user != NULL) {
    view->pfx_user_destructor(v->user);
  }
  v->user = NULL;
  free(v);
}

#define __pfx_peerinfos(iter)                                                  \
  (((iter)->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4)                        \
     ? (kh_val((iter)->view->v4pfxs, (iter)->pfx_it))                          \
     : ((iter)->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6)                    \
         ? (kh_val((iter)->view->v6pfxs, (iter)->pfx_it))                      \
         : NULL)

static int add_v4pfx(bgpview_iter_t *iter, bgpstream_ipv4_pfx_t *pfx)
{
  bwv_peerid_pfxinfo_t *new_pfxpeerinfo;
  khiter_t k;
  int khret;

  k = kh_put(bwv_v4pfx_peerid_pfxinfo, iter->view->v4pfxs, *pfx, &khret);
  if (khret > 0) {
    /* pfx didn't exist */
    if ((new_pfxpeerinfo = peerid_pfxinfo_create()) == NULL) {
      return -1;
    }
    kh_value(iter->view->v4pfxs, k) = new_pfxpeerinfo;

    /* pfx is invalid at this point */
  }

  /* seek the iterator to this prefix */
  iter->pfx_it = k;
  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
  iter->pfx_peer_it_valid = 0; // moving pfx_it invalidates pfx_peer_it

  if (kh_value(iter->view->v4pfxs, k)->state != BGPVIEW_FIELD_INVALID) {
    /* it was already there and active/inactive */
    return 0;
  }

  kh_value(iter->view->v4pfxs, k)->state = BGPVIEW_FIELD_INACTIVE;
  iter->view->v4pfxs_cnt[BGPVIEW_FIELD_INACTIVE]++;

  return 0;
}

static int add_v6pfx(bgpview_iter_t *iter, bgpstream_ipv6_pfx_t *pfx)
{
  bwv_peerid_pfxinfo_t *new_pfxpeerinfo;
  khiter_t k;
  int khret;

  k = kh_put(bwv_v6pfx_peerid_pfxinfo, iter->view->v6pfxs, *pfx, &khret);
  if (khret > 0) {
    /* pfx didn't exist */
    if ((new_pfxpeerinfo = peerid_pfxinfo_create()) == NULL) {
      return -1;
    }
    kh_value(iter->view->v6pfxs, k) = new_pfxpeerinfo;

    /* pfx is invalid at this point */
  }

  /* seek the iterator to this prefix */
  iter->pfx_it = k;
  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV6;
  iter->pfx_peer_it_valid = 0; // moving pfx_it invalidates pfx_peer_it

  if (kh_value(iter->view->v6pfxs, k)->state != BGPVIEW_FIELD_INVALID) {
    /* it was already there and active/inactive */
    return 0;
  }

  kh_value(iter->view->v6pfxs, k)->state = BGPVIEW_FIELD_INACTIVE;
  iter->view->v6pfxs_cnt[BGPVIEW_FIELD_INACTIVE]++;

  return 0;
}

static int add_pfx(bgpview_iter_t *iter, bgpstream_pfx_t *pfx)
{
  if (pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV4) {
    return add_v4pfx(iter, &pfx->bs_ipv4);
  } else if (pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV6) {
    return add_v6pfx(iter, &pfx->bs_ipv6);
  }

  return -1;
}

/* ==================== ITERATOR FUNCTIONS ==================== */

bgpview_iter_t *bgpview_iter_create(bgpview_t *view)
{
  bgpview_iter_t *iter;

  /* DEBUG REMOVE ME */
  fprintf(stderr, "AS Path Store size: %d\n",
          bgpstream_as_path_store_get_size(view->pathstore));

  if ((iter = malloc_zero(sizeof(bgpview_iter_t))) == NULL) {
    return NULL;
  }

  iter->view = view;

  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
  iter->version_filter = 0; // default: all prefix versions

  iter->pfx_it = 0;

  iter->peer_it = kh_end(iter->view->peerinfo);

  iter->pfx_peer_it_valid = 0;

  // default: all valid fields are iterated
  iter->pfx_state_mask = BGPVIEW_FIELD_ALL_VALID;
  iter->peer_state_mask = BGPVIEW_FIELD_ALL_VALID;
  iter->pfx_peer_state_mask = BGPVIEW_FIELD_ALL_VALID;

  return iter;
}

void bgpview_iter_destroy(bgpview_iter_t *iter)
{
  free(iter);
}

/* ==================== ITER GETTER/SETTERS ==================== */

#define __cnt_by_mask(counter, mask)                                           \
  (((mask) == BGPVIEW_FIELD_ACTIVE || (mask) == BGPVIEW_FIELD_INACTIVE)        \
     ? ((counter)[(mask)])                                                     \
     : ((counter)[BGPVIEW_FIELD_ACTIVE] + (counter)[BGPVIEW_FIELD_INACTIVE]))

#define __pfx_field(iter, field) (__pfx_peerinfos((iter))->field)

#define __iter_get_view(iter) ((iter)->view)

bgpview_t *bgpview_iter_get_view(bgpview_iter_t *iter)
{
  if (iter != NULL) {
    return __iter_get_view(iter);
  }
  return NULL;
}

#define __iter_pfx_get_pfx(iter)                                               \
  (((iter)->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4)                        \
     ? ((bgpstream_pfx_t *)&kh_key(iter->view->v4pfxs, iter->pfx_it))          \
     : ((iter)->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6)                    \
         ? ((bgpstream_pfx_t *)&kh_key(iter->view->v6pfxs, iter->pfx_it))      \
         : NULL)

bgpstream_pfx_t *bgpview_iter_pfx_get_pfx(bgpview_iter_t *iter)
{
  return __iter_pfx_get_pfx(iter);
}

#define __iter_pfx_get_peer_cnt(iter, state_mask)                              \
  (__cnt_by_mask(__pfx_field(iter, peers_cnt), state_mask))

int bgpview_iter_pfx_get_peer_cnt(bgpview_iter_t *iter, uint8_t state_mask)
{
  return __iter_pfx_get_peer_cnt(iter, state_mask);
}

#define __iter_pfx_get_state(iter) (__pfx_field(iter, state))

bgpview_field_state_t bgpview_iter_pfx_get_state(bgpview_iter_t *iter)
{
  return __iter_pfx_get_state(iter);
}

#define __iter_pfx_get_user(iter) (__pfx_field(iter, user))

void *bgpview_iter_pfx_get_user(bgpview_iter_t *iter)
{
  return __iter_pfx_get_user(iter);
}

int bgpview_iter_pfx_set_user(bgpview_iter_t *iter, void *user)
{
  bwv_peerid_pfxinfo_t *pfxinfo = __pfx_peerinfos(iter);

  if (pfxinfo->user == user) {
    return 0;
  }

  if (pfxinfo->user != NULL && iter->view->pfx_user_destructor != NULL) {
    iter->view->pfx_user_destructor(pfxinfo->user);
  }
  pfxinfo->user = user;
  return 1;
}

#define __iter_peer_get_peer_id(iter)                                          \
  (kh_key(iter->view->peerinfo, iter->peer_it))

bgpstream_peer_id_t bgpview_iter_peer_get_peer_id(bgpview_iter_t *iter)
{
  return __iter_peer_get_peer_id(iter);
}

#define __iter_peer_get_sig(iter)                                              \
  (bgpstream_peer_sig_map_get_sig((iter)->view->peersigns,                     \
                                  __iter_peer_get_peer_id(iter)))

bgpstream_peer_sig_t *bgpview_iter_peer_get_sig(bgpview_iter_t *iter)
{
  return __iter_peer_get_sig(iter);
}

#define __peer_get_pfx_cnt(iter, state_mask, field)                            \
  (__cnt_by_mask(kh_value(iter->view->peerinfo, iter->peer_it).field,          \
                 state_mask))

#define __iter_peer_get_pfx_cnt(iter, version, state_mask)                     \
  (((version) == BGPSTREAM_ADDR_VERSION_IPV4)                                  \
     ? (__peer_get_pfx_cnt(iter, state_mask, v4_pfx_cnt))                      \
     : ((version) == BGPSTREAM_ADDR_VERSION_IPV6)                              \
         ? (__peer_get_pfx_cnt(iter, state_mask, v6_pfx_cnt))                  \
         : (__peer_get_pfx_cnt(iter, state_mask, v4_pfx_cnt) +                 \
            __peer_get_pfx_cnt(iter, state_mask, v6_pfx_cnt)))

int bgpview_iter_peer_get_pfx_cnt(bgpview_iter_t *iter, int version,
                                  uint8_t state_mask)
{
  return __iter_peer_get_pfx_cnt(iter, version, state_mask);
}

#define __peer_field(iter, field)                                              \
  (kh_val((iter)->view->peerinfo, (iter)->peer_it).field)

#define __iter_peer_get_state(iter) (__peer_field(iter, state))

bgpview_field_state_t bgpview_iter_peer_get_state(bgpview_iter_t *iter)
{
  return __iter_peer_get_state(iter);
}

#define __iter_peer_get_user(iter) (__peer_field(iter, user))

void *bgpview_iter_peer_get_user(bgpview_iter_t *iter)
{
  return __iter_peer_get_user(iter);
}

int bgpview_iter_peer_set_user(bgpview_iter_t *iter, void *user)
{
  void *cur_user = __peer_field(iter, user);

  if (cur_user == user) {
    return 0;
  }

  if (cur_user != NULL && iter->view->peer_user_destructor != NULL) {
    iter->view->peer_user_destructor(cur_user);
  }

  kh_val(iter->view->peerinfo, iter->peer_it).user = user;
  return 1;
}

#define __pfx_peer_field(iter, field)                                          \
  (BWV_PFX_GET_PEER_PTR((iter)->view, __pfx_peerinfos(iter),                   \
                        (iter)->pfx_peer_it)                                   \
     ->field)

#define __iter_pfx_peer_get_as_path_store_path(iter)                           \
  (bgpstream_as_path_store_get_store_path((iter)->view->pathstore,             \
                                          __pfx_peer_field(iter, as_path_id)))

bgpstream_as_path_store_path_t *
bgpview_iter_pfx_peer_get_as_path_store_path(bgpview_iter_t *iter)
{
  return __iter_pfx_peer_get_as_path_store_path(iter);
}

#define __iter_pfx_peer_get_as_path_store_path_id(iter)                        \
  (__pfx_peer_field(iter, as_path_id))

bgpstream_as_path_store_path_id_t
bgpview_iter_pfx_peer_get_as_path_store_path_id(bgpview_iter_t *iter)
{
  return __iter_pfx_peer_get_as_path_store_path_id(iter);
}

#define __iter_pfx_peer_get_as_path(iter)                                      \
  (bgpstream_as_path_store_path_get_path(                                      \
    __iter_pfx_peer_get_as_path_store_path(iter),                              \
    (__iter_peer_get_sig(iter))->peer_asnumber))

bgpstream_as_path_t *bgpview_iter_pfx_peer_get_as_path(bgpview_iter_t *iter)
{
  return __iter_pfx_peer_get_as_path(iter);
}

#define __iter_pfx_peer_get_origin_seg(iter)                                   \
  (bgpstream_as_path_store_path_get_origin_seg(                                \
    __iter_pfx_peer_get_as_path_store_path(iter)))

bgpstream_as_path_seg_t *
bgpview_iter_pfx_peer_get_origin_seg(bgpview_iter_t *iter)
{
  return __iter_pfx_peer_get_origin_seg(iter);
}

#define __iter_pfx_peer_as_path_seg_iter_reset(iter)                           \
  (bgpstream_as_path_store_path_iter_reset(                                    \
    __iter_pfx_peer_get_as_path_store_path(iter), &(iter)->pfx_peer_path_it,   \
    __iter_peer_get_sig(iter)->peer_asnumber))

void bgpview_iter_pfx_peer_as_path_seg_iter_reset(bgpview_iter_t *iter)
{
  __iter_pfx_peer_as_path_seg_iter_reset(iter);
}

#define __iter_pfx_peer_as_path_seg_next(iter)                                 \
  (bgpstream_as_path_store_path_get_next_seg(&iter->pfx_peer_path_it))

bgpstream_as_path_seg_t *
bgpview_iter_pfx_peer_as_path_seg_next(bgpview_iter_t *iter)
{
  return __iter_pfx_peer_as_path_seg_next(iter);
}

int bgpview_iter_pfx_peer_set_as_path(bgpview_iter_t *iter,
                                      bgpstream_as_path_t *as_path)
{
  bgpstream_as_path_store_path_id_t *id = &(__pfx_peer_field(iter, as_path_id));

  bgpstream_peer_sig_t *ps = __iter_peer_get_sig(iter);

  if (bgpstream_as_path_store_get_path_id(iter->view->pathstore, as_path,
                                          ps->peer_asnumber, id) != 0) {
    fprintf(stderr, "ERROR: Failed to get AS Path ID from store\n");
    return -1;
  }

  return 0;
}

int bgpview_iter_pfx_peer_set_as_path_by_id(
  bgpview_iter_t *iter, bgpstream_as_path_store_path_id_t path_id)
{
  (__pfx_peer_field(iter, as_path_id)) = path_id;
  return 0;
}

#define __iter_pfx_peer_get_state(iter)                                        \
  (BWV_PFX_GET_PEER_STATE(iter->view, __pfx_peerinfos(iter), iter->pfx_peer_it))

bgpview_field_state_t bgpview_iter_pfx_peer_get_state(bgpview_iter_t *iter)
{
  return __iter_pfx_peer_get_state(iter);
}

#define __iter_pfx_peer_get_user(iter)                                         \
  (BWV_PFX_GET_PEER_EXT(__pfx_peerinfos(iter), iter->pfx_peer_it).user)

void *bgpview_iter_pfx_peer_get_user(bgpview_iter_t *iter)
{
  ASSERT_BWV_PFX_PEERINFO_EXT(iter->view);
  return __iter_pfx_peer_get_user(iter);
}

int bgpview_iter_pfx_peer_set_user(bgpview_iter_t *iter, void *user)
{
  ASSERT_BWV_PFX_PEERINFO_EXT(iter->view);

  void *cur_user = __iter_pfx_peer_get_user(iter);

  if (cur_user == user) {
    return 0;
  }

  if (cur_user != NULL && iter->view->pfx_peer_user_destructor != NULL) {
    iter->view->pfx_peer_user_destructor(cur_user);
  }

  BWV_PFX_GET_PEER_EXT(__pfx_peerinfos(iter), iter->pfx_peer_it).user = user;
  return 1;
}

/* ==================== PEER ITERATORS ==================== */

/* internal macros, optimized for performance */

#define WHILE_NOT_MATCHED_PEER(iter)                                           \
  while (iter->peer_it != kh_end(iter->view->peerinfo) &&                      \
         (!kh_exist(iter->view->peerinfo, iter->peer_it) ||                    \
          !(iter->peer_state_mask &                                            \
            kh_val(iter->view->peerinfo, iter->peer_it).state)))

#define __iter_first_peer(iter, state_mask)                                    \
  do {                                                                         \
    iter->peer_it = kh_begin(iter->view->peerinfo);                            \
    iter->peer_state_mask = state_mask;                                        \
    iter->pfx_peer_it_valid = 0;                                               \
    /* keep searching if this does not exist */                                \
    WHILE_NOT_MATCHED_PEER(iter)                                               \
    {                                                                          \
      iter->peer_it++;                                                         \
    }                                                                          \
  } while (0)

#define __iter_next_peer(iter)                                                 \
  do {                                                                         \
    do {                                                                       \
      iter->peer_it++;                                                         \
    }                                                                          \
    WHILE_NOT_MATCHED_PEER(iter);                                              \
  } while (0)

#define __iter_has_more_peer(iter)                                             \
  (iter->peer_it != kh_end(iter->view->peerinfo))

#define __iter_seek_peer(iter, peerid, state_mask)                             \
  do {                                                                         \
    iter->peer_state_mask = state_mask;                                        \
    iter->peer_it = kh_get(bwv_peerid_peerinfo, iter->view->peerinfo, peerid); \
  } while (0)

/* external functions */

int bgpview_iter_first_peer(bgpview_iter_t *iter, uint8_t state_mask)
{
  __iter_first_peer(iter, state_mask);
  return __iter_has_more_peer(iter);
}

int bgpview_iter_next_peer(bgpview_iter_t *iter)
{
  __iter_next_peer(iter);
  return __iter_has_more_peer(iter);
}

int bgpview_iter_has_more_peer(bgpview_iter_t *iter)
{
  return __iter_has_more_peer(iter);
}

int bgpview_iter_seek_peer(bgpview_iter_t *iter, bgpstream_peer_id_t peerid,
                           uint8_t state_mask)
{
  iter->pfx_peer_it_valid = 0; // moving peer_it invalidates pfx_peer_it
  __iter_seek_peer(iter, peerid, state_mask);
  if (iter->peer_it == kh_end(iter->view->peerinfo)) {
    return 0;
  }
  if (iter->peer_state_mask &
      kh_val(iter->view->peerinfo, iter->peer_it).state) {
    return 1;
  }
  iter->peer_it = kh_end(iter->view->peerinfo);
  return 0;
}

/* ==================== PFX ITERATORS ==================== */

#define WHILE_NOT_MATCHED_PFX(iter, table)                                     \
  while ((iter)->pfx_it != kh_end((table)) &&   /* each hash item */           \
         (!kh_exist((table), (iter)->pfx_it) || /* in hash? */                 \
          !((iter)->pfx_state_mask &            /* correct state? */           \
            kh_val((table), (iter)->pfx_it)->state)))

#define __pfx_valid(iter, table) ((iter)->pfx_it != kh_end((table)))

#define RETURN_IF_PFX_VALID(iter, table)                                       \
  do {                                                                         \
    if (__pfx_valid(iter, table)) {                                            \
      return 1;                                                                \
    }                                                                          \
  } while (0)

int bgpview_iter_first_pfx(bgpview_iter_t *iter, int version,
                           uint8_t state_mask)
{
  // set the version we iterate through
  iter->version_filter = version;

  // set the version we start iterating through
  if (iter->version_filter == BGPSTREAM_ADDR_VERSION_IPV4 ||
      iter->version_filter == 0) {
    iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
  } else {
    iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV6;
  }

  // set the pfx mask
  iter->pfx_state_mask = state_mask;

  // moving pfx_it invalidates pfx_peer_it
  iter->pfx_peer_it_valid = 0;

  if (iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4) {
    iter->pfx_it = kh_begin(iter->view->v4pfxs);
    /* keep searching if this does not exist */
    WHILE_NOT_MATCHED_PFX(iter, iter->view->v4pfxs)
    {
      iter->pfx_it++;
    }
    RETURN_IF_PFX_VALID(iter, iter->view->v4pfxs);

    // no ipv4 prefix was found, we don't look for other versions
    // unless version_filter is zero
    if (iter->version_filter) {
      return 0;
    }

    // continue to the next IP version
    iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV6;
  }

  if (iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6) {
    iter->pfx_it = kh_begin(iter->view->v6pfxs);
    /* keep searching if this does not exist */
    WHILE_NOT_MATCHED_PFX(iter, iter->view->v6pfxs)
    {
      iter->pfx_it++;
    }
    RETURN_IF_PFX_VALID(iter, iter->view->v6pfxs);
  }

  return 0;
}

#define __iter_next_pfx_v4(iter)                                               \
  do {                                                                         \
    /* skip to the next v4 pfx */                                              \
    do {                                                                       \
      (iter)->pfx_it++;                                                        \
    }                                                                          \
    WHILE_NOT_MATCHED_PFX(iter, (iter)->view->v4pfxs);                         \
    /* if no v4 pfx, but considering all versions... */                        \
    if (__pfx_valid(iter, (iter)->view->v4pfxs) == 0 &&                        \
        (iter)->version_filter == 0) {                                         \
      /* skip to the first v6 pfx */                                           \
      bgpview_iter_first_pfx((iter), BGPSTREAM_ADDR_VERSION_IPV6,              \
                             (iter)->pfx_state_mask);                          \
    }                                                                          \
    /* here: valid v4, valid v6, or invalid */                                 \
  } while (0)

#define __iter_next_pfx_v6(iter)                                               \
  do {                                                                         \
    do {                                                                       \
      iter->pfx_it++;                                                          \
    }                                                                          \
    WHILE_NOT_MATCHED_PFX(iter, iter->view->v6pfxs);                           \
  } while (0)

#define __iter_next_pfx(iter)                                                  \
  do {                                                                         \
    (iter)->pfx_peer_it_valid = 0;                                             \
    if ((iter)->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4) {                  \
      __iter_next_pfx_v4(iter);                                                \
    } else {                                                                   \
      __iter_next_pfx_v6(iter);                                                \
    }                                                                          \
  } while (0)

#define __iter_has_more_pfx_v(iter, table) ((iter)->pfx_it != kh_end((table)))

#define __iter_has_more_pfx(iter)                                              \
  (((iter)->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4)                        \
     ? (__iter_has_more_pfx_v((iter), iter->view->v4pfxs))                     \
     : ((iter)->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6)                    \
         ? (__iter_has_more_pfx_v((iter), iter->view->v6pfxs))                 \
         : 0)

int bgpview_iter_next_pfx(bgpview_iter_t *iter)
{
  __iter_next_pfx(iter);
  return __iter_has_more_pfx(iter);
}

int bgpview_iter_has_more_pfx(bgpview_iter_t *iter)
{
  return __iter_has_more_pfx(iter);
}

int bgpview_iter_seek_pfx(bgpview_iter_t *iter, bgpstream_pfx_t *pfx,
                          uint8_t state_mask)
{
  iter->version_filter = pfx->address.version;
  iter->version_ptr = pfx->address.version;
  iter->pfx_state_mask = state_mask;
  iter->pfx_peer_it_valid = 0;
  iter->pfx_peer_it = 0;

  switch (pfx->address.version) {
  case BGPSTREAM_ADDR_VERSION_IPV4:
    iter->pfx_it = kh_get(bwv_v4pfx_peerid_pfxinfo, iter->view->v4pfxs,
                          pfx->bs_ipv4);
    if (iter->pfx_it == kh_end(iter->view->v4pfxs)) {
      return 0;
    }
    if (iter->pfx_state_mask &
        kh_val(iter->view->v4pfxs, iter->pfx_it)->state) {
      return 1;
    }
    // if the mask does not match, than set the iterator to the end
    iter->pfx_it = kh_end(iter->view->v4pfxs);
    return 0;

  case BGPSTREAM_ADDR_VERSION_IPV6:
    iter->pfx_it = kh_get(bwv_v6pfx_peerid_pfxinfo, iter->view->v6pfxs,
                          pfx->bs_ipv6);
    if (iter->pfx_it == kh_end(iter->view->v6pfxs)) {
      return 0;
    }
    if (iter->pfx_state_mask &
        kh_val(iter->view->v6pfxs, iter->pfx_it)->state) {
      return 1;
    }
    // if the mask does not match, than set the iterator to the end
    iter->pfx_it = kh_end(iter->view->v6pfxs);
    return 0;
  default:
    /* programming error */
    assert(0);
  }
  return 0;
}

/* ==================== PFX-PEER ITERATORS ==================== */

/* optimized macros. be careful when using these */

#define SCAN_FOR_MATCHING_PFX_PEER(iter, peertable)                            \
  do {                                                                         \
    for ( ; (iter)->pfx_peer_it != kh_end(peertable); ++(iter)->pfx_peer_it) { \
      if (!kh_exist(peertable, (iter)->pfx_peer_it)) continue;                 \
      if ((iter)->pfx_peer_state_mask &                                        \
          kh_val(peertable, (iter)->pfx_peer_it).state) {                      \
        __iter_seek_peer((iter), kh_key(peertable, (iter)->pfx_peer_it),       \
            (iter)->pfx_peer_state_mask);                                      \
        (iter)->pfx_peer_it_valid = 1;                                         \
        break;                                                                 \
      }                                                                        \
    }                                                                          \
  } while (0)

#define __iter_pfx_first_peer_tab(iter, peertable, state_mask)                 \
  do {                                                                         \
    (iter)->pfx_peer_state_mask = state_mask;                                  \
    (iter)->pfx_peer_it = 0;                                                   \
    (iter)->pfx_peer_it_valid = 0;                                             \
    if (!peertable) break;                                                     \
    SCAN_FOR_MATCHING_PFX_PEER(iter, peertable);                               \
  } while (0)

#define __iter_pfx_first_peer(iter, state_mask)                                \
  do {                                                                         \
    bwv_peerid_pfxinfo_t *__infos = __pfx_peerinfos((iter));                   \
    if ((iter)->view->disable_extended) {                                      \
      __iter_pfx_first_peer_tab(iter, __infos->peers_min, state_mask);         \
    } else {                                                                   \
      __iter_pfx_first_peer_tab(iter, __infos->peers_ext, state_mask);         \
    }                                                                          \
  } while (0)

#define __iter_pfx_next_peer_tab(iter, peertable)                              \
  do {                                                                         \
    (iter)->pfx_peer_it_valid = 0;                                             \
    (iter)->pfx_peer_it++;                                                     \
    SCAN_FOR_MATCHING_PFX_PEER(iter, peertable);                               \
  } while (0)

#define __iter_pfx_next_peer(iter)                                             \
  do {                                                                         \
    bwv_peerid_pfxinfo_t *__infos = __pfx_peerinfos((iter));                   \
    if ((iter)->view->disable_extended) {                                      \
      __iter_pfx_next_peer_tab(iter, __infos->peers_min);                      \
    } else {                                                                   \
      __iter_pfx_next_peer_tab(iter, __infos->peers_ext);                      \
    }                                                                          \
  } while (0)

#define __iter_pfx_has_more_peer(iter)                                         \
  ((iter)->pfx_peer_it_valid)

#define __iter_pfx_seek_peer_tab(iter, tabtype, peertable, peerid, state_mask) \
  do {                                                                         \
    (iter)->pfx_peer_state_mask = state_mask;                                  \
    khiter_t k;                                                                \
    if (peertable &&                                                           \
        (k = kh_get(tabtype, peertable, peerid)) != kh_end(peertable) &&       \
        ((iter)->pfx_peer_state_mask & kh_val(peertable, k).state)) {          \
      (iter)->pfx_peer_it_valid = 1;                                           \
      (iter)->pfx_peer_it = k;                                                 \
      __iter_seek_peer((iter), peerid, state_mask);                            \
    } else {                                                                   \
      iter->pfx_peer_it_valid = 0;                                             \
    }                                                                          \
  } while (0)

#define __iter_pfx_seek_peer(iter, peerid, state_mask)                         \
  do {                                                                         \
    bwv_peerid_pfxinfo_t *__infos = __pfx_peerinfos((iter));                   \
    if ((iter)->view->disable_extended) {                                      \
      __iter_pfx_seek_peer_tab(iter, bwv_peerid_pfx_peerinfo,                  \
          __infos->peers_min, peerid, state_mask);                             \
    } else {                                                                   \
      __iter_pfx_seek_peer_tab(iter, bwv_peerid_pfx_peerinfo_ext,              \
          __infos->peers_ext, peerid, state_mask);                             \
    }                                                                          \
  } while (0)

/* public accessor functions */

int bgpview_iter_pfx_first_peer(bgpview_iter_t *iter, uint8_t state_mask)
{
  __iter_pfx_first_peer(iter, state_mask);
  assert(iter->pfx_peer_it_valid == 0 || __iter_has_more_peer(iter));
  return (iter->pfx_peer_it_valid);
}

int bgpview_iter_pfx_next_peer(bgpview_iter_t *iter)
{
  __iter_pfx_next_peer(iter);
  assert(iter->pfx_peer_it_valid == 0 || __iter_has_more_peer(iter));
  return (iter->pfx_peer_it_valid);
}

int bgpview_iter_pfx_has_more_peer(bgpview_iter_t *iter)
{
  return __iter_pfx_has_more_peer(iter);
}

int bgpview_iter_pfx_seek_peer(bgpview_iter_t *iter, bgpstream_peer_id_t peerid,
                               uint8_t state_mask)
{
  __iter_pfx_seek_peer(iter, peerid, state_mask);
  return (iter->pfx_peer_it_valid);
}

/* =================== ALL-PFX-PEER ITERATORS ==================== */

int bgpview_iter_first_pfx_peer(bgpview_iter_t *iter, int version,
                                uint8_t pfx_mask, uint8_t peer_mask)
{
  // set the version(s) we iterate through
  iter->version_filter = version;

  // set the version we start iterating through
  iter->version_ptr = (iter->version_filter == BGPSTREAM_ADDR_VERSION_IPV6)
                        ? BGPSTREAM_ADDR_VERSION_IPV6
                        : BGPSTREAM_ADDR_VERSION_IPV4;

  // masks are going to be set by each first function
  iter->pfx_state_mask = 0;
  iter->pfx_peer_state_mask = 0;

  // start from the first matching prefix
  bgpview_iter_first_pfx(iter, version, pfx_mask);
  while (__iter_has_more_pfx(iter)) {
    // look for the first matching peer within the prefix
    if (bgpview_iter_pfx_first_peer(iter, peer_mask)) {
      return 1;
    }
    __iter_next_pfx(iter);
  }
  return 0;
}

#define __iter_has_more_pfx_peer(iter)                                         \
  (__iter_has_more_pfx(iter) && __iter_pfx_has_more_peer(iter))

#define __iter_next_pfx_peer(iter)                                             \
  do {                                                                         \
    while (__iter_has_more_pfx(iter)) {                                        \
      /* look for the next matching peer within the prefix */                  \
      __iter_pfx_next_peer(iter);                                              \
      if (__iter_pfx_has_more_peer(iter))                                      \
        break;                                                                 \
      /* no more peers, go to the next prefix */                               \
      __iter_next_pfx(iter);                                                   \
      if (__iter_has_more_pfx(iter)) {                                         \
        /* go to the first peer */                                             \
        __iter_pfx_first_peer(iter, iter->pfx_peer_state_mask);                \
        break;                                                                 \
      }                                                                        \
    }                                                                          \
  } while (0)

int bgpview_iter_next_pfx_peer(bgpview_iter_t *iter)
{
  __iter_next_pfx_peer(iter);
  return __iter_has_more_pfx_peer(iter);
}

int bgpview_iter_has_more_pfx_peer(bgpview_iter_t *iter)
{
  return bgpview_iter_has_more_pfx(iter) &&
         bgpview_iter_pfx_has_more_peer(iter);
}

int bgpview_iter_seek_pfx_peer(bgpview_iter_t *iter, bgpstream_pfx_t *pfx,
                               bgpstream_peer_id_t peerid, uint8_t pfx_mask,
                               uint8_t peer_mask)
{
  if (bgpview_iter_seek_pfx(iter, pfx, pfx_mask) &&
      bgpview_iter_pfx_seek_peer(iter, peerid, peer_mask)) {
    return 1;
  }

  // if the peer is not found we reset the iterators
  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
  iter->pfx_it = kh_end(iter->view->v4pfxs);
  iter->pfx_peer_it_valid = 0;
  iter->pfx_peer_it = 0;

  return 0;
}

/* ==================== CREATION FUNCS ==================== */

bgpstream_peer_id_t bgpview_iter_add_peer(bgpview_iter_t *iter,
                                          const char *collector_str,
                                          bgpstream_ip_addr_t *peer_address,
                                          uint32_t peer_asnumber)
{
  bgpstream_peer_id_t peer_id;
  khiter_t k;
  int khret;

  /* add peer to signatures' map */
  if ((peer_id =
         bgpstream_peer_sig_map_get_id(iter->view->peersigns, collector_str,
                                       peer_address, peer_asnumber)) == 0) {
    fprintf(stderr, "Could not add peer to peersigns\n");
    fprintf(stderr, "Consider making bgpstream_peer_sig_map_set more robust\n");
    return 0;
  }

  /* populate peer information in peerinfo */

  if ((k = kh_get(bwv_peerid_peerinfo, iter->view->peerinfo, peer_id)) ==
      kh_end(iter->view->peerinfo)) {
    /* new peer!  */
    k = kh_put(bwv_peerid_peerinfo, iter->view->peerinfo, peer_id, &khret);
    memset(&kh_val(iter->view->peerinfo, k), 0, sizeof(bwv_peerinfo_t));
    /* peer is invalid */
  }

  /* seek the iterator */
  iter->peer_it = k;
  iter->peer_state_mask = BGPVIEW_FIELD_ALL_VALID;
  iter->pfx_peer_it_valid = 0; // moving peer_it invalidates pfx_peer_it

  /* here iter->peer_it points to a peer, it could be invalid, inactive,
     active */
  if (kh_val(iter->view->peerinfo, k).state != BGPVIEW_FIELD_INVALID) {
    /* it was already here, and it was inactive/active, just return */
    return peer_id;
  }

  /* by here, it was invalid */
  kh_val(iter->view->peerinfo, k).state = BGPVIEW_FIELD_INACTIVE;

  /* and count one more inactive peer */
  iter->view->peerinfo_cnt[BGPVIEW_FIELD_INACTIVE]++;

  return peer_id;
}

int bgpview_iter_remove_peer(bgpview_iter_t *iter)
{
  bgpview_iter_t *lit;
  /* we have to have a valid peer */
  assert(__iter_has_more_peer(iter));

  /* if the peer is active, then we deactivate it first */
  if (bgpview_iter_peer_get_state(iter) == BGPVIEW_FIELD_ACTIVE) {
    bgpview_iter_deactivate_peer(iter);
  }
  assert(bgpview_iter_peer_get_state(iter) == BGPVIEW_FIELD_INACTIVE);

  /* if the peer had prefixes, then we need to remove all pfx-peers for this
     peer */
  if (bgpview_iter_peer_get_pfx_cnt(iter, 0, BGPVIEW_FIELD_ALL_VALID) > 0) {
    lit = bgpview_iter_create(iter->view);
    assert(lit != NULL);
    for (bgpview_iter_first_pfx_peer(lit, 0, BGPVIEW_FIELD_ALL_VALID,
                                     BGPVIEW_FIELD_ALL_VALID);
         bgpview_iter_has_more_pfx_peer(lit); bgpview_iter_next_pfx_peer(lit)) {
      // remove all the peer-pfx associated with the peer
      if (bgpview_iter_peer_get_peer_id(iter) ==
          bgpview_iter_peer_get_peer_id(lit)) {
        bgpview_iter_pfx_remove_peer(lit);
      }
    }
    bgpview_iter_destroy(lit);
  }

  /* set the state to invalid and reset the counters */
  peerinfo_reset(&kh_value(iter->view->peerinfo, iter->peer_it));
  iter->view->need_gc_peerinfo = 1;
  iter->view->peerinfo_cnt[BGPVIEW_FIELD_INACTIVE]--;

  return 0;
}

int bgpview_iter_add_pfx_peer(bgpview_iter_t *iter,
                                   bgpstream_pfx_t *pfx,
                                   bgpstream_peer_id_t peer_id,
                                   bgpstream_as_path_t *as_path)
{
  bgpstream_as_path_store_path_id_t path_id;
  bgpstream_peer_sig_t *ps;

  /* first seek to the prefix */
  if (bgpview_iter_seek_pfx(iter, pfx, BGPVIEW_FIELD_ALL_VALID) == 0) {
    /* we have to first create (or un-invalid) the prefix */
    if (add_pfx(iter, pfx) != 0) {
      return -1;
    }
  }

  /* the peer must already exist */
  __iter_seek_peer(iter, peer_id, BGPVIEW_FIELD_ALL_VALID);
  iter->pfx_peer_it_valid = 0; // moving peer_it invalidates pfx_peer_it
  if (iter->peer_it == kh_end(iter->view->peerinfo)) {
    return -1;
  }
  /* get the peer ASN */
  ps = bgpview_iter_peer_get_sig(iter);

  if (bgpstream_as_path_store_get_path_id(iter->view->pathstore, as_path,
                                          ps->peer_asnumber, &path_id) != 0) {
    fprintf(stderr, "ERROR: Failed to get AS Path ID from store\n");
    return -1;
  }

  /* now insert the prefix-peer info */
  return bgpview_iter_pfx_add_peer_by_id(iter, peer_id, path_id);
}

int bgpview_iter_add_pfx_peer_by_id(bgpview_iter_t *iter, bgpstream_pfx_t *pfx,
                                    bgpstream_peer_id_t peer_id,
                                    bgpstream_as_path_store_path_id_t path_id)
{
  /* the peer must already exist */
  if (bgpview_iter_seek_peer(iter, peer_id, BGPVIEW_FIELD_ALL_VALID) == 0) {
    return -1;
  }

  /* now seek to the prefix */
  if (bgpview_iter_seek_pfx(iter, pfx, BGPVIEW_FIELD_ALL_VALID) == 0) {
    /* we have to first create (or un-invalid) the prefix */
    if (add_pfx(iter, pfx) != 0) {
      return -1;
    }
  }

  /* now insert the prefix-peer info */
  return bgpview_iter_pfx_add_peer_by_id(iter, peer_id, path_id);
}

int bgpview_iter_remove_pfx(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = __pfx_peerinfos(iter);
  bgpview_iter_t ti;

  /* if the pfx is active, then we deactivate it first */
  if (bgpview_iter_pfx_get_state(iter) == BGPVIEW_FIELD_ACTIVE) {
    bgpview_iter_deactivate_pfx(iter);
  }

  assert(pfxinfo->state == BGPVIEW_FIELD_INACTIVE);

  pfxinfo->state = BGPVIEW_FIELD_INVALID;

  /* if there are any active or inactive pfx-peers, we remove them now */
  if (bgpview_iter_pfx_get_peer_cnt(iter, BGPVIEW_FIELD_ALL_VALID) > 0) {
    ti = *iter;
    /* iterate over all pfx-peers for this pfx */
    __iter_pfx_first_peer(&ti, BGPVIEW_FIELD_ALL_VALID);
    while (bgpview_iter_pfx_has_more_peer(&ti)) {
      bgpview_iter_pfx_remove_peer(&ti);
      __iter_pfx_next_peer(&ti);
    }
  }

  assert(pfxinfo->peers_cnt[BGPVIEW_FIELD_INACTIVE] == 0 &&
         pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] == 0);

  /* set the state to invalid and update counters */

  switch (iter->version_ptr) {
  case BGPSTREAM_ADDR_VERSION_IPV4:
    iter->view->v4pfxs_cnt[BGPVIEW_FIELD_INACTIVE]--;
    iter->view->need_gc_v4pfxs = 1;
    break;

  case BGPSTREAM_ADDR_VERSION_IPV6:
    iter->view->v6pfxs_cnt[BGPVIEW_FIELD_INACTIVE]--;
    iter->view->need_gc_v6pfxs = 1;
    break;

  default:
    return -1;
  }

  return 0;
}

int bgpview_iter_pfx_add_peer(bgpview_iter_t *iter, bgpstream_peer_id_t peer_id,
                              bgpstream_as_path_t *as_path)
{
  bgpstream_as_path_store_path_id_t path_id;

  /* get the peer ASN */
  if (bgpstream_as_path_store_get_path_id(
        iter->view->pathstore, as_path,
        __iter_peer_get_sig(iter)->peer_asnumber, &path_id) != 0) {
    fprintf(stderr, "ERROR: Failed to get AS Path ID from store\n");
    return -1;
  }

  __iter_seek_peer(iter, peer_id, BGPVIEW_FIELD_ALL_VALID);

  return peerid_pfxinfo_insert(iter, __pfx_peerinfos(iter), peer_id, path_id);
}

int bgpview_iter_pfx_add_peer_by_id(bgpview_iter_t *iter,
                                    bgpstream_peer_id_t peer_id,
                                    bgpstream_as_path_store_path_id_t path_id)
{
  /* this code is mostly a duplicate of the above func, for efficiency */
  __iter_seek_peer(iter, peer_id, BGPVIEW_FIELD_ALL_VALID);

  return peerid_pfxinfo_insert(iter, __pfx_peerinfos(iter), peer_id, path_id);
}

int bgpview_iter_pfx_remove_peer(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = __pfx_peerinfos(iter);

  /* if the pfx-peer is active, then we deactivate it first */
  if (__iter_pfx_peer_get_state(iter) == BGPVIEW_FIELD_ACTIVE) {
    bgpview_iter_pfx_deactivate_peer(iter);
  }

  assert(BWV_PFX_GET_PEER_STATE(iter->view, pfxinfo, iter->pfx_peer_it) ==
         BGPVIEW_FIELD_INACTIVE);

  /* now, simply set the state to invalid and reset the pfx counters */
  // XXX view->need_gc_pfx_peers = 1;
  BWV_PFX_SET_PEER_STATE(iter->view, pfxinfo, iter->pfx_peer_it,
      BGPVIEW_FIELD_INVALID);
  pfxinfo->peers_cnt[BGPVIEW_FIELD_INACTIVE]--;

  assert(__iter_has_more_peer(iter));
  switch (iter->version_ptr) {
  case BGPSTREAM_ADDR_VERSION_IPV4:
    kh_value(iter->view->peerinfo, iter->peer_it)
      .v4_pfx_cnt[BGPVIEW_FIELD_INACTIVE]--;
    break;

  case BGPSTREAM_ADDR_VERSION_IPV6:
    kh_value(iter->view->peerinfo, iter->peer_it)
      .v6_pfx_cnt[BGPVIEW_FIELD_INACTIVE]--;
    break;

  default:
    return -1;
  }

  /* if there are no peers left in this pfx, the pfx should be removed */
  if (pfxinfo->state != BGPVIEW_FIELD_INVALID &&
      pfxinfo->peers_cnt[BGPVIEW_FIELD_INACTIVE] == 0 &&
      pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] == 0) {
    return bgpview_iter_remove_pfx(iter);
  }

  return 0;
}

/* ==================== ACTIVATE/DEACTIVATE ==================== */

#define ACTIVATE_FIELD_CNT(field)                                              \
  do {                                                                         \
    field[BGPVIEW_FIELD_INACTIVE]--;                                           \
    field[BGPVIEW_FIELD_ACTIVE]++;                                             \
  } while (0);

#define DEACTIVATE_FIELD_CNT(field)                                            \
  do {                                                                         \
    field[BGPVIEW_FIELD_INACTIVE]++;                                           \
    field[BGPVIEW_FIELD_ACTIVE]--;                                             \
  } while (0)

int bgpview_iter_activate_peer(bgpview_iter_t *iter)
{
  assert(__iter_has_more_peer(iter));
  assert(__iter_peer_get_state(iter) > 0);

  if (__iter_peer_get_state(iter) != BGPVIEW_FIELD_INACTIVE) {
    return 0;
  }

  kh_val(iter->view->peerinfo, iter->peer_it).state = BGPVIEW_FIELD_ACTIVE;
  ACTIVATE_FIELD_CNT(iter->view->peerinfo_cnt);
  return 1;
}

int bgpview_iter_deactivate_peer(bgpview_iter_t *iter)
{
  assert(__iter_has_more_peer(iter));
  assert(__iter_peer_get_state(iter) > 0);

  bgpview_iter_t *lit;
  bgpstream_peer_id_t current_id;

  if (__iter_peer_get_state(iter) != BGPVIEW_FIELD_ACTIVE) {
    return 0;
  }

  /* only do the massive work of deactivating all pfx-peers if this peer has any
     active pfxs */
  if (__iter_peer_get_pfx_cnt(iter, 0, BGPVIEW_FIELD_ACTIVE) > 0) {
    lit = bgpview_iter_create(iter->view);
    assert(lit != NULL);
    current_id = __iter_peer_get_peer_id(iter);

    bgpview_iter_first_pfx_peer(lit, 0, BGPVIEW_FIELD_ACTIVE,
                                BGPVIEW_FIELD_ACTIVE);
    while (__iter_has_more_pfx_peer(lit)) {
      // deactivate all the peer-pfx associated with the peer
      if (__iter_peer_get_peer_id(lit) == current_id) {
        bgpview_iter_pfx_deactivate_peer(lit);
      }
      __iter_next_pfx_peer(lit);
    }
    bgpview_iter_destroy(lit);
  }

  /* mark as inactive */
  kh_val(iter->view->peerinfo, iter->peer_it).state = BGPVIEW_FIELD_INACTIVE;

  /* update the counters */
  DEACTIVATE_FIELD_CNT(iter->view->peerinfo_cnt);

  return 1;
}

static inline int activate_pfx(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = __pfx_peerinfos(iter);

  assert(pfxinfo->state > 0);
  if (pfxinfo->state != BGPVIEW_FIELD_INACTIVE) {
    return 0;
  }

  pfxinfo->state = BGPVIEW_FIELD_ACTIVE;

  switch (iter->version_ptr) {
  case BGPSTREAM_ADDR_VERSION_IPV4:
    ACTIVATE_FIELD_CNT(iter->view->v4pfxs_cnt);
    break;

  case BGPSTREAM_ADDR_VERSION_IPV6:
    ACTIVATE_FIELD_CNT(iter->view->v6pfxs_cnt);
    break;

  default:
    return -1;
  }

  return 1;
}

int bgpview_iter_deactivate_pfx(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = __pfx_peerinfos(iter);
  bgpview_iter_t ti = *iter;

  assert(pfxinfo->state > 0);
  if (pfxinfo->state != BGPVIEW_FIELD_ACTIVE) {
    return 0;
  }

  /* now mark the pfx as inactive */
  pfxinfo->state = BGPVIEW_FIELD_INACTIVE;

  /* deactivate all pfx-peers for this prefix */
  __iter_pfx_first_peer(&ti, BGPVIEW_FIELD_ACTIVE);
  while (__iter_pfx_has_more_peer(&ti)) {
    bgpview_iter_pfx_deactivate_peer(&ti);
    __iter_pfx_next_peer(&ti);
  }

  /* now update the counters */
  switch (iter->version_ptr) {
  case BGPSTREAM_ADDR_VERSION_IPV4:
    DEACTIVATE_FIELD_CNT(iter->view->v4pfxs_cnt);
    break;

  case BGPSTREAM_ADDR_VERSION_IPV6:
    DEACTIVATE_FIELD_CNT(iter->view->v6pfxs_cnt);
    break;

  default:
    return -1;
  }

  return 1;
}

int bgpview_iter_pfx_activate_peer(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = __pfx_peerinfos(iter);

  assert(__iter_pfx_has_more_peer(iter));

  assert(BWV_PFX_GET_PEER_STATE(iter->view, pfxinfo, iter->pfx_peer_it) > 0);
  if (BWV_PFX_GET_PEER_STATE(iter->view, pfxinfo, iter->pfx_peer_it) !=
      BGPVIEW_FIELD_INACTIVE) {
    return 0;
  }

  /* update the number of peers that observe this pfx */
  ACTIVATE_FIELD_CNT(pfxinfo->peers_cnt);

  /* this is the first active peer, so pfx must be activated */
  if (pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] == 1) {
    activate_pfx(iter);
  }

  /* the peer MUST be active */
  assert(__iter_peer_get_state(iter) == BGPVIEW_FIELD_ACTIVE);

  // increment the number of prefixes observed by the peer
  switch (iter->version_ptr) {
  case BGPSTREAM_ADDR_VERSION_IPV4:
    ACTIVATE_FIELD_CNT(
      kh_value(iter->view->peerinfo, iter->peer_it).v4_pfx_cnt);
    break;
  case BGPSTREAM_ADDR_VERSION_IPV6:
    ACTIVATE_FIELD_CNT(
      kh_value(iter->view->peerinfo, iter->peer_it).v6_pfx_cnt);
    break;
  default:
    return -1;
  }

  BWV_PFX_SET_PEER_STATE(iter->view, pfxinfo, iter->pfx_peer_it,
      BGPVIEW_FIELD_ACTIVE);

  return 1;
}

int bgpview_iter_pfx_deactivate_peer(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = __pfx_peerinfos(iter);

  assert(__iter_pfx_has_more_peer(iter));

  assert(BWV_PFX_GET_PEER_STATE(iter->view, pfxinfo, iter->pfx_peer_it) > 0);
  if (BWV_PFX_GET_PEER_STATE(iter->view, pfxinfo, iter->pfx_peer_it) !=
      BGPVIEW_FIELD_ACTIVE) {
    return 0;
  }

  /* set the state to inactive */
  BWV_PFX_SET_PEER_STATE(iter->view, pfxinfo, iter->pfx_peer_it,
      BGPVIEW_FIELD_INACTIVE);

  /* update the number of peers that observe the pfx */
  DEACTIVATE_FIELD_CNT(pfxinfo->peers_cnt);
  if (pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] == 0) {
    bgpview_iter_deactivate_pfx(iter);
  }

  // decrement the number of pfxs observed by the peer
  switch (iter->version_ptr) {
  case BGPSTREAM_ADDR_VERSION_IPV4:
    DEACTIVATE_FIELD_CNT(
      kh_value(iter->view->peerinfo, iter->peer_it).v4_pfx_cnt);
    break;
  case BGPSTREAM_ADDR_VERSION_IPV6:
    DEACTIVATE_FIELD_CNT(
      kh_value(iter->view->peerinfo, iter->peer_it).v6_pfx_cnt);
    break;
  default:
    return -1;
  }

  return 1;
}

/* ========== PUBLIC FUNCTIONS ========== */

bgpview_t *
bgpview_create_shared(bgpstream_peer_sig_map_t *peersigns,
                      bgpstream_as_path_store_t *pathstore,
                      bgpview_destroy_user_t *bwv_user_destructor,
                      bgpview_destroy_user_t *bwv_peer_user_destructor,
                      bgpview_destroy_user_t *bwv_pfx_user_destructor,
                      bgpview_destroy_user_t *bwv_pfx_peer_user_destructor)
{
  bgpview_t *view;
  struct timeval time_created;

  if ((view = malloc_zero(sizeof(bgpview_t))) == NULL) {
    return NULL;
  }

  if ((view->v4pfxs = kh_init(bwv_v4pfx_peerid_pfxinfo)) == NULL) {
    goto err;
  }

  if ((view->v6pfxs = kh_init(bwv_v6pfx_peerid_pfxinfo)) == NULL) {
    goto err;
  }

  if (peersigns != NULL) {
    view->peersigns_shared = 1;
    view->peersigns = peersigns;
  } else {
    if ((view->peersigns = bgpstream_peer_sig_map_create()) == NULL) {
      fprintf(stderr, "Failed to create peersigns table\n");
      goto err;
    }
    view->peersigns_shared = 0;
  }

  if (pathstore != NULL) {
    view->pathstore_shared = 1;
    view->pathstore = pathstore;
  } else {
    if ((view->pathstore = bgpstream_as_path_store_create()) == NULL) {
      fprintf(stderr, "Failed to create AS Path Store\n");
      goto err;
    }
    view->pathstore_shared = 0;
  }

  if ((view->peerinfo = kh_init(bwv_peerid_peerinfo)) == NULL) {
    fprintf(stderr, "Failed to create peer info table\n");
    goto err;
  }

  gettimeofday(&time_created, NULL);
  view->time_created = time_created.tv_sec;

  view->user_destructor = bwv_user_destructor;
  view->peer_user_destructor = bwv_peer_user_destructor;
  view->pfx_user_destructor = bwv_pfx_user_destructor;
  view->pfx_peer_user_destructor = bwv_pfx_peer_user_destructor;

  /* all other fields are memset to 0 */

  return view;

err:
  fprintf(stderr, "Failed to create BGPView\n");
  bgpview_destroy(view);
  return NULL;
}

bgpview_t *bgpview_create(bgpview_destroy_user_t *bwv_user_destructor,
                          bgpview_destroy_user_t *bwv_peer_user_destructor,
                          bgpview_destroy_user_t *bwv_pfx_user_destructor,
                          bgpview_destroy_user_t *bwv_pfx_peer_user_destructor)
{
  return bgpview_create_shared(
    NULL, NULL, bwv_user_destructor, bwv_peer_user_destructor,
    bwv_pfx_user_destructor, bwv_pfx_peer_user_destructor);
}

void bgpview_destroy(bgpview_t *view)
{
  if (view == NULL) {
    return;
  }

  khiter_t k;

  if (view->v4pfxs != NULL) {
    for (k = kh_begin(view->v4pfxs); k != kh_end(view->v4pfxs); ++k) {
      if (kh_exist(view->v4pfxs, k)) {
        peerid_pfxinfo_destroy(view, kh_value(view->v4pfxs, k));
      }
    }
    kh_destroy(bwv_v4pfx_peerid_pfxinfo, view->v4pfxs);
    view->v4pfxs = NULL;
  }

  if (view->v6pfxs != NULL) {
    for (k = kh_begin(view->v6pfxs); k != kh_end(view->v6pfxs); ++k) {
      if (kh_exist(view->v6pfxs, k)) {
        peerid_pfxinfo_destroy(view, kh_value(view->v6pfxs, k));
      }
    }
    kh_destroy(bwv_v6pfx_peerid_pfxinfo, view->v6pfxs);
    view->v6pfxs = NULL;
  }

  if (view->peersigns_shared == 0 && view->peersigns != NULL) {
    bgpstream_peer_sig_map_destroy(view->peersigns);
    view->peersigns = NULL;
  }

  if (view->pathstore_shared == 0 && view->pathstore != NULL) {
    bgpstream_as_path_store_destroy(view->pathstore);
    view->pathstore = NULL;
  }

  if (view->peerinfo != NULL) {
    peerinfo_destroy_user(view);
    kh_destroy(bwv_peerid_peerinfo, view->peerinfo);
    view->peerinfo = NULL;
  }

  if (view->user != NULL) {
    if (view->user_destructor != NULL) {
      view->user_destructor(view->user);
    }
    view->user = NULL;
  }

  free(view);
}

void bgpview_clear(bgpview_t *view)
{
  struct timeval time_created;
  bwv_peerid_pfxinfo_t *pfxinfo;
  bgpview_iter_t *lit = bgpview_iter_create(view);
  assert(lit != NULL);

  view->time = 0;

  gettimeofday(&time_created, NULL);
  view->time_created = time_created.tv_sec;

  /* mark all prefixes as invalid */
  bgpview_iter_first_pfx(lit, 0, BGPVIEW_FIELD_ALL_VALID);
  while (__iter_has_more_pfx(lit)) {
    pfxinfo = __pfx_peerinfos(lit);
    pfxinfo->peers_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
    pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] = 0;
    pfxinfo->state = BGPVIEW_FIELD_INVALID;
    if (view->disable_extended) {
      kh_clear(bwv_peerid_pfx_peerinfo, pfxinfo->peers_min);
    } else {
      kh_clear(bwv_peerid_pfx_peerinfo_ext, pfxinfo->peers_ext);
    }
    __iter_next_pfx(lit);
  }
  view->need_gc_v4pfxs = (kh_size(view->v4pfxs) > 0);
  view->need_gc_v6pfxs = (kh_size(view->v6pfxs) > 0);
  view->v4pfxs_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
  view->v4pfxs_cnt[BGPVIEW_FIELD_ACTIVE] = 0;
  view->v6pfxs_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
  view->v6pfxs_cnt[BGPVIEW_FIELD_ACTIVE] = 0;

  /* clear out the peerinfo table */
  __iter_first_peer(lit, BGPVIEW_FIELD_ALL_VALID);
  while (__iter_has_more_peer(lit)) {
    peerinfo_reset(&kh_value(view->peerinfo, lit->peer_it));
    __iter_next_peer(lit);
  }
  view->need_gc_peerinfo = (kh_size(view->peerinfo) > 0);
  view->peerinfo_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
  view->peerinfo_cnt[BGPVIEW_FIELD_ACTIVE] = 0;

  bgpview_iter_destroy(lit);
}

void bgpview_gc(bgpview_t *view)
{
  khiter_t k;

  /* note: in the current implementation we don't free pfx-peers for pfxs that
     are not invalid since it would be an expensive walk and/or we just haven't
     implemented it yet. */

  if (view->need_gc_v4pfxs) {
    for (k = kh_begin(view->v4pfxs); k != kh_end(view->v4pfxs); ++k) {
      if (kh_exist(view->v4pfxs, k) &&
          kh_value(view->v4pfxs, k)->state == BGPVIEW_FIELD_INVALID) {
        peerid_pfxinfo_destroy(view, kh_value(view->v4pfxs, k));
        kh_del(bwv_v4pfx_peerid_pfxinfo, view->v4pfxs, k);
      }
    }
    view->need_gc_v4pfxs = 0;
  }

  if (view->need_gc_v6pfxs) {
    for (k = kh_begin(view->v6pfxs); k != kh_end(view->v6pfxs); ++k) {
      if (kh_exist(view->v6pfxs, k) &&
          kh_value(view->v6pfxs, k)->state == BGPVIEW_FIELD_INVALID) {
        peerid_pfxinfo_destroy(view, kh_value(view->v6pfxs, k));
        kh_del(bwv_v6pfx_peerid_pfxinfo, view->v6pfxs, k);
      }
    }
    view->need_gc_v6pfxs = 0;
  }

  if (view->need_gc_peerinfo) {
    for (k = kh_begin(view->peerinfo); k != kh_end(view->peerinfo); ++k) {
      if (kh_exist(view->peerinfo, k) &&
          kh_value(view->peerinfo, k).state == BGPVIEW_FIELD_INVALID) {
        if (view->peer_user_destructor != NULL &&
            kh_value(view->peerinfo, k).user != NULL) {
          view->peer_user_destructor(kh_value(view->peerinfo, k).user);
        }
        kh_del(bwv_peerid_peerinfo, view->peerinfo, k);
      }
    }
    view->need_gc_peerinfo = 0;
  }
}

int bgpview_copy(bgpview_t *dst, bgpview_t *src)
{
  bgpview_iter_t *src_iter = NULL;
  bgpview_iter_t *dst_iter = NULL;

  bgpstream_peer_sig_t *ps;
  bgpstream_peer_id_t src_id, dst_id;
  bgpstream_peer_id_t dstids[UINT16_MAX];

  int first;
  bgpstream_pfx_t *pfx;
  bgpstream_as_path_store_path_id_t pathid;
  bgpstream_as_path_t *path;

  dst->time = src->time;

  if (((src_iter = bgpview_iter_create(src)) == NULL) ||
      ((dst_iter = bgpview_iter_create(dst)) == NULL)) {
    goto err;
  }

  for (bgpview_iter_first_peer(src_iter, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_peer(src_iter); bgpview_iter_next_peer(src_iter)) {
    ps = bgpview_iter_peer_get_sig(src_iter);
    src_id = bgpview_iter_peer_get_peer_id(src_iter);
    if ((dst_id = bgpview_iter_add_peer(
           dst_iter, ps->collector_str,
           &ps->peer_ip_addr, ps->peer_asnumber)) == 0) {
      goto err;
    }
    dstids[src_id] = dst_id;
    bgpview_iter_activate_peer(dst_iter);
  }

  for (bgpview_iter_first_pfx(src_iter, 0, BGPVIEW_FIELD_ACTIVE);
       bgpview_iter_has_more_pfx(src_iter); bgpview_iter_next_pfx(src_iter)) {
    first = 1;
    pfx = bgpview_iter_pfx_get_pfx(src_iter);
    for (bgpview_iter_pfx_first_peer(src_iter, BGPVIEW_FIELD_ACTIVE);
         bgpview_iter_pfx_has_more_peer(src_iter);
         bgpview_iter_pfx_next_peer(src_iter)) {
      src_id = bgpview_iter_peer_get_peer_id(src_iter);
      dst_id = dstids[src_id];
      pathid = bgpview_iter_pfx_peer_get_as_path_store_path_id(src_iter);

      /* if they share tables, be more efficient */
      if (dst->pathstore == src->pathstore) {
        if (first != 0) {
          /* this is the first pfx-peer for this prefix */
          if (bgpview_iter_add_pfx_peer_by_id(dst_iter, pfx, dst_id, pathid) !=
              0) {
            goto err;
          }
          first = 0;
        } else {
          if (bgpview_iter_pfx_add_peer_by_id(dst_iter, dst_id, pathid) != 0) {
            goto err;
          }
        }
      } else {
        /* inefficiently copy */
        path = bgpview_iter_pfx_peer_get_as_path(src_iter);

        if (first != 0) {
          if (bgpview_iter_add_pfx_peer(dst_iter, pfx, dst_id, path) != 0) {
            goto err;
          }
          first = 0;
        } else {
          if (bgpview_iter_pfx_add_peer(dst_iter, dst_id, path) != 0) {
            goto err;
          }
        }

        bgpstream_as_path_destroy(path);
      }
      bgpview_iter_pfx_activate_peer(dst_iter);
    }
  }

  bgpview_iter_destroy(src_iter);
  bgpview_iter_destroy(dst_iter);

  return 0;

err:
  bgpview_iter_destroy(src_iter);
  bgpview_iter_destroy(dst_iter);
  return -1;
}

bgpview_t *bgpview_dup(bgpview_t *src)
{
  bgpview_t *dst = NULL;

  if ((dst = bgpview_create_shared(
         src->peersigns, src->pathstore, src->user_destructor,
         src->peer_user_destructor, src->pfx_user_destructor,
         src->pfx_peer_user_destructor)) == NULL) {
    return NULL;
  }

  dst->disable_extended = src->disable_extended;

  if (bgpview_copy(dst, src) != 0) {
    goto err;
  }

  return dst;

err:
  bgpview_destroy(dst);
  return NULL;
}

void bgpview_disable_user_data(bgpview_t *view)
{
  /* the user can't be wanting to destroy pfx-peer user data... */
  assert(view->pfx_peer_user_destructor == NULL);
  /* nor can they have any prefixes... */
  assert(bgpview_pfx_cnt(view, BGPVIEW_FIELD_ALL_VALID) == 0);

  view->disable_extended = 1;
}

/* ==================== SIMPLE ACCESSOR FUNCTIONS ==================== */

uint32_t bgpview_v4pfx_cnt(bgpview_t *view, uint8_t state_mask)
{
  return __cnt_by_mask(view->v4pfxs_cnt, state_mask);
}

uint32_t bgpview_v6pfx_cnt(bgpview_t *view, uint8_t state_mask)
{
  return __cnt_by_mask(view->v6pfxs_cnt, state_mask);
}

uint32_t bgpview_pfx_cnt(bgpview_t *view, uint8_t state_mask)
{
  return __cnt_by_mask(view->v4pfxs_cnt, state_mask) +
         __cnt_by_mask(view->v6pfxs_cnt, state_mask);
}

uint32_t bgpview_peer_cnt(bgpview_t *view, uint8_t state_mask)
{
  return __cnt_by_mask(view->peerinfo_cnt, state_mask);
}

uint32_t bgpview_get_time(bgpview_t *view)
{
  return view->time;
}

void bgpview_set_time(bgpview_t *view, uint32_t time)
{
  view->time = time;
}

uint32_t bgpview_get_time_created(bgpview_t *view)
{
  return view->time_created;
}

void *bgpview_get_user(bgpview_t *view)
{
  return view->user;
}

int bgpview_set_user(bgpview_t *view, void *user)
{
  if (view->user == user) {
    return 0;
  }
  if (view->user != NULL && view->user_destructor != NULL) {
    view->user_destructor(view->user);
  }
  view->user = user;
  return 1;
}

void bgpview_set_user_destructor(bgpview_t *view,
                                 bgpview_destroy_user_t *bwv_user_destructor)
{
  if (view->user_destructor == bwv_user_destructor) {
    return;
  }
  assert(view->user_destructor == NULL);
  view->user_destructor = bwv_user_destructor;
}

void bgpview_set_pfx_user_destructor(
  bgpview_t *view, bgpview_destroy_user_t *bwv_pfx_user_destructor)
{
  if (view->pfx_user_destructor == bwv_pfx_user_destructor) {
    return;
  }
  assert(view->pfx_user_destructor == NULL);
  view->pfx_user_destructor = bwv_pfx_user_destructor;
}

void bgpview_set_peer_user_destructor(
  bgpview_t *view, bgpview_destroy_user_t *bwv_peer_user_destructor)
{
  if (view->peer_user_destructor == bwv_peer_user_destructor) {
    return;
  }
  assert(view->peer_user_destructor == NULL);
  view->peer_user_destructor = bwv_peer_user_destructor;
}

void bgpview_set_pfx_peer_user_destructor(
  bgpview_t *view, bgpview_destroy_user_t *bwv_pfx_peer_user_destructor)
{
  ASSERT_BWV_PFX_PEERINFO_EXT(view);
  if (view->pfx_peer_user_destructor == bwv_pfx_peer_user_destructor) {
    return;
  }
  assert(view->pfx_peer_user_destructor == NULL);
  view->pfx_peer_user_destructor = bwv_pfx_peer_user_destructor;
}

bgpstream_as_path_store_t *bgpview_get_as_path_store(bgpview_t *view)
{
  return view->pathstore;
}

bgpstream_peer_sig_map_t *bgpview_get_peersigns(bgpview_t *view)
{
  return view->peersigns;
}

bgpstream_peer_id_t bgpview_get_peer_id(bgpview_t *view,
                                        bgpstream_peer_sig_t *ps)
{
  return bgpstream_peer_sig_map_get_id(view->peersigns, ps->collector_str,
                                       &ps->peer_ip_addr,
                                       ps->peer_asnumber);
}
