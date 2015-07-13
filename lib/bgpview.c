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

#include "bgpview.h"
#include "bgpstream_utils_pfx.h"

/** Information about a prefix as seen from a peer */
typedef struct bwv_pfx_peerinfo {

  /** AS Path Store ID */
  bgpstream_as_path_store_path_id_t as_path_id;

  /** @todo add other pfx info fields here (AS path, etc) */

} __attribute__((packed)) bwv_pfx_peerinfo_t;

/** Information about a prefix as seen from a peer */
typedef struct bwv_pfx_peerinfo_ext {

  /** AS Path Store ID */
  bgpstream_as_path_store_path_id_t as_path_id;

  /** @todo add other pfx info fields here (AS path, etc) */

  /** Generic pointer to store per-pfx-per-peer information
   * This is ONLY usable if the view was created as extended
   */
  void *user;

} __attribute__((packed)) bwv_pfx_peerinfo_ext_t;

#define BWV_PFX_PEERINFO_SIZE(view)                                     \
  (((view)->disable_extended) ?                                         \
   sizeof(bwv_pfx_peerinfo_t) : sizeof(bwv_pfx_peerinfo_ext_t))

#define BWV_PFX_GET_PEER_PTR(view, pfxinfo, peerid)                     \
  (((view)->disable_extended) ?                                         \
   &BWV_PFX_GET_PEER(pfxinfo, peerid) :                                 \
   (bwv_pfx_peerinfo_t*)&BWV_PFX_GET_PEER_EXT(pfxinfo, peerid))


#define BWV_PFX_GET_PEER(pfxinfo, peerid)              \
  (((bwv_pfx_peerinfo_t*)(pfxinfo->peers))[peerid-1])

#define BWV_PFX_GET_PEER_EXT(pfxinfo, peerid)           \
  (((bwv_pfx_peerinfo_ext_t*)(pfxinfo->peers))[peerid-1])

#define ASSERT_BWV_PFX_PEERINFO_EXT(view)       \
  assert(view->disable_extended == 0)


/** Value for a prefix in the v4pfxs and v6pfxs tables */
typedef struct bwv_peerid_pfxinfo {

  /** Sparse list of peers, where idx is peerid
   *
   * must be cast to either bwv_pfx_peerinfo_t or bwv_pfx_peerinfo_ext_t
   * depending on view->disabled_extended
   */
  void *peers;

  /** Bit array of peer states. The state of peer N is encoded in bits (N-1)*2,
   *  and ((N-1)*2)+1 (i.e. every two bits represents a different peer state)
   */
  uint8_t *peer_states;

  uint16_t peers_alloc_cnt;

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

/* How many bytes are needed to store max_peerid peer states (two bits per
   state) */
#define BWV_PFX_PEER_STATE_LEN(max_peerid)              \
  ((max_peerid==0) ? 0 : ((((max_peerid)-1)/4)+1))

/** What is the byte index of the given state */
#define _BWV_PFX_PEER_STATE_IDX(peerid)          \
  ((peerid-1)*2)/8

/** Get the value of the state byte for the given peer id */
#define _BWV_PFX_PEER_STATE_BYTE(pfxinfo, peerid)        \
  (pfxinfo->peer_states[_BWV_PFX_PEER_STATE_IDX(peerid)])

/** How much to shift a state by to get a byte, or vice-versa */
#define _BWV_PFX_PEER_STATE_SHIFT(peerid)        \
  (8-(((peerid-1)%4)*2)-2)

/** Get the peer state from the given pfxinfo for the given peerid */
#define BWV_PFX_GET_PEER_STATE(pfxinfo, peerid)                         \
  ((_BWV_PFX_PEER_STATE_BYTE(pfxinfo, peerid)>>                          \
     _BWV_PFX_PEER_STATE_SHIFT(peerid))                                  \
   & 0b11)

/** Set the given state for the given peerid in the given pfxinfo */
#define BWV_PFX_SET_PEER_STATE(pfxinfo, peerid, state)          \
  do {                                                          \
    _BWV_PFX_PEER_STATE_BYTE(pfxinfo, peerid) =                  \
      (_BWV_PFX_PEER_STATE_BYTE(pfxinfo, peerid) &               \
       ~(0b11 << _BWV_PFX_PEER_STATE_SHIFT(peerid))) |           \
      ((state & 0b11)<<_BWV_PFX_PEER_STATE_SHIFT(peerid));       \
  } while(0)

/** @todo: add documentation ? */



/************ map from prefix -> peers [-> prefix info] ************/

KHASH_INIT(bwv_v4pfx_peerid_pfxinfo,
           bgpstream_ipv4_pfx_t,
           bwv_peerid_pfxinfo_t *, 1,
	   bgpstream_ipv4_pfx_storage_hash_val,
           bgpstream_ipv4_pfx_storage_equal_val)
typedef khash_t(bwv_v4pfx_peerid_pfxinfo) bwv_v4pfx_peerid_pfxinfo_t;

KHASH_INIT(bwv_v6pfx_peerid_pfxinfo,
           bgpstream_ipv6_pfx_t,
           bwv_peerid_pfxinfo_t *, 1,
	   bgpstream_ipv6_pfx_storage_hash_val,
           bgpstream_ipv6_pfx_storage_equal_val)
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

  /** Current pfx (the pfx it is valid if < kh_end of the appropriate version
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
  if(view->peer_user_destructor == NULL)
    {
      return;
    }
  for(k = kh_begin(view->peerinfo); k < kh_end(view->peerinfo); ++k)
    {
      if(!kh_exist(view->peerinfo, k) ||
	 (kh_value(view->peerinfo, k).user == NULL))
	{
	  continue;
	}
      view->peer_user_destructor(kh_value(view->peerinfo, k).user);
      kh_value(view->peerinfo, k).user = NULL;
    }
}

static bwv_peerid_pfxinfo_t* peerid_pfxinfo_create()
{
  bwv_peerid_pfxinfo_t *v;

  if((v = malloc_zero(sizeof(bwv_peerid_pfxinfo_t))) == NULL)
    {
      return NULL;
    }
  v->state = BGPVIEW_FIELD_INVALID;

  /* all other fields are memset to 0 */

  return v;
}

static int peerid_pfxinfo_insert(bgpview_iter_t *iter,
                                 bgpstream_pfx_t *prefix,
                                 bwv_peerid_pfxinfo_t *v,
                                 bgpstream_peer_id_t peerid,
                                 bgpstream_as_path_store_path_id_t path_id)
{
  int i;
  bwv_pfx_peerinfo_t *peerinfo = NULL;

  /* need to realloc the array? */
  if(peerid > v->peers_alloc_cnt)
    {
      if((v->peers =
          realloc(v->peers,
                  BWV_PFX_PEERINFO_SIZE(iter->view) * peerid)) == NULL)
        {
          return -1;
        }

      /* and we may also need to allocate some more state bits */
      if(BWV_PFX_PEER_STATE_LEN(v->peers_alloc_cnt) <
         BWV_PFX_PEER_STATE_LEN(peerid) &&
         (v->peer_states = realloc(v->peer_states,
                                   BWV_PFX_PEER_STATE_LEN(peerid))) == NULL)
        {
          return -1;
        }

      /* now we have to zero everything between prev_last and the end */
      for(i = v->peers_alloc_cnt+1; i <= peerid; i++)
        {
          if(iter->view->disable_extended == 0)
            {
              BWV_PFX_GET_PEER_EXT(v, i).user = NULL;
            }
          BWV_PFX_SET_PEER_STATE(v, i, BGPVIEW_FIELD_INVALID);
        }

      v->peers_alloc_cnt = peerid;
    }

  peerinfo = BWV_PFX_GET_PEER_PTR(iter->view, v, peerid);

  /* it was already here and active... */
  if(BWV_PFX_GET_PEER_STATE(v, peerid) != BGPVIEW_FIELD_INVALID)
    {
      return 0;
    }

  peerinfo->as_path_id = path_id;

  BWV_PFX_SET_PEER_STATE(v, peerid, BGPVIEW_FIELD_INACTIVE);

  /** peerinfo->user remains untouched */

  /* and count this as a new inactive peer for this prefix */
  v->peers_cnt[BGPVIEW_FIELD_INACTIVE]++;

  /* also count this as an inactive pfx for the peer */
  switch(prefix->address.version)
    {
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

  return 0;
}

static void pfx_peer_info_destroy(bgpview_t *view,
                                  bwv_pfx_peerinfo_t *v)
{
  return;
}

static void pfx_peer_info_ext_destroy(bgpview_t *view,
                                      bwv_pfx_peerinfo_ext_t *v)
{
  ASSERT_BWV_PFX_PEERINFO_EXT(view);
  if(v->user != NULL && view->pfx_peer_user_destructor != NULL)
    {
      view->pfx_peer_user_destructor(v->user);
    }
  v->user = NULL;
}

static void peerid_pfxinfo_destroy(bgpview_t *view,
                                   bwv_peerid_pfxinfo_t *v)
{
  if(v == NULL)
    {
      return;
    }
  int i = 0;
  if(v->peers!=NULL)
    {
      /* our macros expect peerids, so we go from 1 to alloc_cnt */
      for(i = 1; i <= v->peers_alloc_cnt; i++)
        {
          if(view->disable_extended == 0)
            {
              pfx_peer_info_ext_destroy(view, &BWV_PFX_GET_PEER_EXT(v, i));
            }
          else
            {
              pfx_peer_info_destroy(view, &BWV_PFX_GET_PEER(v, i));
            }
        }
        free(v->peers);
        free(v->peer_states);
    }
  v->peers = NULL;
  v->state = BGPVIEW_FIELD_INVALID;
  v->peers_alloc_cnt = 0;
  if(view->pfx_user_destructor != NULL && v->user != NULL)
    {
      view->pfx_user_destructor(v->user);
    }
  v->user = NULL;
  free(v);
}

static bwv_peerid_pfxinfo_t *pfx_get_peerinfos(bgpview_iter_t *iter)
{
  assert(bgpview_iter_has_more_pfx(iter));

  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4)
    {
      return kh_val(iter->view->v4pfxs, iter->pfx_it);
    }
  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6)
    {
      return kh_val(iter->view->v6pfxs, iter->pfx_it);
    }

  return NULL;
}

static int add_v4pfx(bgpview_iter_t *iter,
                     bgpstream_ipv4_pfx_t *pfx)
{
  bwv_peerid_pfxinfo_t *new_pfxpeerinfo;
  khiter_t k;
  int khret;

  if((k = kh_get(bwv_v4pfx_peerid_pfxinfo, iter->view->v4pfxs, *pfx))
     == kh_end(iter->view->v4pfxs))
    {
      /* pfx doesn't exist yet */
      if((new_pfxpeerinfo = peerid_pfxinfo_create()) == NULL)
        {
          return -1;
        }
      k = kh_put(bwv_v4pfx_peerid_pfxinfo, iter->view->v4pfxs, *pfx, &khret);
      kh_value(iter->view->v4pfxs, k) = new_pfxpeerinfo;

      /* pfx is invalid at this point */
    }

  /* seek the iterator to this prefix */
  iter->pfx_it = k;
  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;

  if(kh_value(iter->view->v4pfxs, k)->state != BGPVIEW_FIELD_INVALID)
    {
      /* it was already there and active/inactive */
      return 0;
    }

  kh_value(iter->view->v4pfxs, k)->state = BGPVIEW_FIELD_INACTIVE;
  iter->view->v4pfxs_cnt[BGPVIEW_FIELD_INACTIVE]++;

  return 0;
}

static int add_v6pfx(bgpview_iter_t *iter,
                     bgpstream_ipv6_pfx_t *pfx)
{
  bwv_peerid_pfxinfo_t *new_pfxpeerinfo;
  khiter_t k;
  int khret;

  if((k = kh_get(bwv_v6pfx_peerid_pfxinfo, iter->view->v6pfxs, *pfx))
     == kh_end(iter->view->v6pfxs))
    {
      /* pfx doesn't exist yet */
      if((new_pfxpeerinfo = peerid_pfxinfo_create()) == NULL)
        {
          return -1;
        }
      k = kh_put(bwv_v6pfx_peerid_pfxinfo, iter->view->v6pfxs, *pfx, &khret);
      kh_value(iter->view->v6pfxs, k) = new_pfxpeerinfo;

      /* pfx is invalid at this point */
    }

  /* seek the iterator to this prefix */
  iter->pfx_it = k;
  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV6;

  if(kh_value(iter->view->v6pfxs, k)->state != BGPVIEW_FIELD_INVALID)
    {
      /* it was already there and active/inactive */
      return 0;
    }

  kh_value(iter->view->v6pfxs, k)->state = BGPVIEW_FIELD_INACTIVE;
  iter->view->v6pfxs_cnt[BGPVIEW_FIELD_INACTIVE]++;

  return 0;
}

static int add_pfx(bgpview_iter_t *iter, bgpstream_pfx_t *pfx)
{
  if(pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV4)
    {
      return add_v4pfx(iter, (bgpstream_ipv4_pfx_t *)(pfx));
    }
  else if(pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV6)
    {
      return add_v6pfx(iter, (bgpstream_ipv6_pfx_t *)(pfx));
    }

  return -1;
}

/* ========== PUBLIC FUNCTIONS ========== */

bgpview_t *
bgpview_create_shared(
                   bgpstream_peer_sig_map_t *peersigns,
                   bgpstream_as_path_store_t *pathstore,
                   bgpview_destroy_user_t *bwv_user_destructor,
                   bgpview_destroy_user_t *bwv_peer_user_destructor,
                   bgpview_destroy_user_t *bwv_pfx_user_destructor,
                   bgpview_destroy_user_t *bwv_pfx_peer_user_destructor)
{
  bgpview_t *view;
  struct timeval time_created;

  if((view = malloc_zero(sizeof(bgpview_t))) == NULL)
    {
      return NULL;
    }

  if((view->v4pfxs = kh_init(bwv_v4pfx_peerid_pfxinfo)) == NULL)
    {
      goto err;
    }

  if((view->v6pfxs = kh_init(bwv_v6pfx_peerid_pfxinfo)) == NULL)
    {
      goto err;
    }

  if(peersigns != NULL)
    {
      view->peersigns_shared = 1;
      view->peersigns = peersigns;
    }
  else
    {
      if((view->peersigns = bgpstream_peer_sig_map_create()) == NULL)
	{
	  fprintf(stderr, "Failed to create peersigns table\n");
	  goto err;
	}
      view->peersigns_shared = 0;
    }

  if(pathstore != NULL)
    {
      view->pathstore_shared = 1;
      view->pathstore = pathstore;
    }
  else
    {
      if((view->pathstore = bgpstream_as_path_store_create()) == NULL)
	{
	  fprintf(stderr, "Failed to create AS Path Store\n");
	  goto err;
	}
      view->pathstore_shared = 0;
    }

  if((view->peerinfo = kh_init(bwv_peerid_peerinfo)) == NULL)
    {
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

bgpview_t *
bgpview_create(
                   bgpview_destroy_user_t *bwv_user_destructor,
                   bgpview_destroy_user_t *bwv_peer_user_destructor,
                   bgpview_destroy_user_t *bwv_pfx_user_destructor,
                   bgpview_destroy_user_t *bwv_pfx_peer_user_destructor)
{
  return bgpview_create_shared(NULL,
                               NULL,
                               bwv_user_destructor,
                               bwv_peer_user_destructor,
                               bwv_pfx_user_destructor,
                               bwv_pfx_peer_user_destructor);
}

void bgpview_destroy(bgpview_t *view)
{
  if(view == NULL)
    {
      return;
    }

  khiter_t k;

  if(view->v4pfxs != NULL)
    {
      for(k = kh_begin(view->v4pfxs); k < kh_end(view->v4pfxs); ++k)
        {
          if(kh_exist(view->v4pfxs, k))
            {
              peerid_pfxinfo_destroy(view, kh_value(view->v4pfxs, k));
            }
        }
      kh_destroy(bwv_v4pfx_peerid_pfxinfo, view->v4pfxs);
      view->v4pfxs = NULL;
    }

  if(view->v6pfxs != NULL)
    {
      for(k = kh_begin(view->v6pfxs); k < kh_end(view->v6pfxs); ++k)
        {
          if(kh_exist(view->v6pfxs, k))
            {
              peerid_pfxinfo_destroy(view, kh_value(view->v6pfxs, k));
            }
        }
      kh_destroy(bwv_v6pfx_peerid_pfxinfo, view->v6pfxs);
      view->v6pfxs = NULL;
    }

  if(view->peersigns_shared == 0 && view->peersigns != NULL)
    {
      bgpstream_peer_sig_map_destroy(view->peersigns);
      view->peersigns = NULL;
    }

  if(view->pathstore_shared == 0 && view->pathstore != NULL)
    {
      bgpstream_as_path_store_destroy(view->pathstore);
      view->pathstore = NULL;
    }

  if(view->peerinfo != NULL)
    {
      peerinfo_destroy_user(view);
      kh_destroy(bwv_peerid_peerinfo, view->peerinfo);
      view->peerinfo = NULL;
    }

  if(view->user != NULL)
    {
      if(view->user_destructor != NULL)
        {
          view->user_destructor(view->user);
        }
      view->user = NULL;
    }

  free(view);
}

void bgpview_clear(bgpview_t *view)
{
  int i;
  struct timeval time_created;
  bwv_peerid_pfxinfo_t *pfxinfo;
  bgpview_iter_t *lit = bgpview_iter_create(view);
  assert(lit != NULL);

  view->time = 0;

  gettimeofday(&time_created, NULL);
  view->time_created = time_created.tv_sec;

  /* mark all prefixes as invalid */
  for(bgpview_iter_first_pfx(lit,
                                     0,
                                     BGPVIEW_FIELD_ALL_VALID);
      bgpview_iter_has_more_pfx(lit);
      bgpview_iter_next_pfx(lit))
    {
      pfxinfo = pfx_get_peerinfos(lit);
      assert(pfxinfo != NULL);
      pfxinfo->peers_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
      pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] = 0;
      pfxinfo->state = BGPVIEW_FIELD_INVALID;
      for(i=1; i <= pfxinfo->peers_alloc_cnt; i++)
	{
          BWV_PFX_SET_PEER_STATE(pfxinfo, i, BGPVIEW_FIELD_INVALID);
	}
    }
  view->v4pfxs_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
  view->v4pfxs_cnt[BGPVIEW_FIELD_ACTIVE] = 0;
  view->v6pfxs_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
  view->v6pfxs_cnt[BGPVIEW_FIELD_ACTIVE] = 0;

  /* clear out the peerinfo table */
  for(bgpview_iter_first_peer(lit,
                                      BGPVIEW_FIELD_ALL_VALID);
      bgpview_iter_has_more_peer(lit);
      bgpview_iter_next_peer(lit))
    {
      peerinfo_reset(&kh_value(view->peerinfo, lit->peer_it));
    }
  view->peerinfo_cnt[BGPVIEW_FIELD_INACTIVE] = 0;
  view->peerinfo_cnt[BGPVIEW_FIELD_ACTIVE] = 0;

  bgpview_iter_destroy(lit);
}

void
bgpview_gc(bgpview_t *view)
{
  int k;

  /* note: in the current implementation we cant free pfx-peers for pfxs that
     are not invalid as this is just an array of peers. */

  for(k = kh_begin(view->v4pfxs); k < kh_end(view->v4pfxs); ++k)
    {
      if(kh_exist(view->v4pfxs, k) &&
         kh_value(view->v4pfxs, k)->state == BGPVIEW_FIELD_INVALID)
        {
          peerid_pfxinfo_destroy(view, kh_value(view->v4pfxs, k));
          kh_del(bwv_v4pfx_peerid_pfxinfo, view->v4pfxs, k);
        }
    }

    for(k = kh_begin(view->v6pfxs); k < kh_end(view->v6pfxs); ++k)
    {
      if(kh_exist(view->v6pfxs, k) &&
         kh_value(view->v6pfxs, k)->state == BGPVIEW_FIELD_INVALID)
        {
          peerid_pfxinfo_destroy(view, kh_value(view->v6pfxs, k));
          kh_del(bwv_v6pfx_peerid_pfxinfo, view->v6pfxs, k);
        }
    }

    for(k = kh_begin(view->peerinfo); k < kh_end(view->peerinfo); ++k)
    {
      if(kh_exist(view->peerinfo, k) &&
         kh_value(view->peerinfo, k).state == BGPVIEW_FIELD_INVALID)
        {
          if(view->peer_user_destructor != NULL &&
             kh_value(view->peerinfo, k).user != NULL)
            {
              view->peer_user_destructor(kh_value(view->peerinfo, k).user);
            }
          kh_del(bwv_peerid_peerinfo, view->peerinfo, k);
        }
    }
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

#define RETURN_CNT_BY_MASK(counter, mask)				\
  do {									\
    switch(mask)							\
      {									\
      case BGPVIEW_FIELD_ACTIVE:				\
      case BGPVIEW_FIELD_INACTIVE:				\
	return (counter)[mask];						\
      case BGPVIEW_FIELD_ALL_VALID:				\
	return (counter)[BGPVIEW_FIELD_ACTIVE] +		\
	  (counter)[BGPVIEW_FIELD_INACTIVE];			\
      default:								\
	assert(0);							\
	return 0;							\
      }									\
  } while(0)

uint32_t bgpview_v4pfx_cnt(bgpview_t *view, uint8_t state_mask)
{
  RETURN_CNT_BY_MASK(view->v4pfxs_cnt, state_mask);
}

uint32_t bgpview_v6pfx_cnt(bgpview_t *view, uint8_t state_mask)
{
  RETURN_CNT_BY_MASK(view->v6pfxs_cnt, state_mask);
}

uint32_t bgpview_pfx_cnt(bgpview_t *view, uint8_t state_mask)
{
  return bgpview_v4pfx_cnt(view, state_mask) +
    bgpview_v6pfx_cnt(view, state_mask);
}

uint32_t bgpview_peer_cnt(bgpview_t *view, uint8_t state_mask)
{
  RETURN_CNT_BY_MASK(view->peerinfo_cnt, state_mask);
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
  if(view->user == user)
    {
      return 0;
    }
  if(view->user != NULL && view->user_destructor != NULL)
    {
      view->user_destructor(view->user);
    }
  view->user = user;
  return 1;
}

void bgpview_set_user_destructor(bgpview_t *view,
                                         bgpview_destroy_user_t *bwv_user_destructor)
{
  if(view->user_destructor == bwv_user_destructor)
    {
      return;
    }
  assert(view->user_destructor == NULL);
  view->user_destructor = bwv_user_destructor;
}

void
bgpview_set_pfx_user_destructor(bgpview_t *view,
                                        bgpview_destroy_user_t *bwv_pfx_user_destructor)
{
  if(view->pfx_user_destructor == bwv_pfx_user_destructor)
    {
      return;
    }
  assert(view->pfx_user_destructor == NULL);
  view->pfx_user_destructor = bwv_pfx_user_destructor;
}

void
bgpview_set_peer_user_destructor(bgpview_t *view,
                                         bgpview_destroy_user_t *bwv_peer_user_destructor)
{
  if(view->peer_user_destructor == bwv_peer_user_destructor)
    {
      return;
    }
  assert(view->peer_user_destructor == NULL);
  view->peer_user_destructor = bwv_peer_user_destructor;
}

void
bgpview_set_pfx_peer_user_destructor(bgpview_t *view,
                                             bgpview_destroy_user_t *bwv_pfx_peer_user_destructor)
{
  ASSERT_BWV_PFX_PEERINFO_EXT(view);
  if(view->pfx_peer_user_destructor == bwv_pfx_peer_user_destructor)
    {
      return;
    }
  assert(view->pfx_peer_user_destructor == NULL);
  view->pfx_peer_user_destructor = bwv_pfx_peer_user_destructor;
}

bgpstream_as_path_store_t *
bgpview_get_as_path_store(bgpview_t *view)
{
  return view->pathstore;
}

/* ==================== ITERATOR FUNCTIONS ==================== */

bgpview_iter_t *bgpview_iter_create(bgpview_t *view)
{
  bgpview_iter_t *iter;

  /* DEBUG REMOVE ME */
  fprintf(stderr, "AS Path Store size: %d\n",
          bgpstream_as_path_store_get_size(view->pathstore));

  if((iter = malloc_zero(sizeof(bgpview_iter_t))) == NULL)
    {
      return NULL;
    }

  iter->view = view;

  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
  iter->version_filter = 0; // default: all prefix versions

  iter->pfx_it = 0;

  iter->peer_it  = kh_end(iter->view->peerinfo);

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

/* ==================== PEER ITERATORS ==================== */

#define WHILE_NOT_MATCHED_PEER                                          \
  while(iter->peer_it < kh_end(iter->view->peerinfo) &&                \
        (!kh_exist(iter->view->peerinfo, iter->peer_it) ||              \
         !(iter->peer_state_mask &                                      \
           kh_val(iter->view->peerinfo, iter->peer_it).state)))

int
bgpview_iter_first_peer(bgpview_iter_t *iter,
                                uint8_t state_mask)
{
  iter->peer_it = kh_begin(iter->view->peerinfo);
  iter->peer_state_mask = state_mask;
  /* keep searching if this does not exist */
  WHILE_NOT_MATCHED_PEER
    {
      iter->peer_it++;
    }
  if(iter->peer_it < kh_end(iter->view->peerinfo))
    {
      return 1;
    }
  return 0;
}

int
bgpview_iter_next_peer(bgpview_iter_t *iter)
{
  do {
    iter->peer_it++;
  } WHILE_NOT_MATCHED_PEER;

  return bgpview_iter_has_more_peer(iter);
}

int
bgpview_iter_has_more_peer(bgpview_iter_t *iter)
{
  if(iter->peer_it < kh_end(iter->view->peerinfo))
    {
      return 1;
    }
  return 0;
}

int
bgpview_iter_seek_peer(bgpview_iter_t *iter,
                               bgpstream_peer_id_t peerid,
                               uint8_t state_mask)
{
  iter->peer_state_mask = state_mask;
  iter->peer_it = kh_get(bwv_peerid_peerinfo, iter->view->peerinfo, peerid);
  if(iter->peer_it == kh_end(iter->view->peerinfo) ||
     !kh_exist(iter->view->peerinfo, iter->peer_it))
    {
      return 0;
    }
  if(iter->peer_state_mask & kh_val(iter->view->peerinfo, iter->peer_it).state)
    {
      return 1;
    }
  iter->peer_it = kh_end(iter->view->peerinfo);
  return 0;
}

/* ==================== PFX ITERATORS ==================== */

#define WHILE_NOT_MATCHED_PFX(table)                              \
  while(iter->pfx_it < kh_end(table) && /* each hash item */     \
        (!kh_exist(table, iter->pfx_it) || /* in hash? */         \
         !(iter->pfx_state_mask & /* correct state? */            \
           kh_val(table, iter->pfx_it)->state)))

#define RETURN_IF_PFX_VALID(table)                                      \
  do {                                                                  \
    if(iter->pfx_it < kh_end(table))                                   \
      {                                                                 \
        iter->pfx_peer_it_valid = 0;                                    \
        return 1;                                                       \
      }                                                                 \
  } while(0)

int
bgpview_iter_first_pfx(bgpview_iter_t *iter,
                               int version,
                               uint8_t state_mask)
{
  // set the version we iterate through
  iter->version_filter = version;

  // set the version we start iterating through
  if(iter->version_filter == BGPSTREAM_ADDR_VERSION_IPV4 ||
     iter->version_filter == 0)
    {
      iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
    }
  else
    {
      iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV6;
    }

  // set the pfx mask
  iter->pfx_state_mask = state_mask;

  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4)
    {
      iter->pfx_it = kh_begin(iter->view->v4pfxs);
      /* keep searching if this does not exist */
      WHILE_NOT_MATCHED_PFX(iter->view->v4pfxs)
	{
	  iter->pfx_it++;
	}
      RETURN_IF_PFX_VALID(iter->view->v4pfxs);

      // no ipv4 prefix was found, we don't look for other versions
      // unless version_filter is zero
      if(iter->version_filter)
        {
          return 0;
        }

      // continue to the next IP version
      iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV6;
    }

  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6)
    {
      iter->pfx_it = kh_begin(iter->view->v6pfxs);
      /* keep searching if this does not exist */
      WHILE_NOT_MATCHED_PFX(iter->view->v6pfxs)
	{
	  iter->pfx_it++;
	}
      RETURN_IF_PFX_VALID(iter->view->v6pfxs);
    }

  return 0;
}

int
bgpview_iter_next_pfx(bgpview_iter_t *iter)
{
  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4)
    {
      do {
	iter->pfx_it++;
      } WHILE_NOT_MATCHED_PFX(iter->view->v4pfxs);

      RETURN_IF_PFX_VALID(iter->view->v4pfxs);

      // no ipv4 prefix was found, we don't look for other versions
      if(iter->version_filter == 0)
        {
          // when we reach the end of ipv4 we continue to
          // the next IP version and we look for the first
          // ipv6 prefix
          bgpview_iter_first_pfx(iter, BGPSTREAM_ADDR_VERSION_IPV6,
                                         iter->pfx_state_mask);
        }

      /* here either the iter points at a valid v6 pfx, or we are done */
      return bgpview_iter_has_more_pfx(iter);
    }

  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6)
    {
      do {
	iter->pfx_it++;
      } WHILE_NOT_MATCHED_PFX(iter->view->v6pfxs);

      RETURN_IF_PFX_VALID(iter->view->v6pfxs);
      // there are no more ip versions to look for
      return 0;
    }

  return 0;
}

int
bgpview_iter_has_more_pfx(bgpview_iter_t *iter)
{
  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4)
    {
      // if there are more ipv4 prefixes
      return iter->pfx_it < kh_end(iter->view->v4pfxs);
    }

  // if the version is ipv6, return 1 if there are more ipv6 prefixes
  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6)
    {
      return iter->pfx_it < kh_end(iter->view->v6pfxs);
    }

  return 0;
}

int
bgpview_iter_seek_pfx(bgpview_iter_t *iter,
                              bgpstream_pfx_t *pfx,
                              uint8_t state_mask)
{
  iter->version_filter = pfx->address.version;
  iter->version_ptr = pfx->address.version;
  iter->pfx_state_mask = state_mask;
  iter->pfx_peer_it_valid = 0;
  iter->pfx_peer_it = 1;

  switch(pfx->address.version)
    {
    case BGPSTREAM_ADDR_VERSION_IPV4:
      iter->pfx_it = kh_get(bwv_v4pfx_peerid_pfxinfo,
                            iter->view->v4pfxs,
                            *((bgpstream_ipv4_pfx_t *)pfx));
      if(iter->pfx_it == kh_end(iter->view->v4pfxs))
        {
          return 0;
        }
      if(iter->pfx_state_mask & kh_val(iter->view->v4pfxs, iter->pfx_it)->state)
        {
          return 1;
        }
      // if the mask does not match, than set the iterator to the end
      iter->pfx_it = kh_end(iter->view->v4pfxs);
      return 0;

    case BGPSTREAM_ADDR_VERSION_IPV6:
      iter->pfx_it = kh_get(bwv_v6pfx_peerid_pfxinfo,
                            iter->view->v6pfxs,
                            *((bgpstream_ipv6_pfx_t *)pfx));
      if(iter->pfx_it == kh_end(iter->view->v6pfxs))
        {
          return 0;
        }
      if(iter->pfx_state_mask & kh_val(iter->view->v6pfxs, iter->pfx_it)->state)
        {
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

#define WHILE_NOT_MATCHED_PFX_PEER                                      \
  while((iter->pfx_peer_it <=                                            \
         infos->peers_alloc_cnt) &&                                     \
        (!(iter->pfx_peer_state_mask &                                  \
           BWV_PFX_GET_PEER_STATE(infos, iter->pfx_peer_it))))

int
bgpview_iter_pfx_first_peer(bgpview_iter_t *iter,
                                    uint8_t state_mask)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos != NULL);

  iter->pfx_peer_state_mask = state_mask;
  iter->pfx_peer_it = 1;
  iter->pfx_peer_it_valid = 0;

  WHILE_NOT_MATCHED_PFX_PEER
    {
      iter->pfx_peer_it++;
    }
  if(iter->pfx_peer_it <= infos->peers_alloc_cnt)
    {
      bgpview_iter_seek_peer(iter, iter->pfx_peer_it, state_mask);
      assert(bgpview_iter_has_more_peer(iter));
      iter->pfx_peer_it_valid = 1;
      return 1;
    }

  return 0;
}

int
bgpview_iter_pfx_next_peer(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos != NULL);

  do {
    iter->pfx_peer_it++;
  } WHILE_NOT_MATCHED_PFX_PEER;

  if(iter->pfx_peer_it <= infos->peers_alloc_cnt)
    {
      bgpview_iter_seek_peer(iter, iter->pfx_peer_it,
                                     iter->pfx_peer_state_mask);
      assert(bgpview_iter_has_more_peer(iter));
      iter->pfx_peer_it_valid = 1;
      return 1;
    }

  iter->pfx_peer_it_valid = 0;
  return 0;
}

int
bgpview_iter_pfx_has_more_peer(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos != NULL);

  if(iter->pfx_peer_it_valid == 1 &&
     iter->pfx_peer_it <= infos->peers_alloc_cnt)
    {
      iter->pfx_peer_it_valid = 1;
      return 1;
    }
  iter->pfx_peer_it_valid = 0;
  return 0;
}

int
bgpview_iter_pfx_seek_peer(bgpview_iter_t *iter,
                                   bgpstream_peer_id_t peerid,
                                   uint8_t state_mask)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos != NULL);

  iter->pfx_peer_state_mask = state_mask;

  if((peerid <= infos->peers_alloc_cnt) &&
     (iter->pfx_peer_state_mask &
      BWV_PFX_GET_PEER_STATE(infos, peerid)))
    {
      iter->pfx_peer_it_valid = 1;
      iter->pfx_peer_it = peerid;
      bgpview_iter_seek_peer(iter, iter->pfx_peer_it, state_mask);
      return 1;
    }

  iter->pfx_peer_it = infos->peers_alloc_cnt+1;
  iter->pfx_peer_it_valid = 0;
  return 0;
}

/* =================== ALL-PFX-PEER ITERATORS ==================== */

int
bgpview_iter_first_pfx_peer(bgpview_iter_t *iter,
                                    int version,
                                    uint8_t pfx_mask,
                                    uint8_t peer_mask)
{
    // set the version(s) we iterate through
  iter->version_filter = version;

  // set the version we start iterating through
  if(iter->version_filter == BGPSTREAM_ADDR_VERSION_IPV4 ||
     iter->version_filter == 0)
    {
      iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
    }
  else
    {
      iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV6;
    }

  // masks are going to be set by each first function
  iter->pfx_state_mask = 0;
  iter->pfx_peer_state_mask = 0;

  // start from the first matching prefix
  bgpview_iter_first_pfx(iter, version, pfx_mask);
  while(bgpview_iter_has_more_pfx(iter))
    {
      // look for the first matching peer within the prefix
      if(bgpview_iter_pfx_first_peer(iter, peer_mask))
        {
          return 1;
        }
      bgpview_iter_next_pfx(iter);
    }
  return 0;
}

int
bgpview_iter_next_pfx_peer(bgpview_iter_t *iter)
{
  while(bgpview_iter_has_more_pfx(iter))
    {
      // look for the next matching peer within the prefix
      if(bgpview_iter_pfx_next_peer(iter))
        {
          return 1;
        }
      // if there are no more peers for the given prefix
      // go to the next prefix
      if(bgpview_iter_next_pfx(iter))
        {
          // and check if the first peer is available
          bgpview_iter_pfx_first_peer(iter, iter->pfx_peer_state_mask);
          if(bgpview_iter_pfx_has_more_peer(iter))
            {
              return 1;
            }
        }
    }
    return 0;
}

int
bgpview_iter_has_more_pfx_peer(bgpview_iter_t *iter)
{
  return bgpview_iter_has_more_pfx(iter) && bgpview_iter_pfx_has_more_peer(iter);
}

int
bgpview_iter_seek_pfx_peer(bgpview_iter_t *iter,
                                   bgpstream_pfx_t *pfx,
                                   bgpstream_peer_id_t peerid,
                                   uint8_t pfx_mask,
                                   uint8_t peer_mask)
{
  // all these filters are reset to default, and then
  // set by the single seek fuctions
  iter->version_filter = 0;
  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
  iter->pfx_state_mask = 0;
  iter->pfx_peer_state_mask = 0;

  if(bgpview_iter_seek_pfx(iter, pfx, pfx_mask) &&
     bgpview_iter_pfx_seek_peer(iter, peerid, peer_mask))
    {
      return 1;
    }

  // if the peer is not found we reset the iterators
  iter->version_ptr = BGPSTREAM_ADDR_VERSION_IPV4;
  iter->pfx_it = kh_end(iter->view->v4pfxs);
  iter->pfx_peer_it_valid = 0;
  iter->pfx_peer_it = 1;

  return 0;
}


/* ==================== CREATION FUNCS ==================== */

bgpstream_peer_id_t
bgpview_iter_add_peer(bgpview_iter_t *iter,
                              char *collector_str,
                              bgpstream_ip_addr_t *peer_address,
                              uint32_t peer_asnumber)
{
  bgpstream_peer_id_t peer_id;
  khiter_t k;
  int khret;

  /* add peer to signatures' map */
  if((peer_id = bgpstream_peer_sig_map_get_id(iter->view->peersigns,
                                             collector_str,
                                              peer_address,
                                              peer_asnumber)) == 0)
    {
      fprintf(stderr, "Could not add peer to peersigns\n");
      fprintf(stderr,
              "Consider making bgpstream_peer_sig_map_set more robust\n");
      return 0;
    }

  /* populate peer information in peerinfo */

  if((k = kh_get(bwv_peerid_peerinfo, iter->view->peerinfo, peer_id))
     == kh_end(iter->view->peerinfo))
    {
      /* new peer!  */
      k = kh_put(bwv_peerid_peerinfo, iter->view->peerinfo, peer_id, &khret);
      memset(&kh_val(iter->view->peerinfo, k), 0, sizeof(bwv_peerinfo_t));
      /* peer is invalid */
    }

  /* seek the iterator */
  iter->peer_it = k;
  iter->peer_state_mask = BGPVIEW_FIELD_ALL_VALID;

  /* here iter->peer_it points to a peer, it could be invalid, inactive,
     active */
  if(kh_val(iter->view->peerinfo, k).state != BGPVIEW_FIELD_INVALID)
    {
      /* it was already here, and it was inactive/active, just return */
      return peer_id;
    }

  /* by here, it is invalid or inactive */
  kh_val(iter->view->peerinfo, k).state = BGPVIEW_FIELD_INACTIVE;

  /* and count one more inactive peer */
  iter->view->peerinfo_cnt[BGPVIEW_FIELD_INACTIVE]++;

  return peer_id;
}

int
bgpview_iter_remove_peer(bgpview_iter_t *iter)
{
  bgpview_iter_t *lit;
  /* we have to have a valid peer */
  assert(bgpview_iter_has_more_peer(iter));

  /* if the peer is active, then we deactivate it first */
  if(bgpview_iter_peer_get_state(iter) == BGPVIEW_FIELD_ACTIVE)
    {
      bgpview_iter_deactivate_peer(iter);
    }
  assert(bgpview_iter_peer_get_state(iter) ==
         BGPVIEW_FIELD_INACTIVE);

  /* if the peer had prefixes, then we need to remove all pfx-peers for this
     peer */
  if(bgpview_iter_peer_get_pfx_cnt(iter, 0,
                                           BGPVIEW_FIELD_ALL_VALID) > 0)
    {
      lit = bgpview_iter_create(iter->view);
      assert(lit != NULL);
      for(bgpview_iter_first_pfx_peer(lit,
                                              0,
                                              BGPVIEW_FIELD_ALL_VALID,
                                              BGPVIEW_FIELD_ALL_VALID);
          bgpview_iter_has_more_pfx_peer(lit);
          bgpview_iter_next_pfx_peer(lit))
        {
          // remove all the peer-pfx associated with the peer
          if(bgpview_iter_peer_get_peer_id(iter) ==
             bgpview_iter_peer_get_peer_id(lit))
            {
              bgpview_iter_pfx_remove_peer(lit);
            }
        }
      bgpview_iter_destroy(lit);
    }

  /* set the state to invalid and reset the counters */
  peerinfo_reset(&kh_value(iter->view->peerinfo, iter->peer_it));
  iter->view->peerinfo_cnt[BGPVIEW_FIELD_INACTIVE]--;

  /* and now advance the iterator */
  bgpview_iter_next_peer(iter);

  return 0;
}

int
bgpview_iter_add_pfx_peer(bgpview_iter_t *iter,
                                  bgpstream_pfx_t *pfx,
                                  bgpstream_peer_id_t peer_id,
                                  bgpstream_as_path_t *as_path)
{
  bgpstream_as_path_store_path_id_t path_id;
  bgpstream_peer_sig_t *ps;

  /* the peer must already exist */
  if(bgpview_iter_seek_peer(iter, peer_id,
                                    BGPVIEW_FIELD_ALL_VALID) == 0)
    {
      return -1;
    }
  /* get the peer ASN */
  ps = bgpview_iter_peer_get_sig(iter);

  if(bgpstream_as_path_store_get_path_id(iter->view->pathstore,
                                         as_path,
                                         ps->peer_asnumber,
                                         &path_id) != 0)
    {
      fprintf(stderr, "ERROR: Failed to get AS Path ID from store\n");
      return -1;
    }

  /* now seek to the prefix */
  if(bgpview_iter_seek_pfx(iter, pfx,
                                   BGPVIEW_FIELD_ALL_VALID) == 0)
    {
      /* we have to first create (or un-invalid) the prefix */
      if(add_pfx(iter, pfx) != 0)
        {
          return -1;
        }
    }

  /* now insert the prefix-peer info */
  return bgpview_iter_pfx_add_peer_by_id(iter, peer_id, path_id);
}

int
bgpview_iter_add_pfx_peer_by_id(bgpview_iter_t *iter,
                                bgpstream_pfx_t *pfx,
                                bgpstream_peer_id_t peer_id,
                                bgpstream_as_path_store_path_id_t path_id)
{
  /* the peer must already exist */
  if(bgpview_iter_seek_peer(iter, peer_id,
                                    BGPVIEW_FIELD_ALL_VALID) == 0)
    {
      return -1;
    }

  /* now seek to the prefix */
  if(bgpview_iter_seek_pfx(iter, pfx,
                                   BGPVIEW_FIELD_ALL_VALID) == 0)
    {
      /* we have to first create (or un-invalid) the prefix */
      if(add_pfx(iter, pfx) != 0)
        {
          return -1;
        }
    }

  /* now insert the prefix-peer info */
  return bgpview_iter_pfx_add_peer_by_id(iter, peer_id, path_id);
}

int
bgpview_iter_remove_pfx(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo);

  /* if the pfx is active, then we deactivate it first */
  if(bgpview_iter_pfx_get_state(iter) ==
     BGPVIEW_FIELD_ACTIVE)
    {
      bgpview_iter_deactivate_pfx(iter);
    }

  assert(pfxinfo->state == BGPVIEW_FIELD_INACTIVE);

  pfxinfo->state = BGPVIEW_FIELD_INVALID;

  /* if there are any active or inactive pfx-peers, we remove them now */
  if(bgpview_iter_pfx_get_peer_cnt(iter,
                                           BGPVIEW_FIELD_ALL_VALID) > 0)
    {
      /* iterate over all pfx-peers for this pfx */
      for(bgpview_iter_pfx_first_peer(iter,
                                              BGPVIEW_FIELD_ALL_VALID);
          bgpview_iter_pfx_has_more_peer(iter);
          bgpview_iter_pfx_next_peer(iter))
        {
          bgpview_iter_pfx_remove_peer(iter);
        }
    }

  assert(pfxinfo->peers_cnt[BGPVIEW_FIELD_INACTIVE] == 0 &&
         pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] == 0);

  /* set the state to invalid and update counters */

  switch(iter->version_ptr)
    {
    case BGPSTREAM_ADDR_VERSION_IPV4:
      iter->view->v4pfxs_cnt[BGPVIEW_FIELD_INACTIVE]--;
      break;

    case BGPSTREAM_ADDR_VERSION_IPV6:
      iter->view->v6pfxs_cnt[BGPVIEW_FIELD_INACTIVE]--;
      break;

    default:
      return -1;
    }

  bgpview_iter_next_pfx(iter);

  return 0;
}

int
bgpview_iter_pfx_add_peer(bgpview_iter_t *iter,
                                  bgpstream_peer_id_t peer_id,
                                  bgpstream_as_path_t *as_path)
{
  bwv_peerid_pfxinfo_t *infos;
  bgpstream_pfx_t *pfx;
  bgpstream_as_path_store_path_id_t path_id;
  bgpstream_peer_sig_t *ps;

  infos = pfx_get_peerinfos(iter);
  assert(infos != NULL);

  pfx = bgpview_iter_pfx_get_pfx(iter);
  assert(pfx != NULL);

  bgpview_iter_seek_peer(iter, peer_id,
                                 BGPVIEW_FIELD_ALL_VALID);
  /* get the peer ASN */
  ps = bgpview_iter_peer_get_sig(iter);

  if(bgpstream_as_path_store_get_path_id(iter->view->pathstore,
                                         as_path,
                                         ps->peer_asnumber,
                                         &path_id) != 0)
    {
      fprintf(stderr, "ERROR: Failed to get AS Path ID from store\n");
      return -1;
    }

  if(peerid_pfxinfo_insert(iter, pfx, infos, peer_id, path_id) != 0)
    {
      return -1;
    }

  /* now seek the iterator to this pfx/peer */
  iter->pfx_peer_it = peer_id;
  iter->pfx_peer_it_valid = 1;
  iter->pfx_peer_state_mask = BGPVIEW_FIELD_ALL_VALID;
  return 0;
}

int
bgpview_iter_pfx_add_peer_by_id(bgpview_iter_t *iter,
                                bgpstream_peer_id_t peer_id,
                                bgpstream_as_path_store_path_id_t path_id)
{
  /* this code is mostly a duplicate of the above func, for efficiency */
  bwv_peerid_pfxinfo_t *infos;
  bgpstream_pfx_t *pfx;

  infos = pfx_get_peerinfos(iter);
  assert(infos != NULL);

  pfx = bgpview_iter_pfx_get_pfx(iter);
  assert(pfx != NULL);

  bgpview_iter_seek_peer(iter, peer_id,
                                 BGPVIEW_FIELD_ALL_VALID);

  if(peerid_pfxinfo_insert(iter, pfx, infos, peer_id, path_id) != 0)
    {
      return -1;
    }

  /* now seek the iterator to this pfx/peer */
  iter->pfx_peer_it = peer_id;
  iter->pfx_peer_it_valid = 1;
  iter->pfx_peer_state_mask = BGPVIEW_FIELD_ALL_VALID;
  return 0;
}

int
bgpview_iter_pfx_remove_peer(bgpview_iter_t *iter)
{
  bgpview_iter_t lit;
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo);

    /* we have to have a valid pfx-peer */
  assert(bgpview_iter_pfx_has_more_peer(iter));

  /* if the pfx-peer is active, then we deactivate it first */
  if(bgpview_iter_pfx_peer_get_state(iter) ==
     BGPVIEW_FIELD_ACTIVE)
    {
      lit = *iter;
      bgpview_iter_pfx_deactivate_peer(&lit);
    }

  assert(BWV_PFX_GET_PEER_STATE(pfxinfo, iter->pfx_peer_it) ==
         BGPVIEW_FIELD_INACTIVE);

  /* now, simply set the state to invalid and reset the pfx counters */
  BWV_PFX_SET_PEER_STATE(pfxinfo, iter->pfx_peer_it,
                         BGPVIEW_FIELD_INVALID);
  pfxinfo->peers_cnt[BGPVIEW_FIELD_INACTIVE]--;

  assert(bgpview_iter_has_more_peer(iter));
  switch(iter->version_ptr)
    {
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
  if(pfxinfo->state != BGPVIEW_FIELD_INVALID &&
     pfxinfo->peers_cnt[BGPVIEW_FIELD_INACTIVE] == 0 &&
     pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] == 0)
    {
      /* it will update the iterator */
      return bgpview_iter_remove_pfx(iter);
    }

  /* and now advance the iterator */
  bgpview_iter_pfx_next_peer(iter);

  return 0;
}

/* ==================== ITER GETTER/SETTERS ==================== */

bgpview_t *
bgpview_iter_get_view(bgpview_iter_t *iter)
{
  if(iter != NULL)
    {
      return iter->view;
    }
  return NULL;
}

bgpstream_pfx_t *
bgpview_iter_pfx_get_pfx(bgpview_iter_t *iter)
{
  assert(bgpview_iter_has_more_pfx(iter) != 0);

  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV4)
    {
      return (bgpstream_pfx_t *)&kh_key(iter->view->v4pfxs, iter->pfx_it);
    }
  if(iter->version_ptr == BGPSTREAM_ADDR_VERSION_IPV6)
    {
      return (bgpstream_pfx_t *)&kh_key(iter->view->v6pfxs, iter->pfx_it);
    }
  return NULL;
}

int
bgpview_iter_pfx_get_peer_cnt(bgpview_iter_t *iter,
                                      uint8_t state_mask)
{
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo != NULL);
  RETURN_CNT_BY_MASK(pfxinfo->peers_cnt, state_mask);
}

bgpview_field_state_t
bgpview_iter_pfx_get_state(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo != NULL);
  return pfxinfo->state;
}

void *
bgpview_iter_pfx_get_user(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo != NULL);
  return pfxinfo->user;
}

int
bgpview_iter_pfx_set_user(bgpview_iter_t *iter, void *user)
{
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo != NULL);

  if(pfxinfo->user == user)
    {
      return 0;
    }

  if(pfxinfo->user != NULL &&
     iter->view->pfx_user_destructor != NULL)
    {
      iter->view->pfx_user_destructor(pfxinfo->user);
    }
  pfxinfo->user = user;
  return 1;
}

bgpstream_peer_id_t
bgpview_iter_peer_get_peer_id(bgpview_iter_t *iter)
{
  assert(bgpview_iter_has_more_peer(iter));
  return kh_key(iter->view->peerinfo, iter->peer_it);
}

bgpstream_peer_sig_t *
bgpview_iter_peer_get_sig(bgpview_iter_t *iter)
{
  assert(bgpview_iter_has_more_peer(iter));
  return bgpstream_peer_sig_map_get_sig(iter->view->peersigns,
                                      bgpview_iter_peer_get_peer_id(iter));
}

static int
peer_get_v4pfx_cnt(bgpview_iter_t *iter, uint8_t state_mask)
{
  RETURN_CNT_BY_MASK(kh_value(iter->view->peerinfo, iter->peer_it)
                     .v4_pfx_cnt, state_mask);
}

static int
peer_get_v6pfx_cnt(bgpview_iter_t *iter, uint8_t state_mask)
{
  RETURN_CNT_BY_MASK(kh_value(iter->view->peerinfo, iter->peer_it)
                     .v6_pfx_cnt, state_mask);
}

int
bgpview_iter_peer_get_pfx_cnt(bgpview_iter_t *iter,
                                      int version,
                                      uint8_t state_mask)
{
  assert(bgpview_iter_has_more_peer(iter));

  if(version == BGPSTREAM_ADDR_VERSION_IPV4)
    {
      return peer_get_v4pfx_cnt(iter, state_mask);
    }
  if(version == BGPSTREAM_ADDR_VERSION_IPV6)
    {
      return peer_get_v6pfx_cnt(iter, state_mask);
    }
  if(version == 0)
    {
      return peer_get_v4pfx_cnt(iter, state_mask) +
        peer_get_v6pfx_cnt(iter, state_mask);
    }
  return -1;
}

bgpview_field_state_t
bgpview_iter_peer_get_state(bgpview_iter_t *iter)
{
  assert(bgpview_iter_has_more_peer(iter));
  return kh_val(iter->view->peerinfo, iter->peer_it).state;
}

void *
bgpview_iter_peer_get_user(bgpview_iter_t *iter)
{
  assert(bgpview_iter_has_more_peer(iter));
  return kh_val(iter->view->peerinfo, iter->peer_it).user;
}

int
bgpview_iter_peer_set_user(bgpview_iter_t *iter, void *user)
{
  void *cur_user = bgpview_iter_peer_get_user(iter);

  if(cur_user == user)
    {
      return 0;
    }

  if(cur_user != NULL &&
     iter->view->peer_user_destructor != NULL)
    {
      iter->view->peer_user_destructor(cur_user);
    }

  kh_val(iter->view->peerinfo, iter->peer_it).user = user;
  return 1;
}

bgpstream_as_path_t *
bgpview_iter_pfx_peer_get_as_path(bgpview_iter_t *iter)
{
  bgpstream_as_path_store_path_t *store_path =
    bgpview_iter_pfx_peer_get_as_path_store_path(iter);
  bgpstream_peer_sig_t *ps = bgpview_iter_peer_get_sig(iter);

  return bgpstream_as_path_store_path_get_path(store_path,
                                               ps->peer_asnumber);
}

bgpstream_as_path_store_path_t *
bgpview_iter_pfx_peer_get_as_path_store_path(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos);
  assert(bgpview_iter_pfx_has_more_peer(iter));

  return bgpstream_as_path_store_get_store_path(iter->view->pathstore,
        BWV_PFX_GET_PEER_PTR(iter->view, infos, iter->pfx_peer_it)->as_path_id);
}

int
bgpview_iter_pfx_peer_set_as_path(bgpview_iter_t *iter,
                                  bgpstream_as_path_t *as_path)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos);
  assert(bgpview_iter_pfx_has_more_peer(iter));

  bgpstream_as_path_store_path_id_t *id =
    &(BWV_PFX_GET_PEER_PTR(iter->view, infos, iter->pfx_peer_it)->as_path_id);

  bgpstream_peer_sig_t *ps = bgpview_iter_peer_get_sig(iter);

  if(bgpstream_as_path_store_get_path_id(iter->view->pathstore,
                                         as_path,
                                         ps->peer_asnumber,
                                         id) != 0)
    {
      fprintf(stderr, "ERROR: Failed to get AS Path ID from store\n");
      return -1;
    }

  return 0;
}

bgpview_field_state_t
bgpview_iter_pfx_peer_get_state(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos);
  assert(bgpview_iter_pfx_has_more_peer(iter));

  return BWV_PFX_GET_PEER_STATE(infos, iter->pfx_peer_it);
}

void *
bgpview_iter_pfx_peer_get_user(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos);
  ASSERT_BWV_PFX_PEERINFO_EXT(iter->view);
  assert(bgpview_iter_pfx_has_more_peer(iter));
  return BWV_PFX_GET_PEER_EXT(infos, iter->pfx_peer_it).user;
}

int
bgpview_iter_pfx_peer_set_user(bgpview_iter_t *iter, void *user)
{
  bwv_peerid_pfxinfo_t *infos = pfx_get_peerinfos(iter);
  assert(infos);
  ASSERT_BWV_PFX_PEERINFO_EXT(iter->view);
  assert(bgpview_iter_pfx_has_more_peer(iter));

  void *cur_user = bgpview_iter_pfx_peer_get_user(iter);

  if(cur_user == user)
    {
      return 0;
    }

  if(cur_user != NULL &&
     iter->view->pfx_peer_user_destructor != NULL)
    {
      iter->view->pfx_peer_user_destructor(cur_user);
    }

  BWV_PFX_GET_PEER_EXT(infos, iter->pfx_peer_it).user = user;
  return 1;
}

/* ==================== ACTIVATE/DEACTIVATE ==================== */

#define ACTIVATE_FIELD_CNT(field)                               \
  do {                                                          \
    field[BGPVIEW_FIELD_INACTIVE]--;                    \
    field[BGPVIEW_FIELD_ACTIVE]++;                      \
  } while(0);

#define DEACTIVATE_FIELD_CNT(field)                             \
  do {                                                          \
    field[BGPVIEW_FIELD_INACTIVE]++;                    \
    field[BGPVIEW_FIELD_ACTIVE]--;                      \
  } while(0)


int
bgpview_iter_activate_peer(bgpview_iter_t *iter)
{
  assert(bgpview_iter_has_more_peer(iter) != 0);

  assert(bgpview_iter_peer_get_state(iter) > 0);
  if(bgpview_iter_peer_get_state(iter)
     != BGPVIEW_FIELD_INACTIVE)
    {
      return 0;
    }

  kh_val(iter->view->peerinfo, iter->peer_it).state =
    BGPVIEW_FIELD_ACTIVE;
  ACTIVATE_FIELD_CNT(iter->view->peerinfo_cnt);
  return 1;
}

int
bgpview_iter_deactivate_peer(bgpview_iter_t *iter)
{
  assert(bgpview_iter_has_more_peer(iter) != 0);
  assert(bgpview_iter_peer_get_state(iter) > 0);

  bgpview_iter_t *lit;
  bgpstream_peer_id_t current_id;
  
  if(bgpview_iter_peer_get_state(iter) != BGPVIEW_FIELD_ACTIVE)
    {
      return 0;
    }

  /* only do the massive work of deactivating all pfx-peers if this peer has any
     active pfxs */
  if(bgpview_iter_peer_get_pfx_cnt(iter, 0,
                                           BGPVIEW_FIELD_ACTIVE) > 0)
    {
      lit = bgpview_iter_create(iter->view);      
      assert(lit != NULL);
      current_id = bgpview_iter_peer_get_peer_id(iter);
      for(bgpview_iter_first_pfx_peer(lit,
                                              0,
                                              BGPVIEW_FIELD_ACTIVE,
                                              BGPVIEW_FIELD_ACTIVE);
          bgpview_iter_has_more_pfx_peer(lit);
          bgpview_iter_next_pfx_peer(lit))
        {
          // deactivate all the peer-pfx associated with the peer
          if(bgpview_iter_peer_get_peer_id(lit) == current_id)
            {
              bgpview_iter_pfx_deactivate_peer(lit);
            }
        }
      bgpview_iter_destroy(lit);
    }

  /* mark as inactive */
  kh_val(iter->view->peerinfo, iter->peer_it).state =
    BGPVIEW_FIELD_INACTIVE;

  /* update the counters */
  DEACTIVATE_FIELD_CNT(iter->view->peerinfo_cnt);

  return 1;
}

static int
activate_pfx(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo != NULL);

  assert(pfxinfo->state > 0);
  if(pfxinfo->state != BGPVIEW_FIELD_INACTIVE)
    {
      return 0;
    }

  pfxinfo->state = BGPVIEW_FIELD_ACTIVE;

  switch(iter->version_ptr)
    {
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

int
bgpview_iter_deactivate_pfx(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo != NULL);

  assert(pfxinfo->state > 0);
  if(pfxinfo->state != BGPVIEW_FIELD_ACTIVE)
    {
      return 0;
    }

  /* now mark the pfx as inactive */
  pfxinfo->state = BGPVIEW_FIELD_INACTIVE;

  /* deactivate all pfx-peers for this prefix */
  for(bgpview_iter_pfx_first_peer(iter,
                                          BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_pfx_has_more_peer(iter);
      bgpview_iter_pfx_next_peer(iter))
    {
      bgpview_iter_pfx_deactivate_peer(iter);
    }


  /* now update the counters */
  switch(iter->version_ptr)
    {
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

int
bgpview_iter_pfx_activate_peer(bgpview_iter_t *iter)
{
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo != NULL);

  assert(bgpview_iter_pfx_has_more_peer(iter));

  assert(BWV_PFX_GET_PEER_STATE(pfxinfo, iter->pfx_peer_it) > 0);
  if(BWV_PFX_GET_PEER_STATE(pfxinfo, iter->pfx_peer_it)
     != BGPVIEW_FIELD_INACTIVE)
    {
      return 0;
    }

  /* update the number of peers that observe this pfx */
  ACTIVATE_FIELD_CNT(pfxinfo->peers_cnt);

  /* this is the first active peer, so pfx must be activated */
  if(pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] == 1)
    {
      activate_pfx(iter);
    }

  /* the peer MUST be active */
  assert(bgpview_iter_peer_get_state(iter) ==
         BGPVIEW_FIELD_ACTIVE);

  // increment the number of prefixes observed by the peer
  switch(iter->version_ptr)
    {
    case BGPSTREAM_ADDR_VERSION_IPV4:
      ACTIVATE_FIELD_CNT(kh_value(iter->view->peerinfo, iter->peer_it)
                         .v4_pfx_cnt);
      break;
    case BGPSTREAM_ADDR_VERSION_IPV6:
      ACTIVATE_FIELD_CNT(kh_value(iter->view->peerinfo, iter->peer_it)
                         .v6_pfx_cnt);
      break;
    default:
      return -1;
    }

  BWV_PFX_SET_PEER_STATE(pfxinfo, iter->pfx_peer_it,
                         BGPVIEW_FIELD_ACTIVE);

  return 1;
}

int
bgpview_iter_pfx_deactivate_peer(bgpview_iter_t *iter)
{
  bgpview_iter_t lit;
  
  bwv_peerid_pfxinfo_t *pfxinfo = pfx_get_peerinfos(iter);
  assert(pfxinfo != NULL);

  assert(bgpview_iter_pfx_has_more_peer(iter));

  assert(BWV_PFX_GET_PEER_STATE(pfxinfo, iter->pfx_peer_it) > 0);
  if(BWV_PFX_GET_PEER_STATE(pfxinfo, iter->pfx_peer_it) !=
     BGPVIEW_FIELD_ACTIVE)
    {
      return 0;
    }

  /* set the state to inactive */
  BWV_PFX_SET_PEER_STATE(pfxinfo, iter->pfx_peer_it,
                         BGPVIEW_FIELD_INACTIVE);

  /* update the number of peers that observe the pfx */
  DEACTIVATE_FIELD_CNT(pfxinfo->peers_cnt);
  if(pfxinfo->peers_cnt[BGPVIEW_FIELD_ACTIVE] == 0)
    {
      lit = *iter;
      bgpview_iter_deactivate_pfx(&lit);
    }

  // decrement the number of pfxs observed by the peer
  switch(iter->version_ptr)
    {
    case BGPSTREAM_ADDR_VERSION_IPV4:
      DEACTIVATE_FIELD_CNT(kh_value(iter->view->peerinfo, iter->peer_it)
                           .v4_pfx_cnt);
      break;
    case BGPSTREAM_ADDR_VERSION_IPV6:
      DEACTIVATE_FIELD_CNT(kh_value(iter->view->peerinfo, iter->peer_it)
                           .v6_pfx_cnt);
      break;
    default:
      return -1;
    }

  return 1;
}
