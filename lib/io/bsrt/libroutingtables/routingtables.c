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
#include "config.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "utils.h"

#include "routingtables_int.h"
#include "routingtables.h"
#include "../bgpview_io_bsrt_int.h"

// Convenience macro for iterating over a khash
#define rt_kh_for(iter, h)  for (khint_t iter = kh_begin(h); iter != kh_end(h); ++iter)

/** When the Quagga process starts dumping the
 *  RIB (at time t0), not all of the previous update
 *  messages have been processed, in other words
 *  there is a backlog queue of update that has not
 *  been processed yet, when the updates in this queue
 *  refer to timestamps before the RIB, then considering
 *  the RIB state as the most updated leads to wrong conclusions,
 *  as well as the installation of stale routes in the routing table.
 *  To prevent this case, we say that: if an update message applied
 *  to our routing table is older than the timestamp of the UC RIB
 *  and the update happened within ROUTINGTABLES_RIB_BACKLOG_TIME from
 *  the RIB start, then the update message is the one which is
 *  considered the more consistent (and therefore it should remain
 *  in the routing table after the end_of_rib process). */
#define ROUTINGTABLES_RIB_BACKLOG_TIME 60

/** If a peer does not receive any data for
 *  ROUTINGTABLES_MAX_INACTIVE_TIME and it is not
 *  in the RIB, then it is considered UNKNOWN */
#define ROUTINGTABLES_MAX_INACTIVE_TIME 3600

/** string buffer to contain debugging infos */
#define BUFFER_LEN 1024
static char buffer[BUFFER_LEN];

/* ========== PRIVATE FUNCTIONS ========== */

static char *graphite_safe(char *p)
{
  if (p == NULL) {
    return p;
  }

  char *r = p;
  while (*p != '\0') {
    if (*p == '.') {
      *p = '-';
    }
    if (*p == '*') {
      *p = '-';
    }
    p++;
  }
  return r;
}

static uint32_t get_wall_time_now(void)
{
  struct timeval tv;
  gettimeofday_wrap(&tv);
  return tv.tv_sec;
}

static perpfx_perpeer_info_t *perpfx_perpeer_info_create(void)
{
  perpfx_perpeer_info_t *pfxpeeri =
    (perpfx_perpeer_info_t *)malloc_zero(sizeof(perpfx_perpeer_info_t));
  if (pfxpeeri != NULL) {
    pfxpeeri->pfx_status = ROUTINGTABLES_INITIAL_PFXSTATUS;
    pfxpeeri->bgp_time_last_ts = 0;
    pfxpeeri->bgp_time_uc_delta_ts = 0;
    /* the path id is ignored unless it is set by a RIB message
     * (i.e. ROUTINGTABLES_UC_ANNOUNCED_PFXSTATUS is on), that's
     * why we do not initialize it:
     * pfxpeeri->uc_as_path_id = ? */
  }
  return pfxpeeri;
}

static void perpeer_info_destroy(void *p)
{
  if (p == NULL)
    return;

  perpeer_info_t *pi = (perpeer_info_t *)p;

  if (pi->announcing_ases != NULL) {
    rt_kh_for (k, pi->announcing_ases) {
      if (!kh_exist(pi->announcing_ases, k)) continue;
      bgpstream_as_path_seg_destroy(kh_key(pi->announcing_ases, k));
    }
    kh_destroy(origin_segments, pi->announcing_ases);
    pi->announcing_ases = NULL;
  }
  if (pi->announced_v4_pfxs != NULL) {
    bgpstream_ipv4_pfx_set_destroy(pi->announced_v4_pfxs);
    pi->announced_v4_pfxs = NULL;
  }
  if (pi->withdrawn_v4_pfxs != NULL) {
    bgpstream_ipv4_pfx_set_destroy(pi->withdrawn_v4_pfxs);
    pi->withdrawn_v4_pfxs = NULL;
  }
  if (pi->announced_v6_pfxs != NULL) {
    bgpstream_ipv6_pfx_set_destroy(pi->announced_v6_pfxs);
    pi->announced_v6_pfxs = NULL;
  }
  if (pi->withdrawn_v6_pfxs != NULL) {
    bgpstream_ipv6_pfx_set_destroy(pi->withdrawn_v6_pfxs);
    pi->withdrawn_v6_pfxs = NULL;
  }
  free(p);
}

/* default: all ts are 0, while the peer state is
 * BGPSTREAM_ELEM_PEERSTATE_UNKNOWN */
static perpeer_info_t *perpeer_info_create(routingtables_t *rt, collector_t *c,
                                           uint32_t peer_id)
{
  char ip_str[INET6_ADDRSTRLEN];
  unsigned v = 0;
  perpeer_info_t *p;
  if ((p = (perpeer_info_t *)malloc_zero(sizeof(perpeer_info_t))) == NULL)
    goto err;

  strcpy(p->collector_str, c->collector_str);

  bgpstream_peer_sig_t *sg =
    bgpstream_peer_sig_map_get_sig(rt->peersigns, peer_id);

  v = (sg->peer_ip_addr.version == BGPSTREAM_ADDR_VERSION_IPV4) ? 4 :
      (sg->peer_ip_addr.version == BGPSTREAM_ADDR_VERSION_IPV6) ? 6 : 0;

  if (bgpstream_addr_ntop(ip_str, sizeof(ip_str), &sg->peer_ip_addr) == NULL) {
    fprintf(stderr, "Warning: can't print peer ip address \n");
  }
  graphite_safe(ip_str);
  if (snprintf(p->peer_str, BGPSTREAM_UTILS_STR_NAME_LEN,
               "peer_asn.%" PRIu32 ".ipv%u_peer.__IP_%s",
               sg->peer_asnumber, v, ip_str) >= BGPSTREAM_UTILS_STR_NAME_LEN) {
    fprintf(stderr,
            "Warning: can't print peer signature: truncated output\n");
  }
  p->bgp_fsm_state = BGPSTREAM_ELEM_PEERSTATE_UNKNOWN;
  p->bgp_time_ref_rib_start = 0;
  p->bgp_time_ref_rib_end = 0;
  p->bgp_time_uc_rib_start = 0;
  p->bgp_time_uc_rib_end = 0;
  p->last_ts = 0;
  p->rib_positive_mismatches_cnt = 0;
  p->rib_negative_mismatches_cnt = 0;
  p->metrics_generated = 0;

  if ((p->announcing_ases = kh_init(origin_segments)) == NULL)
    goto err;

  if ((p->announced_v4_pfxs = bgpstream_ipv4_pfx_set_create()) == NULL)
    goto err;

  if ((p->withdrawn_v4_pfxs = bgpstream_ipv4_pfx_set_create()) == NULL)
    goto err;

  if ((p->announced_v6_pfxs = bgpstream_ipv6_pfx_set_create()) == NULL)
    goto err;

  if ((p->withdrawn_v6_pfxs = bgpstream_ipv6_pfx_set_create()) == NULL)
    goto err;

  return p;
err:
  fprintf(stderr, "Error: can't create per-peer info\n");
  perpeer_info_destroy(p);
  return NULL;
}

static void destroy_collector_data(collector_t *c)
{
  if (c != NULL) {
    if (c->collector_peerids != NULL) {
      kh_destroy(peer_id_set, c->collector_peerids);
    }
    c->collector_peerids = NULL;

    if (c->active_ases != NULL) {
      bgpstream_id_set_destroy(c->active_ases);
    }
    c->active_ases = NULL;
  }
}

static collector_t *get_collector_data(routingtables_t *rt, const char *project,
                                       const char *collector)
{
  khiter_t k;
  int khret;
  collector_t c_data;

  /* create new collector-related structures if it is the first time
   * we see it */
  if ((k = kh_get(collector_data, rt->collectors, collector)) ==
      kh_end(rt->collectors)) {

    /* collector data initialization (all the fields needs to be */
    /* explicitely initialized */

    char project_name[BGPSTREAM_UTILS_STR_NAME_LEN];
    strncpy(project_name, project, BGPSTREAM_UTILS_STR_NAME_LEN);
    graphite_safe(project_name);

    char collector_name[BGPSTREAM_UTILS_STR_NAME_LEN];
    strncpy(collector_name, collector, BGPSTREAM_UTILS_STR_NAME_LEN);
    graphite_safe(collector_name);

    if (snprintf(c_data.collector_str, BGPSTREAM_UTILS_STR_NAME_LEN, "%s.%s",
                 project_name,
                 collector_name) >= BGPSTREAM_UTILS_STR_NAME_LEN) {
      fprintf(stderr,
        "Warning: could not print collector signature: truncated output\n");
    }

    if ((c_data.collector_peerids = kh_init(peer_id_set)) == NULL)
      goto err;

    if ((c_data.active_ases = bgpstream_id_set_create()) == NULL)
      goto err;

    c_data.bgp_time_last = 0;
    c_data.wall_time_last = 0;
    c_data.bgp_time_ref_rib_dump_time = 0;
    c_data.bgp_time_ref_rib_start_time = 0;
    c_data.bgp_time_uc_rib_dump_time = 0;
    c_data.bgp_time_uc_rib_start_time = 0;
    c_data.state = ROUTINGTABLES_COLLECTOR_STATE_UNKNOWN;
    c_data.active_peers_cnt = 0;
    c_data.valid_record_cnt = 0;
    c_data.corrupted_record_cnt = 0;
    c_data.empty_record_cnt = 0;
    c_data.eovrib_flag = 0;
    c_data.publish_flag = 0;

    collector_generate_metrics(rt, &c_data);

    /* insert key,value in map */
    k = kh_put(collector_data, rt->collectors, strdup(collector), &khret);
    kh_val(rt->collectors, k) = c_data;
  }

  return &kh_val(rt->collectors, k);

err:
  fprintf(stderr, "Error: can't create collector data\n");
  destroy_collector_data(&c_data);
  return NULL;
}

/** Stop the under construction process
 *  @note: this function does not deactivate the peer-pfx fields,
 *  the peer may be active */
static void stop_uc_process(routingtables_t *rt, collector_t *c)
{
  perpeer_info_t *p;
  perpfx_perpeer_info_t *pp;

  for (bgpview_iter_first_pfx_peer(rt->iter, 0, BGPVIEW_FIELD_ALL_VALID,
                                   BGPVIEW_FIELD_ALL_VALID);
       bgpview_iter_has_more_pfx_peer(rt->iter);
       bgpview_iter_next_pfx_peer(rt->iter)) {

    /* check if the current field refers to a peer to reset */
    if (kh_get(peer_id_set, c->collector_peerids,
               bgpview_iter_peer_get_peer_id(rt->iter)) !=
        kh_end(c->collector_peerids)) {
      /* the peer belongs to the collector's peers, then reset the
       * information on its rib related status */
      pp = bgpview_iter_pfx_peer_get_user(rt->iter);
      pp->bgp_time_uc_delta_ts = 0;
      pp->pfx_status &= ~ROUTINGTABLES_UC_ANNOUNCED_PFXSTATUS;
    }
  }

  /* reset all the uc information for the peers */
  for (bgpview_iter_first_peer(rt->iter, BGPVIEW_FIELD_ALL_VALID);
       bgpview_iter_has_more_peer(rt->iter); bgpview_iter_next_peer(rt->iter)) {
    /* check if the current field refers to a peer to reset */
    if (kh_get(peer_id_set, c->collector_peerids,
               bgpview_iter_peer_get_peer_id(rt->iter)) !=
        kh_end(c->collector_peerids)) {
      p = bgpview_iter_peer_get_user(rt->iter);
      p->bgp_time_uc_rib_start = 0;
      p->bgp_time_uc_rib_end = 0;
    }
  }

  /* reset all the uc information for the  collector */
  c->bgp_time_uc_rib_dump_time = 0;
  c->bgp_time_uc_rib_start_time = 0;
}

static void reset_peerpfxdata_version(routingtables_t *rt,
                                      bgpstream_peer_id_t peer_id,
                                      uint8_t reset_uc, int pfx_version)
{
  perpfx_perpeer_info_t *pp;
  for (bgpview_iter_first_pfx(rt->iter, pfx_version, BGPVIEW_FIELD_ALL_VALID);
       bgpview_iter_has_more_pfx(rt->iter); bgpview_iter_next_pfx(rt->iter)) {
    if (bgpview_iter_pfx_seek_peer(rt->iter, peer_id,
                                   BGPVIEW_FIELD_ALL_VALID) == 0) {
      continue;
    }
    pp = bgpview_iter_pfx_peer_get_user(rt->iter);
    pp->pfx_status &= ~ROUTINGTABLES_ANNOUNCED_PFXSTATUS;
    pp->bgp_time_last_ts = 0;
    if (reset_uc) {
      pp->bgp_time_uc_delta_ts = 0;
      pp->pfx_status = ROUTINGTABLES_INITIAL_PFXSTATUS;
    }
    bgpview_iter_pfx_deactivate_peer(rt->iter);
  }
}

/** Reset all the pfxpeer data associated with the
 *  provided peer id
 *  @note: this is the function to call when putting a peer down*/
static void reset_peerpfxdata(routingtables_t *rt, bgpstream_peer_id_t peer_id,
                              uint8_t reset_uc)
{
  /* is this a real peer? */
  if (bgpview_iter_seek_peer(rt->iter, peer_id, BGPVIEW_FIELD_ALL_VALID) == 0) {
    return;
  }

  /* disable v4 pfxs if there are any */
  if (bgpview_iter_peer_get_pfx_cnt(rt->iter, BGPSTREAM_ADDR_VERSION_IPV4,
                                    BGPVIEW_FIELD_ALL_VALID) > 0) {
    reset_peerpfxdata_version(rt, peer_id, reset_uc,
                              BGPSTREAM_ADDR_VERSION_IPV4);
  }

  /* disable v6 pfxs if there are any */
  if (bgpview_iter_peer_get_pfx_cnt(rt->iter, BGPSTREAM_ADDR_VERSION_IPV6,
                                    BGPVIEW_FIELD_ALL_VALID) > 0) {
    reset_peerpfxdata_version(rt, peer_id, reset_uc,
                              BGPSTREAM_ADDR_VERSION_IPV6);
  }

  /* reset the iterator to point to the peer */
  bgpview_iter_seek_peer(rt->iter, peer_id, BGPVIEW_FIELD_ALL_VALID);
}

static inline int end_of_valid_rib(routingtables_t *rt, collector_t *c)
{
  c->eovrib_flag = 1;
  return 0;
}

static void update_collector_state(routingtables_t *rt, collector_t *c)
{

  /** we update the status of the collector based on the state of its peers
   * a collector is in an unknown state if all of its peers
   * are in an unknown state, it is down if all of its peers
   * states are either down or unknown, it is up if at least
   * one peer is up */

  perpeer_info_t *p;
  uint8_t unknown = 1;

  c->active_peers_cnt = 0;

  for (bgpview_iter_first_peer(rt->iter, BGPVIEW_FIELD_ALL_VALID);
       bgpview_iter_has_more_peer(rt->iter); bgpview_iter_next_peer(rt->iter)) {
    if (kh_get(peer_id_set, c->collector_peerids,
               bgpview_iter_peer_get_peer_id(rt->iter)) !=
        kh_end(c->collector_peerids)) {
      switch (bgpview_iter_peer_get_state(rt->iter)) {
      case BGPVIEW_FIELD_ACTIVE:
        c->active_peers_cnt++;
        break;
      case BGPVIEW_FIELD_INACTIVE:
        p = bgpview_iter_peer_get_user(rt->iter);
        if (p->bgp_fsm_state != BGPSTREAM_ELEM_PEERSTATE_UNKNOWN) {
          unknown = 0;
        }
        break;
      default:
        /* a valid peer cannot be in state invalid */
        assert(0);
      }
    }
  }

  if (c->active_peers_cnt) {
    c->state = ROUTINGTABLES_COLLECTOR_STATE_UP;
  } else if (unknown) {
    c->state = ROUTINGTABLES_COLLECTOR_STATE_UNKNOWN;
  } else {
    c->state = ROUTINGTABLES_COLLECTOR_STATE_DOWN;
  }
  return;
}

static int apply_end_of_valid_rib_operations(routingtables_t *rt)
{
  collector_t *c;
  int khret;

  perpeer_info_t *p;
  perpfx_perpeer_info_t *pp;
  bgpstream_pfx_t *pfx;

  rt_kh_for(k, rt->collectors) {
    if (!kh_exist(rt->collectors, k)) continue;
    c = &kh_val(rt->collectors, k);
    if (c->eovrib_flag != 0) {
      rt_kh_for(i, c->collector_peerids) {
        if (!kh_exist(c->collector_peerids, i)) continue;
        khiter_t j = kh_put(peer_id_collector, rt->eorib_peers,
                   kh_key(c->collector_peerids, i), &khret);
        kh_value(rt->eorib_peers, j) = c;
      }
    }
  }

  /* if there is at least one peer to promote, then
   * go through the view */
  if (kh_size(rt->eorib_peers) > 0) {

    /** Read the entire collector RIB and update the items according to
     *  timestamps (either promoting the RIB UC data, or maintaining
     *  (the current state) based on the comparison with the UC RIB */
    for (bgpview_iter_first_pfx_peer(rt->iter, 0, BGPVIEW_FIELD_ALL_VALID,
                                     BGPVIEW_FIELD_ALL_VALID);
         bgpview_iter_has_more_pfx_peer(rt->iter);
         bgpview_iter_next_pfx_peer(rt->iter)) {
      p = bgpview_iter_peer_get_user(rt->iter);
      pfx = bgpview_iter_pfx_get_pfx(rt->iter);
      pp = bgpview_iter_pfx_peer_get_user(rt->iter);

      /* check if the current field refers to a peer involved
       * in the rib process  */
      if (kh_get(peer_id_collector, rt->eorib_peers,
                 bgpview_iter_peer_get_peer_id(rt->iter)) !=
            kh_end(rt->eorib_peers) &&
          p->bgp_time_uc_rib_start != 0) {
        /* if the RIB timestamp is greater than the last updated time in the
         * current state, AND  the update did not happen within
         * ROUTINGTABLES_RIB_BACKLOG_TIME seconds before the beginning of the
         * RIB (if that is so, the update message may be still buffered in the
         * quagga process), then the RIB has more updated data than our state
         * */
        if (pp->bgp_time_uc_delta_ts + p->bgp_time_uc_rib_start >
              pp->bgp_time_last_ts &&
            !(pp->bgp_time_last_ts >
              p->bgp_time_uc_rib_start - ROUTINGTABLES_RIB_BACKLOG_TIME)) {

          /* if the prefix is observed in the RIB */
          if (pp->pfx_status & ROUTINGTABLES_UC_ANNOUNCED_PFXSTATUS) {

            /* if the prefix was set (that's why we look for ts!= 0)
             * inactive in the previous state and now it is in the rib */
            if (pp->bgp_time_last_ts != 0 &&
                !(pp->pfx_status & ROUTINGTABLES_ANNOUNCED_PFXSTATUS)) {
              p->rib_negative_mismatches_cnt++;

              fprintf(stderr, "Warning RIB MISMATCH @ %s.%s: %s RIB-A: %" PRIu32
                              " STATE-W: %" PRIu32 "\n",
                      p->collector_str, p->peer_str,
                      bgpstream_pfx_snprintf(buffer, INET6_ADDRSTRLEN + 3, pfx),
                      pp->bgp_time_uc_delta_ts + p->bgp_time_uc_rib_start,
                      pp->bgp_time_last_ts);
            }

            /* Updating the state with RIB information */
            if (bgpview_iter_pfx_peer_set_as_path_by_id(
                  rt->iter, pp->uc_as_path_id) != 0) {
              fprintf(stderr, "Error: could not set AS path\n");
              return -1;
            }
            pp->pfx_status = ROUTINGTABLES_ANNOUNCED_PFXSTATUS;
            pp->bgp_time_last_ts =
              pp->bgp_time_uc_delta_ts + p->bgp_time_uc_rib_start;

            bgpview_iter_activate_peer(rt->iter);
            p->bgp_fsm_state = BGPSTREAM_ELEM_PEERSTATE_ESTABLISHED;
            p->bgp_time_ref_rib_start = p->bgp_time_uc_rib_start;
            p->bgp_time_ref_rib_end = p->bgp_time_uc_rib_end;
            bgpview_iter_pfx_activate_peer(rt->iter);
          } else {
            /* the last modification of the current pfx is before the current
             * uc rib but the prefix is not in the uc rib: therefore we
             * deactivate the field (it may be already inactive) */
            if (bgpview_iter_pfx_peer_get_state(rt->iter) ==
                BGPVIEW_FIELD_ACTIVE) {
              p->rib_positive_mismatches_cnt++;
              fprintf(stderr, "Warning RIB MISMATCH @ %s.%s: %s RIB-W: %" PRIu32
                              " STATE-A: %" PRIu32 "\n",
                      p->collector_str, p->peer_str,
                      bgpstream_pfx_snprintf(buffer, INET6_ADDRSTRLEN + 3, pfx),
                      pp->bgp_time_uc_delta_ts + p->bgp_time_uc_rib_start,
                      pp->bgp_time_last_ts);
            }

            bgpview_iter_pfx_peer_set_as_path(rt->iter, NULL);
            pp->pfx_status = ROUTINGTABLES_INITIAL_PFXSTATUS;
            pp->bgp_time_last_ts = 0;
            bgpview_iter_pfx_deactivate_peer(rt->iter);
          }
        } else {
          /* if an update is more recent than the uc information, or if
           * the last update message was applied just
           * ROUTINGTABLES_RIB_BACKLOG_TIME
           * before the RIB dumping process started then
           * we decide to keep this data and activate the field if it
           * is an announcement */
          if (pp->pfx_status & ROUTINGTABLES_ANNOUNCED_PFXSTATUS) {
            bgpview_iter_activate_peer(rt->iter);
            p->bgp_fsm_state = BGPSTREAM_ELEM_PEERSTATE_ESTABLISHED;
            p->bgp_time_ref_rib_start = p->bgp_time_uc_rib_start;
            p->bgp_time_ref_rib_end = p->bgp_time_uc_rib_end;
            bgpview_iter_pfx_activate_peer(rt->iter);
          }
        }
        /* reset uc fields anyway */
        pp->bgp_time_uc_delta_ts = 0;
        pp->pfx_status =
          pp->pfx_status & (~ROUTINGTABLES_UC_ANNOUNCED_PFXSTATUS);
      }

      /* if state is inactive and ts is older than
       * ROUTINGTABLES_DEPRECATED_INFO_INTERVAL
       * then remove the prefix peer (the garbage collection system will
       * eventually take care
       * of it */
      if (bgpview_iter_pfx_peer_get_state(rt->iter) == BGPVIEW_FIELD_INACTIVE) {
        if (pp->bgp_time_last_ts < rt->bgp_time_interval_start -
                                     ROUTINGTABLES_DEPRECATED_INFO_INTERVAL) {
          if (bgpview_iter_pfx_remove_peer(rt->iter) != 0) {
            return -1;
          }
        }
      }
    }

    /* reset all the uc information for the peers and check if
     * some peers disappeared from the routing table (i.e., if some active
     * peers are not in this RIB, then it means they went down in between
     * the previous RIB and this RIB  and we have to deactivate them */
    for (bgpview_iter_first_peer(rt->iter, BGPVIEW_FIELD_ALL_VALID);
         bgpview_iter_has_more_peer(rt->iter); bgpview_iter_next_peer(rt->iter)) {
      /* check if the current field refers to a peer that belongs to
       * the current collector */
      bgpstream_peer_id_t peerid = bgpview_iter_peer_get_peer_id(rt->iter);
      khiter_t j = kh_get(peer_id_collector, rt->eorib_peers, peerid);
      if (j == kh_end(rt->eorib_peers)) continue;
      p = bgpview_iter_peer_get_user(rt->iter);
      c = kh_value(rt->eorib_peers, j);

      /* if the uc rib start was never touched it means
       * that this peer was not part of the RIB and, therefore,
       * if it claims to be active, we deactivate it */
      if (p->bgp_time_uc_rib_start == 0 &&
          p->last_ts < c->bgp_time_last - ROUTINGTABLES_MAX_INACTIVE_TIME) {
        if (p->bgp_fsm_state == BGPSTREAM_ELEM_PEERSTATE_ESTABLISHED) {
          p->bgp_fsm_state = BGPSTREAM_ELEM_PEERSTATE_UNKNOWN;
          reset_peerpfxdata(rt, peerid, 0);
          bgpview_iter_deactivate_peer(rt->iter);
        }
      } else {
        /* if the peer was actively involved in the uc process
         * we reset its variables */
        p->bgp_time_uc_rib_start = 0;
        p->bgp_time_uc_rib_end = 0;
      }
    }
  } // (kh_size(rt->eorib_peers) > 0)

  rt_kh_for (k, rt->collectors) {
    if (!kh_exist(rt->collectors, k)) continue;
    c = &kh_val(rt->collectors, k);
    if (c->eovrib_flag != 0) {
      c->publish_flag = 1;
      c->eovrib_flag = 0;

      /* reset all the uc information for the  collector */
      c->bgp_time_ref_rib_dump_time = c->bgp_time_uc_rib_dump_time;
      c->bgp_time_ref_rib_start_time = c->bgp_time_uc_rib_start_time;
      c->bgp_time_uc_rib_dump_time = 0;
      c->bgp_time_uc_rib_start_time = 0;
    }
  }

  /* reset the eorib_peers */
  kh_clear(peer_id_collector, rt->eorib_peers);

  /* call the garbage collection process */
  bgpview_gc(rt->view);

  /* check the number of active peers and update the collector's state */
  rt_kh_for (k, rt->collectors) {
    if (!kh_exist(rt->collectors, k)) continue;
    c = &kh_val(rt->collectors, k);
    update_collector_state(rt, c);
  }

  return 0;
}

static int update_peer_stats(perpeer_info_t *p, bgpstream_elem_t *elem)
{
  if (elem->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) {
    /* increase announcements count for current peer */
    p->pfx_announcements_cnt++;

    bgpstream_as_path_seg_t *origin =
      bgpstream_as_path_get_origin_seg(elem->as_path);
    if (kh_get(origin_segments, p->announcing_ases, origin) ==
        kh_end(p->announcing_ases)) {
      int khret;
      if ((origin = bgpstream_as_path_seg_dup(origin)) == NULL) {
        fprintf(stderr, "ERROR: could not duplicate origin segment\n");
        return -1;
      }
      kh_put(origin_segments, p->announcing_ases, origin, &khret);
    }
    if (elem->prefix.address.version == BGPSTREAM_ADDR_VERSION_IPV4) {
      bgpstream_ipv4_pfx_set_insert(p->announced_v4_pfxs,
                                    &elem->prefix.bs_ipv4);
      return 0;
    }
    if (elem->prefix.address.version == BGPSTREAM_ADDR_VERSION_IPV6) {
      bgpstream_ipv6_pfx_set_insert(p->announced_v6_pfxs,
                                    &elem->prefix.bs_ipv6);
      return 0;
    }

  } else {
    assert(elem->type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL);
    /* increase withdrawals count for current peer */
    p->pfx_withdrawals_cnt++;
    if (elem->prefix.address.version == BGPSTREAM_ADDR_VERSION_IPV4) {
      bgpstream_ipv4_pfx_set_insert(p->withdrawn_v4_pfxs,
                                    &elem->prefix.bs_ipv4);
      return 0;
    }
    if (elem->prefix.address.version == BGPSTREAM_ADDR_VERSION_IPV6) {
      bgpstream_ipv6_pfx_set_insert(p->withdrawn_v6_pfxs,
                                    &elem->prefix.bs_ipv6);
      return 0;
    }
  }

  return -1;
}

/** Apply an announcement update or a withdrawal update
 *  return 0 if it finishes correctly, < 0 if something
 *          went wrong
 *  Prerequisites:
 *  the peer exists and it is either active or inactive
 *  the current iterator points at the right peer
 *  the update time >= collector->bgp_time_ref_rib_start_time
 */
static int apply_prefix_update(routingtables_t *rt, collector_t *c,
                               bgpstream_peer_id_t peer_id,
                               bgpstream_elem_t *elem, uint32_t ts)
{
  assert(peer_id);
  assert(peer_id == bgpview_iter_peer_get_peer_id(rt->iter));

  perpeer_info_t *p = bgpview_iter_peer_get_user(rt->iter);
  perpfx_perpeer_info_t *pp = NULL;

  /* if an entry already exists for the prefix-peer, then check
   * that this update is not old  */
  if (bgpview_iter_seek_pfx_peer(rt->iter, &elem->prefix,
                                 peer_id, BGPVIEW_FIELD_ALL_VALID,
                                 BGPVIEW_FIELD_ALL_VALID) != 0) {
    pp = (perpfx_perpeer_info_t *)bgpview_iter_pfx_peer_get_user(rt->iter);
    assert(pp);
    if (ts < pp->bgp_time_last_ts) {
      /* the update is old and it does not change the state */
      return 0;
    }

  } else { /* otherwise we create the prefix-peer and the associated info ds */
    if (bgpview_iter_add_pfx_peer(rt->iter, &elem->prefix,
                                  peer_id, NULL) != 0) {
      fprintf(stderr, "bgpview_iter_add_pfx_peer fails\n");
      return -1;
    }
    /* when we create a new pfx peer this has to be inactive */
    bgpview_iter_pfx_deactivate_peer(rt->iter);
    if ((pp = perpfx_perpeer_info_create()) == NULL) {
      return -1;
    }
    bgpview_iter_pfx_peer_set_user(rt->iter, pp);
  }

  /* the ts received is more recent than the information in the pfx-peer
   * we update both ts and path */
  pp->bgp_time_last_ts = ts;

  /* set the pfx status and as path  */
  if (elem->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) {
    /* set announced status */
    pp->pfx_status |= ROUTINGTABLES_ANNOUNCED_PFXSTATUS;
    bgpview_iter_pfx_peer_set_as_path(rt->iter, elem->as_path);
  } else { /* reset announced status */
    pp->pfx_status &= ~ROUTINGTABLES_ANNOUNCED_PFXSTATUS;
    bgpview_iter_pfx_peer_set_as_path(rt->iter, NULL);
  }

  /* update stats associated with the peer */
  if (update_peer_stats(p, elem) != 0) {
    return -1;
  }

  /* check whether this message changes the active state of the pfx-peer element
   * or the peer state */
  if (bgpview_iter_peer_get_state(rt->iter) == BGPVIEW_FIELD_ACTIVE) {
    /* the announcement moved the pfx-peer state from inactive to active */
    if (bgpview_iter_pfx_peer_get_state(rt->iter) == BGPVIEW_FIELD_INACTIVE &&
        elem->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) {
      bgpview_iter_pfx_activate_peer(rt->iter);
      return 0;
    }

    /* the withdrawal moved the pfx-peer state from active to inactive */
    if (bgpview_iter_pfx_peer_get_state(rt->iter) == BGPVIEW_FIELD_ACTIVE &&
        elem->type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL) {
      bgpview_iter_pfx_deactivate_peer(rt->iter);
      return 0;
    }

    /* no state change required */
    return 0;

  } else { // peer is INACTIVE
    /* if the peer is inactive, all if its pfx-peers must be inactive */
    assert(bgpview_iter_pfx_peer_get_state(rt->iter) == BGPVIEW_FIELD_INACTIVE);

    if (p->bgp_fsm_state == BGPSTREAM_ELEM_PEERSTATE_UNKNOWN) {

      if (p->bgp_time_uc_rib_start != 0) {
        /* case 1: the peer is inactive because its state is unknown and there
         * is an under construction process going on:
         * the peer remains inactive, the information already inserted
         * in the pfx-peer (pp) will be used when the uc rib becomes active,
         * while the pfx-peer remains inactive */
        return 0;
      } else {
        /* case 2: the peer is inactive because its state is unknown and there
         * is no under construction process going on:
         * the peer remains inactive, the information already inserted
         * in the pfx-peer (pp) remains there (might be useful later)
         * while the pfx-peer remains inactive */
        return 0;
      }
    } else {
      /* case 3: the peer is inactive because its fsm state went down,
       * if we receive a new update we assume the state is established
       * and the peer is up again  */
      bgpview_iter_activate_peer(rt->iter);
      p->bgp_fsm_state = BGPSTREAM_ELEM_PEERSTATE_ESTABLISHED;
      p->bgp_time_ref_rib_start = ts;
      p->bgp_time_ref_rib_end = ts;
      if (elem->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) {
        /* the pfx-peer goes active only if we received an announcement */
        bgpview_iter_pfx_activate_peer(rt->iter);
      }
      return 0;
    }
  }
}

static int apply_state_update(routingtables_t *rt, collector_t *c,
                              bgpstream_peer_id_t peer_id,
                              bgpstream_elem_peerstate_t new_state, uint32_t ts)
{

  assert(peer_id);
  assert(peer_id == bgpview_iter_peer_get_peer_id(rt->iter));
  perpeer_info_t *p = bgpview_iter_peer_get_user(rt->iter);

  p->state_messages_cnt++;

  uint8_t reset_uc = 0;

  if (p->bgp_fsm_state != new_state) {
    if (p->bgp_fsm_state == BGPSTREAM_ELEM_PEERSTATE_ESTABLISHED) {
      reset_uc = 0;
      /* check whether the state message affects the uc process */
      if (ts >= p->bgp_time_uc_rib_start) {
        reset_uc = 1;

        /* if the end of valid rib was scheduled for the end
         * of this interval, then anticipate it so that we
         * can deal correctly with a state change */
        if (c->eovrib_flag) {
          apply_end_of_valid_rib_operations(rt);
          bgpview_iter_seek_peer(rt->iter, peer_id, BGPVIEW_FIELD_ALL_VALID);
        }

        p->bgp_time_uc_rib_start = 0;
        p->bgp_time_uc_rib_end = 0;
      }
      /* the peer is active and we receive a peer down message */
      p->bgp_fsm_state = new_state;
      p->bgp_time_ref_rib_start = ts;
      p->bgp_time_ref_rib_end = ts;

      /* reset all peer pfx data associated with the peer */
      reset_peerpfxdata(rt, peer_id, reset_uc);
      bgpview_iter_deactivate_peer(rt->iter);
    } else if (new_state == BGPSTREAM_ELEM_PEERSTATE_ESTABLISHED) {
      /* the peer is inactive and we receive a peer up message */
      p->bgp_fsm_state = new_state;
      p->bgp_time_ref_rib_start = ts;
      p->bgp_time_ref_rib_end = ts;
      bgpview_iter_activate_peer(rt->iter);
    } else {
      /* if the new state does not change the peer active/inactive status,
       * update the FSM state anyway */
      p->bgp_fsm_state = new_state;
      p->bgp_time_ref_rib_start = ts;
      p->bgp_time_ref_rib_end = ts;
    }
  }

  if (p->bgp_fsm_state == BGPSTREAM_ELEM_PEERSTATE_ESTABLISHED) {
    assert(bgpview_iter_peer_get_state(rt->iter) == BGPVIEW_FIELD_ACTIVE);
  } else {
    assert(bgpview_iter_peer_get_state(rt->iter) == BGPVIEW_FIELD_INACTIVE);
  }

  return 0;
}

static int apply_rib_message(routingtables_t *rt, collector_t *c,
                             bgpstream_peer_id_t peer_id,
                             bgpstream_elem_t *elem, uint32_t ts)
{

  assert(peer_id);
  assert(peer_id == bgpview_iter_peer_get_peer_id(rt->iter));

  perpeer_info_t *p = bgpview_iter_peer_get_user(rt->iter);
  perpfx_perpeer_info_t *pp = NULL;

  if (p->bgp_time_uc_rib_start == 0) {
    /* first rib message for this peer */
    p->bgp_time_uc_rib_start = ts;
  }
  p->bgp_time_uc_rib_end = ts;
  p->rib_messages_cnt++;

  if (bgpview_iter_seek_pfx_peer(rt->iter, &elem->prefix,
                                 peer_id, BGPVIEW_FIELD_ALL_VALID,
                                 BGPVIEW_FIELD_ALL_VALID) == 0) {
    /* the prefix-peer does not exist, therefore we
     * create a new empty structure to populate */
    if (bgpview_iter_add_pfx_peer(rt->iter, &elem->prefix,
                                  peer_id, NULL) != 0) {
      fprintf(stderr, "bgpview_iter_add_pfx_peer fails\n");
      return -1;
    }
    /* when we create a new pfx peer this has to be inactive */
    bgpview_iter_pfx_deactivate_peer(rt->iter);
  }

  if ((pp = (perpfx_perpeer_info_t *)bgpview_iter_pfx_peer_get_user(
         rt->iter)) == NULL) {
    pp = perpfx_perpeer_info_create();
    bgpview_iter_pfx_peer_set_user(rt->iter, pp);
  }

  /* we update only the uc part of the pfx-peer, i.e.:
   * the timestamp, the uc_as_path_id, and the pfx status */
  pp->bgp_time_uc_delta_ts = ts - p->bgp_time_uc_rib_start;
  pp->pfx_status |= ROUTINGTABLES_UC_ANNOUNCED_PFXSTATUS;
  if (bgpstream_as_path_store_get_path_id(rt->pathstore, elem->as_path,
                                          elem->peer_asn,
                                          &pp->uc_as_path_id) == -1) {
    return -1;
  }

  return 0;
}

static void refresh_collector_time(routingtables_t *rt, collector_t *c,
                                   bgpstream_record_t *record)
{
  /** we update the bgp_time_last and every
   * ROUTINGTABLES_COLLECTOR_WALL_UPDATE_FR
   *  seconds we also update the last wall time */
  if (record->time_sec > c->bgp_time_last) {
    if (record->time_sec >
        (c->bgp_time_last + ROUTINGTABLES_COLLECTOR_WALL_UPDATE_FR)) {
      c->wall_time_last = get_wall_time_now();
    }
    c->bgp_time_last = record->time_sec;
  }
}

/* debug static int peerscount = 0; */
static int collector_process_valid_bgpinfo(routingtables_t *rt, collector_t *c,
                                           bgpstream_record_t *record)
{
  bgpstream_elem_t *elem;
  bgpstream_peer_id_t peer_id;
  perpeer_info_t *p;
  bgpstream_as_path_iter_t pi;
  bgpstream_as_path_seg_t *seg;
  int rc;

  int khret;
  khiter_t k;

  /* prepare the current collector for a new rib file
   * if that is the case */
  if (record->type == BGPSTREAM_RIB) {
    /* start a new RIB construction process if there is a
     * new START message */
    if (record->dump_pos == BGPSTREAM_DUMP_START) {
      /* if there is already another under construction
       * process going on, then we have to reset the process */
      if (c->bgp_time_uc_rib_dump_time != 0) {
        stop_uc_process(rt, c);
      }
      c->bgp_time_uc_rib_dump_time = record->dump_time_sec;
      c->bgp_time_uc_rib_start_time = record->time_sec;
    }
    /* we process RIB information (ALL of them: start,middle,end)
     * only if there is an under construction process that refers
     * to the same RIB dump */
    if (record->dump_time_sec != c->bgp_time_uc_rib_dump_time) {
      return 0;
    }
  }

  while ((rc = bsrt_record_get_next_elem(record, &elem)) > 0) {

    /* see https://trac.caida.org/hijacks/wiki/ASpaths for more details */

    if (elem->type == BGPSTREAM_ELEM_TYPE_RIB ||
        elem->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT) {
      /* we do not maintain status for prefixes announced locally by the
       * collector */
      if (bgpstream_as_path_get_len(elem->as_path) == 0) {
        continue;
      }

      /* in order to avoid to maintain status for route servers, we only accept
       * reachability information from external BGP sessions that do prepend
       * their
       * peer AS number */
      bgpstream_as_path_iter_reset(&pi);
      seg = bgpstream_as_path_get_next_seg(elem->as_path, &pi);
      if (seg->type == BGPSTREAM_AS_PATH_SEG_ASN &&
          ((bgpstream_as_path_seg_asn_t *)seg)->asn != elem->peer_asn) {
        continue;
      }
    }

    /* get the peer id or create a new peer with state inactive
     * (if it did not exist already) */
    if ((peer_id =
           bgpview_iter_add_peer(rt->iter, record->collector_name,
                                 (bgpstream_ip_addr_t *)&elem->peer_ip,
                                 elem->peer_asn)) == 0) {
      return -1;
    }

    if ((p = (perpeer_info_t *)bgpview_iter_peer_get_user(rt->iter)) == NULL) {
      p = perpeer_info_create(rt, c, peer_id);
      bgpview_iter_peer_set_user(rt->iter, p);
    }
    p->last_ts = record->time_sec;

    /* insert the peer id in the collector peer ids set */
    k = kh_put(peer_id_set, c->collector_peerids, peer_id, &khret);

    // fprintf(stderr, "Applying %s\n", bgpstream_record_elem_snprintf(buffer,
    // BUFFER_LEN, record, elem));

    /* processs each elem based on the type */
    if (elem->type == BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT ||
        elem->type == BGPSTREAM_ELEM_TYPE_WITHDRAWAL) {

      /* update involving a single prefix */
      if (apply_prefix_update(rt, c, peer_id, elem,
                              record->time_sec) != 0) {
        return -1;
      }
    } else if (elem->type == BGPSTREAM_ELEM_TYPE_PEERSTATE) {
      /* update involving an entire peer */
      if (apply_state_update(rt, c, peer_id, elem->new_state,
                             record->time_sec) != 0) {
        return -1;
      }
    } else if (elem->type == BGPSTREAM_ELEM_TYPE_RIB) {
      /* apply the rib message */
      if (apply_rib_message(rt, c, peer_id, elem,
                            record->time_sec) != 0) {
        return -1;
      }
    } else {
      assert(0 && "bgpstream bug: unknown elem type");
    }
  }
  if (rc < 0) {
    /* error when getting next elem */
    return -1;
  }

  /* if we just processed the end of a rib file */
  if (record->type == BGPSTREAM_RIB &&
      record->dump_pos == BGPSTREAM_DUMP_END) {
    /* promote the current uc information to active
     * information and reset the uc info */
    end_of_valid_rib(rt, c);
  }

  return 0;
}

static int collector_process_corrupted_message(routingtables_t *rt,
                                               collector_t *c,
                                               bgpstream_record_t *record)
{
  bgpstream_peer_id_t peer_id;
  perpeer_info_t *p;
  perpfx_perpeer_info_t *pp;

  /* list of peers whose current active rib is affected by the
   * corrupted message */
  bgpstream_id_set_t *cor_affected = bgpstream_id_set_create();
  /* list of peers whose current under construction rib is affected by the
   * corrupted message */
  bgpstream_id_set_t *cor_uc_affected = bgpstream_id_set_create();

  /* get all the peers that belong to the current collector */
  rt_kh_for (k, c->collector_peerids) {
    if (!kh_exist(c->collector_peerids, k)) continue;
    peer_id = kh_key(c->collector_peerids, k);
    bgpview_iter_seek_peer(rt->iter, peer_id, BGPVIEW_FIELD_ALL_VALID);
    p = bgpview_iter_peer_get_user(rt->iter);
    assert(p);

    /* save all the peers affected by the corrupted record */
    if (p->bgp_time_ref_rib_start != 0 &&
        record->time_sec >= p->bgp_time_ref_rib_start) {
      bgpstream_id_set_insert(cor_affected, peer_id);
    }

    /* save all the peers whose under construction process is
     * affected by the corrupted record */
    if (p->bgp_time_uc_rib_start != 0 &&
        record->time_sec >= p->bgp_time_uc_rib_start) {
      bgpstream_id_set_insert(cor_uc_affected, peer_id);
      /* also if an end of valid rib operation was scheduled for this
       * collector now it is not happening anymore */
      c->eovrib_flag = 0;
    }
  }

  /* if the corrupted message is a RIB we can clear the uc state and just ignore
   * the message */

  /* @note: in principle is possible for the under construction process to be
   * affected by the corrupted record without the active information being
   * affected.  That's why we check the impact of the corrupted record (and
   * deal with it) treating the active and uc information of a prefix peer
   * separately */

  /* update all the prefix-peer information */
  for (bgpview_iter_first_pfx_peer(rt->iter, 0, BGPVIEW_FIELD_ALL_VALID,
                                   BGPVIEW_FIELD_ALL_VALID);
       bgpview_iter_has_more_pfx_peer(rt->iter);
       bgpview_iter_next_pfx_peer(rt->iter)) {
    pp = bgpview_iter_pfx_peer_get_user(rt->iter);

    if (record->type == BGPSTREAM_UPDATE) {
      /* we reset the active state iff the corrupted dump is an update */
      if (bgpstream_id_set_exists(cor_affected,
                                  bgpview_iter_peer_get_peer_id(rt->iter))) {
        if (pp->bgp_time_last_ts != 0 &&
            pp->bgp_time_last_ts <= record->time_sec) {
          /* reset the active information if the active state is affected */
          pp->bgp_time_last_ts = 0;
          pp->pfx_status &= ~ROUTINGTABLES_ANNOUNCED_PFXSTATUS;
          /* bgpview_iter_pfx_peer_set_as_path(rt->iter, NULL); */
          bgpview_iter_pfx_deactivate_peer(rt->iter);
        }
      }
    }

    /* we reset the under construction process at all times */

    if (bgpstream_id_set_exists(cor_uc_affected,
                                bgpview_iter_peer_get_peer_id(rt->iter))) {
      /* reset the uc information if the under construction process is affected
       */
      pp->bgp_time_uc_delta_ts = 0;
      pp->pfx_status &= ~ROUTINGTABLES_UC_ANNOUNCED_PFXSTATUS;
    }
  }

  /* update all the peer information */
  for (bgpview_iter_first_peer(rt->iter, BGPVIEW_FIELD_ALL_VALID);
       bgpview_iter_has_more_peer(rt->iter); bgpview_iter_next_peer(rt->iter)) {

    p = bgpview_iter_peer_get_user(rt->iter);

    /* the peer goes down iff we receive a corrupted update */
    if (record->type == BGPSTREAM_UPDATE) {
      if (bgpstream_id_set_exists(cor_affected,
                                  bgpview_iter_peer_get_peer_id(rt->iter))) {
        p->bgp_fsm_state = BGPSTREAM_ELEM_PEERSTATE_UNKNOWN;
        p->bgp_time_ref_rib_start = 0;
        p->bgp_time_ref_rib_end = 0;
        bgpview_iter_deactivate_peer(rt->iter);
      }
    }
    /* and reset the under construction process always */

    if (bgpstream_id_set_exists(cor_uc_affected,
                                bgpview_iter_peer_get_peer_id(rt->iter))) {
      p->bgp_time_uc_rib_start = 0;
      p->bgp_time_uc_rib_end = 0;
    }
  }

  bgpstream_id_set_destroy(cor_affected);
  bgpstream_id_set_destroy(cor_uc_affected);

  return 0;
}

/* ========== PUBLIC FUNCTIONS ========== */

routingtables_t *routingtables_create(char *plugin_name,
                                      timeseries_t *timeseries)
{
  routingtables_t *rt = (routingtables_t *)malloc_zero(sizeof(routingtables_t));
  if (rt == NULL)
    goto err;

  if ((rt->peersigns = bgpstream_peer_sig_map_create()) == NULL)
    goto err;
  if ((rt->pathstore = bgpstream_as_path_store_create()) == NULL)
    goto err;

  if ((rt->view = bgpview_create_shared(
         rt->peersigns, rt->pathstore, free /* view user destructor */,
         perpeer_info_destroy /* peer user destructor */,
         NULL /* pfx destructor */, free /* pfxpeer user destructor */)) ==
      NULL) {
    goto err;
  }

  if ((rt->iter = bgpview_iter_create(rt->view)) == NULL)
    goto err;

  /* all bgpcorsaro plugins share the same timeseries instance */
  rt->timeseries = timeseries;

  if ((rt->kp = timeseries_kp_init(rt->timeseries, 1)) == NULL) {
    fprintf(stderr, "Error: Could not create timeseries key package\n");
    goto err;
  }

  if ((rt->collectors = kh_init(collector_data)) == NULL)
    goto err;

  if ((rt->eorib_peers = kh_init(peer_id_collector)) == NULL)
    goto err;

  strcpy(rt->plugin_name, plugin_name);

  // set the metric prefix string to the default value
  routingtables_set_metric_prefix(rt, ROUTINGTABLES_DEFAULT_METRIC_PFX);
  rt->metrics_output_on = 1;

  rt->bgp_time_interval_start = 0;
  rt->bgp_time_interval_end = 0;
  rt->wall_time_interval_start = 0;

  return rt;

err:
  fprintf(stderr, "routingtables_create failed\n");
  routingtables_destroy(rt);
  return NULL;
}

bgpview_t *routingtables_get_view_ptr(routingtables_t *rt)
{
  return rt->view;
}

void routingtables_set_metric_prefix(routingtables_t *rt,
    const char *metric_prefix)
{
  if (metric_prefix == NULL ||
      strlen(metric_prefix) - 1 > ROUTINGTABLES_METRIC_PFX_LEN) {
    fprintf(stderr, "Warning: could not set metric prefix, using default %s \n",
            ROUTINGTABLES_DEFAULT_METRIC_PFX);
    strcpy(rt->metric_prefix, ROUTINGTABLES_DEFAULT_METRIC_PFX);
    return;
  }
  strcpy(rt->metric_prefix, metric_prefix);
}

char *routingtables_get_metric_prefix(routingtables_t *rt)
{
  return &rt->metric_prefix[0];
}

void routingtables_turn_metric_output_off(routingtables_t *rt)
{
  rt->metrics_output_on = 0;
}

int routingtables_interval_start(routingtables_t *rt, int start_time)
{
  rt->bgp_time_interval_start = (uint32_t)start_time;
  rt->wall_time_interval_start = get_wall_time_now();
  /* setting the time of the view */
  bgpview_set_time(rt->view, rt->bgp_time_interval_start);
  return 0;
}

int routingtables_interval_end(routingtables_t *rt, int end_time)
{
  rt->bgp_time_interval_end = (uint32_t)end_time;
  apply_end_of_valid_rib_operations(rt);

  uint32_t time_now = get_wall_time_now();

  if (rt->metrics_output_on) {
    routingtables_dump_metrics(rt, time_now);
  }

  return 0;
}

int routingtables_process_record(routingtables_t *rt,
                                 bgpstream_record_t *record)
{
  int ret = 0;
  collector_t *c;

  /* get a pointer to the current collector data, if no data
   * exists yet, a new structure will be created */
  if ((c = get_collector_data(rt, record->project_name,
                              record->collector_name)) == NULL) {
    return -1;
  }

  /* if a record refer to a time prior to the current reference time,
   * then we discard it, unless we are in the process of building a
   * new rib, in that case we check the time against the uc starting
   * time and if it is a prior record we discard it */
  if (record->time_sec < c->bgp_time_ref_rib_start_time) {
    if (c->bgp_time_uc_rib_dump_time != 0) {
      if (record->time_sec < c->bgp_time_ref_rib_start_time) {
        return 0;
      }
    }
  }

  switch (record->status) {
  case BGPSTREAM_RECORD_STATUS_VALID_RECORD:
    ret = collector_process_valid_bgpinfo(rt, c, record);
    c->valid_record_cnt++;
    break;
  case BGPSTREAM_RECORD_STATUS_CORRUPTED_SOURCE:
  case BGPSTREAM_RECORD_STATUS_CORRUPTED_RECORD:
    ret = collector_process_corrupted_message(rt, c, record);
    c->corrupted_record_cnt++;
    break;
  case BGPSTREAM_RECORD_STATUS_FILTERED_SOURCE:
  case BGPSTREAM_RECORD_STATUS_EMPTY_SOURCE:
  case BGPSTREAM_RECORD_STATUS_OUTSIDE_TIME_INTERVAL:
    /** An empty or filtered source does not change the current
     *  state of a collector, however we update the last_ts
     *  observed */
    if (record->time_sec < c->bgp_time_last) {
      c->bgp_time_last = record->time_sec;
    }
    c->empty_record_cnt++;
    break;
  default:
    /* programming error */
    assert(0);
  }

  refresh_collector_time(rt, c, record);

  return ret;
}

void routingtables_destroy(routingtables_t *rt)
{
  if (rt != NULL) {
    if (rt->collectors != NULL) {
      rt_kh_for (k, rt->collectors) {
        if (!kh_exist(rt->collectors, k)) continue;
        /* deallocating value dynamic memory */
        destroy_collector_data(&kh_val(rt->collectors, k));
        /* deallocating string dynamic memory */
        free(kh_key(rt->collectors, k));
      }
      kh_destroy(collector_data, rt->collectors);
      rt->collectors = NULL;
    }

    if (rt->eorib_peers != NULL) {
      kh_destroy(peer_id_collector, rt->eorib_peers);
      rt->eorib_peers = NULL;
    }

    if (rt->iter != NULL) {
      bgpview_iter_destroy(rt->iter);
      rt->iter = NULL;
    }

    if (rt->view != NULL) {
      bgpview_destroy(rt->view);
      rt->view = NULL;
    }

    if (rt->peersigns != NULL) {
      bgpstream_peer_sig_map_destroy(rt->peersigns);
      rt->peersigns = NULL;
    }

    if (rt->pathstore != NULL) {
      bgpstream_as_path_store_destroy(rt->pathstore);
      rt->pathstore = NULL;
    }

    if (rt->kp != NULL) {
      timeseries_kp_free(&rt->kp);
      rt->kp = NULL;
    }

    free(rt);
  }
}
