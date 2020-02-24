/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini, Ken Keys
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
#include <string.h>

#include "utils.h"

#include "routingtables_int.h"
#include "routingtables.h"

// <metric-prefix>.<plugin-name>.<collector-signature>.<metric-name>
#define RT_COLLECTOR_METRIC_FORMAT "%s.%s.%s.%s"

// <metric-prefix>.meta.bgpcorsaro.<plugin-name>.<collector-signature>.<metric-name>
#define RT_COLLECTOR_META_METRIC_FORMAT "%s.meta.bgpcorsaro.%s.%s.%s"

// <metric-prefix>.<plugin-name>.<collector-signature>.<peer-signature>.<metric-name>
#define RT_PEER_METRIC_FORMAT "%s.%s.%s.%s.%s"

// <metric-prefix>.meta.bgpcorsaro.<plugin-name>.<collector-signature>.<peer-signature>.<metric-name>
#define RT_PEER_META_METRIC_FORMAT "%s.meta.bgpcorsaro.%s.%s.%s.%s"

#define BUFFER_LEN 1024
static char metric_buffer[BUFFER_LEN];

// These "X-macros" let us reuse the same list of parameters with different
// function-like macros "X" without repeating the parameters each time.
//X(metric_idx,                      metric_name,                       extra)
//
// peer metrics
#define P_METRICS(X, extra)                                                    \
  X(status_idx,                      status,                            extra) \
  X(active_v4_pfxs_idx,              active_v4_pfxs_cnt,                extra) \
  X(active_v6_pfxs_idx,              active_v6_pfxs_cnt,                extra) \
  X(announcing_origin_as_idx,        unique_announcing_origin_ases_cnt, extra) \
  X(announced_v4_pfxs_idx,           announced_v4_unique_pfxs_cnt,      extra) \
  X(withdrawn_v4_pfxs_idx,           withdrawn_v4_unique_pfxs_cnt,      extra) \
  X(announced_v6_pfxs_idx,           announced_v6_unique_pfxs_cnt,      extra) \
  X(withdrawn_v6_pfxs_idx,           withdrawn_v6_unique_pfxs_cnt,      extra) \
  X(rib_messages_cnt_idx,            rib_messages_cnt,                  extra) \
  X(pfx_announcements_cnt_idx,       announcements_cnt,                 extra) \
  X(pfx_withdrawals_cnt_idx,         withdrawals_cnt,                   extra) \
  X(state_messages_cnt_idx,          state_messages_cnt,                extra)

#define META_P_METRICS(X, extra)                                               \
  X(inactive_v4_pfxs_idx,            inactive_v4_pfxs_cnt,              extra) \
  X(inactive_v6_pfxs_idx,            inactive_v6_pfxs_cnt,              extra) \
  X(rib_positive_mismatches_cnt_idx, rib_subtracted_pfxs_cnt,           extra) \
  X(rib_negative_mismatches_cnt_idx, rib_added_pfxs_cnt,                extra)

// collector metrics
#define C_METRICS(X, extra)                                                    \
  X(status_idx,                      status,                            extra) \
  X(peers_cnt_idx,                   peers_cnt,                         extra) \
  X(active_peers_cnt_idx,            active_peers_cnt,                  extra) \
  X(active_asns_cnt_idx,             active_peer_asns_cnt,              extra)

#define META_C_METRICS(X, extra)                                               \
  X(processing_time_idx,             processing_time,                   extra) \
  X(realtime_delay_idx,              realtime_delay,                    extra) \
  X(valid_record_cnt_idx,            valid_record_cnt,                  extra) \
  X(corrupted_record_cnt_idx,        corrupted_record_cnt,              extra) \
  X(empty_record_cnt_idx,            empty_record_cnt,                  extra)

void peer_generate_metrics(routingtables_t *rt, perpeer_info_t *p)
{
#define ADD_P_METRIC(metric_idx, metric_name, fmt)                             \
  do {                                                                         \
    snprintf(metric_buffer, BUFFER_LEN, fmt, rt->metric_prefix,                \
             rt->plugin_name, p->collector_str, p->peer_str, #metric_name);    \
    p->kp_idxs.metric_idx = timeseries_kp_add_key(rt->kp, metric_buffer);      \
    assert(p->kp_idxs.metric_idx >= 0);                                        \
  } while (0);

  P_METRICS(ADD_P_METRIC, RT_PEER_METRIC_FORMAT)
  META_P_METRICS(ADD_P_METRIC, RT_PEER_META_METRIC_FORMAT)

  p->metrics_generated = 1;
}

void collector_generate_metrics(routingtables_t *rt, collector_t *c)
{
#define ADD_C_METRIC(metric_idx, metric_name, fmt)                             \
  do {                                                                         \
    snprintf(metric_buffer, BUFFER_LEN, fmt, rt->metric_prefix,                \
             rt->plugin_name, c->collector_str, #metric_name);                 \
    c->kp_idxs.metric_idx = timeseries_kp_add_key(rt->kp, metric_buffer);      \
    assert(c->kp_idxs.metric_idx >= 0);                                        \
  } while (0);

  META_C_METRICS(ADD_C_METRIC, RT_COLLECTOR_META_METRIC_FORMAT)
  C_METRICS(ADD_C_METRIC, RT_COLLECTOR_METRIC_FORMAT)
}

#define ENABLE_METRIC(metric_idx, metric_name, ptr) \
    timeseries_kp_enable_key(kp, ptr->kp_idxs.metric_idx);

#define DISABLE_METRIC(metric_idx, metric_name, ptr) \
    timeseries_kp_disable_key(kp, ptr->kp_idxs.metric_idx);

static void enable_peer_metrics(timeseries_kp_t *kp, perpeer_info_t *p)
{
  P_METRICS(ENABLE_METRIC, p)
  META_P_METRICS(ENABLE_METRIC, p)
}

static void disable_peer_metrics(timeseries_kp_t *kp, perpeer_info_t *p)
{
  P_METRICS(DISABLE_METRIC, p)
  META_P_METRICS(DISABLE_METRIC, p)
}

static void enable_collector_metrics(timeseries_kp_t *kp, collector_t *c)
{
  META_C_METRICS(ENABLE_METRIC, c)
  C_METRICS(ENABLE_METRIC, c)
}

static void disable_collector_metrics(timeseries_kp_t *kp, collector_t *c)
{
  META_C_METRICS(DISABLE_METRIC, c)
  C_METRICS(DISABLE_METRIC, c)
}

void routingtables_dump_metrics(routingtables_t *rt, uint32_t time_now)
{
  khiter_t k;
  khiter_t kp;
  collector_t *c;
  uint32_t peer_id;
  perpeer_info_t *p;
  bgpstream_peer_sig_t *sg;
  int processing_time = time_now - rt->wall_time_interval_start;
  uint32_t real_time_delay = time_now - rt->bgp_time_interval_start;

  /* collectors metrics */
  for (k = kh_begin(rt->collectors); k != kh_end(rt->collectors); ++k) {
    if (kh_exist(rt->collectors, k)) {
      c = kh_val(rt->collectors, k);

      /* compute metrics that requires peers aggregation */
      /* get all the peers that belong to the current collector */
      for (kp = kh_begin(c->collector_peerids);
           kp != kh_end(c->collector_peerids); ++kp) {
        if (kh_exist(c->collector_peerids, kp)) {
          peer_id = kh_key(c->collector_peerids, kp);
          bgpview_iter_seek_peer(rt->iter, peer_id, BGPVIEW_FIELD_ALL_VALID);
          p = bgpview_iter_peer_get_user(rt->iter);
          assert(p);
          if (bgpview_iter_peer_get_state(rt->iter) == BGPVIEW_FIELD_ACTIVE) {
            sg = bgpstream_peer_sig_map_get_sig(rt->peersigns, peer_id);
            bgpstream_id_set_insert(rt->c_active_ases, sg->peer_asnumber);
          }
        }
      }

      /* set statistics here */
      timeseries_kp_set(rt->kp, c->kp_idxs.processing_time_idx,
                        processing_time);
      timeseries_kp_set(rt->kp, c->kp_idxs.realtime_delay_idx, real_time_delay);

      timeseries_kp_set(rt->kp, c->kp_idxs.valid_record_cnt_idx,
                        c->valid_record_cnt);
      timeseries_kp_set(rt->kp, c->kp_idxs.corrupted_record_cnt_idx,
                        c->corrupted_record_cnt);
      timeseries_kp_set(rt->kp, c->kp_idxs.empty_record_cnt_idx,
                        c->empty_record_cnt);

      timeseries_kp_set(rt->kp, c->kp_idxs.status_idx, c->state);
      timeseries_kp_set(rt->kp, c->kp_idxs.peers_cnt_idx,
                        kh_size(c->collector_peerids));
      timeseries_kp_set(rt->kp, c->kp_idxs.active_peers_cnt_idx,
                        c->active_peers_cnt);
      timeseries_kp_set(rt->kp, c->kp_idxs.active_asns_cnt_idx,
                        bgpstream_id_set_size(rt->c_active_ases));

      /* flush metrics ? */
      if (c->publish_flag) {
        enable_collector_metrics(rt->kp, c);
      } else {
        disable_collector_metrics(rt->kp, c);
      }

      /* in all cases we have to reset the metrics */
      c->valid_record_cnt = 0;
      c->corrupted_record_cnt = 0;
      c->empty_record_cnt = 0;
      /* c->active_peers_cnt is updated by every single message */
      bgpstream_id_set_clear(rt->c_active_ases);
    }
  }

  /* peers metrics */
  for (bgpview_iter_first_peer(rt->iter, BGPVIEW_FIELD_ALL_VALID);
       bgpview_iter_has_more_peer(rt->iter); bgpview_iter_next_peer(rt->iter)) {
    p = bgpview_iter_peer_get_user(rt->iter);
    if (p->bgp_fsm_state != BGPSTREAM_ELEM_PEERSTATE_UNKNOWN) {
      /* metrics are generated the first time a peer has a not UNKNOWN state */
      if (p->metrics_generated == 0) {
        peer_generate_metrics(rt, p);
      }

      timeseries_kp_set(rt->kp, p->kp_idxs.status_idx, p->bgp_fsm_state);
      timeseries_kp_set(
        rt->kp, p->kp_idxs.active_v4_pfxs_idx,
        bgpview_iter_peer_get_pfx_cnt(rt->iter, BGPSTREAM_ADDR_VERSION_IPV4,
                                      BGPVIEW_FIELD_ACTIVE));
      timeseries_kp_set(
        rt->kp, p->kp_idxs.inactive_v4_pfxs_idx,
        bgpview_iter_peer_get_pfx_cnt(rt->iter, BGPSTREAM_ADDR_VERSION_IPV4,
                                      BGPVIEW_FIELD_INACTIVE));
      timeseries_kp_set(
        rt->kp, p->kp_idxs.active_v6_pfxs_idx,
        bgpview_iter_peer_get_pfx_cnt(rt->iter, BGPSTREAM_ADDR_VERSION_IPV6,
                                      BGPVIEW_FIELD_ACTIVE));
      timeseries_kp_set(
        rt->kp, p->kp_idxs.inactive_v6_pfxs_idx,
        bgpview_iter_peer_get_pfx_cnt(rt->iter, BGPSTREAM_ADDR_VERSION_IPV6,
                                      BGPVIEW_FIELD_INACTIVE));

      timeseries_kp_set(rt->kp, p->kp_idxs.announcing_origin_as_idx,
                        kh_size(p->announcing_ases));

      timeseries_kp_set(rt->kp, p->kp_idxs.announced_v4_pfxs_idx,
                        bgpstream_pfx_set_version_size(p->announced_pfxs, BGPSTREAM_ADDR_VERSION_IPV4));

      timeseries_kp_set(rt->kp, p->kp_idxs.withdrawn_v4_pfxs_idx,
                        bgpstream_pfx_set_version_size(p->withdrawn_pfxs, BGPSTREAM_ADDR_VERSION_IPV4));

      timeseries_kp_set(rt->kp, p->kp_idxs.announced_v6_pfxs_idx,
                        bgpstream_pfx_set_version_size(p->announced_pfxs, BGPSTREAM_ADDR_VERSION_IPV6));

      timeseries_kp_set(rt->kp, p->kp_idxs.withdrawn_v6_pfxs_idx,
                        bgpstream_pfx_set_version_size(p->withdrawn_pfxs, BGPSTREAM_ADDR_VERSION_IPV6));

      timeseries_kp_set(rt->kp, p->kp_idxs.rib_messages_cnt_idx,
                        p->rib_messages_cnt);
      timeseries_kp_set(rt->kp, p->kp_idxs.pfx_announcements_cnt_idx,
                        p->pfx_announcements_cnt);
      timeseries_kp_set(rt->kp, p->kp_idxs.pfx_withdrawals_cnt_idx,
                        p->pfx_withdrawals_cnt);
      timeseries_kp_set(rt->kp, p->kp_idxs.state_messages_cnt_idx,
                        p->state_messages_cnt);
      timeseries_kp_set(rt->kp, p->kp_idxs.rib_positive_mismatches_cnt_idx,
                        p->rib_positive_mismatches_cnt);
      timeseries_kp_set(rt->kp, p->kp_idxs.rib_negative_mismatches_cnt_idx,
                        p->rib_negative_mismatches_cnt);

      enable_peer_metrics(rt->kp, p);
    } else {
      if (p->metrics_generated == 1) {
        disable_peer_metrics(rt->kp, p);
      }
    }

    /* in all cases we have to reset the metrics */
    kh_free(origin_segments, p->announcing_ases, bgpstream_as_path_seg_destroy);
    kh_clear(origin_segments, p->announcing_ases);
    bgpstream_pfx_set_clear(p->announced_pfxs);
    bgpstream_pfx_set_clear(p->withdrawn_pfxs);
    p->rib_messages_cnt = 0;
    p->pfx_announcements_cnt = 0;
    p->pfx_withdrawals_cnt = 0;
    p->state_messages_cnt = 0;
    p->rib_positive_mismatches_cnt = 0;
    p->rib_negative_mismatches_cnt = 0;
  }

  if (timeseries_kp_flush(rt->kp, rt->bgp_time_interval_start) != 0) {
    fprintf(stderr, "Warning: could not flush routingtables %" PRIu32 "\n",
            rt->bgp_time_interval_start);
  }
}
