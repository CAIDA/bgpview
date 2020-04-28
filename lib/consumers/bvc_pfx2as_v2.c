/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2020 The Regents of the University of California.
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

#include "bvc_pfx2as_v2.h"
#include "bgpview_consumer_interface.h"
#include "bgpview_consumer_utils.h"
#include "bgpstream_utils_addr.h"
#include "bgpstream_utils_pfx_set.h"
#include "bgpstream_utils_id_set.h"
#include "khash.h"
#include "utils.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <wandio.h>

#define NAME "pfx2as-v2"

#define MAX_ORIGIN_CNT 512
#define MAX_ORIGIN_PEER_CNT 1024
#define OUTPUT_INTERVAL 86400

#define STATE (BVC_GET_STATE(consumer, pfx2as_v2))

#define CHAIN_STATE (BVC_GET_CHAIN_STATE(consumer))

/* our 'class' */
static bvc_t bvc_pfx2as_v2 = { //
  BVC_ID_PFX2AS_V2, //
  NAME, //
  BVC_GENERATE_PTRS(pfx2as_v2) //
};

typedef struct peerviews {
  uint16_t full_cnt;     // count of views in which pfx-origin was seen by this peer and this peer was considered full-feed
  uint16_t partial_cnt;  // count of views in which pfx-origin was seen by this peer and this peer was considered partial-feed
} peerviews_t;

KHASH_INIT(map_peerid_viewcnt, bgpstream_peer_id_t, peerviews_t, 1,
           kh_int_hash_func, kh_int_hash_equal)
typedef khash_t(map_peerid_viewcnt) map_peerid_viewcnt_t;

typedef struct origin_info {
  bgpstream_as_path_store_path_id_t path_id; // id of path containing the origin
  uint8_t counted_as_full;             // has full_feed_peer_view_cnt been incremented yet in the current view?
  uint8_t counted_as_partial;          // has partial_feed_peer_view_cnt been incremented yet in the current view?
  uint32_t full_feed_peer_cnt;         // count of full-feed peers that observed this pfx-origin
  uint32_t partial_feed_peer_cnt;      // count of partial-feed peers that observed this pfx-origin
  uint32_t full_feed_peer_view_cnt;    // count of views in which any full-feed peer observed this pfx-origin
  uint32_t partial_feed_peer_view_cnt; // count of views in which any partial-feed peer observed this pfx-origin
  map_peerid_viewcnt_t *peers;         // peers that observed this pfx-origin, and in how many views
} origin_info_t;

typedef struct pfx_info {
  uint32_t origin_cnt;
  origin_info_t origins[]; // FAM (but most pfxs have just 1 origin)
} pfx_info_t;

#define pfxinfo_size(n)  (sizeof(pfx_info_t) + n * sizeof(origin_info_t))

KHASH_INIT(map_v4pfx_pfxinfo, bgpstream_ipv4_pfx_t, pfx_info_t*, 1,
           bgpstream_ipv4_pfx_hash_val, bgpstream_ipv4_pfx_equal_val)
typedef khash_t(map_v4pfx_pfxinfo) map_v4pfx_pfxinfo_t;

KHASH_INIT(map_v6pfx_pfxinfo, bgpstream_ipv6_pfx_t, pfx_info_t*, 1,
           bgpstream_ipv6_pfx_hash_val, bgpstream_ipv6_pfx_equal_val)
typedef khash_t(map_v6pfx_pfxinfo) map_v6pfx_pfxinfo_t;

typedef struct pfxcnt {
  int counted_this_pfx;   // has the current pfx been counted yet?
  uint32_t pfx_cnt;       // count of prefixes seen by peer
} pfxcnt_t;

KHASH_INIT(map_peerid_pfxcnt, bgpstream_peer_id_t, pfxcnt_t, 1,
           kh_int_hash_func, kh_int_hash_equal)
typedef khash_t(map_peerid_pfxcnt) map_peerid_pfxcnt_t;

/* our 'instance' */
typedef struct bvc_pfx2as_v2_state {

  /* ----- configuration ----- */

  /** output directory */
  char *outdir;

  /** prefix origins output file name */
  char outfile_name[BVCU_PATH_MAX];

  /** prefix origins output file */
  iow_t *outfile;

  /** output interval */
  uint32_t out_interval;

  /** only output peer counts */
  int peer_count_only;

  /** split prefixes into files by IP version */
  int split_ipv;

  /* ----- working state ----- */

  bgpstream_as_path_store_t *pathstore;
  bgpstream_peer_sig_map_t *peersigs;

  /** data for all pfxs */
  map_v4pfx_pfxinfo_t *v4pfxs;
  map_v6pfx_pfxinfo_t *v6pfxs;

  /** peers that observed pfxes (used only in dump_results(); stored here so
   * memory can be reused) */
  map_peerid_pfxcnt_t *peers;

  /** count of views in current output interval */
  uint32_t view_cnt;

  /** time of first view */
  uint32_t first_view_time;

  /** when next to dump output */
  uint32_t next_output_time;

  /** time of most recent view */
  uint32_t prev_view_time;

  /** interval between previous view and the one before that */
  uint32_t prev_view_interval;

  /** first view_time in the current output interval */
  uint32_t out_interval_start;

#if 0
  /** sets of peers that were considered full-feed in any view within out_interval */
  bgpstream_id_set_t *full_feed_peer_set[BGPSTREAM_MAX_IP_VERSION_IDX];
#endif

} bvc_pfx2as_v2_state_t;

typedef struct pfx2as_v2_stats {
  uint32_t pfxorigin_cnt;   // count of pfx-origins
  uint32_t max_origin_cnt;  // max origin count for any pfx
  uint32_t mop_cnt;         // count of pfxs with multiple origins
  uint32_t overwrite_cnt;
  uint32_t grow_cnt;
} pfx2as_v2_stats_t;

/* ==================== CONSUMER INTERNAL FUNCTIONS ==================== */

#define path_get_origin_seg(pathstore, path_id)                                \
    bgpstream_as_path_store_path_get_origin_seg(                               \
        bgpstream_as_path_store_get_store_path(pathstore, path_id))

static int open_outfiles(bvc_t *consumer, int version, uint32_t vtime)
{
  char version_str[4] = "";
  if (version != 0)
    sprintf(version_str, ".v%d", bgpstream_ipv2number(version));
  if (!(STATE->outfile = bvcu_open_outfile(STATE->outfile_name,
        "%s/" NAME "%s.%" PRIu32 ".gz", STATE->outdir, version_str, vtime)))
    return -1;

  return 0;
}

static int close_outfiles(bvc_t *consumer)
{
  wandio_wdestroy(STATE->outfile);
  STATE->outfile = NULL;
  bvcu_create_donefile(STATE->outfile_name);

  return 0;
}


// XXX This belongs in bgpstream
#define path_id_equal(a, b) (memcmp(&a, &b, sizeof(a)) == 0)


static int dump_results(bvc_t *consumer, int version, uint32_t view_interval)
{
  int indent = 0;
  int v4idx = bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4);

  // for each ipv
  for (int vidx = 0; vidx < BGPSTREAM_MAX_IP_VERSION_IDX; vidx++) {
    khint_t end;
    if (version && bgpstream_ipv2idx(version) != vidx)
      continue;
    if (vidx == v4idx) {
      end = kh_end(STATE->v4pfxs);
    } else { // v6
      end = kh_end(STATE->v6pfxs);
    }

    // for each prefix in the selected ipv
    for (khint_t pi = 0; pi != end; ++pi) {
      bgpstream_pfx_t *pfx;
      pfx_info_t *pfxinfo;
      if (vidx == v4idx) {
        if (!kh_exist(STATE->v4pfxs, pi)) continue;
        pfx = (bgpstream_pfx_t*)&kh_key(STATE->v4pfxs, pi);
        pfxinfo = kh_val(STATE->v4pfxs, pi);
      } else { // v6
        if (!kh_exist(STATE->v6pfxs, pi)) continue;
        pfx = (bgpstream_pfx_t*)&kh_key(STATE->v6pfxs, pi);
        pfxinfo = kh_val(STATE->v6pfxs, pi);
      }

      // reset flag in each peer of STATE->peers
      for (khint_t k = 0; k != kh_end(STATE->peers); ++k) {
        if (!kh_exist(STATE->peers, k)) continue;
        kh_val(STATE->peers, k).counted_this_pfx = 0;
      }

      // for each origin in prefix
      for (uint32_t oi = 0; oi < pfxinfo->origin_cnt; ++oi) {
        origin_info_t *originfo = &pfxinfo->origins[oi];
        originfo->full_feed_peer_cnt = 0;
        originfo->partial_feed_peer_cnt = 0;

        // for each peer in origin
        for (uint32_t mi = 0; mi != kh_end(originfo->peers); ++mi) {
          if (!kh_exist(originfo->peers, mi)) continue;
          if (kh_val(originfo->peers, mi).full_cnt > 0)
            originfo->full_feed_peer_cnt++;
          if (kh_val(originfo->peers, mi).partial_cnt > 0)
            originfo->partial_feed_peer_cnt++;
          if (kh_val(originfo->peers, mi).full_cnt > 0 || kh_val(originfo->peers, mi).partial_cnt > 0) {
            int khret;
            bgpstream_peer_id_t peer_id = kh_key(originfo->peers, mi);
            khint_t k = kh_put(map_peerid_pfxcnt, STATE->peers, peer_id,
                &khret);
            // if peer has not yet counted this pfx, do so now
            if (khret > 0) {
              kh_val(STATE->peers, k).counted_this_pfx = 1;
              kh_val(STATE->peers, k).pfx_cnt = 1;
            } else if (!kh_val(STATE->peers, k).counted_this_pfx) {
              kh_val(STATE->peers, k).counted_this_pfx = 1;
              kh_val(STATE->peers, k).pfx_cnt++;
            }
          }
        }
      }
    }
  }

  uint32_t peer_cnt = kh_size(STATE->peers);
  if (peer_cnt == 0) // e.g., peers are ipv-specific, and split_ipv is true
    return 0; // nothing to report

  if (open_outfiles(consumer, version, STATE->out_interval_start) != 0) {
    return -1;
  }

// DUMP_LINE(delim, fmt, args...)
// depends on `indent` being in scope
#define DUMP_LINE(delim, ...) \
  do {                                                                         \
    wandio_printf(STATE->outfile, "%s\n%*s", delim, indent, "");               \
    wandio_printf(STATE->outfile, __VA_ARGS__);                                \
  } while (0)

  // Dump dataset metadata

  wandio_printf(STATE->outfile, "dataset: {");
  indent += 2;

  DUMP_LINE("", "start: %d", STATE->out_interval_start);
  DUMP_LINE(",", "duration: %d", STATE->view_cnt * view_interval);
  DUMP_LINE(",", "monitor_count: %d", peer_cnt);
  uint32_t pfx_cnt =
    version == BGPSTREAM_ADDR_VERSION_IPV4 ? kh_size(STATE->v4pfxs) :
    version == BGPSTREAM_ADDR_VERSION_IPV6 ? kh_size(STATE->v6pfxs) :
    kh_size(STATE->v4pfxs) + kh_size(STATE->v6pfxs);
  DUMP_LINE(",", "prefix_count: %"PRIu32, pfx_cnt);

  indent -= 2;
  DUMP_LINE("", "}"); // dataset

  // Dump monitors

  if (!STATE->peer_count_only) {
    DUMP_LINE(",", "monitors: [");
    indent += 2;

    const char *mon_delim = "";
    for (khint_t k = 0; k != kh_end(STATE->peers); ++k) {
      if (!kh_exist(STATE->peers, k)) continue;
      bgpstream_peer_id_t peer_id = kh_key(STATE->peers, k);
      uint32_t peer_pfx_cnt = kh_val(STATE->peers, k).pfx_cnt;
      if (peer_pfx_cnt == 0)
        continue; // skip peer with no prefixes with the requested ipv

      bgpstream_peer_sig_t *ps =
          bgpstream_peer_sig_map_get_sig(STATE->peersigs, peer_id);
      DUMP_LINE(mon_delim, "{");
      mon_delim = ",";
      indent += 2;
      DUMP_LINE("", "monitor_idx: %d", peer_id);
      // DUMP_LINE(",", "project: \"%s\"", ???); // not available from bgpview
      DUMP_LINE(",", "collector: \"%s\"", ps->collector_str);
      DUMP_LINE(",", "prefix_count: %"PRIu32, peer_pfx_cnt);
      DUMP_LINE(",", "asn: %"PRIu32, ps->peer_asnumber);
      indent -= 2;
      DUMP_LINE("", "}");
    }

    indent -= 2;
    DUMP_LINE("", "]"); // monitors list
  }
  // Dump prefixes

  DUMP_LINE(",", "prefix_as_meta_data: [");
  indent += 2;

  const char *pfx_delim = "";

  // for each ipv
  for (int vidx = 0; vidx < BGPSTREAM_MAX_IP_VERSION_IDX; vidx++) {
    khint_t end;
    if (version && bgpstream_ipv2idx(version) != vidx)
      continue;
    if (vidx == v4idx) {
      end = kh_end(STATE->v4pfxs);
    } else { // v6
      end = kh_end(STATE->v6pfxs);
    }

    // for each prefix in the selected ipv
    for (khint_t pi = 0; pi != end; ++pi) {
      bgpstream_pfx_t *pfx;
      pfx_info_t *pfxinfo;
      if (vidx == v4idx) {
        if (!kh_exist(STATE->v4pfxs, pi)) continue;
        pfx = (bgpstream_pfx_t*)&kh_key(STATE->v4pfxs, pi);
        pfxinfo = kh_val(STATE->v4pfxs, pi);
      } else { // v6
        if (!kh_exist(STATE->v6pfxs, pi)) continue;
        pfx = (bgpstream_pfx_t*)&kh_key(STATE->v6pfxs, pi);
        pfxinfo = kh_val(STATE->v6pfxs, pi);
      }

      char pfx_str[INET6_ADDRSTRLEN + 4];
      bgpstream_pfx_snprintf(pfx_str, sizeof(pfx_str), pfx);

      // dump {pfx,origin} => ...
      for (uint32_t oi = 0; oi < pfxinfo->origin_cnt; ++oi) {
        origin_info_t *originfo = &pfxinfo->origins[oi];
        bgpstream_as_path_store_path_id_t path_id = originfo->path_id;
        bgpstream_as_path_seg_t *seg = path_get_origin_seg(STATE->pathstore, path_id);

        char orig_str[4096];
        bgpstream_as_path_seg_snprintf(orig_str, sizeof(orig_str), seg);

        DUMP_LINE(pfx_delim, "{"); // prefix_as_meta_data obj
        pfx_delim = ",";
        indent += 2;
        DUMP_LINE("", "network: \"%s\"", pfx_str);
        DUMP_LINE(",", "asn: \"%s\"", orig_str);

        // full/partial-feed monitor counts
        DUMP_LINE(",", "monitors: { full: %d, partial: %d }",
            originfo->full_feed_peer_cnt, originfo->partial_feed_peer_cnt);

        // announced_duration
        DUMP_LINE(",", "announced_duration: { full: %d, partial: %d }",
            originfo->full_feed_peer_view_cnt * view_interval,
            originfo->partial_feed_peer_view_cnt * view_interval);

        // list of {monitor_idx, duration}
        if (!STATE->peer_count_only) {
          DUMP_LINE(",", "monitors: [");
          indent += 2;
          const char *pfxmon_delim = "";
          for (uint32_t mi = 0; mi != kh_end(originfo->peers); ++mi) {
            if (!kh_exist(originfo->peers, mi)) continue;
            uint32_t duration = view_interval *
              (kh_val(originfo->peers, mi).full_cnt +
               kh_val(originfo->peers, mi).partial_cnt);
            DUMP_LINE(pfxmon_delim, "{ monitor:%"PRIu16", duration:%"PRIu32" }",
                kh_key(originfo->peers, mi), duration);
            pfxmon_delim = ",";
          }
          indent -= 2;
          DUMP_LINE("", "]"); // monitors
        }

        indent -= 2;
        DUMP_LINE("", "}"); // prefix_as_meta_data obj
      }
      free(pfxinfo);
    }
  }
  indent -= 2;
  DUMP_LINE("", "]\n"); // prefix_as_meta_data list

  /* close the output files and create .done file */
  if (close_outfiles(consumer) != 0) {
    return -1;
  }

  return 0;
}

static int init_my_state(bvc_t *consumer, bgpview_t *srcview)
{
  // receiving first view; initialize my state
  uint32_t vtime = bgpview_get_time(srcview);
  STATE->peersigs = bgpview_get_peersigns(srcview);
  STATE->pathstore = bgpview_get_as_path_store(srcview);
  STATE->view_cnt = 0;
  STATE->first_view_time = vtime;
  STATE->prev_view_time = 0;
  STATE->prev_view_interval = 0;
  STATE->out_interval_start = vtime;
  STATE->next_output_time = vtime + STATE->out_interval;
  STATE->v4pfxs = kh_init(map_v4pfx_pfxinfo);
  STATE->v6pfxs = kh_init(map_v6pfx_pfxinfo);
  STATE->peers = kh_init(map_peerid_pfxcnt);

  return 0;
}

static int end_output_interval(bvc_t *consumer, uint32_t vtime,
    uint32_t view_interval)
{
  if (STATE->split_ipv) {
    for (int vidx = 0; vidx < BGPSTREAM_MAX_IP_VERSION_IDX; vidx++) {
      if (dump_results(consumer, bgpstream_idx2ipv(vidx), view_interval) < 0)
        return -1;
    }
  } else {
    if (dump_results(consumer, 0, view_interval) < 0)
      return -1;
  }

  // reset state
  kh_clear(map_peerid_pfxcnt, STATE->peers);
  kh_clear(map_v4pfx_pfxinfo, STATE->v4pfxs);
  kh_clear(map_v6pfx_pfxinfo, STATE->v6pfxs);
  STATE->view_cnt = 0;
  STATE->out_interval_start = vtime;
  STATE->next_output_time += STATE->out_interval;
  return 0;
}

static void dump_stats(bvc_t *consumer, pfx2as_v2_stats_t *stats)
{
  // for each prefix
  int v4idx = bgpstream_ipv2idx(BGPSTREAM_ADDR_VERSION_IPV4);
  for (int vidx = 0; vidx < BGPSTREAM_MAX_IP_VERSION_IDX; ++vidx) {
    khint_t end;
    if (vidx == v4idx) {
      end = kh_end(STATE->v4pfxs);
    } else { // v6
      end = kh_end(STATE->v6pfxs);
    }

    // for each prefix in the selected ipv
    for (khint_t pi = 0; pi != end; ++pi) {
      bgpstream_pfx_t *pfx;
      pfx_info_t *pfxinfo;
      if (vidx == v4idx) {
        if (!kh_exist(STATE->v4pfxs, pi)) continue;
        pfx = (bgpstream_pfx_t*)&kh_key(STATE->v4pfxs, pi);
        pfxinfo = kh_val(STATE->v4pfxs, pi);
      } else { // v6
        if (!kh_exist(STATE->v6pfxs, pi)) continue;
        pfx = (bgpstream_pfx_t*)&kh_key(STATE->v6pfxs, pi);
        pfxinfo = kh_val(STATE->v6pfxs, pi);
      }

      stats->pfxorigin_cnt += pfxinfo->origin_cnt;
      if (pfxinfo->origin_cnt > 1) {
        stats->mop_cnt++;
        char pfx_str[INET6_ADDRSTRLEN + 4];
        bgpstream_pfx_snprintf(pfx_str, sizeof(pfx_str), pfx);
        printf("## mop %s:", pfx_str);
        for (uint32_t i = 0; i < pfxinfo->origin_cnt; ++i) {
          char orig_str[4096];
          bgpstream_as_path_seg_snprintf(orig_str, sizeof(orig_str),
                path_get_origin_seg(STATE->pathstore, pfxinfo->origins[i].path_id));
          printf(" origin %s:", orig_str);
          for (khint_t mi = 0; mi != kh_end(pfxinfo->origins[i].peers); ++mi) {
            if (!kh_exist(pfxinfo->origins[i].peers, mi)) continue;
            printf(" %d %d+%d;",
                kh_key(pfxinfo->origins[i].peers, mi), // peer_id
                kh_val(pfxinfo->origins[i].peers, mi).full_cnt,
                kh_val(pfxinfo->origins[i].peers, mi).partial_cnt);
          }
        }
        printf("\n");
      }
      if (pfxinfo->origin_cnt > stats->max_origin_cnt)
        stats->max_origin_cnt = pfxinfo->origin_cnt;
    }
  }

  uint32_t pfx_cnt = kh_size(STATE->v4pfxs) + kh_size(STATE->v6pfxs);

  printf("# pfxs=%d; po: tot=%d, max=%d; po/pfxs=%f; mop=%d; grow=%d\n",
      pfx_cnt,
      stats->pfxorigin_cnt,
      stats->max_origin_cnt,
      (double)stats->pfxorigin_cnt / pfx_cnt,
      stats->mop_cnt,
      stats->grow_cnt);
}

int bvc_pfx2as_v2_process_view(bvc_t *consumer, bgpview_t *view)
{
  uint32_t vtime = bgpview_get_time(view);
  uintptr_t view_interval = 0;
  bgpview_iter_t *vit = NULL;

  if (STATE->prev_view_time == 0) {
    // receiving first view; initialize my state
    if (init_my_state(consumer, view) < 0)
      goto err;

  } else {
    view_interval = vtime - STATE->prev_view_time;
    if (STATE->prev_view_interval == 0) {
      // second view (end of first view_interval)
      if (STATE->out_interval % view_interval != 0) {
        fprintf(stderr, "WARNING: " NAME ": output interval %d is not a multiple "
            "of view interval %"PRIuPTR" at %"PRIu32"\n",
            STATE->out_interval, view_interval, vtime);
      }
    } else {
      if (STATE->prev_view_interval != view_interval) {
        // third+ view (end of second+ view_interval)
        fprintf(stderr, "ERROR: " NAME ": view interval changed from %d to "
            "%"PRIuPTR" at %"PRIu32"\n",
            STATE->prev_view_interval, view_interval, vtime);
        goto err;
      }
    }
    if (vtime >= STATE->next_output_time) {
      // End the output interval BEFORE processing current view
      if (end_output_interval(consumer, vtime, view_interval) < 0)
        goto err;
    }
  }

  vit = bgpview_iter_create(view);
  pfx2as_v2_stats_t stats;
  memset(&stats, 0, sizeof(stats));
  STATE->view_cnt++;

  // for each prefix
  for (bgpview_iter_first_pfx(vit, 0, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(vit);
      bgpview_iter_next_pfx(vit)) {

    bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(vit);
    int vidx = bgpstream_ipv2idx(pfx->address.version);
    int khret;

    khint_t pi;
    if (pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV4) {
      pi = kh_put(map_v4pfx_pfxinfo, STATE->v4pfxs, pfx->bs_ipv4, &khret);
    } else {
      pi = kh_put(map_v6pfx_pfxinfo, STATE->v6pfxs, pfx->bs_ipv6, &khret);
    }
    int pfx_existed = (khret == 0);
    pfx_info_t *pfxinfo = NULL;
    int origin_cnt = 0;
    if (pfx_existed) {
      if (pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV4) {
        pfxinfo = kh_val(STATE->v4pfxs, pi);
      } else {
        pfxinfo = kh_val(STATE->v6pfxs, pi);
      }
      origin_cnt = pfxinfo->origin_cnt;
    } else {
      // pfxinfo will be allocated later, for the first peer-origin
    }

    char pfx_str[INET6_ADDRSTRLEN + 4];
    bgpstream_pfx_snprintf(pfx_str, sizeof(pfx_str), pfx);

    // reset counted flags
    if (pfxinfo) {
      for (uint32_t oi = 0; oi < pfxinfo->origin_cnt; ++oi) {
        pfxinfo->origins[oi].counted_as_full = 0;
        pfxinfo->origins[oi].counted_as_partial = 0;
      }
    }

    // for each peer in pfx
    for (bgpview_iter_pfx_first_peer(vit, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_pfx_has_more_peer(vit);
        bgpview_iter_pfx_next_peer(vit)) {
      khint_t mi; // peer (monitor) index

      bgpstream_peer_id_t peer_id = bgpview_iter_peer_get_peer_id(vit);
      bgpstream_as_path_store_path_id_t path_id =
          bgpview_iter_pfx_peer_get_as_path_store_path_id(vit);
      bgpstream_as_path_seg_t *origin =
          bgpview_iter_pfx_peer_get_origin_seg(vit);
      int is_full = bgpstream_id_set_exists(
          CHAIN_STATE->full_feed_peer_ids[vidx], peer_id);

      char orig_str[4096];
      bgpstream_as_path_seg_snprintf(orig_str, sizeof(orig_str), origin);

      // Most prefixes have one origin, so a linear search is efficient
      int oi = -1; // origin index
      for (int i = 0; i < origin_cnt; ++i) {
        // Comparing path_ids is cheaper, but if that fails we must still
        // compare origins because different paths can have the same origin.
        if (path_id_equal(path_id, pfxinfo->origins[i].path_id) ||
            bgpstream_as_path_seg_equal(origin,
              path_get_origin_seg(STATE->pathstore, pfxinfo->origins[i].path_id))) {
          oi = i;
          break;
        }
      }
      // XXX TODO: if (oi<0), see if we can recycle an empty origin
      if (oi < 0) {
        // allocate or grow pfxinfo to accommodate a new pfxinfo->origins entry
        oi = origin_cnt++;
        if (pfxinfo)
          stats.grow_cnt++;
        pfxinfo = realloc(pfxinfo, pfxinfo_size(origin_cnt));
        if (pfx->address.version == BGPSTREAM_ADDR_VERSION_IPV4) {
          kh_val(STATE->v4pfxs, pi) = pfxinfo;
        } else {
          kh_val(STATE->v6pfxs, pi) = pfxinfo;
        }
        pfxinfo->origin_cnt = origin_cnt;
        pfxinfo->origins[oi].path_id = path_id;
        pfxinfo->origins[oi].counted_as_full = 0;
        pfxinfo->origins[oi].full_feed_peer_cnt = 0;
        pfxinfo->origins[oi].full_feed_peer_view_cnt = 0;
        pfxinfo->origins[oi].counted_as_partial = 0;
        pfxinfo->origins[oi].partial_feed_peer_cnt = 0;
        pfxinfo->origins[oi].partial_feed_peer_view_cnt = 0;
        pfxinfo->origins[oi].peers = kh_init(map_peerid_viewcnt);
      }

      // count pfx-origin peertype
      if (is_full) {
        if (!pfxinfo->origins[oi].counted_as_full) {
          pfxinfo->origins[oi].full_feed_peer_view_cnt++;
          pfxinfo->origins[oi].counted_as_full = 1;
        }
      } else {
        if (!pfxinfo->origins[oi].counted_as_partial) {
          pfxinfo->origins[oi].partial_feed_peer_view_cnt++;
          pfxinfo->origins[oi].counted_as_partial = 1;
        }
      }

      // count pfx-origin-peer
      mi = kh_put(map_peerid_viewcnt, pfxinfo->origins[oi].peers, peer_id, &khret);
      if (khret > 0) { // new entry?
        memset(&kh_val(pfxinfo->origins[oi].peers, mi), 0, sizeof(peerviews_t));
      }
      if (is_full) {
        kh_val(pfxinfo->origins[oi].peers, mi).full_cnt++;
      } else {
        kh_val(pfxinfo->origins[oi].peers, mi).partial_cnt++;
      }
    }
  }

  bgpview_iter_destroy(vit);

  STATE->prev_view_interval = view_interval;
  STATE->prev_view_time = vtime;

  dump_stats(consumer, &stats);

  return 0;
err:
  bgpview_iter_destroy(vit);
  return -1;
}

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
      "consumer usage: %s\n"
      "       -i <output-interval>  output interval in seconds (default %d)\n"
      "       -o <path>             output directory\n"
      "       -c                    output peer counts, not full list\n"
      "       -v                    split prefixes into files by IP version\n",
      consumer->name, OUTPUT_INTERVAL);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  assert(argc > 0 && argv != NULL);

  bvc_pfx2as_v2_state_t *state = STATE;

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, "i:o:cv?")) >= 0) {
    switch (opt) {
    case 'i':
      state->out_interval = strtoul(optarg, NULL, 10);
      break;
    case 'o':
      state->outdir = strdup(optarg);
      break;
    case 'c':
      state->peer_count_only = 1;
      break;
    case 'v':
      state->split_ipv = 1;
      break;
    case '?':
    case ':':
    default:
      usage(consumer);
      return -1;
    }
  }

  if (state->outdir == NULL) {
    fprintf(stderr, "ERROR: " NAME " output directory required\n");
    usage(consumer);
    return -1;
  }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_pfx2as_v2_alloc()
{
  return &bvc_pfx2as_v2;
}

int bvc_pfx2as_v2_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_pfx2as_v2_state_t *state = NULL;

  if ((state = malloc_zero(sizeof(bvc_pfx2as_v2_state_t))) == NULL) {
    return -1;
  }
  BVC_SET_STATE(consumer, state);

  state->out_interval = OUTPUT_INTERVAL;

  /* parse the command line args */
  if (parse_args(consumer, argc, argv) != 0) {
    goto err;
  }

  fprintf(stderr, "INFO: output directory: %s\n", state->outdir);

  if (CHAIN_STATE->visibility_computed == 0) {
    fprintf(stderr, "ERROR: " NAME " requires the Visibility consumer "
                    "to be run first\n");
    goto err;
  }

  // Test ability to open output files now so user gets immediate feedback on
  // any errors, instead of waiting for the end of the first out_interval
  if (open_outfiles(consumer, 0, 0) != 0) {
    goto err;
  }
  wandio_wdestroy(STATE->outfile);
  STATE->outfile = NULL;
  remove(STATE->outfile_name);

  return 0;

err:
  bvc_pfx2as_v2_destroy(consumer);
  return -1;
}

void bvc_pfx2as_v2_destroy(bvc_t *consumer)
{
  if (STATE == NULL) {
    return;
  }

  if (STATE->outfile) {
    if (STATE->prev_view_time > STATE->out_interval_start) {
      fprintf(stderr, "WARNING: omitting incomplete %s output interval %d-%d\n",
          NAME, STATE->out_interval_start, STATE->prev_view_time);
    }
    wandio_wdestroy(STATE->outfile);
    STATE->outfile = NULL;
  }

  if (STATE->v4pfxs) {
    for (khint_t k = 0; k < kh_end(STATE->v4pfxs); ++k) {
      if (!kh_exist(STATE->v4pfxs, k)) continue;
      free(kh_val(STATE->v4pfxs, k));
    }
    kh_destroy(map_v4pfx_pfxinfo, STATE->v4pfxs);
  }

  if (STATE->v6pfxs) {
    for (khint_t k = 0; k < kh_end(STATE->v6pfxs); ++k) {
      if (!kh_exist(STATE->v6pfxs, k)) continue;
      free(kh_val(STATE->v6pfxs, k));
    }
    kh_destroy(map_v6pfx_pfxinfo, STATE->v6pfxs);
  }

  if (STATE->peers)
    kh_destroy(map_peerid_pfxcnt, STATE->peers);

#if 0
  for (int i = 0; i < BGPSTREAM_MAX_IP_VERSION_IDX; ++i) {
    if (STATE->full_feed_peer_set[i]) {
      bgpstream_id_set_destroy(STATE->full_feed_peer_set[i]);
    }
  }
#endif

  free(STATE->outdir);

  free(STATE);
  BVC_SET_STATE(consumer, NULL);
}

