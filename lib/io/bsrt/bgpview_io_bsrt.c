/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2019 The Regents of the University of California.
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

#include "bgpview_io_bsrt.h"
#include "bgpview_io_bsrt_int.h"
#include "bgpview.h"
#include "config.h"
#include "utils.h"
#include "parse_cmd.h"
#include "bgpstream.h"
#include "bgpcorsaro.h"
#include "bgpcorsaro_log.h"
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#ifdef HAVE_TIME_H
#include <time.h>
#endif
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif


#define BGPVIEW_IO_BSRT_GAPLIMIT_DEFAULT 0
#define BGPVIEW_IO_BSRT_INTERVAL_DEFAULT 60


struct bgpview_io_bsrt {
  bgpstream_t *stream;
  bgpstream_data_interface_id_t di_id_default;
  bgpstream_data_interface_id_t di_id;
  bgpstream_data_interface_info_t *di_info;
  bgpcorsaro_t *bgpcorsaro;
  struct {
    int gap_limit;
    char *tmpl;
    char *name;
    int interval;
    int align;
    int rotate;
    int meta_rotate;
    int logfile_disable;
    uint32_t minimum_time;
  } cfg;
};

struct window {
  uint32_t start;
  uint32_t end;
};

/* ========== PRIVATE FUNCTIONS ========== */

typedef struct {
  bgpview_t *view;
  bgpview_iter_t *iter;
  bgpview_iter_t *nextiter;
  long t_base;
  int need_rib_start;
  int rib_in_progress;
  int update_complete;
  int view_size;
  int step;
  int verbose;
} test_state_t;

// operations for test script
#define OP_EOS                0x001
#define OP_RIB                0x002
#define OP_PFX_ANNOUNCE       0x003
#define OP_PFX_WITHDRAW       0x004
#define OP_PEER_UP            0x005
#define OP_PEER_DOWN          0x006
#define OP_MASK               0x0FF
// operation modifiers
#define OP_LOST               0x100 // simulate a lost update message

typedef struct {
  int t_sec;
  int op;
  int peer;
  const char *pfx;
} test_instruction_t;

static test_state_t test = {0};
static char buf[65536];

static test_instruction_t testscript[] = {
//  time  operation                  peer  pfx
  {    0, OP_RIB,                     0,   NULL },
  {  199, OP_PFX_ANNOUNCE,            3,   "10.3.3.0/24" },
  {  299, OP_PFX_WITHDRAW,            4,   "10.4.4.0/24" },
  {  399, OP_PFX_ANNOUNCE | OP_LOST,  6,   "10.6.6.0/24" },
  {  499, OP_PFX_WITHDRAW | OP_LOST,  7,   "10.7.7.0/24" },
  {  599, OP_PEER_DOWN,               5,   NULL },
  {  699, OP_PEER_DOWN,               2,   NULL },
  {  799, OP_PEER_UP,                 5,   NULL },
  // RT will deactivate a peer if it is missing from a RIB and has sent no
  // messages for at least RT_MAX_INACTIVE_TIME (3600s).
  { 7200, OP_RIB,                     0,   NULL },
  {   -1, OP_EOS,                     0,   NULL },
};

#define TEST_PEER_ASN(peer)         (1000 * (peer))
#define TEST_PEER_ADDR(peer)        htonl((100<<24) | (peer)) // "100.0.0.peer"
#define TEST_PFX(peer, i)           htonl((10<<24) | ((peer)<<16) | ((i)<<8)) // "10.peer.i.0"
#define TEST_ORIGIN_ASN(peer, i)    (1000 * (peer) + 10 * (i))
#define TEST_HOP_ASN(peer, i, hop)  (1000 * (peer) + 10 * (i) + hop)

static int print_record(bgpstream_record_t *record)
{
  if (bgpstream_record_snprintf(buf, sizeof(buf), record) == NULL) {
    fprintf(stderr, "ERROR: Could not convert record to string\n");
    return -1;
  }

  printf("REC: %s\n", buf);
  return 0;
}

static int print_elem(bgpstream_record_t *record, bgpstream_elem_t *elem)
{
  if (bgpstream_record_elem_snprintf(buf, sizeof(buf), record, elem) == NULL) {
    fprintf(stderr, "ERROR: Could not convert record/elem to string\n");
    return -1;
  }

  printf("ELEM: %s\n", buf);
  return 0;
}

// Internal bgpstream function for testing only
int bgpstream_as_path_append(bgpstream_as_path_t *path,
                             bgpstream_as_path_seg_type_t type, uint32_t *asns,
                             int asns_cnt);

static int test_get_next_record(bgpstream_t *bgpstream,
    bgpstream_record_t **bsrecord)
{
  int rc;
  bgpstream_pfx_t pfx;
  static bgpstream_record_t *testrec = NULL;

  if (!test.view) {
    // initialize
    static const char *test_collector_name = "TEST_COLLECTOR";
    static int test_peer_cnt = 7;
    static int test_table_size = 9;

    test.verbose = 1;
    test.t_base = 1000000000;
    test.need_rib_start = 1;
    test.rib_in_progress = 0;
    test.view_size = 0;
    test.step = 0;
    testrec = malloc_zero(sizeof(*testrec));
    strcpy(testrec->project_name, "TEST_PROJECT");
    strcpy(testrec->collector_name, test_collector_name);

    if (!(test.view = bgpview_create(NULL, NULL, NULL, NULL))) {
      fprintf(stderr, "ERROR: can't create test view\n");
      goto err;
    }
    if (!(test.iter = bgpview_iter_create(test.view)) ||
      !(test.nextiter = bgpview_iter_create(test.view)))
    {
      fprintf(stderr, "ERROR: can't create test view iterators\n");
      goto err;
    }

    bgpview_set_time(test.view, test.t_base + testscript[0].t_sec);

    bgpstream_as_path_t *test_as_path;
    test_as_path = bgpstream_as_path_create();

    for (int peer = 1; peer <= test_peer_cnt; peer++) {
      int peer_id;
      int pfx_cnt = 0;
      bgpstream_ip_addr_t test_peer_ip;
      test_peer_ip.version = BGPSTREAM_ADDR_VERSION_IPV4;
      test_peer_ip.bs_ipv4.addr.s_addr = TEST_PEER_ADDR(peer);
      // test_peer_status = 2;
      uint32_t peer_asn = TEST_PEER_ASN(peer);
      peer_id = bgpview_iter_add_peer(test.iter, test_collector_name, &test_peer_ip, peer_asn);
      if (peer_id == 0) {
        fprintf(stderr, "ERROR: can't add peer to test view\n");
        goto err;
      }
      assert(peer_id == peer);
      if (bgpview_iter_activate_peer(test.iter) != 1) {
        fprintf(stderr, "ERROR: can't activate peer in test view\n");
        goto err;
      }

      for (int i = 1; i <= test_table_size; i++) {
        bgpstream_pfx_t test_prefix;
        test_prefix.address.version = BGPSTREAM_ADDR_VERSION_IPV4;
        test_prefix.address.bs_ipv4.addr.s_addr = TEST_PFX(peer, i);
        test_prefix.mask_len = 24;

        int seg_cnt = (peer + i) % 5 + 2; // 2 - 6 segments

        uint32_t asn_set[3];
        bgpstream_as_path_clear(test_as_path);
        bgpstream_as_path_append(test_as_path, BGPSTREAM_AS_PATH_SEG_ASN, &peer_asn, 1);
        for (int hop = seg_cnt-2; hop > 0; hop--) {
          asn_set[0] = TEST_HOP_ASN(peer, i, hop);
          bgpstream_as_path_append(test_as_path, BGPSTREAM_AS_PATH_SEG_ASN, asn_set, 1);
        }
        asn_set[0] = TEST_ORIGIN_ASN(peer, i);
        if (peer == 4 && i % 3 == 0) {
          // insert an AS SET to make things interesting
          asn_set[1] = asn_set[0] + 100;
          asn_set[2] = asn_set[0] + 200;
          bgpstream_as_path_append(test_as_path, BGPSTREAM_AS_PATH_SEG_SET, asn_set, 2+i%2);
        } else {
          bgpstream_as_path_append(test_as_path, BGPSTREAM_AS_PATH_SEG_ASN, asn_set, 1);
        }

        if (bgpview_iter_add_pfx_peer(test.iter, &test_prefix, peer_id, test_as_path) != 0) {
          fprintf(stderr, "ERROR: can't add prefix to test view\n");
          goto err;
        }
        if ((peer % 3 == 0) && i == peer) {
          // leave a few routes unactivated to make the test more interesting
        } else {
          if (bgpview_iter_pfx_activate_peer(test.iter) != 1) {
            fprintf(stderr, "ERROR: can't activate pfx-peer int test view\n");
            goto err;
          }
        }
        pfx_cnt++;
        test.view_size++;
      }
      fprintf(stderr, "TEST: added %d prefixes\n", pfx_cnt); // XXX
    }
    bgpstream_as_path_destroy(test_as_path);
  }

  *bsrecord = NULL;
  test_instruction_t *instr = &testscript[test.step];
  switch (instr->op & OP_MASK) {

    case OP_RIB:
      if (!test.rib_in_progress) {
        // generate RIB START record
        test.rib_in_progress = 1;
        testrec->type = BGPSTREAM_RIB;
        testrec->dump_pos = BGPSTREAM_DUMP_START;
        testrec->status = BGPSTREAM_RECORD_STATUS_VALID_RECORD;
        testrec->dump_time_sec = test.t_base + instr->t_sec;
        testrec->time_sec = test.t_base + instr->t_sec;
        rc = bgpview_iter_first_pfx(test.iter, 0, BGPVIEW_FIELD_ACTIVE);
        assert(rc);
        bgpview_iter_first_pfx(test.nextiter, 0, BGPVIEW_FIELD_ACTIVE);
        bgpview_iter_next_pfx(test.nextiter); // stay one step ahead of iter
        rc = bgpview_iter_pfx_first_peer(test.iter, BGPVIEW_FIELD_ACTIVE);
        assert(rc);
        if (test.verbose) {
          print_record(testrec);
        }
        *bsrecord = testrec;
        return 1; // got record

      } else {
        // generate RIB MIDDLE or RIB END record
        assert(bgpview_iter_has_more_pfx(test.iter));
        testrec->type = BGPSTREAM_RIB;
        testrec->status = BGPSTREAM_RECORD_STATUS_VALID_RECORD;
        testrec->dump_time_sec = test.t_base + instr->t_sec;
        testrec->time_sec = test.t_base + instr->t_sec + 1;
        rc = bgpview_iter_next_pfx(test.iter);
        assert(rc);
        rc = bgpview_iter_pfx_first_peer(test.iter, BGPVIEW_FIELD_ACTIVE);
        assert(rc);

        bgpview_iter_next_pfx(test.nextiter); // stay one step ahead of iter
        if (bgpview_iter_has_more_pfx(test.nextiter)) {
          testrec->dump_pos = BGPSTREAM_DUMP_MIDDLE;
        } else {
          testrec->time_sec += 1;
          testrec->dump_pos = BGPSTREAM_DUMP_END;
        }

        *bsrecord = testrec;
        return 1; // got record
      }

    case OP_PFX_ANNOUNCE:
    case OP_PFX_WITHDRAW:
      bgpstream_str2pfx(instr->pfx, &pfx);
      bgpview_iter_seek_pfx_peer(test.iter, &pfx, instr->peer, BGPVIEW_FIELD_ALL_VALID, BGPVIEW_FIELD_ALL_VALID);
      if ((instr->op & OP_MASK) == OP_PFX_ANNOUNCE) {
        bgpview_iter_pfx_activate_peer(test.iter);
      } else {
        bgpview_iter_pfx_deactivate_peer(test.iter);
      }
      if (instr->op & OP_LOST) {
        // simulate a lost update record; move on to next instruction
        test.step++;
        return test_get_next_record(bgpstream, bsrecord);
      }
      // generate UPDATE record
      test.update_complete = 0;
      testrec->type = BGPSTREAM_UPDATE;
      testrec->status = BGPSTREAM_RECORD_STATUS_VALID_RECORD;
      testrec->time_sec = test.t_base + instr->t_sec;
      *bsrecord = testrec;
      return 1; // got record

    case OP_PEER_DOWN:
      bgpview_iter_seek_peer(test.iter, instr->peer, BGPVIEW_FIELD_ALL_VALID);
      bgpview_iter_deactivate_peer(test.iter); // also deactivates pfx-peers
      // don't generate a record; go to next instruction
      test.step++;
      return test_get_next_record(bgpstream, bsrecord);

    case OP_PEER_UP:
      bgpview_iter_seek_peer(test.iter, instr->peer, BGPVIEW_FIELD_ALL_VALID);
      bgpview_iter_activate_peer(test.iter); // does NOT activate pfx-peers
      bgpview_iter_first_pfx_peer(test.iter, 0, BGPVIEW_FIELD_INACTIVE, BGPVIEW_FIELD_INACTIVE);
      while (bgpview_iter_has_more_pfx_peer(test.iter)) {
        if (bgpview_iter_peer_get_peer_id(test.iter) == instr->peer) {
          bgpview_iter_pfx_activate_peer(test.iter);
        }
        bgpview_iter_next_pfx_peer(test.iter);
      }
      // don't generate a record; go to next instruction
      test.step++;
      return test_get_next_record(bgpstream, bsrecord);

    case OP_EOS:
      // end-of-stream
      bgpview_iter_destroy(test.iter);
      bgpview_iter_destroy(test.nextiter);
      bgpview_destroy(test.view);
      test.view = NULL;
      free(testrec);
      testrec = NULL;
      return 0; // EOS
  }


err:
  return -1;
}

static int test_record_get_next_elem(bgpstream_record_t *bsrecord,
    bgpstream_elem_t **elem)
{
  static bgpstream_elem_t *testel = NULL;
  bgpstream_peer_sig_t *ps;
  bgpstream_pfx_t *pfx;
  bgpstream_as_path_t *path;

  assert(test.view);
  *elem = NULL;

  if (!testel) {
    testel = bgpstream_elem_create();
  }


  test_instruction_t *instr = &testscript[test.step];
  switch (instr->op) {

    case OP_RIB:
      if (!bgpview_iter_pfx_has_more_peer(test.iter)) {
        if (!bgpview_iter_has_more_pfx(test.nextiter)) {
          // no more records in RIB
          test.rib_in_progress = 0;
          test.step++;
          if (test.verbose) {
            print_record(bsrecord);
          }
        }
        return 0; // no more elems in record
      }

      // generate RIB elem
      testel->type = BGPSTREAM_ELEM_TYPE_RIB;
      testel->orig_time_sec = test.t_base + instr->t_sec;
      testel->orig_time_usec = 0;

      ps = bgpview_iter_peer_get_sig(test.iter);

      // peer_ip
      bgpstream_addr_copy(&testel->peer_ip, &ps->peer_ip_addr);

      // peer_asn
      testel->peer_asn = ps->peer_asnumber;

      // prefix
      pfx = bgpview_iter_pfx_get_pfx(test.iter);
      bgpstream_pfx_copy(&testel->prefix, pfx);

      // nexthop
      bgpstream_addr_copy(&testel->nexthop, &ps->peer_ip_addr);

      // as_path
      path = bgpview_iter_pfx_peer_get_as_path(test.iter);
      bgpstream_as_path_copy(testel->as_path, path);
      bgpstream_as_path_destroy(path);

      // communities

      // origin

      // med

      // local_pref

      // atomic_aggregate

      // aggregator
      testel->aggregator.has_aggregator = 0;

      if (test.verbose) {
        print_elem(bsrecord, testel);
      }
      *elem = testel;
      bgpview_iter_pfx_next_peer(test.iter);
      return 1;

    case OP_PFX_ANNOUNCE:
    case OP_PFX_WITHDRAW:
      if (test.update_complete) {
        test.step++;
        return 0; // no more elems in record
      }

      // generate UPDATE elem
      testel->type = (instr->op == OP_PFX_ANNOUNCE) ?
        BGPSTREAM_ELEM_TYPE_ANNOUNCEMENT : BGPSTREAM_ELEM_TYPE_WITHDRAWAL;
      testel->orig_time_sec = test.t_base + instr->t_sec;
      testel->orig_time_usec = 0;

      ps = bgpview_iter_peer_get_sig(test.iter);

      // peer_ip
      bgpstream_addr_copy(&testel->peer_ip, &ps->peer_ip_addr);

      // peer_asn
      testel->peer_asn = ps->peer_asnumber;

      // prefix
      pfx = bgpview_iter_pfx_get_pfx(test.iter);
      bgpstream_pfx_copy(&testel->prefix, pfx);

      // nexthop
      bgpstream_addr_copy(&testel->nexthop, &ps->peer_ip_addr);

      // as_path
      path = bgpview_iter_pfx_peer_get_as_path(test.iter);
      bgpstream_as_path_copy(testel->as_path, path);
      bgpstream_as_path_destroy(path);

      if (test.verbose) {
        print_elem(bsrecord, testel);
      }
      *elem = testel;
      test.update_complete = 1;
      return 1;


    default:
      assert(0 && "not yet implemented");
  }
}


static void data_if_usage(bgpview_io_bsrt_t *bsrt)
{
  bgpstream_data_interface_id_t *ids = NULL;
  int id_cnt = 0;
  int i;

  bgpstream_data_interface_info_t *info = NULL;

  id_cnt = bgpstream_get_data_interfaces(bsrt->stream, &ids);

  for (i = 0; i < id_cnt; i++) {
    info = bgpstream_get_data_interface_info(bsrt->stream, ids[i]);

    if (info != NULL) {
      fprintf(stderr, "       %-13s%s%s\n", info->name, info->description,
              (ids[i] == bsrt->di_id_default) ? " (default)" : "");
    }
  }
}

static void dump_if_options(bgpview_io_bsrt_t *bsrt)
{
  assert(bsrt->di_id != 0);

  bgpstream_data_interface_option_t *options;
  int opt_cnt = 0;
  int i;

  opt_cnt = bgpstream_get_data_interface_options(bsrt->stream, bsrt->di_id,
      &options);

  fprintf(stderr, "Data interface options for '%s':\n", bsrt->di_info->name);
  if (opt_cnt == 0) {
    fprintf(stderr, "   [NONE]\n");
  } else {
    for (i = 0; i < opt_cnt; i++) {
      fprintf(stderr, "   %-13s%s\n", options[i].name, options[i].description);
    }
  }
  fprintf(stderr, "\n");
}

static void usage(bgpview_io_bsrt_t *bsrt)
{
  fprintf(stderr,
    "BSRT IO Options:\n"
    "   -d <interface> use the given bgpstream data interface to find "
    "available data\n"
    "                  available data interfaces are:\n"
    );
  data_if_usage(bsrt);
  fprintf(stderr,
    "   -o <option-name=option-value>*\n"
    "                  set an option for the current data interface.\n"
    "                  use '-o ?' to get a list of available options for the "
    "current\n"
    "                  data interface. (data interface can be selected using "
    "-d)\n"
    "   -p <project>   process records from only the given project "
    "(routeviews, ris)*\n"
    "   -c <collector> process records from only the given collector*\n"
    "   -t <type>      process records with only the given type (ribs, "
    "updates)*\n"
    "   -w <start>[,<end>]\n"
    "                  process records within the given time window\n"
#if 0
    "                    (omitting the end parameter enables live mode)*\n"
#endif
    "   -P <period>    process a rib files every <period> seconds (bgp time)\n"
    "   -j <peer ASN>  return valid elems originated by a specific peer ASN*\n"
    "   -k <prefix>    return valid elems associated with a specific prefix*\n"
    "   -y <community> return valid elems with the specified community*\n"
    "                  (format: asn:value, the '*' metacharacter is "
    "recognized)\n"
#if 0
    "   -l             enable live mode (make blocking requests for BGP "
    "records)\n"
    "                  allows bgpcorsaro to be used to process data in "
    "real-time\n"
#endif
    "\n"
    "   -i <interval>  distribution interval in seconds (default: %d)\n"
    "   -a             align the end time of the first interval\n"
    "   -g <gap-limit> maximum allowed gap between packets (0 is no limit) "
    "(default: %d)\n",
    BGPVIEW_IO_BSRT_INTERVAL_DEFAULT, BGPVIEW_IO_BSRT_GAPLIMIT_DEFAULT);
  fprintf(
    stderr,
    "   -n <name>      monitor name (default: %s)\n",
      bgpcorsaro_get_monitorname(bsrt->bgpcorsaro));
  fprintf(
    stderr,
    "   -O <outfile>   use <outfile> as a template for file names.\n"
    "                   - %%X => plugin name\n"
    "                   - %%N => monitor name\n"
    "                   - see man strftime(3) for more options\n"
    "   -r <intervals> rotate output files after n intervals\n"
    "   -R <intervals> rotate bgpcorsaro meta files after n intervals\n"
    "\n"
    "   -h             print this help menu\n"
    "* denotes an option that can be given multiple times\n");

}


int (*bsrt_get_next_record)(bgpstream_t *, bgpstream_record_t **) =
  bgpstream_get_next_record;
int (*bsrt_record_get_next_elem)(bgpstream_record_t *, bgpstream_elem_t **) =
  bgpstream_record_get_next_elem;


static int parse_args(bgpview_io_bsrt_t *bsrt, int argc, char **argv)
{
#define PROJECT_CMD_CNT 10
#define TYPE_CMD_CNT 10
#define COLLECTOR_CMD_CNT 100
#define PREFIX_CMD_CNT 1000
#define COMMUNITY_CMD_CNT 1000
#define PEERASN_CMD_CNT 1000
#define WINDOW_CMD_CNT 1024
#define OPTION_CMD_CNT 1024

  char *projects[PROJECT_CMD_CNT];
  int projects_cnt = 0;

  char *types[TYPE_CMD_CNT];
  int types_cnt = 0;

  char *collectors[COLLECTOR_CMD_CNT];
  int collectors_cnt = 0;

  char *endp;
  struct window windows[WINDOW_CMD_CNT];
  int windows_cnt = 0;

  char *peerasns[PEERASN_CMD_CNT];
  int peerasns_cnt = 0;

  char *prefixes[PREFIX_CMD_CNT];
  int prefixes_cnt = 0;

  char *communities[COMMUNITY_CMD_CNT];
  int communities_cnt = 0;

  char *interface_options[OPTION_CMD_CNT];
  int interface_options_cnt = 0;

  int rib_period = 0;


  int opt;
  assert(argc > 0 && argv != NULL);
  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while ((opt = getopt(argc, argv, "d:o:p:c:t:w:j:k:y:P:i:ag:lLB:n:O:r:R:h")) >= 0) {
    switch (opt) {
    case 'd':
      if (strcmp(optarg, "test") == 0) {
        bsrt->di_id = 0;
        bsrt->di_info = NULL;
        bsrt_get_next_record = test_get_next_record;
        bsrt_record_get_next_elem = test_record_get_next_elem;
        if (bsrt->cfg.interval == -1000)
          bsrt->cfg.interval = 100;
      } else {
        if ((bsrt->di_id = bgpstream_get_data_interface_id_by_name(bsrt->stream, optarg)) ==
            0) {
          fprintf(stderr, "ERROR: Invalid data interface name '%s'\n", optarg);
          usage(bsrt);
          exit(-1);
        }
        bsrt->di_info = bgpstream_get_data_interface_info(bsrt->stream, bsrt->di_id);
      }
      break;

    case 'p':
      if (projects_cnt == PROJECT_CMD_CNT) {
        fprintf(stderr, "ERROR: A maximum of %d projects can be specified on "
                        "the command line\n",
                PROJECT_CMD_CNT);
        usage(bsrt);
        exit(-1);
      }
      projects[projects_cnt++] = strdup(optarg);
      break;

    case 'c':
      if (collectors_cnt == COLLECTOR_CMD_CNT) {
        fprintf(stderr, "ERROR: A maximum of %d collectors can be specified on "
                        "the command line\n",
                COLLECTOR_CMD_CNT);
        usage(bsrt);
        exit(-1);
      }
      collectors[collectors_cnt++] = strdup(optarg);
      break;

    case 't':
      if (types_cnt == TYPE_CMD_CNT) {
        fprintf(stderr, "ERROR: A maximum of %d types can be specified on "
                        "the command line\n",
                TYPE_CMD_CNT);
        usage(bsrt);
        exit(-1);
      }
      types[types_cnt++] = strdup(optarg);
      break;

    case 'w':
      if (windows_cnt == WINDOW_CMD_CNT) {
        fprintf(stderr, "ERROR: A maximum of %d windows can be specified on "
                        "the command line\n",
                WINDOW_CMD_CNT);
        usage(bsrt);
        exit(-1);
      }
      /* split the window into a start and end */
      if ((endp = strchr(optarg, ',')) == NULL) {
        windows[windows_cnt].end = BGPSTREAM_FOREVER;
      } else {
        *endp = '\0';
        endp++;
        windows[windows_cnt].end = atoi(endp);
      }
      windows[windows_cnt].start = atoi(optarg);
      windows_cnt++;
      break;

    case 'j':
      if (peerasns_cnt == PEERASN_CMD_CNT) {
        fprintf(stderr, "ERROR: A maximum of %d peer asns can be specified on "
                        "the command line\n",
                PEERASN_CMD_CNT);
        usage(bsrt);
        exit(-1);
      }
      peerasns[peerasns_cnt++] = strdup(optarg);
      break;

    case 'k':
      if (prefixes_cnt == PREFIX_CMD_CNT) {
        fprintf(stderr, "ERROR: A maximum of %d peer asns can be specified on "
                        "the command line\n",
                PREFIX_CMD_CNT);
        usage(bsrt);
        exit(-1);
      }
      prefixes[prefixes_cnt++] = strdup(optarg);
      break;

    case 'y':
      if (communities_cnt == COMMUNITY_CMD_CNT) {
        fprintf(stderr,
                "ERROR: A maximum of %d communities can be specified on "
                "the command line\n",
                PREFIX_CMD_CNT);
        usage(bsrt);
        exit(-1);
      }
      communities[communities_cnt++] = strdup(optarg);
      break;

    case 'o':
      if (interface_options_cnt == OPTION_CMD_CNT) {
        fprintf(stderr,
                "ERROR: A maximum of %d interface options can be specified\n",
                OPTION_CMD_CNT);
        usage(bsrt);
        exit(-1);
      }
      interface_options[interface_options_cnt++] = strdup(optarg);
      break;

    case 'P':
      rib_period = atoi(optarg);
      break;

#if 0
    case 'l':
      live = 1;
      break;

    case 'B':
      backends[backends_cnt++] = strdup(optarg);
      break;
#endif

    case 'g':
      bsrt->cfg.gap_limit = atoi(optarg);
      break;

    case 'a':
      bsrt->cfg.align = 1;
      break;

    case 'i':
      bsrt->cfg.interval = atoi(optarg);
      break;

    case 'L':
      bsrt->cfg.logfile_disable = 1;
      break;

    case 'n':
      bsrt->cfg.name = strdup(optarg);
      break;

    case 'O':
      bsrt->cfg.tmpl = strdup(optarg);
      break;

    case 'r':
      bsrt->cfg.rotate = atoi(optarg);
      break;

    case 'R':
      bsrt->cfg.meta_rotate = atoi(optarg);
      break;

    case ':':
      fprintf(stderr, "ERROR: Missing option argument for -%c\n", optopt);
      usage(bsrt);
      exit(-1);

    case '?':
    default:
      usage(bsrt);
      exit(-1);
    }
  }

  // reset getopt for others
  optind = 1;

  bgpstream_set_data_interface(bsrt->stream, bsrt->di_id);

  if (bsrt_get_next_record == test_get_next_record) {
    if (projects_cnt || types_cnt || collectors_cnt || windows_cnt ||
        peerasns_cnt || prefixes_cnt || communities_cnt ||
        interface_options_cnt || rib_period)
    {
        fprintf(stderr, "ERROR: most options are not allowed with bsrt -dtest.\n");
        usage(bsrt);
        exit(-1);
    }
    return 0;
  }

  for (int i = 0; i < interface_options_cnt; i++) {
    if (*interface_options[i] == '?') {
      dump_if_options(bsrt);
      usage(bsrt);
      exit(0);
    } else {
      /* actually set this option */
      bgpstream_data_interface_option_t *option;
      if ((endp = strchr(interface_options[i], '=')) == NULL) {
        fprintf(stderr, "ERROR: Malformed data interface option (%s)\n",
                interface_options[i]);
        fprintf(stderr, "ERROR: Expecting <option-name>=<option-value>\n");
        usage(bsrt);
        exit(-1);
      }
      *endp = '\0';
      endp++;
      if ((option = bgpstream_get_data_interface_option_by_name(
             bsrt->stream, bsrt->di_id, interface_options[i])) == NULL) {
        fprintf(stderr, "ERROR: Invalid option '%s' for data interface '%s'\n",
                interface_options[i], bsrt->di_info->name);
        usage(bsrt);
        exit(-1);
      }
      bgpstream_set_data_interface_option(bsrt->stream, option, endp);
    }
    free(interface_options[i]);
    interface_options[i] = NULL;
  }
  interface_options_cnt = 0;

  if (windows_cnt == 0) {
    fprintf(stderr,
            "ERROR: At least one time window must be specified using -w\n");
    usage(bsrt);
    return -1;
  }

  /* pass along the user's filter requests to bgpstream */

  /* types */
  for (int i = 0; i < types_cnt; i++) {
    bgpstream_add_filter(bsrt->stream, BGPSTREAM_FILTER_TYPE_RECORD_TYPE, types[i]);
    free(types[i]);
  }

  /* projects */
  for (int i = 0; i < projects_cnt; i++) {
    bgpstream_add_filter(bsrt->stream, BGPSTREAM_FILTER_TYPE_PROJECT, projects[i]);
    free(projects[i]);
  }

  /* collectors */
  for (int i = 0; i < collectors_cnt; i++) {
    bgpstream_add_filter(bsrt->stream, BGPSTREAM_FILTER_TYPE_COLLECTOR,
                         collectors[i]);
    free(collectors[i]);
  }

  /* windows */
  uint32_t current_time = 0;
  for (int i = 0; i < windows_cnt; i++) {
    bgpstream_add_interval_filter(bsrt->stream, windows[i].start, windows[i].end);
    current_time = windows[i].start;
    if (bsrt->cfg.minimum_time == 0 || current_time < bsrt->cfg.minimum_time) {
      bsrt->cfg.minimum_time = current_time;
    }
  }

  /* peer asns */
  for (int i = 0; i < peerasns_cnt; i++) {
    bgpstream_add_filter(bsrt->stream, BGPSTREAM_FILTER_TYPE_ELEM_PEER_ASN,
                         peerasns[i]);
    free(peerasns[i]);
  }

  /* prefixes */
  for (int i = 0; i < prefixes_cnt; i++) {
    bgpstream_add_filter(bsrt->stream, BGPSTREAM_FILTER_TYPE_ELEM_PREFIX,
                         prefixes[i]);
    free(prefixes[i]);
  }

  /* communities */
  for (int i = 0; i < communities_cnt; i++) {
    bgpstream_add_filter(bsrt->stream, BGPSTREAM_FILTER_TYPE_ELEM_COMMUNITY,
                         communities[i]);
    free(communities[i]);
  }

  /* frequencies */
  if (rib_period > 0) {
    bgpstream_add_rib_period_filter(bsrt->stream, rib_period);
  }

#if 0
  /* live mode */
  if (live != 0) {
    bgpstream_set_live_mode(bsrt->stream);
  }
#endif

  return 0;
}

/* ========== PROTECTED FUNCTIONS ========== */


/* ========== PUBLIC FUNCTIONS ========== */

bgpview_io_bsrt_t *bgpview_io_bsrt_init(const char *opts, timeseries_t *timeseries)
{
#define MAXOPTS 1024
  char *local_args = NULL;
  char *process_argv[MAXOPTS];
  int len;
  int process_argc = 0;

  bgpview_io_bsrt_t *bsrt;
  if ((bsrt = malloc_zero(sizeof(bgpview_io_bsrt_t))) == NULL) {
    return NULL;
  }
  bsrt->cfg.gap_limit = BGPVIEW_IO_BSRT_GAPLIMIT_DEFAULT;
  bsrt->cfg.interval = -1000;
  bsrt->cfg.meta_rotate = -1;

  if ((bsrt->stream = bgpstream_create()) == NULL) {
    fprintf(stderr, "ERROR: Could not create BGPStream instance\n");
    goto err;
  }
  if (bsrt->di_id != 0) {
    bsrt->di_id_default = bsrt->di_id = bgpstream_get_data_interface_id(bsrt->stream);
    bsrt->di_info = bgpstream_get_data_interface_info(bsrt->stream, bsrt->di_id);
  }

  if (opts != NULL && (len = strlen(opts)) > 0) {
    // parse the option string ready for getopt
    local_args = strdup(opts);
    parse_cmd(local_args, &process_argc, process_argv, MAXOPTS, "bsrt");
    // now parse the arguments using getopt
    if (parse_args(bsrt, process_argc, process_argv) != 0) {
      goto err;
    }
  }

  if (bsrt->cfg.tmpl == NULL) {
    fprintf(stderr,
            "ERROR: An output file template must be specified using -O\n");
    usage(bsrt);
    goto err;
  }

  /* alloc bgpcorsaro */
  if ((bsrt->bgpcorsaro = bgpcorsaro_alloc_output(bsrt->cfg.tmpl, timeseries)) == NULL) {
    usage(bsrt);
    goto err;
  }
  bsrt->bgpcorsaro->minimum_time = bsrt->cfg.minimum_time;
  bsrt->bgpcorsaro->gap_limit = bsrt->cfg.gap_limit;

  if (bsrt->cfg.name && bgpcorsaro_set_monitorname(bsrt->bgpcorsaro, bsrt->cfg.name) != 0) {
    bgpcorsaro_log(__func__, bsrt->bgpcorsaro, "failed to set monitor name");
    goto err;
  }

  if (bsrt->cfg.interval > -1000) {
    bgpcorsaro_set_interval(bsrt->bgpcorsaro, bsrt->cfg.interval);
  }

  if (bsrt->cfg.align) {
    bgpcorsaro_set_interval_alignment_flag(bsrt->bgpcorsaro, bsrt->cfg.align);
  }

  if (bsrt->cfg.rotate > 0) {
    bgpcorsaro_set_output_rotation(bsrt->bgpcorsaro, bsrt->cfg.rotate);
  }

  if (bsrt->cfg.meta_rotate >= 0) {
    bgpcorsaro_set_meta_output_rotation(bsrt->bgpcorsaro, bsrt->cfg.meta_rotate);
  }

  if (bsrt->cfg.logfile_disable != 0) {
    bgpcorsaro_disable_logfile(bsrt->bgpcorsaro);
  }

  if (bgpcorsaro_start_output(bsrt->bgpcorsaro) != 0) {
    usage(bsrt);
    goto err;
  }

  free(local_args);
  return bsrt;

err:
  free(local_args);
  bgpview_io_bsrt_destroy(bsrt);
  return NULL;
}

void bgpview_io_bsrt_destroy(bgpview_io_bsrt_t *bsrt)
{
  if (bsrt == NULL) {
    return;
  }

  if (bsrt->bgpcorsaro) {
    bgpcorsaro_finalize_output(bsrt->bgpcorsaro);
  }
  if (bsrt->stream) {
    bgpstream_destroy(bsrt->stream);
    bsrt->stream = NULL;
  }
  if (bsrt->cfg.name)
    free(bsrt->cfg.name);
  if (bsrt->cfg.tmpl)
    free(bsrt->cfg.tmpl);

  free(bsrt);
  return;
}

int bgpview_io_bsrt_start(bgpview_io_bsrt_t *bsrt)
{
  if (bgpstream_start(bsrt->stream) < 0) {
    fprintf(stderr, "ERROR: Could not init BGPStream\n");
    return -1;
  }

  /* let bgpcorsaro have the trace pointer */
  bgpcorsaro_set_stream(bsrt->bgpcorsaro, bsrt->stream);
  return 0;
}

int bgpview_io_bsrt_recv_view(bgpview_io_bsrt_t *bsrt)
{
  int rc;
  rc = bgpcorsaro_process_interval(bsrt->bgpcorsaro);
  if (rc < 0) // error
    return -1;
  if (rc == 0) // EOF
    return -1;

  return 0;
}

bgpview_t *bgpview_io_bsrt_get_view_ptr(bgpview_io_bsrt_t *bsrt)
{
  return bsrt->bgpcorsaro->shared_view;
}
