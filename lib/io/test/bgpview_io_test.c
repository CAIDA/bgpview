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
#include "bgpview_io_test.h"
#include "bgpview.h"
#include "utils.h"
#include "parse_cmd.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define VIEW_INTERVAL 300

#define TEST_TIME_DEFAULT 1320969600

#define TEST_TABLE_NUM_DEFAULT 1
#define TEST_TABLE_SIZE_DEFAULT 50
#define TEST_PEER_NUM_DEFAULT 1

#define ORIG_ASN_MAX 50000
#define CORE_ASN_MAX 4000

#define MAX_PEER_CNT 1024

struct bgpview_io_test {
  /* pfx table */
  char                     *test_collector_name;
  uint32_t                  test_time;
  uint32_t                  test_peer_first_ip;
  bgpstream_addr_storage_t  test_peer_ip;
  uint32_t                  test_peer_asns[MAX_PEER_CNT];
  uint8_t                   test_peer_status;

  /* pfx row */
  bgpstream_pfx_storage_t   test_prefix;
  uint32_t                  test_prefix_first_addr;
  bgpstream_as_path_t      *test_as_path;
  bgpstream_as_path_seg_asn_t test_as_path_segs[100];

  /* tunables */
  uint32_t test_table_size;
  uint32_t test_table_num;
  uint32_t test_peer_num;

  int use_random_peers;
  int use_random_pfxs;

  int current_tbl;
};

static void create_test_data(bgpview_io_test_t *generator)
{
  int i;

  /* PREFIX TABLE */

  /* TIME */
  generator->test_time = TEST_TIME_DEFAULT;

  /* COLLECTOR NAME */
  generator->test_collector_name = "TEST-COLLECTOR";

  /* FIRST PEER IP */
  generator->test_peer_ip.ipv4.s_addr =
    generator->test_peer_first_ip = 0x00FAD982; /* add one each time */
  generator->test_peer_ip.version = BGPSTREAM_ADDR_VERSION_IPV4;

  /* PEER ASNS */
  for(i=0; i<MAX_PEER_CNT; i++) {
    generator->test_peer_asns[i] = rand() % ORIG_ASN_MAX;
  }

  /* FIRST PEER STATUS */
  generator->test_peer_status = 0x01;

  /* FIRST PREFIX */
  generator->test_prefix_first_addr =
    generator->test_prefix.address.ipv4.s_addr = 0x00000000;
  generator->test_prefix.address.version = BGPSTREAM_ADDR_VERSION_IPV4;
  generator->test_prefix.mask_len = 24;

  /* AS PATH */
  generator->test_as_path = bgpstream_as_path_create();
  assert(generator->test_as_path != NULL);

  /* TUNABLES */
  generator->test_table_size = TEST_TABLE_SIZE_DEFAULT;
  generator->test_table_num = TEST_TABLE_NUM_DEFAULT;
  generator->test_peer_num = TEST_PEER_NUM_DEFAULT;
}

static void build_as_path(bgpview_io_test_t *generator, uint32_t peer_asn)
{
  /* pick how many segments will be in this path (2-5) */
  int seg_cnt = (rand() % 5) + 2;
  int i;
  uint32_t origin_asn = rand() % ORIG_ASN_MAX;

  /* semi-randomly build the AS path, starting from the peer_asn */
  generator->test_as_path_segs[0].type = BGPSTREAM_AS_PATH_SEG_ASN;
  generator->test_as_path_segs[0].asn = peer_asn;
  for(i=1; i<seg_cnt-1; i++) {
      generator->test_as_path_segs[i].type = BGPSTREAM_AS_PATH_SEG_ASN;
      generator->test_as_path_segs[i].asn =
        (peer_asn + origin_asn + i) % ORIG_ASN_MAX;
  }
  generator->test_as_path_segs[seg_cnt-1].type = BGPSTREAM_AS_PATH_SEG_ASN;
  generator->test_as_path_segs[seg_cnt-1].asn = origin_asn;

  /* now populate the path */
  bgpstream_as_path_populate_from_data_zc(generator->test_as_path,
                                          (uint8_t*)generator->test_as_path_segs,
                                          sizeof(bgpstream_as_path_seg_asn_t)
                                          * seg_cnt);
}

static void usage()
{
  fprintf(stderr,
	  "Test IO Module Options:\n"
          "       -c                    Randomly decide if peers are up or down\n"
          "       -C                    Initial test time (default: %d)\n"
          "       -N <table-cnt>        Number of tables (default: %d)\n"
          "       -p                    Randomly decide if a peer observes each prefix\n"
          "       -P <peer-cnt>         Number of peers (default: %d)\n"
	  "       -T <table-size>       Size of prefix tables (default: %d)\n",
          TEST_TIME_DEFAULT,
          TEST_TABLE_NUM_DEFAULT,
          TEST_PEER_NUM_DEFAULT,
	  TEST_TABLE_SIZE_DEFAULT);
}

static int parse_args(bgpview_io_test_t *generator, int argc, char **argv)
{
  int opt, prevoptind;
  assert(argc > 0 && argv != NULL);
  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

    while(prevoptind = optind,
	(opt = getopt(argc, argv, ":cC:N:pP:T:v?")) >= 0)
    {
      if (optind == prevoptind + 2 && *optarg == '-' ) {
        opt = ':';
        -- optind;
      }
      switch(opt)
	{
	case ':':
	  fprintf(stderr, "ERROR: Missing option argument for -%c\n", optopt);
	  usage();
	  return -1;
	  break;

        case 'c':
          generator->use_random_peers = 1;
          break;

        case 'C':
          generator->test_time = atoi(optarg);
          break;

	case 'N':
	  generator->test_table_num = atoi(optarg);
	  break;

        case 'p':
          generator->use_random_pfxs = 1;
          break;

	case 'P':
	  generator->test_peer_num = atoi(optarg);
          assert(generator->test_peer_num <= MAX_PEER_CNT);
	  break;

	case 'T':
	  generator->test_table_size = atoi(optarg);
	  break;

	case '?':
	default:
	  usage();
	  return -1;
	  break;
	}
    }

  return 0;
}

/* ==================== PUBLIC FUNCTIONS ==================== */

bgpview_io_test_t *bgpview_io_test_create(const char *opts)
{
  bgpview_io_test_t *generator = NULL;
  #define MAXOPTS 1024
  char *local_args = NULL;
  char *process_argv[MAXOPTS];
  int len;
  int process_argc = 0;

  if ((generator = malloc_zero(sizeof(bgpview_io_test_t))) == NULL) {
    return NULL;
  }

  create_test_data(generator);

  /* initialize random seed: */
  srand(1);

  /* nothing to process */
  if (opts != NULL && (len = strlen(opts)) > 0) {
    /* parse the option string ready for getopt */
    local_args = strdup(opts);
    parse_cmd(local_args, &process_argc, process_argv, MAXOPTS, "zmq");
    /* now parse the arguments using getopt */
    if (parse_args(generator, process_argc, process_argv) != 0) {
      goto err;
    }
  }

  free(local_args);
  return generator;

 err:
  free(local_args);
  bgpview_io_test_destroy(generator);
  return NULL;
}

void bgpview_io_test_destroy(bgpview_io_test_t *generator)
{
  if (generator == NULL) {
    return;
  }

  bgpstream_as_path_destroy(generator->test_as_path);
  generator->test_as_path = NULL;

  free(generator);
  return;
}

int bgpview_io_test_generate_view(bgpview_io_test_t *generator,
                                  bgpview_t *view)
{
  bgpview_iter_t *iter;
  int i;
  int peer;
  int peer_id;
  int pfx_cnt;

  if((iter = bgpview_iter_create(view)) == NULL) {
    fprintf(stderr, "Could not create view iterator\n");
    goto err;
  }

  if (generator->current_tbl >= generator->test_table_num) {
    goto done;
  }

  bgpview_clear(view);

  fprintf(stderr,
          "--------------------[ PREFIX START %03d ]--------------------\n",
          generator->current_tbl);

  bgpview_set_time(view, generator->test_time+
                   (generator->current_tbl*VIEW_INTERVAL));

  /* reset peer ip */
  generator->test_peer_ip.ipv4.s_addr = generator->test_peer_first_ip;

  fprintf(stderr, "TEST: Simulating %d peer(s)\n", generator->test_peer_num);
  for(peer = 0; peer < generator->test_peer_num; peer++) {
    generator->test_peer_ip.ipv4.s_addr =
      htonl(ntohl(generator->test_peer_ip.ipv4.s_addr) + 1);

    // returns number from 0 to 2
    generator->test_peer_status = (generator->use_random_peers) ? rand() % 3 : 2;
    if((peer_id = bgpview_iter_add_peer(iter,
                                        generator->test_collector_name,
                                        (bgpstream_ip_addr_t *)&generator->test_peer_ip,
                                        generator->test_peer_asns[peer])) == 0) {
      fprintf(stderr, "Could not add peer to table\n");
      goto err;
    }
    if(bgpview_iter_activate_peer(iter) != 1) {
      fprintf(stderr, "Failed to activate peer\n");
      goto err;
    }
    fprintf(stderr, "TEST: Added peer %d (asn: %"PRIu32") ",
            peer_id, generator->test_peer_asns[peer]);

    if(generator->test_peer_status != 2) {
      fprintf(stderr, "(down)\n");
      continue;
    } else {
      fprintf(stderr, "(up)\n");
    }

    generator->test_prefix.address.ipv4.s_addr =
      generator->test_prefix_first_addr;
    pfx_cnt = 0;
    for(i=0; i < generator->test_table_size; i++) {
      generator->test_prefix.address.ipv4.s_addr =
        htonl(ntohl(generator->test_prefix.address.ipv4.s_addr) + 256);

      build_as_path(generator, generator->test_peer_asns[peer]);

      /* there is a 1/10 chance that we don't observe this prefix */
      if(generator->use_random_pfxs && (rand() % 10) == 0) {
        /* randomly we don't see this prefix */
        continue;
      }
      if(bgpview_iter_add_pfx_peer(iter,
                                   (bgpstream_pfx_t *)&generator->test_prefix,
                                   peer_id,
                                   generator->test_as_path) != 0) {
        fprintf(stderr, "Could not add pfx info to table\n");
        goto err;
      }
      if(bgpview_iter_pfx_activate_peer(iter) != 1) {
        fprintf(stderr, "Failed to activate pfx-peer\n");
        goto err;
      }
      pfx_cnt++;
    }
    fprintf(stderr, "TEST: Added %d prefixes...\n", pfx_cnt);
  }

  fprintf(stderr,
          "--------------------[ PREFIX DONE %03d ]--------------------\n\n",
          generator->current_tbl);

  generator->current_tbl++;

  bgpview_iter_destroy(iter);
  return 0;

 done:
 err:
  bgpview_iter_destroy(iter);
  return -1;
}

