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

// #include "bgpview_io_bsrt_int.h"
#include "bgpview_io_bsrt.h"
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
    int minimum_time;
  } cfg;
};

struct window {
  uint32_t start;
  uint32_t end;
};

/* ========== PRIVATE FUNCTIONS ========== */

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
    "   -P <period>    process a rib files every <period> seconds (bgp "
    "time)\n"
    "   -j <peer ASN>  return valid elems originated by a specific peer "
    "ASN*\n"
    "   -k <prefix>    return valid elems associated with a specific "
    "prefix*\n"
    "   -y <community> return valid elems with the specified community* \n"
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
    "   -n <name>      monitor name (default: " STR(
      BGPCORSARO_MONITOR_NAME) ")\n"
                               "   -O <outfile>   use <outfile> as a "
                               "template for file names.\n"
                               "                   - %%X => plugin name\n"
                               "                   - %%N => monitor name\n"
                               "                   - see man strftime(3) "
                               "for more options\n"
                               "   -r <intervals> rotate output files "
                               "after n intervals\n"
                               "   -R <intervals> rotate bgpcorsaro meta "
                               "files after n intervals\n"
                               "\n"
                               "   -h             print this help menu\n"
                               "* denotes an option that can be given "
                               "multiple times\n");

}

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
      if ((bsrt->di_id = bgpstream_get_data_interface_id_by_name(bsrt->stream, optarg)) ==
          0) {
        fprintf(stderr, "ERROR: Invalid data interface name '%s'\n", optarg);
        usage(bsrt);
        exit(-1);
      }
      bsrt->di_info = bgpstream_get_data_interface_info(bsrt->stream, bsrt->di_id);
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
      break;

    case '?':
    default:
      usage(bsrt);
      exit(-1);
    }
  }

  // reset getopt for others
  optind = 1;

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
  int current_time = 0;
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

  bgpstream_set_data_interface(bsrt->stream, bsrt->di_id);

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
  bsrt->di_id_default = bsrt->di_id = bgpstream_get_data_interface_id(bsrt->stream);
  bsrt->di_info = bgpstream_get_data_interface_info(bsrt->stream, bsrt->di_id);
  assert(bsrt->di_id != 0);

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

  if (bsrt->cfg.name != NULL && bgpcorsaro_set_monitorname(bsrt->bgpcorsaro, bsrt->cfg.name) != 0) {
    bgpcorsaro_log(__func__, bsrt->bgpcorsaro, "failed to set monitor name");
    goto err;
  }

  if (bsrt->cfg.interval > -1000) {
    bgpcorsaro_set_interval(bsrt->bgpcorsaro, bsrt->cfg.interval);
  }

  if (bsrt->cfg.align == 1) {
    bgpcorsaro_set_interval_alignment(bsrt->bgpcorsaro,
                                      BGPCORSARO_INTERVAL_ALIGN_YES);
  }

  if (bsrt->cfg.rotate > 0) {
    bgpcorsaro_set_output_rotation(bsrt->bgpcorsaro, bsrt->cfg.rotate);
  }

  if (bsrt->cfg.meta_rotate >= 0) {
    bgpcorsaro_set_meta_output_rotation(bsrt->bgpcorsaro, bsrt->cfg.meta_rotate);
  }

  // hardcoded "routingtables -q" plugin
  if (bgpcorsaro_enable_plugin(bsrt->bgpcorsaro, "routingtables", "-q") != 0) {
    fprintf(stderr, "ERROR: Could not enable plugin %s\n", "routingtables");
    usage(bsrt);
    goto err;
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

  if (bsrt->stream) {
    bgpstream_destroy(bsrt->stream);
    bsrt->stream = NULL;
  }

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
