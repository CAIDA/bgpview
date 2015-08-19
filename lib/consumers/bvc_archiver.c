/*
 * This file is part of bgpstream
 *
 * Copyright (C) 2015 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
 *
 * All rights reserved.
 *
 * This code has been developed by CAIDA at UC San Diego.
 * For more information, contact bgpstream-info@caida.org
 *
 * This source code is proprietary to the CAIDA group at UC San Diego and may
 * not be redistributed, published or disclosed without prior permission from
 * CAIDA.
 *
 * Report any bugs, questions or comments to bgpstream-info@caida.org
 *
 */

#include <assert.h>
#include <stdio.h>
#include <unistd.h>

#include <czmq.h> /*< for zclock_time */
#include <wandio.h>

#include "utils.h"
#include "wandio_utils.h"

#include "bgpview_io.h"

#include "bgpview_consumer_interface.h"
#include "bvc_archiver.h"

#define NAME                        "archiver"

#define BUFFER_LEN 1024
#define META_METRIC_PREFIX_FORMAT  "%s.meta.bgpview.consumer."NAME



#define DUMP_METRIC(value, time, fmt, ...)                              \
  do {                                                                  \
    char buf[1024];                                                     \
    snprintf(buf,1024, META_METRIC_PREFIX_FORMAT"."fmt, __VA_ARGS__);   \
    graphite_safe(buf);                                                 \
    timeseries_set_single(BVC_GET_TIMESERIES(consumer),                 \
                          buf, value, time);                            \
  } while(0)


#define STATE					\
  (BVC_GET_STATE(consumer, archiver))

#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))

static bvc_t bvc_archiver = {
  BVC_ID_ARCHIVER,
  NAME,
  BVC_GENERATE_PTRS(archiver)
};

typedef struct bvc_archiver_state {

  /** Output filename pattern */
  char *outfile_pattern;

  /** Output file compression level */
  int outfile_compress_level;

  /** Current output file */
  iow_t *outfile;

} bvc_archiver_state_t;

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s\n"
          "       -f <filename> output file pattern for writing views\n"
          "       -c <level>    output compression level to use (default: 6)\n",
	  consumer->name);
}


static char *graphite_safe(char *p)
{
  if(p == NULL)
    {
      return p;
    }

  char *r = p;
  while(*p != '\0')
    {
      if(*p == '.')
	{
	  *p = '_';
	}
      if(*p == '*')
	{
	  *p = '-';
	}
      p++;
    }
  return r;
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  bvc_archiver_state_t *state = STATE;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while((opt = getopt(argc, argv, ":f:c:?")) >= 0)
    {
      switch(opt)
	{
        case 'c':
          state->outfile_compress_level = atoi(optarg);
          break;

        case 'f':
          if((state->outfile_pattern = strdup(optarg)) == NULL)
            {
              return -1;
            }
          break;

	case '?':
	case ':':
	default:
	  usage(consumer);
	  return -1;
	}
    }

  return 0;
}

bvc_t *bvc_archiver_alloc()
{
  return &bvc_archiver;
}

int bvc_archiver_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_archiver_state_t *state = NULL;
  int compress_type;

  if((state = malloc_zero(sizeof(bvc_archiver_state_t))) == NULL)
    {
      return -1;
    }
  BVC_SET_STATE(consumer, state);

  /* set defaults here */

  state->outfile_compress_level = 6;

  /* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      return -1;
    }

  /* react to args here */

  if(state->outfile_pattern == NULL)
    {
      fprintf(stderr, "ERROR: Output file pattern must be set using -f\n");
      usage(consumer);
      return -1;
    }

  compress_type = wandio_detect_compression_type(state->outfile_pattern);

  /** @todo support patterns and rotation like we do in corsaro */
  if((state->outfile = wandio_wcreate(state->outfile_pattern,
                                      compress_type,
                                      state->outfile_compress_level,
                                      O_CREAT)) == NULL)
    {
      fprintf(stderr, "ERROR: Could not open %s for writing\n",
              state->outfile_pattern);
      return -1;
    }

  return 0;
}

void bvc_archiver_destroy(bvc_t *consumer)
{
  bvc_archiver_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  wandio_wdestroy(state->outfile);
  state->outfile = NULL;

  free(state->outfile_pattern);
  state->outfile_pattern = NULL;

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_archiver_process_view(bvc_t *consumer, uint8_t interests,
				 bgpview_t *view)
{
  bvc_archiver_state_t *state = STATE;
  uint32_t time_begin = zclock_time()/1000;

  /* simply as the IO library to dump the view to a file */
  if(bgpview_io_write(state->outfile, view, NULL) != 0)
    {
      fprintf(stderr, "ERROR: Failed to write view to file\n");
      return -1;
    }

  uint32_t time_end = zclock_time()/1000;
  DUMP_METRIC(time_end-time_begin,
              bgpview_get_time(view),
	      "%s", CHAIN_STATE->metric_prefix, "processing_time");

  return 0;
}

