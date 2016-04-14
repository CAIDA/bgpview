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

#include "config.h"
#include "bgpview_consumer_interface.h"
#include "bvc_archiver.h"
#include "file/bgpview_io_file.h"
#include "utils.h"
#include "wandio_utils.h"
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <czmq.h> /*< for zclock_time */
#include <wandio.h>

#define NAME                        "archiver"

#define BUFFER_LEN 1024
#define META_METRIC_PREFIX_FORMAT  "%s.meta.bgpview.consumer."NAME

#define DEFAULT_COMPRESS_LEVEL 6

#define DUMP_METRIC(value, time, fmt, ...)                              \
  do {                                                                  \
    char buf[1024];                                                     \
    snprintf(buf,1024, META_METRIC_PREFIX_FORMAT"."fmt, __VA_ARGS__);   \
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

enum format {BINARY, ASCII};

typedef struct bvc_archiver_state {

  /** Output filename pattern */
  char *outfile_pattern;

  /** Current output filename */
  char *outfile_name;

  /** Output file compression level */
  int outfile_compress_level;

  /** Current output file */
  iow_t *outfile;

  /** Output format (binary or ascii) */
  enum format output_format;

  /** Filename to use for the 'latest file' file */
  char *latest_filename;

  /** File rotation interval */
  uint32_t rotation_interval;

  /** Align rotation times to multiples of the interval */
  int rotate_noalign;

  /** First view written to the current output file */
  uint32_t next_rotate_time;

} bvc_archiver_state_t;

#define SHOULD_ROTATE(state, time)              \
  (((state)->rotation_interval > 0) && ((time) >= (state)->next_rotate_time))

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s\n"
          "       -f <filename> output file pattern for writing views\n"
          "                       accepts same format parameters as strftime(3)\n"
          "                       as well as '%%s' to write unix time\n"
          "       -r <seconds>  output file rotation period (default: no rotation)\n"
          "       -a            disable alignment of output file rotation to multiples of the rotation interval\n"
          "       -l <filename> file to write the filename of the latest complete output file to\n"
          "       -c <level>    output compression level to use (default: %d)\n"
          "       -m <mode>     output mode: 'ascii' or 'binary' (default: binary)\n",
	  consumer->name,
          DEFAULT_COMPRESS_LEVEL);
}

#if 0
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
#endif

static int complete_file(bvc_t *consumer)
{
  bvc_archiver_state_t *state = STATE;
  iow_t *latest = NULL;

  /* first, close the current output file */
  if(state->outfile == NULL)
    {
      return 0;
    }

  wandio_wdestroy(state->outfile);
  state->outfile = NULL;

  /* now write the name of that file to the latest file */
  if(state->latest_filename == NULL)
    {
      return 0;
    }

  /* force no compression, ignore the extension */
  if((latest = wandio_wcreate(state->latest_filename,
                              WANDIO_COMPRESS_NONE, 0, O_CREAT)) == NULL)
    {
      fprintf(stderr, "ERROR: Could not create latest file '%s'\n",
              state->latest_filename);
      return -1;
    }

  wandio_printf(latest, "%s\n", state->outfile_name);

  wandio_wdestroy(latest);

  free(state->outfile_name);
  state->outfile_name = NULL;

  return 0;
}

#define stradd(str, bufp, buflim)                               \
  do {                                                          \
    char *strp = str;                                           \
    while(bufp < buflim && (*bufp = *strp++) != '\0') ++bufp;  \
  } while(0)

static char *generate_file_name(const char *template, uint32_t time)
{
  /* some of the structure of this code is borrowed from the
     FreeBSD implementation of strftime */

  /* the output buffer */
  /* @todo change the code to dynamically realloc this if we need more
     space */
  char buf[1024];
  char tbuf[1024];
  char *bufp = buf;
  char *buflim = buf+sizeof(buf);

  char *tmpl = (char*)template;
  char secs[11]; /* length of UINT32_MAX +1 */
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 0;

  for(; *tmpl; ++tmpl)
    {
      if(*tmpl == '%')
	{
	  switch(*++tmpl)
	    {
	    case '\0':
	      --tmpl;
	      break;

	    case 's': /* unix timestamp */
              snprintf(secs, sizeof(secs), "%"PRIu32, time);
              stradd(secs, bufp, buflim);
              continue;

	    default:
	      /* we want to be generous and leave non-recognized formats
		 intact - especially for strftime to use */
	      --tmpl;
	    }
	}
      if (bufp == buflim)
	break;
      *bufp++ = *tmpl;
    }

  *bufp = '\0';

  /* now let strftime have a go */
  tv.tv_sec = time;
  strftime(tbuf, sizeof(tbuf), buf, gmtime(&tv.tv_sec));
  return strdup(tbuf);
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
  while((opt = getopt(argc, argv, ":c:f:l:m:r:?a")) >= 0)
    {
      switch(opt)
	{
        case 'a':
          state->rotate_noalign = 1;
          break;

        case 'c':
          state->outfile_compress_level = atoi(optarg);
          break;

        case 'f':
          if((state->outfile_pattern = strdup(optarg)) == NULL)
            {
              return -1;
            }
          break;

        case 'l':
          if((state->latest_filename = strdup(optarg)) == NULL)
            {
              return -1;
            }
          break;

        case 'm':
          if(strcmp(optarg, "ascii") == 0)
            {
              state->output_format = ASCII;
            }
          else if(strcmp(optarg, "binary") == 0)
            {
              state->output_format = BINARY;
            }
          else
            {
              fprintf(stderr,
                      "ERROR: Output mode must be either 'ascii' or 'binary'\n");
              usage(consumer);
              return -1;
            }
          break;

        case 'r':
          state->rotation_interval = atoi(optarg);
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

  if((state = malloc_zero(sizeof(bvc_archiver_state_t))) == NULL)
    {
      return -1;
    }
  BVC_SET_STATE(consumer, state);

  /* set defaults here */

  state->outfile_compress_level = DEFAULT_COMPRESS_LEVEL;

  state->output_format = BINARY;

  /* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      return -1;
    }

  /* react to args here */

  if(state->outfile_pattern == NULL)
    {
      if(state->output_format == ASCII)
        {
          /* default to stdout for ascii */
          state->outfile_pattern = strdup("-");
        }
      else
        {
          /* refuse to write binary to stdout by default */
          fprintf(stderr,
                  "ERROR: Output file pattern must be set using -f when using the binary output format\n");
          usage(consumer);
          return -1;
        }
    }

  if(strcmp("-", state->outfile_pattern) == 0 && (state->rotation_interval > 0))
    {
      fprintf(stderr,
              "WARN: Cannot rotate output files when writing to stdout\n");
      state->rotation_interval = 0;
    }

  /* outfile is opened when first view is processed */

  return 0;
}

void bvc_archiver_destroy(bvc_t *consumer)
{
  bvc_archiver_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  /* frees outfile and outfile_name */
  if(complete_file(consumer) != 0)
    {
      fprintf(stderr, "WARN: Failed to cleanly close output files\n");
    }

  free(state->outfile_pattern);
  state->outfile_pattern = NULL;

  free(state->outfile_name);
  state->outfile_name = NULL;

  free(state->latest_filename);
  state->latest_filename = NULL;

  free(state);

  BVC_SET_STATE(consumer, NULL);
}

int bvc_archiver_process_view(bvc_t *consumer, bgpview_t *view)
{
  bvc_archiver_state_t *state = STATE;
  uint32_t time_begin = zclock_time()/1000;
  uint32_t view_time = bgpview_get_time(view);
  uint32_t file_time = view_time;
  int compress_type;

  if(state->outfile == NULL || SHOULD_ROTATE(state, view_time))
    {
      if(state->rotation_interval > 0)
        {
          if(state->outfile != NULL && complete_file(consumer) != 0)
            {
              fprintf(stderr, "ERROR: Failed to rotate output file\n");
              goto err;
            }

          /* align the time to a multiple of the interval */
          if(state->rotate_noalign == 0)
            {
              file_time =
                (view_time/state->rotation_interval)*state->rotation_interval;
            }
          state->next_rotate_time = file_time + state->rotation_interval;
        }

      /* compute the output filename */
      if((state->outfile_name =
          generate_file_name(state->outfile_pattern, file_time)) == NULL)
        {
          goto err;
        }
      compress_type = wandio_detect_compression_type(state->outfile_name);
      if((state->outfile = wandio_wcreate(state->outfile_name,
                                          compress_type,
                                          state->outfile_compress_level,
                                          O_CREAT)) == NULL)
        {
          fprintf(stderr, "ERROR: Could not open %s for writing\n",
                  state->outfile_name);
          goto err;
        }
    }

  switch(state->output_format)
    {
    case ASCII:
      if(bgpview_io_file_print(state->outfile, view) != 0)
        {
          fprintf(stderr, "ERROR: Failed to write view to file\n");
          goto err;
        }
      break;

    case BINARY:
      /* simply ask the IO library to dump the view to a file */
      if(bgpview_io_file_write(state->outfile, view, NULL, NULL) != 0)
        {
          fprintf(stderr, "ERROR: Failed to write view to file\n");
          goto err;
        }
      break;
    }

  uint32_t time_end = zclock_time()/1000;
  DUMP_METRIC(time_end-time_begin, view_time,
	      "%s", CHAIN_STATE->metric_prefix, "processing_time");

  return 0;

 err:
  return -1;
}

