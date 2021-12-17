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
#include "bgpcorsaro_int.h"
#include "plugins/bgpcorsaro_routingtables.h"
#include "../bgpview_io_bsrt_int.h"
#include "config.h"

#include <assert.h>
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bgpcorsaro_io.h"
#include "bgpcorsaro_log.h"
#include "utils.h"
#include "parse_cmd.h"

/** @file
 *
 * @brief Code which implements the public functions of libbgpcorsaro-ish
 *
 * This code was taken from bgpcorsaro and modified:
 * - turned "inside out" to expose bgpcorsaro_process_interval() instead of
 *   bgpcorsaro_per_record()
 * - store a shareable bgpview on the bgpcorsaro object
 * - remove plugin manager
 * - hardcode a single plugin: routingtables
 *
 * @author Alistair King, Ken Keys
 *
 */

/** Cleanup and free the given bgpcorsaro instance */
static void bgpcorsaro_free(bgpcorsaro_t *bc)
{
  if (bc == NULL) {
    /* nothing to be done... */
    return;
  }

  /* free up the plugins first, they may try and use some of our info before
     closing */
  bgpcorsaro_routingtables_close_output(bc);

  if (bc->template) {
    free(bc->template);
    bc->template = NULL;
  }

  if (bc->bsrecord) {
    /* we will assume that somebody else is taking care of the bgpstream record */
    bc->bsrecord = NULL;
  }

  /* close this as late as possible */
  bgpcorsaro_log_close(bc);

  free(bc);

  return;
}

/** Fill the given interval object with values */
static inline void populate_interval(bgpcorsaro_interval_t *interval,
                                     uint32_t number, uint32_t time)
{
  interval->number = number;
  interval->time = time;
}

/** Check if the meta output files should be rotated */
static int is_meta_rotate_interval(bgpcorsaro_t *bc)
{
  assert(bc);

  if (bc->meta_output_rotate < 0) {
    return bgpcorsaro_is_rotate_interval(bc);
  } else {
    return (bc->meta_output_rotate > 0 &&
            (bc->interval_start.number + 1) % bc->meta_output_rotate == 0);
  }
}

/** Initialize a new bgpcorsaro object */
static bgpcorsaro_t *bgpcorsaro_init(char *template, timeseries_t *timeseries)
{
  bgpcorsaro_t *e;

  if ((e = malloc_zero(sizeof(bgpcorsaro_t))) == NULL) {
    bgpcorsaro_log(__func__, NULL, "could not malloc bgpcorsaro_t");
    return NULL;
  }
  e->last_ts = -1;

  /* what time is it? */
  gettimeofday(&e->init_time, NULL);

  /* set a default monitorname for when im bad and directly retrieve it
     from the structure */
  gethostname(e->monitorname, BGPCORSARO_HOST_NAME_MAX);
  e->monitorname[BGPCORSARO_HOST_NAME_MAX] = '\0';

  /* template does, however */
  /* check that it is valid */
  if (bgpcorsaro_io_validate_template(e, template) == 0) {
    bgpcorsaro_log(__func__, e, "invalid template %s", template);
    goto err;
  }
  if ((e->template = strdup(template)) == NULL) {
    bgpcorsaro_log(__func__, e, "could not duplicate template string");
    goto err;
  }

  /* check if compression should be used based on the file name */
  e->compress = wandio_detect_compression_type(e->template);

  e->timeseries = timeseries;

  /* use the default compression level for now */
  e->compress_level = 6;

  /* set the default interval alignment value */
  e->align_intervals = BGPCORSARO_INTERVAL_ALIGN_DEFAULT;

  /* interval doesn't need to be actively set, use the default for now */
  e->interval = BGPCORSARO_INTERVAL_DEFAULT;

  /* default for meta rotate should be to follow output_rotate */
  e->meta_output_rotate = -1;

  /* initialize the current interval */
  populate_interval(&e->interval_start, 0, 0);

  /* the rest are zero, as they should be. */

  /* ready to rock and roll! */

  return e;

err:
  /* 02/26/13 - ak comments because it is up to the user to call
     bgpcorsaro_finalize_output to free the memory */
  /*bgpcorsaro_free(e);*/
  return NULL;
}

/** Start a new interval */
static int start_interval(bgpcorsaro_t *bc, long int_start)
{
  /* record this so we know when the interval started */
  /* the interval number is already incremented by start_interval_for_record */
  bc->interval_start.time = int_start;

  /* the following is to support file rotation */
  /* initialize the log file */
  if (!bc->logfile_disabled && bc->logfile == NULL) {
    /* if this is the first interval, let them know we are switching to
       logging to a file */
    if (bc->interval_start.number == 0) {
      /* there is a replica of this message in the other place that
       * bgpcorsaro_log_init is called */
      bgpcorsaro_log(__func__, bc, "now logging to file"
#ifdef DEBUG
                                           " (and stderr)"
#endif
                     );
    }

    if (bgpcorsaro_log_init(bc) != 0) {
      bgpcorsaro_log(__func__, bc, "could not initialize log file");
      bgpcorsaro_free(bc);
      return -1;
    }
  }

  /* ask each plugin to start a new interval */
  /* plugins should rotate their files now too */
#ifdef WITH_PLUGIN_TIMING
  TIMER_START(start_interval);
#endif
  if (bgpcorsaro_routingtables_start_interval(bc, &bc->interval_start) != 0) {
    bgpcorsaro_log(__func__, bc, "%s failed to start interval at %ld",
                   PLUGIN_NAME, int_start);
    return -1;
  }
#ifdef WITH_PLUGIN_TIMING
  TIMER_END(start_interval);
  bgpcorsaro_routingtables_plugin.start_interval_usec += TIMER_VAL(start_interval);
#endif

  return 0;
}

/** End the current interval */
static int end_interval(bgpcorsaro_t *bc, long int_end)
{
  bgpcorsaro_interval_t interval_end;

  populate_interval(&interval_end, bc->interval_start.number, int_end);

  /* ask each plugin to end the current interval */
#ifdef WITH_PLUGIN_TIMING
  TIMER_START(end_interval);
#endif
  if (bgpcorsaro_routingtables_end_interval(bc, &interval_end) != 0) {
    bgpcorsaro_log(__func__, bc, "%s failed to end interval at %ld",
                   PLUGIN_NAME, int_end);
    return -1;
  }
#ifdef WITH_PLUGIN_TIMING
  TIMER_END(end_interval);
  bgpcorsaro_routingtables_plugin.end_interval_usec += TIMER_VAL(end_interval);
#endif

  /* if we are rotating, now is the time to close our output files */
  if (is_meta_rotate_interval(bc)) {
    /* this MUST be the last thing closed in case any of the other things want
       to log their end-of-interval activities (e.g. the pkt cnt from writing
       the trailers */
    if (bc->logfile) {
      bgpcorsaro_log_close(bc);
    }
  }

  bc->interval_end_needed = 0;
  return 0;
}

/** Process the given bgpcorsaro record */
static inline int process_record(bgpcorsaro_t *bc,
                                 bgpstream_record_t *bsrecord)
{
#ifdef WITH_PLUGIN_TIMING
  TIMER_START(process_record);
#endif
  if (bgpcorsaro_routingtables_process_record(bc, bsrecord) < 0) {
    bgpcorsaro_log(__func__, bc, "%s failed to process record",
                   PLUGIN_NAME);
    return -1;
  }
#ifdef WITH_PLUGIN_TIMING
  TIMER_END(process_record);
  bgpcorsaro_routingtables_plugin.process_record_usec += TIMER_VAL(process_record);
#endif

  return 0;
}

/* == PUBLIC BGPCORSARO API FUNCS BELOW HERE == */

bgpcorsaro_t *bgpcorsaro_alloc_output(char *template, timeseries_t *timeseries)
{
  bgpcorsaro_t *bc;

  /* quick sanity check that folks aren't trying to write to stdout */
  if (template == NULL || strcmp(template, "-") == 0) {
    bgpcorsaro_log(__func__, NULL, "writing to stdout not supported");
    return NULL;
  }

  /* initialize the bgpcorsaro object */
  if ((bc = bgpcorsaro_init(template, timeseries)) == NULL) {
    bgpcorsaro_log(__func__, NULL, "could not initialize bgpcorsaro object");
    return NULL;
  }

  /* 10/17/13 AK moves logging init to bgpcorsaro_start_output so that the user
     may call bgpcorsaro_disable_logfile after alloc */

  return bc;
}

int bgpcorsaro_start_output(bgpcorsaro_t *bc)
{
  bgpcorsaro_plugin_t *p = NULL;

  assert(bc);

  /* only initialize the log file if there are no time format fields in the file
     name (otherwise they will get a log file with a zero timestamp. */
  /* Note that if indeed it does have a timestamp, the initialization error
     messages will not be logged to a file. In fact nothing will be logged until
     the first record is received. */
  assert(bc->logfile == NULL);
  if (!bc->logfile_disabled &&
      bgpcorsaro_io_template_has_timestamp(bc) == 0) {
    bgpcorsaro_log(__func__, bc, "now logging to file"
#ifdef DEBUG
                                         " (and stderr)"
#endif
                   );

    if (bgpcorsaro_log_init(bc) != 0) {
      return -1;
    }
  }

  /* now, ask each plugin to open its output file */
  /* we need to do this here so that the bgpcorsaro object is fully set up
     that is, the traceuri etc is set */
  p = &bgpcorsaro_routingtables_plugin;
#ifdef WITH_PLUGIN_TIMING
  TIMER_START(init_output);
#endif
  if (bgpcorsaro_routingtables_init_output(bc) != 0) {
    return -1;
  }
#ifdef WITH_PLUGIN_TIMING
  TIMER_END(init_output);
  p->init_output_usec += TIMER_VAL(init_output);
#endif

  bc->started = 1;

  return 0;
}

void bgpcorsaro_set_interval_alignment_flag(bgpcorsaro_t *bc, int flag)
{
  assert(bc);
  /* you cant set interval alignment once bgpcorsaro has started */
  assert(!bc->started);

  bgpcorsaro_log(__func__, bc, "setting interval alignment to %d",
                 flag);

  bc->align_intervals = flag;
}

void bgpcorsaro_set_interval(bgpcorsaro_t *bc, unsigned int i)
{
  assert(bc);
  /* you can't set the interval once bgpcorsaro has been started */
  assert(!bc->started);

  bgpcorsaro_log(__func__, bc, "setting interval length to %d", i);

  bc->interval = i;
}

void bgpcorsaro_set_output_rotation(bgpcorsaro_t *bc, int intervals)
{
  assert(bc);
  /* you can't enable rotation once bgpcorsaro has been started */
  assert(!bc->started);

  bgpcorsaro_log(__func__, bc, "setting output rotation after %d interval(s)",
                 intervals);

  /* if they have asked to rotate, but did not put a timestamp in the template,
   * we will end up clobbering files. warn them. */
  if (bgpcorsaro_io_template_has_timestamp(bc) == 0) {
    /* we skip the log and write directly out so that it is clear even if they
     * have debugging turned off */
    fprintf(stderr, "WARNING: using output rotation without any timestamp "
                    "specifiers in the template.\n");
    fprintf(stderr,
            "WARNING: output files will be overwritten upon rotation\n");
    /* @todo consider making this a fatal error */
  }

  bc->output_rotate = intervals;
}

void bgpcorsaro_set_meta_output_rotation(bgpcorsaro_t *bc, int intervals)
{
  assert(bc);
  /* you can't enable rotation once bgpcorsaro has been started */
  assert(!bc->started);

  bgpcorsaro_log(__func__, bc,
                 "setting meta output rotation after %d intervals(s)",
                 intervals);

  bc->meta_output_rotate = intervals;
}

int bgpcorsaro_is_rotate_interval(bgpcorsaro_t *bc)
{
  assert(bc);

  return (bc->output_rotate != 0) &&
    ((bc->interval_start.number + 1) % bc->output_rotate == 0);
}

int bgpcorsaro_set_stream(bgpcorsaro_t *bc, bgpstream_t *stream)
{
  assert(bc);

  /* this function can actually be called once bgpcorsaro is started */
  bgpcorsaro_log(__func__, bc, "%s stream pointer",
      bc->stream ? "updating" : "setting");

  bc->stream = stream;
  return 0;
}

void bgpcorsaro_disable_logfile(bgpcorsaro_t *bc)
{
  assert(bc);
  bc->logfile_disabled = 1;
}

int bgpcorsaro_set_monitorname(bgpcorsaro_t *bc, const char *name)
{
  assert(bc);

  if (bc->started) {
    bgpcorsaro_log(__func__, bc, "monitor name can only be set before "
                                 "bgpcorsaro_start_output is called");
    return -1;
  }

  if (bc->monitorname) {
    bgpcorsaro_log(__func__, bc, "updating monitor name from %s to %s",
                   bc->monitorname, name);
  } else {
    bgpcorsaro_log(__func__, bc, "setting monitor name to %s", name);
  }

  strncpy(bc->monitorname, name, BGPCORSARO_HOST_NAME_MAX);
  bc->monitorname[BGPCORSARO_HOST_NAME_MAX] = '\0';
  bgpcorsaro_log(__func__, bc, "%s", bc->monitorname);
  return 0;
}

const char *bgpcorsaro_get_monitorname(bgpcorsaro_t *bc)
{
  static char monitorname[BGPCORSARO_HOST_NAME_MAX+1];
  if (!bc || !*bc->monitorname) {
    gethostname(monitorname, BGPCORSARO_HOST_NAME_MAX);
    monitorname[BGPCORSARO_HOST_NAME_MAX] = '\0';
    return monitorname;
  }
  return bc->monitorname;
}

// factored out from bgpcorsaro_per_record()
static int bgpcorsaro_start_record(bgpcorsaro_t *bc,
                                   bgpstream_record_t *bsrecord)
{
  long ts;

  /* poke this bsrecord into our bgpcorsaro record */
  bc->bsrecord = bsrecord;

  /* this is now the latest record we have seen */
  /** @chiara is this correct? */
  /** @chiara might be nice to provide an accessor func:
      bgpstream_get_time(record) */
  bc->last_ts = ts = bsrecord->time_sec;

  /* if this is the first record we record, keep the timestamp */
  if (bc->record_cnt == 0) {
    bc->first_ts = ts;

    long start = ts;
    /* if we are aligning our intervals, truncate the start down */
    if (bc->align_intervals) {
      start = (start / bc->interval) * bc->interval;
    }

    if (start_interval(bc, start) != 0) {
      bgpcorsaro_log(__func__, bc, "could not start interval at %ld",
                     ts);
      return -1;
    }

    bc->next_report = start + bc->interval;
  }
  return 0;
}

static int bgpcorsaro_end_interval_for_record(bgpcorsaro_t *bc)
{
  /* we want to mark the end of the interval such that all pkt times are <=
     the time of the end of the interval.
     because we deal in second granularity, we simply subtract one from the
     time */
  long report = bc->next_report - 1;

  if (end_interval(bc, report) != 0) {
    bgpcorsaro_log(__func__, bc, "could not end interval at %ld",
                   bc->last_ts);
    /* we don't free in case the client wants to try to carry on */
    return -1;
  }
  return 0;
}

static int bgpcorsaro_start_interval_for_record(bgpcorsaro_t *bc)
{
  bc->interval_start.number++;

  /* we now add the second back on to the time to get the start time */
  long report = bc->next_report;
  if (start_interval(bc, report) != 0) {
    bgpcorsaro_log(__func__, bc, "could not start interval at %ld",
                   bc->last_ts);
    /* we don't free in case the client wants to try to carry on */
    return -1;
  }
  bc->next_report += bc->interval;
  return 0;
}

// return:
//   -1: error
//   0: EOF
//   1: end interval
int bgpcorsaro_process_interval(bgpcorsaro_t *bc)
{
  assert(bc);
  assert(bc->started &&
         "bgpcorsaro_start_output must be called before records can be "
         "processed");

  if (bc->eof)
    return 0;

  if (bc->record_cnt > 0) {
    if (bgpcorsaro_start_interval_for_record(bc) < 0)
      return -1;
  } // else, start_interval() will be called by first bgpcorsaro_start_record()

  while (!bc->eof) {
    // from bgpcorsaro_per_record()
    /* using an interval value of less than zero disables intervals
       such that only one distribution will be generated upon completion */
    if (bc->interval >= 0 && bc->last_ts >= bc->next_report) {
      if (bgpcorsaro_end_interval_for_record(bc) < 0)
        return -1;
      return 1; // successful interval end.  caller can use shared_view.
    }

    /* Ignore records outside of our filtered time interval */
    if (bc->bsrecord && bc->bsrecord->status !=
		    BGPSTREAM_RECORD_STATUS_OUTSIDE_TIME_INTERVAL) {
      /* count this record for our overall record count */
      bc->record_cnt++;

      /* ask each plugin to process this record */
      if (process_record(bc, bc->bsrecord) < 0) {
        return -1;
      }
      /* only set this flag IF we have a valid record inside our allowed time
       * interval. */
      bc->interval_end_needed = 1;
    }

    // adapted from bgpcorsaro-caida:tools/bgpcorsaro.c:main()
    {
      static double last_time = 0;
      bgpstream_record_t *bsrecord = NULL;

      /* remove records that preceed the beginning of the stream */
      do {
        int rc = bsrt_get_next_record(bc->stream, &bsrecord);
        if (rc < 0) { // error
          return rc;
        } else if (rc == 0) { // EOF
          bgpcorsaro_log(__func__, bc,
                         "EOF from bgpstream (last_time=%f, bc->last_ts=%ld",
                         last_time, bc->last_ts);
          bc->eof = 1;
          if (!bc->interval_end_needed)
            return 0; // EOF
          // end the final interval
          if ((rc = end_interval(bc, bc->last_ts)) < 0)
            return rc; // error
          return 1; // successful interval end.  caller can use shared_view.
        }
      } while (bsrecord->time_sec < bc->minimum_time);

      /* check the gap limit is not exceeded */
      double this_time = bsrecord->time_sec;
      if (bc->gap_limit > 0 &&                     /* gap limit is enabled */
          last_time > 0 &&                         /* this is not the first packet */
          ((this_time - last_time) > 0) &&         /* packet doesn't go backward */
          (this_time - last_time) > bc->gap_limit) /* packet exceeds gap */
      {
        bgpcorsaro_log(__func__, bc,
                       "gap limit exceeded (prev: %f this: %f diff: %f)",
                       last_time, this_time, (this_time - last_time));
        return -1;
      }
      last_time = this_time;

      /*bgpcorsaro_log(__func__, bc, "got a record!");*/
#if 0
      {
        char buf[2048];
        bgpstream_record_snprintf(buf, sizeof(buf), bsrecord);
        bgpcorsaro_log(__func__, bc, "got a record: %s", buf);
      }
#endif

      if (bgpcorsaro_start_record(bc, bsrecord) < 0)
        return -1;
    }
  }
  return 0; // EOF
}

int bgpcorsaro_finalize_output(bgpcorsaro_t *bc)
{
#ifdef WITH_PLUGIN_TIMING
  struct timeval total_end, total_diff;
  gettimeofday(&total_end, NULL);
  timeval_subtract(&total_diff, &total_end, &bc->init_time);
  uint64_t total_time_usec =
    ((total_diff.tv_sec * 1000000) + total_diff.tv_usec);
#endif

  if (bc == NULL) {
    return 0;
  }
  if (bc->interval_end_needed) {
    if (end_interval(bc, bc->last_ts) != 0) {
      bgpcorsaro_log(__func__, bc, "could not end interval at %ld",
                     bc->last_ts);
      bgpcorsaro_free(bc);
      return -1;
    }
  }

#ifdef WITH_PLUGIN_TIMING
  fprintf(stderr, "========================================\n");
  fprintf(stderr, "Plugin Timing\n");
  bgpcorsaro_plugin_t *p = &bgpcorsaro_routingtables_plugin;
  {
    fprintf(stderr, "----------------------------------------\n");
    fprintf(stderr, "%s\n", p->name);
    fprintf(stderr, "\tinit_output    %" PRIu64 " (%0.2f%%)\n",
            p->init_output_usec, p->init_output_usec * 100.0 / total_time_usec);
    fprintf(stderr, "\tprocess_record %" PRIu64 " (%0.2f%%)\n",
            p->process_record_usec,
            p->process_record_usec * 100.0 / total_time_usec);
    fprintf(stderr, "\tstart_interval %" PRIu64 " (%0.2f%%)\n",
            p->start_interval_usec,
            p->start_interval_usec * 100.0 / total_time_usec);
    fprintf(stderr, "\tend_interval   %" PRIu64 " (%0.2f%%)\n",
            p->end_interval_usec,
            p->end_interval_usec * 100.0 / total_time_usec);
    fprintf(stderr, "\ttotal   %" PRIu64 " (%0.2f%%)\n",
            p->init_output_usec + p->process_record_usec +
              p->start_interval_usec + p->end_interval_usec,
            (p->init_output_usec + p->process_record_usec +
             p->start_interval_usec + p->end_interval_usec) *
              100.0 / total_time_usec);
  }
  fprintf(stderr, "========================================\n");
  fprintf(stderr, "Total Time (usec): %" PRIu64 "\n", total_time_usec);
#endif

  bgpcorsaro_free(bc);
  return 0;
}
