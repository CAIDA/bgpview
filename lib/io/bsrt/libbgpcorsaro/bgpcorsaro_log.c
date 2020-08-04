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
#include "config.h"

#include <assert.h>
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_TIME_H
#include <time.h>
#endif

#include "utils.h"

#include "bgpcorsaro_io.h"
#include "bgpcorsaro_log.h"

static char *timestamp_str(char *buf, const size_t len)
{
  struct timeval tv;
  struct tm *tm;

  buf[0] = '\0';
  gettimeofday(&tv, NULL);
  if ((tm = localtime(&tv.tv_sec)) == NULL)
    return buf;

  int ms = tv.tv_usec / 1000;
  snprintf(buf, len, "[%02d:%02d:%02d:%03d] ", tm->tm_hour, tm->tm_min,
           tm->tm_sec, ms);

  return buf;
}

ATTR_FORMAT_PRINTF(3, 0)
static void generic_log(const char *func, iow_t *logfile, const char *format,
                 va_list ap)
{
  char message[512];
  char ts[16];
  char fs[64];

  assert(format != NULL);

  vsnprintf(message, sizeof(message), format, ap);

  timestamp_str(ts, sizeof(ts));

  if (func != NULL)
    snprintf(fs, sizeof(fs), "%s: ", func);
  else
    fs[0] = '\0';

  if (logfile == NULL) {
    fprintf(stderr, "%s%s%s\n", ts, fs, message);
    fflush(stderr);
  } else {
    wandio_printf(logfile, "%s%s%s\n", ts, fs, message);
/*wandio_flush(logfile);*/
wandio_wflush(logfile); // XXX

#ifdef DEBUG
    /* we've been asked to dump debugging information */
    fprintf(stderr, "%s%s%s\n", ts, fs, message);
    fflush(stderr);
#endif
  }
}

void bgpcorsaro_log_va(const char *func, bgpcorsaro_t *bc,
                       const char *format, va_list args)
{
  iow_t *lf = (bc == NULL) ? NULL : bc->logfile;
  generic_log(func, lf, format, args);
}

void bgpcorsaro_log(const char *func, bgpcorsaro_t *bc, const char *format, ...)
{
  va_list ap;
  va_start(ap, format);
  bgpcorsaro_log_va(func, bc, format, ap);
  va_end(ap);
}

void bgpcorsaro_log_file(const char *func, iow_t *logfile, const char *format,
                         ...)
{
  va_list ap;
  va_start(ap, format);
  generic_log(func, logfile, format, ap);
  va_end(ap);
}

int bgpcorsaro_log_init(bgpcorsaro_t *bc)
{
  if ((bc->logfile = bgpcorsaro_io_prepare_file_full(bc, BGPCORSARO_IO_LOG_NAME,
          &bc->interval_start, WANDIO_COMPRESS_NONE, 0, O_CREAT)) == NULL) {
    fprintf(stderr, "could not open log for writing\n");
    return -1;
  }
  return 0;
}

void bgpcorsaro_log_close(bgpcorsaro_t *bc)
{
  if (bc->logfile != NULL) {
    wandio_wdestroy(bc->logfile);
    bc->logfile = NULL;
  }
}
