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

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "utils.h"
#include "bgpview.h"
#include "bgpview_consumer_utils.h"

iow_t* bvcu_open_outfile(char *namebuf, const char *fmt, ...)
{
  iow_t *file;
  va_list ap;

  va_start(ap, fmt);
  int size = vsnprintf(namebuf, BVCU_PATH_MAX-1, fmt, ap);
  va_end(ap);
  if (size >= BVCU_PATH_MAX) {
    fprintf(stderr, "ERROR: File name too long\n");
    return NULL;
  }
  if ((file = wandio_wcreate(namebuf, wandio_detect_compression_type(namebuf),
      BVCU_DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL) {
    fprintf(stderr, "ERROR: Could not open %s for writing\n", namebuf);
    return NULL;
  }
  return file;
}

int bvcu_create_donefile(const char *filename)
{
  char buf[BVCU_PATH_MAX];
  FILE *file;
  if (snprintf(buf, BVCU_PATH_MAX, "%s.done", filename) >= BVCU_PATH_MAX) {
    fprintf(stderr, "ERROR: File name too long\n");
    return -1;
  }
  if (!(file = fopen(buf, "w"))) {
    fprintf(stderr, "ERROR: could not create %s: %s\n", buf, strerror(errno));
    return -1;
  }
  fclose(file);
  return 0;
}

int bvcu_is_writable_folder(const char *path)
{
  struct stat st;
  errno = 0;
  if (stat(path, &st) == -1) {
    fprintf(stderr, "ERROR: %s: %s\n", path, strerror(errno));
    return 0;
  } else if (!S_ISDIR(st.st_mode)) {
    errno = ENOTDIR;
    fprintf(stderr, "ERROR: %s: %s\n", path, strerror(errno));
    return 0;
  } else if (access(path, W_OK) == -1) {
    fprintf(stderr, "ERROR: %s: %s\n", path, strerror(errno));
    return 0;
  }

  return 1;
}

int bvcu_print_pfx_peer_as_path(iow_t *wf, bgpview_iter_t *it,
    const char *delim1, const char *delim2)
{
  char asn_buffer[1024];
  const char *delim = delim1;
  bgpstream_as_path_seg_t *seg;

  bgpview_iter_pfx_peer_as_path_seg_iter_reset(it);

  while ((seg = bgpview_iter_pfx_peer_as_path_seg_next(it))) {
    if (bgpstream_as_path_seg_snprintf(asn_buffer, sizeof(asn_buffer), seg) >=
        sizeof(asn_buffer)) {
      fprintf(stderr, "ERROR: ASN print truncated output\n");
      return -1;
    }
    if (wandio_printf(wf, "%s%s", delim, asn_buffer) == -1) {
      fprintf(stderr, "ERROR: Could not write data to file\n");
      return -1;
    }
    delim = delim2;
  }
  return 0;
}
