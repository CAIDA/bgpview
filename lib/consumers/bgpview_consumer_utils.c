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
