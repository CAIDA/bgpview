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

#include "bgpview.h"
#include "bgpview_io_file.h"
#include "config.h"
#include "utils.h"
#include <stdio.h>
#include <wandio.h>

static bgpview_t *view = NULL;
static iow_t *wstdout = NULL;

static int cat_file(const char *file)
{
  io_t *infile = NULL;
  int ret;

  if ((infile = wandio_create(file)) == NULL) {
    goto err;
  }

  while ((ret = bgpview_io_file_read(infile, view, NULL, NULL, NULL)) > 0) {
    if (bgpview_io_file_print(wstdout, view) != 0) {
      goto err;
    }
    bgpview_clear(view);
  }

  if (ret < 0) {
    goto err;
  }

  if (infile != NULL) {
    wandio_destroy(infile);
  }
  return 0;

err:
  if (infile != NULL) {
    wandio_destroy(infile);
  }
  return -1;
}

int main(int argc, char **argv)
{
  int i;

  if ((view = bgpview_create(NULL, NULL, NULL, NULL)) == NULL) {
    goto err;
  }

  if ((wstdout = wandio_wcreate("-", WANDIO_COMPRESS_NONE, 0, 0)) == NULL) {
    goto err;
  }

  if (argc == 1) {
    if (cat_file("-") != 0) {
      goto err;
    }
  } else {
    for (i = 1; i < argc; i++) {
      if (cat_file(argv[i]) != 0) {
        goto err;
      }
    }
  }

  if (stdout != NULL) {
    wandio_wdestroy(wstdout);
  }
  bgpview_destroy(view);
  return 0;

err:
  if (wstdout != NULL) {
    wandio_wdestroy(wstdout);
  }
  bgpview_destroy(view);
  return -1;
}
