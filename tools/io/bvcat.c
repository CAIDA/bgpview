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
