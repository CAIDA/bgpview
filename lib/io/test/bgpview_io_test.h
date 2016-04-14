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

#ifndef __BGPVIEW_IO_TEST_H
#define __BGPVIEW_IO_TEST_H

#include "bgpview.h"

typedef struct bgpview_io_test bgpview_io_test_t;

/** Create a test generator instance */
bgpview_io_test_t *bgpview_io_test_create(const char *opts);

/** Destroy the given test generator instance */
void bgpview_io_test_destroy(bgpview_io_test_t *generator);

/** Generate a semi-random view */
int bgpview_io_test_generate_view(bgpview_io_test_t *generator,
                                  bgpview_t *view);

#endif /* __BGPVIEW_IO_TEST_H */
