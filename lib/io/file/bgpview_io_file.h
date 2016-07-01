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

#ifndef __BGPVIEW_IO_FILE_H
#define __BGPVIEW_IO_FILE_H

#include "bgpview.h"
#include "bgpview_io.h"
#include <wandio.h>

/** Write the given view to the given file (in binary format)
 *
 * @param outfile       wandio file handle to write to
 * @param view          pointer to the view to send
 * @param cb            callback function to use to filter entries (may be NULL)
 * @param cb_user       user pointer provided to callback function
 * @return 0 if the view was written successfully, -1 otherwise
 */
int bgpview_io_file_write(iow_t *outfile, bgpview_t *view,
                          bgpview_io_filter_cb_t *cb, void *cb_user);

/** Receive a view from the given file
 *
 * @param infile        wandio file handle to read from
 * @param view          pointer to the clear/new view to receive into
 * @param cb            callback function to use to filter entries (may be NULL)
 * @return 1 if a view was successfully read, 0 if EOF was reached, -1 if an
 * error occurred
 */
int bgpview_io_file_read(io_t *infile, bgpview_t *view,
                         bgpview_io_filter_peer_cb_t *peer_cb,
                         bgpview_io_filter_pfx_cb_t *pfx_cb,
                         bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb);

/** Print the given view to the given file (in ASCII format)
 *
 * @param outfile       wandio file handle to print to
 * @param view          pointer to the view to output
 * @return 0 if the view was output successfully, -1 otherwise
 */
int bgpview_io_file_print(iow_t *outfile, bgpview_t *view);

/** Dump the given BGP View to stdout
 *
 * @param view        pointer to a view structure
 */
void bgpview_io_file_dump(bgpview_t *view);

#endif /* __BGPVIEW_IO_FILE_H */
