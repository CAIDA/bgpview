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

#ifndef __BGPVIEW_IO_H
#define __BGPVIEW_IO_H

#include <wandio.h>

#include "bgpview.h"

/** Dump the given BGP View to stdout
 *
 * @param view        pointer to a view structure
 */
void
bgpview_io_dump(bgpview_t *view);

/** Send the given view to the given socket
 *
 * @param dest          socket to send the view to
 * @param view          pointer to the view to send
 * @param cb            callback function to use to filter peers (may be NULL)
 * @return 0 if the view was sent successfully, -1 otherwise
 */
int bgpview_io_send(void *dest, bgpview_t *view,
                    bgpview_filter_peer_cb_t *cb);

/** Receive a view from the given socket
 *
 * @param src           socket to receive on
 * @param view          pointer to the clear/new view to receive into
 * @return pointer to the view instance received, NULL if an error occurred.
 */
int bgpview_io_recv(void *src, bgpview_t *view);

/** Write the given view to the given file (in binary format)
 *
 * @param outfile       wandio file handle to write to
 * @param view          pointer to the view to send
 * @param cb            callback function to use to filter peers (may be NULL)
 * @return 0 if the view was written successfully, -1 otherwise
 */
int bgpview_io_write(iow_t *outfile, bgpview_t *view,
                     bgpview_filter_peer_cb_t *cb);

/** Receive a view from the given file
 *
 * @param infile        wandio file handle to read from
 * @param view          pointer to the clear/new view to receive into
 * @return 1 if a view was successfully read, 0 if EOF was reached, -1 if an
 * error occurred
 */
int bgpview_io_read(io_t *infile, bgpview_t *view);

/** Print the given view to the given file (in ASCII format)
 *
 * @param outfile       wandio file handle to print to
 * @param view          pointer to the view to output
 * @return 0 if the view was output successfully, -1 otherwise
 */
int bgpview_io_print(iow_t *outfile, bgpview_t *view);

#endif /* __BGPVIEW_IO_H */
