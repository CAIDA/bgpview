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
