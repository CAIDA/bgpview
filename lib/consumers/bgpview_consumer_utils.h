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

#include <wandio.h>

#ifdef __GNUC__
#define ATTR_FORMAT_PRINTF(i,j) __attribute__((format(printf, i, j)))
#else
#define ATTR_FORMAT_PRINTF(i,j) /* empty */
#endif

#define BVCU_PATH_MAX 1024
#define BVCU_DEFAULT_COMPRESS_LEVEL 6

/** Open a wandio file for writing.
 *
 * @param namebuf  Pointer to a char[BVCU_PATH_MAX] buffer to hold the filename
 * @param fmt      printf format string to generate file name
 * @param ...      printf arguments
 * @return         Pointer to an iow_t if the file was opened, NULL if an
 *                 error occurred
 *
 * This function will store a file name into namebuf, generated using fmt and
 * the remaining args.
 * Then the file will be opened with wandio_wcreate(), with compression type
 * automatically determined by the file name.
 *
 * Any error messages are printed to stderr.
 */
ATTR_FORMAT_PRINTF(2, 3)
iow_t* bvcu_open_outfile(char *namebuf, const char *fmt, ...);

/** Create an empty file with the given name plus ".done".
 *
 * @param filename  Name of the file to create, without ".done".
 * @return          0 for success, -1 for error.
 *
 * Any error messages are printed to stderr.
 */
int bvcu_create_donefile(const char *filename);

/** Check that path is the name of an existing writable directory.
 *
 * @param path  Name of the directory to check
 * @return      1 for true, 0 for false.
 *
 * Any error messages are printed to stderr.
 */
int bvcu_is_writable_folder(const char *path);

/** Write a pfx-peer iterator's AS path to a wandio file.
 *
 * @param wf      the wandio file to write to
 * @param it      the bgpview pfx-peer iterator with the AS path
 * @param delim1  string to print before the first segment
 * @param delim2  string to print before the 2nd-Nth segments
 * @return        0 for succcess, -1 for error
 *
 * Any error messages are printed to stderr.
 */
int bvcu_print_pfx_peer_as_path(iow_t *wf, bgpview_iter_t *it,
    const char *delim1, const char *delim2);
