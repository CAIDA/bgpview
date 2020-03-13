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
