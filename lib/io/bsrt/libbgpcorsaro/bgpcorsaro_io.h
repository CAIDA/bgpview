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
#ifndef __BGPCORSARO_IO_H
#define __BGPCORSARO_IO_H

#include "config.h"

#include "bgpcorsaro_int.h"

#include "bgpcorsaro_plugin.h"

/** @file
 *
 * @brief Header file dealing with the bgpcorsaro file IO
 *
 * @author Alistair King
 *
 */

/** The character to replace with the name of the plugin */
#define BGPCORSARO_IO_PLUGIN_PATTERN 'X'
/** The pattern to replace in the output file name with the name of the plugin
 */
#define BGPCORSARO_IO_PLUGIN_PATTERN_STR "%X"

/** The character to replace with the monitor name */
#define BGPCORSARO_IO_MONITOR_PATTERN 'N'
/** The pattern to replace in the output file name with monitor name */
#define BGPCORSARO_IO_MONITOR_PATTERN_STR "%N"

/** The name to use for the log 'plugin' file */
#define BGPCORSARO_IO_LOG_NAME "log"

/** Uses the given settings to open an bgpcorsaro file for the given plugin
 *
 * @param bgpcorsaro          The bgpcorsaro object associated with the file
 * @param plugin_name    The name of the plugin (inserted into the template)
 * @param interval       The first interval start time represented in the file
 *                       (inserted into the template)
 * @param compress       The wandio file compression type to use
 * @param compress_level The wandio file compression level to use
 * @param flags          The flags to use when opening the file (e.g. O_CREAT)
 * @return A pointer to a new wandio output file, or NULL if an error occurs
 */
iow_t *bgpcorsaro_io_prepare_file_full(bgpcorsaro_t *bgpcorsaro,
                                       const char *plugin_name,
                                       bgpcorsaro_interval_t *interval,
                                       int compress, int compress_level,
                                       int flags);

/** Uses the current settings to open an bgpcorsaro file for the given plugin
 *
 * @param bgpcorsaro   The bgpcorsaro object associated with the file
 * @param plugin_name  The name of the plugin (inserted into the template)
 * @param interval     The first interval start time represented in the file
 *                     (inserted into the template)
 * @return A pointer to a new wandio output file, or NULL if an error occurs
 */
iow_t *bgpcorsaro_io_prepare_file(bgpcorsaro_t *bgpcorsaro,
                                  const char *plugin_name,
                                  bgpcorsaro_interval_t *interval);

/** Validates a output file template for needed features
 *
 * @param bgpcorsaro    The bgpcorsaro object associated with the template
 * @param template      The file template to be validated
 * @return 1 if the template is valid, 0 if it is invalid
 */
int bgpcorsaro_io_validate_template(bgpcorsaro_t *bgpcorsaro, char *template);

/** Determines whether there are any time-related patterns in the file template.
 *
 * @param bgpcorsaro       The bgpcorsaro object to check
 * @return 1 if there are time-related patterns, 0 if not
 */
int bgpcorsaro_io_template_has_timestamp(bgpcorsaro_t *bgpcorsaro);

/** Write the appropriate interval headers to the file
 *
 * @param bgpcorsaro     The bgpcorsaro object associated with the file
 * @param file           The wandio output file to write to
 * @param int_start      The start interval to write out
 * @return The amount of data written, or -1 if an error occurs
 */
off_t bgpcorsaro_io_write_interval_start(bgpcorsaro_t *bgpcorsaro, iow_t *file,
                                         bgpcorsaro_interval_t *int_start);

/** Write the appropriate interval trailers to the file
 *
 * @param bgpcorsaro     The bgpcorsaro object associated with the file
 * @param file           The wandio output file to write to
 * @param int_end        The end interval to write out
 * @return The amount of data written, or -1 if an error occurs
 */
off_t bgpcorsaro_io_write_interval_end(bgpcorsaro_t *bgpcorsaro, iow_t *file,
                                       bgpcorsaro_interval_t *int_end);

/** Write the appropriate plugin header to the file
 *
 * @param bgpcorsaro     The bgpcorsaro object associated with the file
 * @param file           The wandio output file to write to
 * @param plugin         The plugin object to write a start record for
 * @return The amount of data written, or -1 if an error occurs
 */
off_t bgpcorsaro_io_write_plugin_start(bgpcorsaro_t *bgpcorsaro, iow_t *file,
                                       bgpcorsaro_plugin_t *plugin);

/** Write the appropriate plugin trailer to the file
 *
 * @param bgpcorsaro     The bgpcorsaro object associated with the file
 * @param file           The wandio output file to write to
 * @param plugin         The plugin object to write an end record for
 * @return The amount of data written, or -1 if an error occurs
 */
off_t bgpcorsaro_io_write_plugin_end(bgpcorsaro_t *bgpcorsaro, iow_t *file,
                                     bgpcorsaro_plugin_t *plugin);

#endif /* __BGPCORSARO_IO_H */
