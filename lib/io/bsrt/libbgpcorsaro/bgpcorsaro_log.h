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
#ifndef __BGPCORSARO_LOG_H
#define __BGPCORSARO_LOG_H

#include "config.h"

#include <stdarg.h>

#include "bgpcorsaro_int.h"

/** @file
 *
 * @brief Header file dealing with the bgpcorsaro logging sub-system
 *
 * @author Alistair King
 *
 */

#ifdef __GNUC__
#define ATTR_FORMAT_PRINTF(i,j) __attribute__((format(printf, i, j)))
#else
#define ATTR_FORMAT_PRINTF(i,j) /* empty */
#endif

/** Write a formatted string to the logfile associated with an bgpcorsaro object
 *
 * @param func         The name of the calling function (__func__)
 * @param bgpcorsaro   The bgpcorsaro output object to log for
 * @param format       The printf style formatting string
 * @param args         Variable list of arguments to the format string
 *
 * This function takes the same style of arguments that printf(3) does.
 */
ATTR_FORMAT_PRINTF(3, 0)
void bgpcorsaro_log_va(const char *func, bgpcorsaro_t *bgpcorsaro,
                       const char *format, va_list args);

/** Write a formatted string to the logfile associated with an bgpcorsaro object
 *
 * @param func         The name of the calling function (__func__)
 * @param bgpcorsaro        The bgpcorsaro output object to log for
 * @param format       The printf style formatting string
 * @param ...          Variable list of arguments to the format string
 *
 * This function takes the same style of arguments that printf(3) does.
 */
void bgpcorsaro_log(const char *func, bgpcorsaro_t *bgpcorsaro,
                    const char *format, ...) ATTR_FORMAT_PRINTF(3, 4);

/** Write a formatted string to a generic log file
 *
 * @param func         The name of the calling function (__func__)
 * @param logfile      The file to log to (STDERR if NULL is passed)
 * @param format       The printf style formatting string
 * @param ...          Variable list of arguments to the format string
 *
 * This function takes the same style of arguments that printf(3) does.
 */
void bgpcorsaro_log_file(const char *func, iow_t *logfile, const char *format,
                         ...) ATTR_FORMAT_PRINTF(3, 4);

/** Initialize the logging sub-system for an bgpcorsaro output object
 *
 * @param bgpcorsaro        The bgpcorsaro object to associate the logger with
 * @return 0 if successful, -1 if an error occurs
 */
int bgpcorsaro_log_init(bgpcorsaro_t *bgpcorsaro);

/** Close the log file for an bgpcorsaro output object
 *
 * @param bgpcorsaro         The bgpcorsaro output object to close logging for
 */
void bgpcorsaro_log_close(bgpcorsaro_t *bgpcorsaro);

#endif /* __BGPCORSARO_LOG_H */
