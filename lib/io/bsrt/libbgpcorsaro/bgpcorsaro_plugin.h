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
#ifndef __BGPCORSARO_PLUGIN_H
#define __BGPCORSARO_PLUGIN_H

#include "bgpcorsaro_int.h"

/** @file
 *
 * @brief Header file dealing with the bgpcorsaro plugin manager
 *
 * @author Alistair King
 *
 */

/** The name of this plugin */
#define PLUGIN_NAME "routingtables"

/** An bgpcorsaro packet processing plugin */
/* All functions should return -1, or NULL on failure */
typedef struct bgpcorsaro_plugin {
// #ifdef WITH_PLUGIN_TIMING
  /* variables that hold timing information for this plugin */

  /** Number of usec spent in the init_output function */
  uint64_t init_output_usec;

  /** Number of usec spent in the process_packet or process_flowtuple
      functions */
  uint64_t process_packet_usec;

  /** Number of usec spent in the start_interval function */
  uint64_t start_interval_usec;

  /** Number of usec spent in the end_interval function */
  uint64_t end_interval_usec;
// #endif

} bgpcorsaro_plugin_t;

extern bgpcorsaro_plugin_t bgpcorsaro_routingtables_plugin;

#endif /* __BGPCORSARO_PLUGIN_H */
