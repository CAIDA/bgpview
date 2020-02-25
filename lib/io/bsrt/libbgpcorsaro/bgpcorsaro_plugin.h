/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini, Ken Keys
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
