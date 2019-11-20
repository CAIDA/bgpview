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

/** Convenience macro that defines all the function prototypes for the
 * bgpcorsaro
 * plugin API
 *
 * @todo split this into bgpcorsaro-out and bgpcorsaro-in macros
 */
#define BGPCORSARO_PLUGIN_GENERATE_PROTOS(plugin)                              \
  int plugin##_init_output(struct bgpcorsaro *bgpcorsaro);                     \
  int plugin##_close_output(struct bgpcorsaro *bgpcorsaro);                    \
  int plugin##_start_interval(struct bgpcorsaro *bgpcorsaro,                   \
                              struct bgpcorsaro_interval *int_start);          \
  int plugin##_end_interval(struct bgpcorsaro *bgpcorsaro,                     \
                            struct bgpcorsaro_interval *int_end);              \
  int plugin##_process_record(struct bgpcorsaro *bgpcorsaro,                   \
                              struct bgpcorsaro_record *record);

/** Convenience macro that defines all the function pointers for the bgpcorsaro
 * plugin API
 */
#define BGPCORSARO_PLUGIN_GENERATE_PTRS(plugin)                                \
  plugin##_init_output, plugin##_close_output, plugin##_start_interval,        \
    plugin##_end_interval, plugin##_process_record

/** Convenience macro that defines all the 'remaining' blank fields in a
 * bgpcorsaro
 *  plugin object
 *
 *  This becomes useful if we add more fields to the end of the plugin
 *  structure, because each plugin does not need to be updated in order to
 *  correctly 'zero' these fields.
 */
#define BGPCORSARO_PLUGIN_GENERATE_TAIL                                        \
    0,   /* argc */                                                            \
    NULL /* argv */

/** A unique identifier for a plugin, used when writing binary data
 *
 * @note this identifier does not affect the order in which plugins are passed
 *       records. Plugin precedence is determined either by the order of the
 *       ED_WITH_PLUGIN macros in configure.ac, or by the order of the plugins
 *       that have been explicitly enabled using \ref bgpcorsaro_enable_plugin
 */
typedef enum bgpcorsaro_plugin_id {
  /** RoutingTables plugin */
  BGPCORSARO_PLUGIN_ID_ROUTINGTABLES = 3,
} bgpcorsaro_plugin_id_t;

/** An bgpcorsaro packet processing plugin */
/* All functions should return -1, or NULL on failure */
typedef struct bgpcorsaro_plugin {
  /** The name of this plugin used in the ascii output and eventually to allow
   * plugins to be enabled and disabled */
  const char *name;

  /** The version of this plugin */
  const char *version;

  /** The bgpcorsaro plugin id for this plugin */
  const bgpcorsaro_plugin_id_t id;

  /** Initializes an output file using the plugin
   *
   * @param bgpcorsaro	The bgpcorsaro output to be initialized
   * @return 0 if successful, -1 in the event of error
   */
  int (*init_output)(struct bgpcorsaro *bgpcorsaro);

  /** Concludes an output file and cleans up the plugin data.
   *
   * @param bgpcorsaro 	The output file to be concluded
   * @return 0 if successful, -1 if an error occurs
   */
  int (*close_output)(struct bgpcorsaro *bgpcorsaro);

  /** Starts a new interval
   *
   * @param bgpcorsaro 	The output object to start the interval on
   * @param int_start   The start structure for the interval
   * @return 0 if successful, -1 if an error occurs
   */
  int (*start_interval)(struct bgpcorsaro *bgpcorsaro,
                        bgpcorsaro_interval_t *int_start);

  /** Ends an interval
   *
   * @param bgpcorsaro 	The output object end the interval on
   * @param int_end     The end structure for the interval
   * @return 0 if successful, -1 if an error occurs
   *
   * This is likely when the plugin will write it's data to it's output file
   */
  int (*end_interval)(struct bgpcorsaro *bgpcorsaro,
                      bgpcorsaro_interval_t *int_end);

  /**
   * Process a record
   *
   * @param bgpcorsaro  The output object to process the record for
   * @param packet      The packet to process
   * @return 0 if successful, -1 if an error occurs
   *
   * This is where the magic happens, the plugin should do any processing needed
   * for this record and update internal state and optionally update the
   * bgpcorsaro_record_state object to pass on discoveries to later plugins.
   */
  int (*process_record)(struct bgpcorsaro *bgpcorsaro,
                        struct bgpcorsaro_record *record);

  /** Count of arguments in argv */
  int argc;

  /** Array of plugin arguments. */
  char **argv;

#ifdef WITH_PLUGIN_TIMING
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
#endif

} bgpcorsaro_plugin_t;

/** Attempt to enable a plugin
 *
 * @param plugin       The plugin to enable
 * @param plugin_args  The arguments to pass to the plugin (for config)
 * @return 0 if the plugin was successfully enabled, -1 otherwise
 *
 * See bgpcorsaro_enable_plugin for more details.
 */
int bgpcorsaro_plugin_enable_plugin(bgpcorsaro_plugin_t *plugin,
                                    const char *plugin_args);

#endif /* __BGPCORSARO_PLUGIN_H */
