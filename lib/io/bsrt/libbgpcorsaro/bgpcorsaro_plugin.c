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
#include "bgpcorsaro_int.h"
#include "config.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"
#include "parse_cmd.h"

#include "bgpcorsaro_log.h"

#include "bgpcorsaro_plugin.h"

/* include the plugins that have been enabled */
#ifdef WITH_PLUGIN_ROUTINGTABLES
#include "bgpcorsaro_routingtables.h"
#endif

#define MAXOPTS 1024

static int copy_argv(bgpcorsaro_plugin_t *plugin, int argc, char *argv[])
{
  int i;
  plugin->argc = argc;

  /* malloc the pointers for the array */
  if ((plugin->argv = malloc(sizeof(char *) * (plugin->argc + 1))) == NULL) {
    return -1;
  }

  for (i = 0; i < plugin->argc; i++) {
    if ((plugin->argv[i] = malloc(strlen(argv[i]) + 1)) == NULL) {
      return -1;
    }
    strncpy(plugin->argv[i], argv[i], strlen(argv[i]) + 1);
  }

  /* as per ANSI spec, the last element in argv must be a NULL pointer */
  /* can't find the actual spec, but http://en.wikipedia.org/wiki/Main_function
     as well as other sources confirm this is standard */
  plugin->argv[plugin->argc] = NULL;

  return 0;
}

/** ==== PUBLIC API FUNCTIONS BELOW HERE ==== */

int bgpcorsaro_plugin_enable_plugin(bgpcorsaro_plugin_t *plugin,
                                    const char *plugin_args)
{
  char *local_args = NULL;
  char *process_argv[MAXOPTS];
  int process_argc = 0;

  bgpcorsaro_log(__func__, NULL, "enabling %s", plugin->name);

  /* now lets set the arguments for the plugin */
  /* we do this here, before we check if it is enabled to allow the args
     to be re-set, so long as it is before the plugin is started */
  if (plugin_args == NULL)
    plugin_args = "";
  /* parse the args into argc and argv */
  local_args = strdup(plugin_args);
  parse_cmd(local_args, &process_argc, process_argv, MAXOPTS, plugin->name);

  plugin->argc = 0;

  if (copy_argv(plugin, process_argc, process_argv) != 0) {
    if (local_args != NULL) {
      bgpcorsaro_log(__func__, NULL, "freeing local args");
      free(local_args);
    }
    return -1;
  }

  if (local_args != NULL) {
    /* this is the storage for the strings until copy_argv is complete */
    free(local_args);
  }
}

