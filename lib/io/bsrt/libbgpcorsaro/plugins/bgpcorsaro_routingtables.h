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
#ifndef __BGPCORSARO_ROUTINGTABLES_H
#define __BGPCORSARO_ROUTINGTABLES_H

#include "bgpcorsaro_plugin.h"

/** @file
 *
 * @brief Header file which exports bgpcorsaro_stats plugin API
 *
 * @author Chiara Orsini
 *
 */

int bgpcorsaro_routingtables_init_output(struct bgpcorsaro *bgpcorsaro);
int bgpcorsaro_routingtables_close_output(struct bgpcorsaro *bgpcorsaro);
int bgpcorsaro_routingtables_start_interval(struct bgpcorsaro *bgpcorsaro,
    struct bgpcorsaro_interval *int_start);
int bgpcorsaro_routingtables_end_interval(struct bgpcorsaro *bgpcorsaro,
    struct bgpcorsaro_interval *int_end);
int bgpcorsaro_routingtables_process_record(struct bgpcorsaro *bgpcorsaro,
    struct bgpcorsaro_record *record);

#endif /* __BGPCORSARO_ROUTINGTABLES_H */
