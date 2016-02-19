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

#include "config.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>       /* time */

/* this must be all we include from bgpview */
#include "bgpview.h"
#include "bgpview_io_kafka.h"

#include "config.h"
#include "utils.h"


int main(int argc, char **argv)
{

	// DEFAULT TMP VALUES
	kafka_data_t src;
	src.brokers = "192.172.226.44:9092,192.172.226.46:9092";
	src.pfxs_paths_topic="views";
	src.peers_topic="peers";
	src.metadata_topic="metadata";
	src.pfxs_paths_partition=0;
	src.peers_partition=0;
	src.metadata_partition=0;
	src.pfxs_paths_offset=0;
	src.peers_offset=0;
	src.metadata_offset=0;

	bgpview_t *view = bgpview_create(NULL, NULL, NULL, NULL);
	bgpview_io_kafka_recv(src,view,-1);

  return 0;
}
