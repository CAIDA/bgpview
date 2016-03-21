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

#ifndef __BGPVIEW_IO_KAFKA_CLIENT_INT_H
#define __BGPVIEW_IO_KAFKA_CLIENT_INT_H


#include <stdint.h>
#include "bgpview_io_kafka.h"
#include "bgpview_io_common.h"

/** @file
 *
 * @brief Header file that exposes the private interface of the bgpview
 * client
 *
 * @author Danilo Giordano
 *
 */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

/** @} */

/**
 * @name Protected Data Structures
 *
 * @{ */

struct bgpview_io_kafka_client {

	  /** Error status */
	  bgpview_io_err_t err;

	  kafka_data_t kafka_config;

	  kafka_view_data_t view_data;

  /* Historical View*/
	bgpview_t *viewH;

};

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** @} */

#endif
