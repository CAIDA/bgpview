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

  /** shared config that we have prepared for our broker(s) */
  bgpview_io_client_broker_config_t broker_config;

  /** handle to communicate with our broker */
  zactor_t *broker;

  /** Socket to communicate data with the broker */
  zsock_t *broker_sock;

  /** raw socket to the broker */
  void *broker_zocket;

  /** Error status */
  bgpview_io_err_t err;

  /** Next request sequence number to use */
  seq_num_t seq_num;

  /** Indicates that the client has been signaled to shutdown */
  int shutdown;

};

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** @} */

#endif
