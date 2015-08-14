/*
 * This file is part of bgpstream
 *
 * Copyright (C) 2015 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini
 *
 * All rights reserved.
 *
 * This code has been developed by CAIDA at UC San Diego.
 * For more information, contact bgpstream-info@caida.org
 *
 * This source code is proprietary to the CAIDA group at UC San Diego and may
 * not be redistributed, published or disclosed without prior permission from
 * CAIDA.
 *
 * Report any bugs, questions or comments to bgpstream-info@caida.org
 *
 */

#ifndef __BGPVIEW_IO_CLIENT_INT_H
#define __BGPVIEW_IO_CLIENT_INT_H

#include <czmq.h>
#include <stdint.h>

#include "bgpview_io_client.h"
#include "bgpview_io_client_broker.h"

/** @file
 *
 * @brief Header file that exposes the private interface of the bgpview
 * client
 *
 * @author Alistair King
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

struct bgpview_io_client {

  /** shared config that we have prepared for our broker(s) */
  bgpview_io_client_broker_config_t broker_config;

  /** handle to communicate with our broker */
  zactor_t *broker;

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
