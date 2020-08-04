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

#ifndef __BGPVIEW_IO_ZMQ_CLIENT_INT_H
#define __BGPVIEW_IO_ZMQ_CLIENT_INT_H

#include <czmq.h>
#include <stdint.h>

#include "bgpview_io_zmq_client.h"
#include "bgpview_io_zmq_client_broker.h"

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

struct bgpview_io_zmq_client {

  /** shared config that we have prepared for our broker(s) */
  bgpview_io_zmq_client_broker_config_t broker_config;

  /** handle to communicate with our broker */
  zactor_t *broker;

  /** Socket to communicate data with the broker */
  zsock_t *broker_sock;

  /** raw socket to the broker */
  void *broker_zocket;

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

#endif /* __BGPVIEW_IO_ZMQ_CLIENT_INT_H */
