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

#ifndef __BGPVIEW_COMMON_H
#define __BGPVIEW_COMMON_H

#include <stdint.h>

#include "bgpstream_utils_addr.h"

/** @file
 *
 * @brief Header file that exposes the public structures used by both
 * bgpview_io_client and bgpview_io_server
 *
 * @author Alistair King
 *
 */

/**
 * @name Public Constants
 *
 * @{ */

/** Default URI for the server to listen for client requests on */
#define BGPVIEW_IO_CLIENT_URI_DEFAULT "tcp://*:6300"

/** Default URI for the server to publish tables on (subscribed to by consumer
    clients) */
#define BGPVIEW_IO_CLIENT_PUB_URI_DEFAULT "tcp://*:6301"

/** Default the server/client heartbeat interval to 2000 msec */
#define BGPVIEW_IO_HEARTBEAT_INTERVAL_DEFAULT 2000

/** Default the server/client heartbeat liveness to 450 beats (15min) */
#define BGPVIEW_IO_HEARTBEAT_LIVENESS_DEFAULT 450

/** Default the client reconnect minimum interval to 1 second */
#define BGPVIEW_IO_RECONNECT_INTERVAL_MIN 1000

/** Default the client reconnect maximum interval to 32 seconds */
#define BGPVIEW_IO_RECONNECT_INTERVAL_MAX 32000

/** @} */

/**
 * @name Public Opaque Data Structures
 *
 * @{ */

/** @} */

/**
 * @name Public Data Structures
 *
 * @{ */

/** Type of a sequence number */
typedef uint32_t seq_num_t;

/** bgpview error information */
typedef struct bgpview_io_err {
  /** Error code */
  int err_num;

  /** String representation of the error that occurred */
  char problem[255];
} bgpview_io_err_t;

/** @} */

/**
 * @name Public Enums
 *
 * @{ */

/** Consumer interests
 *
 * A consumer has interests: it interested in being sent notifications about
 * something. E.g. a new prefix table being available.
 */
typedef enum {
  BGPVIEW_CONSUMER_INTEREST_FIRSTFULL    = 0b001,
  BGPVIEW_CONSUMER_INTEREST_FULL         = 0b010,
  BGPVIEW_CONSUMER_INTEREST_PARTIAL      = 0b100,
} bgpview_consumer_interest_t;

/* Consumer subscription strings.
 *
 * 0MQ subscriptions are simply a prefix match on the first message part. We can
 * leverage this to get hierarchical subscriptions (i.e. the most general
 * subscription should be the shortest, and all others should contain the
 * subscription of their parent. Clear as mud?
 */
#define BGPVIEW_CONSUMER_INTEREST_SUB_PARTIAL "P"

#define BGPVIEW_CONSUMER_INTEREST_SUB_FULL    \
  BGPVIEW_CONSUMER_INTEREST_SUB_PARTIAL"F"

#define BGPVIEW_CONSUMER_INTEREST_SUB_FIRSTFULL \
  BGPVIEW_CONSUMER_INTEREST_SUB_FULL"1"

/** Producer Intents
 *
 * A producer has intents: it intends to send messages about something. E.g. a
 * new prefix table.
 */
typedef enum {

  /** Prefix Table */
  BGPVIEW_PRODUCER_INTENT_PREFIX = 0x01,

} bgpview_producer_intent_t;


/** Enumeration of error codes
 *
 * @note these error codes MUST be <= 0
 */
typedef enum {

  /** No error has occured */
  BGPVIEW_IO_ERR_NONE         = 0,

  /** bgpview failed to initialize */
  BGPVIEW_IO_ERR_INIT_FAILED  = -1,

  /** bgpview failed to start */
  BGPVIEW_IO_ERR_START_FAILED = -2,

  /** bgpview was interrupted */
  BGPVIEW_IO_ERR_INTERRUPT    = -3,

  /** unhandled error */
  BGPVIEW_IO_ERR_UNHANDLED    = -4,

  /** protocol error */
  BGPVIEW_IO_ERR_PROTOCOL     = -5,

  /** malloc error */
  BGPVIEW_IO_ERR_MALLOC       = -6,

  /** store error */
  BGPVIEW_IO_ERR_STORE        = -7,

} bgpview_io_err_code_t;

/** @} */

/** Set an error state on the given IO error instance
 *
 * @param err           pointer to an error status instance to set the error on
 * @param errcode       error code to set (> 0 indicates errno)
 * @param msg...        string message to set
 */
void bgpview_io_err_set_err(bgpview_io_err_t *err, int errcode,
			const char *msg, ...);

/** Check if the given error status instance has an error set
 *
 * @param err           pointer to an error status instance to check for error
 * @return 0 if there is no error, 1 otherwise
 */
int bgpview_io_err_is_err(bgpview_io_err_t *err);

/** Prints the error status (if any) to standard error and clears the error
 * state
 *
 * @param err       pointer to bgpview error status instance
 */
void bgpview_io_err_perr(bgpview_io_err_t *err);

/** Dump the given interests to stdout in a human-readable format
 *
 * @param interests     set of interests
 */
void bgpview_consumer_interest_dump(int interests);

#endif
