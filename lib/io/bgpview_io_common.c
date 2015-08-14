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

#include <assert.h>
#include <czmq.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

#include "bgpview_io_common_int.h"

#include "utils.h"

static char *recv_str(void *src)
{
  zmq_msg_t llm;
  size_t len;
  char *str = NULL;

  if(zmq_msg_init(&llm) == -1 || zmq_msg_recv(&llm, src, 0) == -1)
    {
      goto err;
    }
  len = zmq_msg_size(&llm);
  if((str = malloc(len + 1)) == NULL)
    {
      goto err;
    }
  memcpy(str, zmq_msg_data(&llm), len);
  str[len] = '\0';
  zmq_msg_close(&llm);

  return str;

 err:
  free(str);
  return NULL;
}

/* ========== PROTECTED FUNCTIONS BELOW HERE ========== */

/* ========== MESSAGE TYPES ========== */

bgpview_msg_type_t bgpview_recv_type(void *src, int flags)
{
  bgpview_msg_type_t type = BGPVIEW_MSG_TYPE_UNKNOWN;

  if((zmq_recv(src, &type, bgpview_msg_type_size_t, flags)
      != bgpview_msg_type_size_t) ||
     (type > BGPVIEW_MSG_TYPE_MAX))
    {
      return BGPVIEW_MSG_TYPE_UNKNOWN;
    }

  return type;
}


/* ========== INTERESTS/VIEWS ========== */

const char *bgpview_consumer_interest_pub(int interests)
{
  /* start with the most specific and work backward */
  /* NOTE: a view CANNOT satisfy FIRSTFULL and NOT satisfy FULL/PARTIAL */
  if(interests & BGPVIEW_CONSUMER_INTEREST_FIRSTFULL)
    {
      return BGPVIEW_CONSUMER_INTEREST_SUB_FIRSTFULL;
    }
  else if(interests & BGPVIEW_CONSUMER_INTEREST_FULL)
    {
      return BGPVIEW_CONSUMER_INTEREST_SUB_FULL;
    }
  else if(interests & BGPVIEW_CONSUMER_INTEREST_PARTIAL)
    {
      return BGPVIEW_CONSUMER_INTEREST_SUB_PARTIAL;
    }

  return NULL;
}

const char *bgpview_consumer_interest_sub(int interests)
{
  /* start with the least specific and work backward */
  if(interests & BGPVIEW_CONSUMER_INTEREST_PARTIAL)
    {
      return BGPVIEW_CONSUMER_INTEREST_SUB_PARTIAL;
    }
  else if(interests & BGPVIEW_CONSUMER_INTEREST_FULL)
    {
      return BGPVIEW_CONSUMER_INTEREST_SUB_FULL;
    }
  else if(interests & BGPVIEW_CONSUMER_INTEREST_FIRSTFULL)
    {
      return BGPVIEW_CONSUMER_INTEREST_SUB_FIRSTFULL;
    }

  return NULL;
}

uint8_t bgpview_consumer_interest_recv(void *src)
{
  char *pub_str = NULL;
  uint8_t interests = 0;

  /* grab the subscription frame and convert to interests */
  if((pub_str = recv_str(src)) == NULL)
    {
      goto err;
    }

  /** @todo make all this stuff less hard-coded and extensible */
  if(strcmp(pub_str, BGPVIEW_CONSUMER_INTEREST_SUB_FIRSTFULL) == 0)
    {
      interests |= BGPVIEW_CONSUMER_INTEREST_PARTIAL;
      interests |= BGPVIEW_CONSUMER_INTEREST_FULL;
      interests |= BGPVIEW_CONSUMER_INTEREST_FIRSTFULL;
    }
  else if(strcmp(pub_str, BGPVIEW_CONSUMER_INTEREST_SUB_FULL) == 0)
    {
      interests |= BGPVIEW_CONSUMER_INTEREST_PARTIAL;
      interests |= BGPVIEW_CONSUMER_INTEREST_FULL;
    }
  else if(strcmp(pub_str, BGPVIEW_CONSUMER_INTEREST_SUB_PARTIAL) == 0)
    {
      interests |= BGPVIEW_CONSUMER_INTEREST_PARTIAL;
    }
  else
    {
      goto err;
    }

  free(pub_str);
  return interests;

 err:
  free(pub_str);
  return 0;
}

/* ========== PUBLIC FUNCTIONS BELOW HERE ========== */
/*      See bgpview_io_common.h for declarations     */

void bgpview_io_err_set_err(bgpview_io_err_t *err, int errcode,
			const char *msg, ...)
{
  char buf[256];
  va_list va;

  va_start(va,msg);

  assert(errcode != 0 && "An error occurred, but it is unknown what it is");

  err->err_num=errcode;

  if (errcode>0) {
    vsnprintf(buf, sizeof(buf), msg, va);
    snprintf(err->problem, sizeof(err->problem), "%s: %s", buf,
	     strerror(errcode));
  } else {
    vsnprintf(err->problem, sizeof(err->problem), msg, va);
  }

  va_end(va);
}

int bgpview_io_err_is_err(bgpview_io_err_t *err)
{
  return err->err_num != 0;
}

void bgpview_io_err_perr(bgpview_io_err_t *err)
{
  if(err->err_num) {
    fprintf(stderr,"%s (%d)\n", err->problem, err->err_num);
  } else {
    fprintf(stderr,"No error\n");
  }
  err->err_num = 0; /* "OK" */
  err->problem[0]='\0';
}
