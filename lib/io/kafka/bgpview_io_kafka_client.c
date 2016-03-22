/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Danilo Giordano
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

#include <stdint.h>
#include <assert.h>

#include "bgpview_io_kafka_client_int.h"
#include "bgpview_io_kafka_client.h"
#include "bgpview_io_kafka.h"

#include "khash.h"
#include "utils.h"


/* ========== PUBLIC FUNCS BELOW HERE ========== */

bgpview_io_kafka_client_t *bgpview_io_kafka_client_init()
{
  bgpview_io_kafka_client_t *client;
  if((client = malloc_zero(sizeof(bgpview_io_kafka_client_t))) == NULL)
    {
      /* cannot set an err at this point */
      return NULL;
    }
  /* now we are ready to set errors... */

  /* now init the shared state for our broker */



  if((client->kafka_config.brokers =
      strdup(BGPVIEW_IO_KAFKA_CLIENT_SERVER_URI_DEFAULT)) == NULL)
    {
      fprintf(stderr, "Failed to duplicate kafka server uri string\n");
      goto err;
    }

  if((client->kafka_config.pfxs_paths_topic =
      strdup(BGPVIEW_IO_KAFKA_CLIENT_PFXS_PATHS_TOPIC_DEFAULT)) == NULL)
    {
      fprintf(stderr,
              "Failed to duplicate kafka prefixes paths topic string\n");
      goto err;
    }

  if((client->kafka_config.peers_topic =
      strdup(BGPVIEW_IO_KAFKA_CLIENT_PEERS_TOPIC_DEFAULT)) == NULL)
    {
      fprintf(stderr, "Failed to duplicate kafka peers topic string\n");
      goto err;
    }

  if((client->kafka_config.metadata_topic =
      strdup(BGPVIEW_IO_KAFKA_CLIENT_METADATA_TOPIC_DEFAULT)) == NULL)
    {
      fprintf(stderr, "Failed to duplicate kafka metadata topic string\n");
      goto err;
    }


  client->kafka_config.peers_partition=BGPVIEW_IO_KAFKA_CLIENT_PEERS_PARTITION_DEFAULT;
  client->kafka_config.metadata_partition=BGPVIEW_IO_KAFKA_CLIENT_METADATA_PARTITION_DEFAULT;

  client->kafka_config.peers_offset=0;
  client->kafka_config.metadata_offset=0;
  client->kafka_config.pfxs_paths_offset=0;
  client->kafka_config.pfxs_paths_partition=0;

  client->kafka_config.pfxs_paths_rk=NULL;
  client->kafka_config.peers_rk=NULL;
  client->kafka_config.metadata_rk=NULL;

  client->kafka_config.pfxs_paths_rkt=NULL;
  client->kafka_config.peers_rkt=NULL;
  client->kafka_config.metadata_rkt=NULL;

  client->kafka_config.view_frequency=BGPVIEW_IO_KAFKA_CLIENT_DIFF_FREQUENCY;

  client->view_data.viewH=NULL;
  client->view_data.sync_view_id=0;


  int i;
  for(i=0;i<2048;i++)
	  client->view_data.peerid_map[i]=0;

  client->view_data.current_pfxs_paths_offset=0;
  client->view_data.current_peers_offset=0;

  client->view_data.pfxs_paths_sync_partition=0;
  client->view_data.pfxs_paths_sync_offset=0;
  client->view_data.peers_sync_offset=0;

  for(i=0;i<KAFKA_CLIENT_DIFF_FREQUENCY;i++){
	  client->view_data.pfxs_paths_diffs_partition[i]=0;
	  client->view_data.pfxs_paths_diffs_offset[i]=0;
	  client->view_data.peers_offset[i]=0;
  }

  client->view_data.num_diffs=0;

  return client;

 err:
  if(client != NULL)
    {
      bgpview_io_kafka_client_free(client);
    }
  return NULL;
}


void bgpview_io_kafka_client_free(bgpview_io_kafka_client_t *client)
{
  assert(client != NULL);

  free(client->kafka_config.brokers);
  client->kafka_config.brokers = NULL;

  free(client->kafka_config.pfxs_paths_topic);
  client->kafka_config.pfxs_paths_topic = NULL;

  free(client->kafka_config.peers_topic);
  client->kafka_config.peers_topic = NULL;

  free(client->kafka_config.metadata_topic);
  client->kafka_config.metadata_topic = NULL;

  if(client->kafka_config.peers_rk!=NULL){
	  rd_kafka_topic_destroy(client->kafka_config.peers_rkt);
  	  rd_kafka_destroy(client->kafka_config.peers_rk);

  }
  if(client->kafka_config.pfxs_paths_rk!=NULL){
	  rd_kafka_topic_destroy(client->kafka_config.pfxs_paths_rkt);
  	  rd_kafka_destroy(client->kafka_config.pfxs_paths_rk);
  }

  if(client->kafka_config.metadata_rk!=NULL){
	  rd_kafka_topic_destroy(client->kafka_config.metadata_rkt);
  	  rd_kafka_destroy(client->kafka_config.metadata_rk);
  }

  client->kafka_config.pfxs_paths_rk=NULL;
  client->kafka_config.peers_rk=NULL;
  client->kafka_config.metadata_rk=NULL;

  client->kafka_config.pfxs_paths_rkt=NULL;
  client->kafka_config.peers_rkt=NULL;
  client->kafka_config.metadata_rkt=NULL;


  bgpview_destroy(client->view_data.viewH);

  free(client);

  return;
}



int bgpview_io_kafka_client_start_producer(bgpview_io_kafka_client_t *client)
{

	if(initialize_producer_connection(
			&client->kafka_config.metadata_rk,
			&client->kafka_config.metadata_rkt,
			client->kafka_config.brokers,
			client->kafka_config.metadata_topic)==-1){
			fprintf(stderr,"error");
			goto err;
	}
	if(initialize_producer_connection(
			&client->kafka_config.peers_rk,
			&client->kafka_config.peers_rkt,
			client->kafka_config.brokers,
			client->kafka_config.peers_topic)==-1){
			fprintf(stderr,"error");
			goto err;
	}
	if(initialize_producer_connection(
			&client->kafka_config.pfxs_paths_rk,
			&client->kafka_config.pfxs_paths_rkt,
			client->kafka_config.brokers,
			client->kafka_config.pfxs_paths_topic)==-1){
			fprintf(stderr,"error");
			goto err;
	}

	return 0;

	err:
	return -1;

}

int bgpview_io_kafka_client_start_consumer(bgpview_io_kafka_client_t *client)
{
	if(initialize_consumer_connection(
			&client->kafka_config.metadata_rk,
			&client->kafka_config.metadata_rkt,
			client->kafka_config.brokers,
			client->kafka_config.metadata_topic)==-1){
			fprintf(stderr,"error");
			goto err;
	}
	if(initialize_consumer_connection(
			&client->kafka_config.peers_rk,
			&client->kafka_config.peers_rkt,
			client->kafka_config.brokers,
			client->kafka_config.peers_topic)==-1){
			fprintf(stderr,"error");
			goto err;
	}
	if(initialize_consumer_connection(
			&client->kafka_config.pfxs_paths_rk,
			&client->kafka_config.pfxs_paths_rkt,
			client->kafka_config.brokers,
			client->kafka_config.pfxs_paths_topic)==-1){
			fprintf(stderr,"error");
			goto err;
	}

	return 0;

	err:
	return -1;

}



void bgpview_io_kafka_client_set_diff_frequency(bgpview_io_kafka_client_t *client, int frequency)
{
  assert(client != NULL);

  assert(frequency >= 0);

  client->kafka_config.view_frequency=frequency;

}

int bgpview_io_kafka_client_set_broker_addresses(bgpview_io_kafka_client_t *client, const char *addresses)
{
  assert(client != NULL);

  if(client->kafka_config.brokers != NULL)
	  free(client->kafka_config.brokers);

  if((client->kafka_config.brokers = strdup(addresses)) == NULL)
    {
      fprintf(stderr, "Could not set server addresses\n");
      return -1;
    }

  return 0;

}

int bgpview_io_kafka_client_set_pfxs_paths_topic(bgpview_io_kafka_client_t *client, const char *topic)
{
  assert(client != NULL);

  if(client->kafka_config.pfxs_paths_topic != NULL)
	  free(client->kafka_config.pfxs_paths_topic);

  if((client->kafka_config.pfxs_paths_topic = strdup(topic)) == NULL)
    {
      fprintf(stderr, "Could not set server uri\n");
      return -1;
    }

  return 0;

}

int bgpview_io_kafka_client_set_peers_topic(bgpview_io_kafka_client_t *client, const char *topic)
{
  assert(client != NULL);

  if(client->kafka_config.peers_topic != NULL)
	  free(client->kafka_config.peers_topic);

  if((client->kafka_config.peers_topic = strdup(topic)) == NULL)
    {
      fprintf(stderr, "Could not set server uri\n");
      return -1;
    }

  return 0;

}

int bgpview_io_kafka_client_set_metadata_topic(bgpview_io_kafka_client_t *client, const char *topic)
{
  assert(client != NULL);

  if(client->kafka_config.metadata_topic != NULL)
	  free(client->kafka_config.metadata_topic);

  if((client->kafka_config.metadata_topic = strdup(topic)) == NULL)
    {
      fprintf(stderr, "Could not set server uri\n");
      return -1;
    }

  return 0;

}

void bgpview_io_kafka_client_set_pfxs_paths_partition(bgpview_io_kafka_client_t *client, int partition)
{
  assert(client != NULL);

  client->kafka_config.pfxs_paths_partition = partition;

}

void bgpview_io_kafka_client_set_peers_partition(bgpview_io_kafka_client_t *client, int partition)
{
  assert(client != NULL);

  client->kafka_config.peers_partition = partition;

}

void bgpview_io_kafka_client_set_metadata_partition(bgpview_io_kafka_client_t *client, int partition)
{
  assert(client != NULL);

  client->kafka_config.metadata_partition =partition;

}


int bgpview_io_kafka_client_send_view(bgpview_io_kafka_client_t *client,
                                bgpview_t *view,
								kafka_performance_t *metrics,
                                bgpview_io_filter_cb_t *cb)
{

  if(bgpview_io_kafka_send(client->kafka_config,&client->view_data,view,metrics, cb) != 0)
    {
      goto err;
    }

  return 0;

 err:
  return -1;
}

int bgpview_io_kafka_client_recv_view(bgpview_io_kafka_client_t *client,
									bgpview_t *view,
									bgpview_io_filter_peer_cb_t *peer_cb,
									bgpview_io_filter_pfx_cb_t *pfx_cb,
									bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)

{
  uint8_t interests = 1;

  assert(view != NULL);

  if(bgpview_io_kafka_recv(&client->kafka_config,&client->view_data,view,
		  	  	  	  	  peer_cb,pfx_cb,pfx_peer_cb) != 0)
    {
      fprintf(stderr, "Failed to receive view\n");
      return -1;
    }

  return interests;
}
