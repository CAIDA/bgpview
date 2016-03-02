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

#define ERR (&client->err)


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
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Failed to duplicate kafka server uri string");
      goto err;
    }

  if((client->kafka_config.pfxs_paths_topic =
      strdup(BGPVIEW_IO_KAFKA_CLIENT_PFXS_PATHS_TOPIC_DEFAULT)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Failed to duplicate kafka prefixes paths topic string");
      goto err;
    }

  if((client->kafka_config.peers_topic =
      strdup(BGPVIEW_IO_KAFKA_CLIENT_PEERS_TOPIC_DEFAULT)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
    		  "Failed to duplicate kafka peers topic string");
      goto err;
    }

  if((client->kafka_config.metadata_topic =
      strdup(BGPVIEW_IO_KAFKA_CLIENT_METADATA_TOPIC_DEFAULT)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
    		  "Failed to duplicate kafka metadata topic string");
      goto err;
    }


  client->kafka_config.peers_partition=BGPVIEW_IO_KAFKA_CLIENT_PEERS_PARTITION_DEFAULT;
  client->kafka_config.metadata_partition=BGPVIEW_IO_KAFKA_CLIENT_METADATA_PARTITION_DEFAULT;

  client->kafka_config.peers_offset=BGPVIEW_IO_KAFKA_CLIENT_PEERS_OFFSET_DEFAULT;
  client->kafka_config.metadata_offset=BGPVIEW_IO_KAFKA_CLIENT_METADATA_OFFSET_DEFAULT;
  client->kafka_config.pfxs_paths_offset=0;
  client->kafka_config.pfxs_paths_partition=0;

  client->kafka_config.pfxs_paths_rk=NULL;
  client->kafka_config.peers_rk=NULL;
  client->kafka_config.metadata_rk=NULL;

  client->kafka_config.pfxs_paths_rkt=NULL;
  client->kafka_config.peers_rkt=NULL;
  client->kafka_config.metadata_rkt=NULL;

  client->kafka_config.pfxs_paths_conf=NULL;
  client->kafka_config.peers_conf=NULL;
  client->kafka_config.metadata_conf=NULL;

  client->kafka_config.pfxs_paths_topic_conf=NULL;
  client->kafka_config.peers_topic_conf=NULL;
  client->kafka_config.metadata_topic_conf=NULL;

  return client;

 err:
  if(client != NULL)
    {
      bgpview_io_kafka_client_free(client);
    }
  return NULL;
}

void bgpview_io_kafka_client_perr(bgpview_io_kafka_client_t *client)
{
  assert(client != NULL);
  bgpview_io_err_perr(ERR);
}


int bgpview_io_kafka_client_send_view(bgpview_io_kafka_client_t *client,
                                bgpview_t *view,
                                bgpview_io_filter_cb_t *cb)
{
  /* now just transmit the view */
  if(bgpview_io_kafka_send(client->kafka_config, view, cb) != 0)
    {
      goto err;
    }

  return 0;

 err:
  return -1;
}

int bgpview_io_kafka_client_recv_view(bgpview_io_kafka_client_t *client,
								bgpview_t *view,
								int interest_view,
								bgpview_io_filter_peer_cb_t *peer_cb,
                                bgpview_io_filter_pfx_cb_t *pfx_cb,
                                bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)

{
  uint8_t interests = 1;

  assert(view != NULL);

  if(bgpview_io_kafka_recv(client->kafka_config, view,interest_view,
                     peer_cb, pfx_cb, pfx_peer_cb) != 0)
    {
      fprintf(stderr, "Failed to receive view");
      return -1;
    }

  return interests;
}


int bgpview_io_kafka_client_start_producer(bgpview_io_kafka_client_t *client, char* topic)
{
	if(strcmp(topic,"all")==0){
		if((client->kafka_config.metadata_rkt =initialize_producer_connection(&client->kafka_config.metadata_rk,&client->kafka_config.metadata_conf,&client->kafka_config.metadata_topic_conf,
				client->kafka_config.brokers,client->kafka_config.metadata_topic,client->kafka_config.metadata_partition,client->kafka_config.metadata_offset))==NULL){
				fprintf(stderr,"error");
		}
		if((client->kafka_config.peers_rkt =initialize_producer_connection(&client->kafka_config.peers_rk,&client->kafka_config.peers_conf,&client->kafka_config.peers_topic_conf,
				client->kafka_config.brokers,client->kafka_config.peers_topic,client->kafka_config.peers_partition,client->kafka_config.peers_offset))==NULL){
				fprintf(stderr,"error");
		}
		if((client->kafka_config.pfxs_paths_rkt =initialize_producer_connection(&client->kafka_config.pfxs_paths_rk,&client->kafka_config.pfxs_paths_conf,&client->kafka_config.pfxs_paths_topic_conf,
				client->kafka_config.brokers,client->kafka_config.pfxs_paths_topic,client->kafka_config.pfxs_paths_partition,client->kafka_config.pfxs_paths_offset))==NULL){
				fprintf(stderr,"error");
		}
	}
	else if(strcmp(topic,"peers")==0){
		if((client->kafka_config.peers_rkt =initialize_producer_connection(&client->kafka_config.peers_rk,&client->kafka_config.peers_conf,&client->kafka_config.peers_topic_conf,
				client->kafka_config.brokers,client->kafka_config.peers_topic,client->kafka_config.peers_partition,client->kafka_config.peers_offset))==NULL){
				fprintf(stderr,"error");
		}
	}
	//get and set the offset
	else if(strcmp(topic,"pfxs_paths")==0){
		if((client->kafka_config.pfxs_paths_rkt =initialize_producer_connection(&client->kafka_config.pfxs_paths_rk,&client->kafka_config.pfxs_paths_conf,&client->kafka_config.pfxs_paths_topic_conf,
				client->kafka_config.brokers,client->kafka_config.pfxs_paths_topic,client->kafka_config.pfxs_paths_partition,client->kafka_config.pfxs_paths_offset))==NULL){
				fprintf(stderr,"error");
		}
	}
	else if(strcmp(topic,"metadata")==0){
		if((client->kafka_config.metadata_rkt =initialize_producer_connection(&client->kafka_config.metadata_rk,&client->kafka_config.metadata_conf,&client->kafka_config.metadata_topic_conf,
				client->kafka_config.brokers,client->kafka_config.metadata_topic,client->kafka_config.metadata_partition,client->kafka_config.metadata_offset))==NULL){
				fprintf(stderr,"error");
		}
	}
	else goto err;

	return 0;

	err:
	return -1;

}

int bgpview_io_kafka_client_start_consumer(bgpview_io_kafka_client_t *client, char* topic)
{
	if(strcmp(topic,"all")==0){
		if((client->kafka_config.metadata_rkt =initialize_consumer_connection(&client->kafka_config.metadata_rk,&client->kafka_config.metadata_conf,&client->kafka_config.metadata_topic_conf,
				client->kafka_config.brokers,client->kafka_config.metadata_topic,client->kafka_config.metadata_partition,client->kafka_config.metadata_offset))==NULL){
				fprintf(stderr,"error");
		}
	if((client->kafka_config.peers_rkt =initialize_consumer_connection(&client->kafka_config.peers_rk,&client->kafka_config.peers_conf,&client->kafka_config.peers_topic_conf,
			client->kafka_config.brokers,client->kafka_config.peers_topic,client->kafka_config.peers_partition,client->kafka_config.peers_offset))==NULL){
			fprintf(stderr,"error");
	}
	if((client->kafka_config.pfxs_paths_rkt =initialize_consumer_connection(&client->kafka_config.pfxs_paths_rk,&client->kafka_config.pfxs_paths_conf,&client->kafka_config.pfxs_paths_topic_conf,
			client->kafka_config.brokers,client->kafka_config.pfxs_paths_topic,client->kafka_config.pfxs_paths_partition,client->kafka_config.pfxs_paths_offset))==NULL){
			fprintf(stderr,"error");
	}
	}
	if(strcmp(topic,"peers")==0){
		if((client->kafka_config.peers_rkt =initialize_consumer_connection(&client->kafka_config.peers_rk,&client->kafka_config.peers_conf,&client->kafka_config.peers_topic_conf,
			client->kafka_config.brokers,client->kafka_config.peers_topic,client->kafka_config.peers_partition,client->kafka_config.peers_offset))==NULL){
			fprintf(stderr,"error");
		}
	}
	if(strcmp(topic,"pfxs_paths")==0){
		if((client->kafka_config.pfxs_paths_rkt =initialize_consumer_connection(&client->kafka_config.pfxs_paths_rk,&client->kafka_config.pfxs_paths_conf,&client->kafka_config.pfxs_paths_topic_conf,
			client->kafka_config.brokers,client->kafka_config.pfxs_paths_topic,client->kafka_config.pfxs_paths_partition,client->kafka_config.pfxs_paths_offset))==NULL){
			fprintf(stderr,"error");
		}
	}
	if(strcmp(topic,"metadata")==0){
		if((client->kafka_config.metadata_rkt =initialize_consumer_connection(&client->kafka_config.metadata_rk,&client->kafka_config.metadata_conf,&client->kafka_config.metadata_topic_conf,
			client->kafka_config.brokers,client->kafka_config.metadata_topic,client->kafka_config.metadata_partition,client->kafka_config.metadata_offset))==NULL){
			fprintf(stderr,"error");
		}
	}
	else goto err;

	return 0;

	err:
	return -1;
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

  client->kafka_config.pfxs_paths_conf=NULL;
  client->kafka_config.peers_conf=NULL;
  client->kafka_config.metadata_conf=NULL;

  client->kafka_config.pfxs_paths_topic_conf=NULL;
  client->kafka_config.peers_topic_conf=NULL;
  client->kafka_config.metadata_topic_conf=NULL;

  free(client);

  return;
}

int bgpview_io_kafka_client_set_server_uri(bgpview_io_kafka_client_t *client, const char *uri)
{
  assert(client != NULL);

  if(client->kafka_config.brokers != NULL)
	  free(client->kafka_config.brokers);

  if((client->kafka_config.brokers = strdup(uri)) == NULL)
    {
      bgpview_io_err_set_err(ERR, BGPVIEW_IO_ERR_MALLOC,
			     "Could not set server uri");
      return -1;
    }

  return 0;

}

int bgpview_io_kafka_client_send_diffs(bgpview_io_kafka_client_t *dest,char *topic, void* messages[], int messages_len[] ,int num_messages){

	return send_diffs(dest->kafka_config,topic,messages,messages_len,num_messages);
}

int bgpview_io_kafka_client_send_message_to_topic(bgpview_io_kafka_client_t *dest, char *topic, char* message, int len){
	return send_message_to_topic(dest->kafka_config,topic,message,len);

}

int bgpview_kafka_client_publish_metadata(bgpview_io_kafka_client_t *dest, bgpview_t *view, kafka_sync_view_data_t sync_view_data, char *type){
	return publish_metadata(dest->kafka_config, view,&sync_view_data,type);

}

int bgpview_view_set_sync_view_data(bgpview_io_kafka_client_t *dest,bgpview_t *view, kafka_sync_view_data_t *sync_view_data){

	return set_sync_view_data(dest->kafka_config, view,sync_view_data);

}


