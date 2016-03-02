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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <string.h>
#include <librdkafka/rdkafka.h>
#include <errno.h>
#include <time.h>

#include "utils.h"
#include "bgpview_io_kafka_peer.pb.h"
#include "bgpview_io_kafka_client.h"
#include "bgpview_consumer_interface.h"
#include "bvc_kafkasender.h"

#define NAME "kafka-sender"


/* macro to access the current consumer state */
#define STATE					\
  (BVC_GET_STATE(consumer, kafkasender))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))


/* our 'class' */
static bvc_t bvc_kafkasender = {
  BVC_ID_KAFKASENDER,
  NAME,
  BVC_GENERATE_PTRS(kafkasender)
};



/* our 'instance' */
typedef struct bvc_kafkasender_state {

  /* Historical View*/
	bgpview_t *viewH;
	uint32_t sync_view;
	kafka_sync_view_data_t sync_view_data;

} bvc_kafkasender_state_t;

/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s\n",
	  consumer->name);
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;

  assert(argc > 0 && argv != NULL);

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while((opt = getopt(argc, argv, ":?")) >= 0)
    {
      switch(opt)
	{
	case '?':
	case ':':
	default:
	  usage(consumer);
	  return -1;
	}
    }

  return 0;
}

/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_kafkasender_alloc()
{
  return &bvc_kafkasender;
}


int bvc_kafkasender_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_kafkasender_state_t *state = NULL;

  if((state = malloc_zero(sizeof(bvc_kafkasender_state_t))) == NULL)
    {
      return -1;
    }
  BVC_SET_STATE(consumer, state);


  /* allocate dynamic memory HERE */


  /* set defaults */

  state->viewH=NULL;


  /* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      goto err;
    }

  /* react to args HERE */


  return 0;

 err:
  return -1;
}


void bvc_kafkasender_destroy(bvc_t *consumer)
{
  bvc_kafkasender_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  /* deallocate dynamic memory HERE */

  free(state);

  BVC_SET_STATE(consumer, NULL);
}



int diff_paths(bgpview_t *viewH, bgpview_iter_t *itH,bgpview_iter_t *itC){


	bgpstream_as_path_store_t *store=bgpview_get_as_path_store(viewH);

	bgpstream_as_path_store_path_id_t idxH;
	bgpstream_as_path_store_path_id_t idxC;

	bgpstream_peer_sig_t *ps=bgpview_iter_peer_get_sig(itC);
	uint32_t peer_asn = ps->peer_asnumber;

	bgpstream_as_path_t *pathH=bgpview_iter_pfx_peer_get_as_path(itH);
	bgpstream_as_path_t *pathC=bgpview_iter_pfx_peer_get_as_path(itC);

	if(bgpstream_as_path_store_get_path_id(store,pathH,peer_asn,&idxH)!=0)
	{
      fprintf(stderr, "ERROR: Failed to get AS Path ID from store\n");
      return -1;
    }
	if(bgpstream_as_path_store_get_path_id(store,pathC,peer_asn,&idxC)!=0)
	{
      fprintf(stderr, "ERROR: Failed to get AS Path ID from store\n");
      return -1;
    }

	//printf("%d\n",(int)idxC.path_id);
	//TODO: check how to compare
	/*int tmp_idxH= 0;
	int tmp_idxC= 0;*/
	//printf("%d %d %d %d\n",(int) idxH.path_id,(int) idxC.path_id,(int) idxH.path_hash,(int) idxC.path_hash);
	if(idxH.path_id != idxC.path_id || idxH.path_hash != idxC.path_hash) return 1;

	return 0;
}


int diff_rows(bgpview_t *viewH, bgpview_iter_t *itH,bgpview_iter_t *itC){

	int npeersH = bgpview_iter_pfx_get_peer_cnt(itH,BGPVIEW_FIELD_ACTIVE);
	int npeersC = bgpview_iter_pfx_get_peer_cnt(itC,BGPVIEW_FIELD_ACTIVE);

	if(npeersH != npeersC) return 1;

	bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itC);
	bgpstream_peer_id_t peerid;
	for(bgpview_iter_pfx_first_peer(itC, BGPVIEW_FIELD_ACTIVE);
	          bgpview_iter_pfx_has_more_peer(itC);
	          bgpview_iter_pfx_next_peer(itC))
		{

		peerid=bgpview_iter_peer_get_peer_id(itC);
		if(bgpview_iter_seek_pfx_peer(itH,pfx,peerid,BGPVIEW_FIELD_ACTIVE,BGPVIEW_FIELD_ACTIVE)==1){
			if(diff_paths(viewH,itH,itC)==1)
				return 1;
	        }
		else
			return 1;
		}

	return 0;
}

int peer_diffs(bgpview_io_kafka_client_t *client, bgpview_iter_t *itH, bgpview_iter_t *itC, int viewid ,bvc_kafkasender_state_t *state, int peer_batch){

	void* peer_msgs[peer_batch];
	int peer_msgs_len[peer_batch];
	void *buf;                     // Buffer to store serialized data
	unsigned len=0;                  // Length of serialized data
	char *topic="peers";

	char message[256];

	unsigned int strlen= sprintf(message,"BEGIN DIFF PEERS VIEW %d WITH SYNC VIEW: %d",viewid,state->sync_view);
	message[strlen]='\0';
	bgpview_io_kafka_client_send_message_to_topic(client,"peers",message,strlen);

	int np=0;
	int total_np=0;
	for(bgpview_iter_pfx_first_peer(itC, BGPVIEW_FIELD_ACTIVE);
	          bgpview_iter_pfx_has_more_peer(itC);
	          bgpview_iter_pfx_next_peer(itC))
	        {
		//check if current peer id exist in old view
		bgpstream_peer_id_t peerid=bgpview_iter_peer_get_peer_id(itC);
		bgpstream_peer_sig_t * ps;
		if(bgpview_iter_seek_peer(itH,peerid,BGPVIEW_FIELD_ACTIVE)==0){
			ps = bgpview_iter_peer_get_sig(itC);
			assert(ps);

			Peer peer_msg = PEER__INIT;

			peer_msg.peerid_orig=bgpview_iter_peer_get_peer_id(itC);
			peer_msg.collector_str=ps->collector_str;
			peer_msg.peer_ip_addr.len=sizeof(bgpstream_addr_storage_t);
			peer_msg.peer_ip_addr.data=(void*)&(ps->peer_ip_addr);
			peer_msg.peer_asnumber=ps->peer_asnumber;

			len = peer__get_packed_size(&peer_msg);
			peer_msgs_len[np]=len;
			buf = malloc(len);
			peer__pack(&peer_msg,buf);

			peer_msgs[np++]=buf;

			if(np==peer_batch){
				bgpview_io_kafka_client_send_diffs(client,topic,peer_msgs,peer_msgs_len,np);
				np=0;
			}
			total_np++;
		}
	}
	bgpview_io_kafka_client_send_diffs(client,topic,peer_msgs,peer_msgs_len,np);

	strlen= sprintf(message,"END DIFF PEERS VIEW %d WITH SYNC VIEW: %d NEW PEERS %d",viewid,state->sync_view,total_np);
	message[strlen]='\0';
	bgpview_io_kafka_client_send_message_to_topic(client,"peers",message,strlen);
	//printf("Number of new peers: %d\n",total_np);

	return 0;
}

void pfxs_paths_diffs(bgpview_io_kafka_client_t *client, bgpview_t *viewH, bgpview_t *viewC, bgpview_iter_t *itH, bgpview_iter_t *itC,
		int viewid ,bvc_kafkasender_state_t *state, int pfx_path_batch){

	int npfxsH = bgpview_pfx_cnt(viewH,BGPVIEW_FIELD_ACTIVE);
	int npfxsC = bgpview_pfx_cnt(viewC,BGPVIEW_FIELD_ACTIVE);

	void* pfx_path_msgs[pfx_path_batch];
	int pfx_path_msgs_len[pfx_path_batch];
	int total_prefixes=0,len=0;                  // Length of serialized data

	char *topic="views";
	char message[256];

	unsigned int strlen= sprintf(message,"BEGIN DIFF VIEW %d WITH SYNC VIEW: %d",viewid,state->sync_view);
	message[strlen]='\0';
	bgpview_io_kafka_client_send_message_to_topic(client,"views",message,strlen);

	int common=0, change=0;
	int np=0;
	total_prefixes=0;


	for(bgpview_iter_first_pfx(itC, 0 , BGPVIEW_FIELD_ACTIVE);
	      bgpview_iter_has_more_pfx(itC);
	      bgpview_iter_next_pfx(itC))
	    {

		bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itC);

		if(bgpview_iter_seek_pfx(itH,pfx,BGPVIEW_FIELD_ACTIVE)==1){
			common++;
			if(diff_rows(viewH,itH,itC)==1){
				change++;
				pfx_path_msgs[np]=row_serialize('M',itC,&len);
				pfx_path_msgs_len[np++]=len;
				total_prefixes++;
			}
		}
		else{
			pfx_path_msgs[np]=row_serialize('A',itC,&len);
			pfx_path_msgs_len[np++]=len;
			total_prefixes++;
		}
		if(np==pfx_path_batch){
			bgpview_io_kafka_client_send_diffs(client,topic,pfx_path_msgs,pfx_path_msgs_len,np);
			np=0;
	    }
	    }

	int rm_pfxsH = npfxsH-common;

	printf("HC %d CC %d common %d remove %d change %d\n",npfxsH,npfxsC,common,rm_pfxsH,change);
	assert(rm_pfxsH>=0);

	int tmpn=0;
	if(rm_pfxsH > 0){
		for(bgpview_iter_first_pfx(itH, 0 , BGPVIEW_FIELD_ACTIVE);
			  bgpview_iter_has_more_pfx(itH);
			  bgpview_iter_next_pfx(itH))
			{
			bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itH);

			if(bgpview_iter_seek_pfx(itC,pfx,BGPVIEW_FIELD_ACTIVE)==0){
				pfx_path_msgs[np]=row_serialize('R',itH,&len);
				pfx_path_msgs_len[np++]=len;
				rm_pfxsH--;
				total_prefixes++;
			}
			if(np==pfx_path_batch){
				bgpview_io_kafka_client_send_diffs(client,topic,pfx_path_msgs,pfx_path_msgs_len,np);
				np=0;
		    }
			if(rm_pfxsH==0) break;
			tmpn++;
			}
	}
	bgpview_io_kafka_client_send_diffs(client,topic,pfx_path_msgs,pfx_path_msgs_len,np);

	strlen= sprintf(message,"END DIFF VIEW %d WITH SYNC VIEW: %d AND %d PFXS",viewid,state->sync_view,total_prefixes);
	message[strlen]='\0';
	bgpview_io_kafka_client_send_message_to_topic(client,"views",message,strlen);


}


int bgpview_view_diffs(bgpview_io_kafka_client_t *client, bgpview_t *viewH, bgpview_t *viewC, int viewid, bvc_kafkasender_state_t *state){


	bgpview_iter_t *itH, *itC;

	if(((itH = bgpview_iter_create(viewH)) == NULL) || ((itC = bgpview_iter_create(viewC)) == NULL) )
	{
	  return -1;
	}
	bgpview_kafka_client_publish_metadata(client,viewC,state->sync_view_data,"DIFF");

	peer_diffs(client, itH,itC,viewid,state,10);

	pfxs_paths_diffs(client,viewH,viewC,itH,itC,viewid,state,100);

	return 0;
}

int bvc_kafkasender_process_view(bvc_t *consumer, uint8_t interests, bgpview_t *viewC)
{
	bvc_kafkasender_state_t *state = STATE;


	int npfxsC=0,npfxsH = 0;
	int npH=0,npC=0;

	npfxsC = bgpview_pfx_cnt(viewC,BGPVIEW_FIELD_ACTIVE);
	npC = bgpview_peer_cnt(viewC,BGPVIEW_FIELD_ACTIVE);
	//printf("Next historical %d\n",npfxsC);


	if(state->viewH!=NULL){
		npfxsH = bgpview_pfx_cnt(state->viewH,BGPVIEW_FIELD_ACTIVE);
		npH = bgpview_peer_cnt(state->viewH,BGPVIEW_FIELD_ACTIVE);
	}

	bgpview_io_kafka_client_t *client=bgpview_io_kafka_client_init();
	bgpview_io_kafka_client_start_producer(client,"all");


	if(state->viewH==NULL){
		bgpview_view_set_sync_view_data(client,viewC,&state->sync_view_data);
		bgpview_io_kafka_client_send_view(client,viewC,NULL);
		state->sync_view=bgpview_get_time(viewC);
	}
	else{
		bgpview_view_diffs(client,state->viewH,viewC,bgpview_get_time(viewC),state);
		//printf("%d %d\n",(int)state->viewH,(int)viewC);
		bgpview_destroy(state->viewH);
	}

	/*printf("SYNC %d %d %d\n",
			state->sync_view_data.pfxs_paths_sync_partition,
			state->sync_view_data.pfxs_paths_sync_offset,
			state->sync_view_data.pfxs_paths_sync_view_id);*/
	state->viewH=bgpview_clone_view(viewC);

	return 0;
}
