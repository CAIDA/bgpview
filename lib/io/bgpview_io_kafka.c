/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Danilo Giordano, Alistair King, Chiara Orsini
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
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stddef.h>
#include "bgpview.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>
#include "librdkafka/rdkafka.h"
#include <errno.h>
#include "bgpview_io_kafka.h"
#include "bgpview_io_kafka_row.pb.h"
#include "bgpview_io_kafka_peer.pb.h"
#include <czmq.h> /* for zclock_time() */

#include <unistd.h>

#define BUFFER_LEN 16384
#define BUFFER_1M  1048576

/* because the values of AF_INET* vary from system to system we need to use
   our own encoding for the version */
#define BW_INTERNAL_AF_INET  4
#define BW_INTERNAL_AF_INET6 6

#define END_OF_PEERS 0xffff

/* ==========START KAFKA FUNCTIONS ========== */

int initialize_consumer_connection(rd_kafka_t **rk, rd_kafka_topic_t **rkt, char *brokers, char *topic){

  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;


  char errstr[512];

  // Kafka configuration
  conf = rd_kafka_conf_new();

  // Topic configuration
  topic_conf = rd_kafka_topic_conf_new();
  //printf("%s %s %d\n",brokers,topic,partition);

  // Create Kafka handle
  if (!(*rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,errstr, sizeof(errstr)))) {
	  fprintf(stderr,"%% Failed to create new consumer: %s\n",errstr);
	  goto err;
  }

  // Add brokers
  if (rd_kafka_brokers_add(*rk, brokers) == 0) {
	  fprintf(stderr, "%% No valid brokers specified\n");
	  goto err;
  }

  // Create topic
  *rkt = rd_kafka_topic_new(*rk, topic, topic_conf);

  // Start consuming
  if (rd_kafka_consume_start(*rkt, 0, RD_KAFKA_OFFSET_BEGINNING) == -1){
	  fprintf(stderr, "%% Failed to start consuming: %s\n",rd_kafka_err2str(rd_kafka_errno2err(errno)));
	  if (errno == EINVAL)
		  fprintf(stderr,"%% Broker based offset storage requires a group.id, add: -X group.id=yourGroup\n");
	  goto err;
  }

  return 1;

  err:

  return -1;
}

int initialize_producer_connection(rd_kafka_t **rk, rd_kafka_topic_t **rkt, char *brokers, char *topic){

  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;

  char errstr[512];

  conf = rd_kafka_conf_new();
  topic_conf = rd_kafka_topic_conf_new();


  if (rd_kafka_conf_set(conf, "queue.buffering.max.messages","7000000", errstr, sizeof(errstr)) !=RD_KAFKA_CONF_OK) {
	  fprintf(stderr, "%% %s\n", errstr);
	  goto err;
  }

  if (rd_kafka_conf_set(conf, "compression.codec", "snappy", errstr, sizeof(errstr)) !=RD_KAFKA_CONF_OK) {
 	fprintf(stderr, "%% %s\n", errstr);
 	goto err;
  }

  if (!(*rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,errstr, sizeof(errstr)))) {
 	 fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
 	 goto err;
  }

  if (rd_kafka_brokers_add(*rk, brokers) == 0) {
	 fprintf(stderr, "%% No valid brokers specified\n");
	 goto err;
  }

  *rkt = rd_kafka_topic_new(*rk, topic, topic_conf);

  return 1;

  err:

  return -1;

}

static int change_consumer_offset_partition(rd_kafka_topic_t *rkt, int partition, long int offset){

  int err;

	if ((err = rd_kafka_seek(rkt, partition, offset, 1000))){
		fprintf(stderr,"consume_seek(%s, %"PRId32", %"PRId64") "
			  "failed: %s\n",
			  rd_kafka_topic_name(rkt), partition, offset,
			  rd_kafka_err2str(err));
		return -1;

	}

  return 0;

}

/* ==========END KAFKA FUNCTIONS ========== */

/* ==========START SUPPORT FUNCTIONS ========== */

static long int get_offset(kafka_data_t dest,char *topic,int partition){


	rd_kafka_t *rk=NULL;
	rd_kafka_topic_t *rkt=NULL;
	rd_kafka_message_t *rkmessage;

	long int offset;

	char *brokers=dest.brokers;



	if(initialize_consumer_connection(&rk,&rkt,brokers,
											  topic)==-1){
		 fprintf(stderr,"Error initializing consumer connection");
			goto err;
	}

	if(change_consumer_offset_partition(rkt,partition,RD_KAFKA_OFFSET_END)!=0){
	  fprintf(stderr,"Error changing the offset");
	  goto err;
	}

	rkmessage = rd_kafka_consume(rkt, partition, 1000);
	if(rkmessage == NULL){
		  rd_kafka_topic_destroy(rkt);
		  rd_kafka_destroy(rk);
		  return 0;
	}

	offset=rkmessage->offset;

	rd_kafka_message_destroy(rkmessage);

	//printf("offset %ld\n",offset);
	// Destroy topic
	rd_kafka_topic_destroy(rkt);

	// Destroy the handle
	rd_kafka_destroy(rk);


	return offset;

	err:
	rd_kafka_topic_destroy(rkt);

	// Destroy the handle
	rd_kafka_destroy(rk);

	return -1;
}

static int num_elements(char* real_string, int position){

	char* token;
	int j=0;

	char *string=strdup(real_string);
	if (string != NULL) {
	  while ((token = strsep(&string, " ")) != NULL)
	  {
		  if(j==position){
			return atoi(token);
		  }
			j++;
	  }

	}
	free(string);
	return -1;
}

static void prefix_allocation(BGPRow *row, bgpview_iter_t *it){

	bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(it);
	assert(pfx != NULL);

	bgpstream_ip_addr_t addr = pfx->address;

    int memory=0;
    if(addr.version == BGPSTREAM_ADDR_VERSION_IPV4) memory=sizeof(bgpstream_ipv4_pfx_t);
    else if(addr.version == BGPSTREAM_ADDR_VERSION_IPV6) memory=sizeof(bgpstream_ipv6_pfx_t);
    else memory=sizeof(bgpstream_pfx_storage_t);

    row->pfx.len=memory;
    row->pfx.data=(uint8_t *)pfx;
}

static long int set_current_offsets(kafka_data_t dest, kafka_view_data_t *view_data){

	if((view_data->current_pfxs_paths_offset=get_offset(dest,dest.pfxs_paths_topic,dest.pfxs_paths_partition))==-1){
		goto err;
	}
	if((view_data->current_peers_offset=get_offset(dest,dest.peers_topic,dest.peers_partition))==-1){
		goto err;
	}
	return 0;

	err:

	return -1;
}

static long int set_sync_view_data(kafka_data_t dest,bgpview_t *view, kafka_view_data_t *view_data){



	view_data->pfxs_paths_sync_partition=dest.pfxs_paths_partition;
	view_data->sync_view_id=bgpview_get_time(view);

	if((view_data->pfxs_paths_sync_offset=get_offset(dest,dest.pfxs_paths_topic,dest.pfxs_paths_partition))==-1){
		goto err;
	}

	return 0;

	err:
	return -1;
}

static void *row_serialize(char* operation,bgpview_iter_t *it, unsigned int *len){

	BGPCell **cells;
	uint8_t *path_data;
	uint16_t ndata;
	void *buf;
	int size;
	int peers_cnt = bgpview_iter_pfx_get_peer_cnt(it, BGPVIEW_FIELD_ACTIVE);

	BGPRow row = BGPROW__INIT;
	prefix_allocation(&row,it);

	row.operation=operation;

	if(strcmp(operation,"R")==0){
		row.n_cells=0;
		row.cells=NULL;
		*len = bgprow__get_packed_size(&row);
		buf = malloc(*len);
		bgprow__pack(&row,buf);
	}

	else{
		row.n_cells=peers_cnt;

		cells =(BGPCell**)malloc(sizeof(BGPCell*)*peers_cnt);

		bgpstream_as_path_t *paths[1024];
		bgpstream_peer_id_t peerid;
		bgpstream_as_path_t *path;

		int i=0,j;
		for(bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
		  bgpview_iter_pfx_has_more_peer(it);
		  bgpview_iter_pfx_next_peer(it))
		{

		  peerid=bgpview_iter_peer_get_peer_id(it);

		  size = sizeof(bgpstream_peer_id_t);
		  cells[i] = malloc (sizeof (BGPCell));
		  bgpcell__init(cells[i]);
		  cells[i]->peerid=peerid;

		  path = bgpview_iter_pfx_peer_get_as_path(it);
		  assert(path != NULL);

		  ndata = bgpstream_as_path_get_data(path,&path_data);

		  paths[i]=path;
		  cells[i]->aspath.len=ndata;
		  cells[i]->aspath.data=path_data;

		  i++;
		}


		row.cells=cells;

	*len = bgprow__get_packed_size(&row);

	buf = malloc(*len);
	bgprow__pack(&row,buf);

	//SAFE???
	  for(j=0;j<peers_cnt;j++){
		  bgpstream_as_path_destroy(paths[j]); //free all created paths
		  free(cells[j]); //Free all cells
	  }
	  free(cells);
	}


	return buf;

}

static int diff_paths(bgpview_t *viewH, bgpview_iter_t *itH,bgpview_iter_t *itC){



	bgpstream_as_path_store_path_id_t idxH = bgpview_iter_pfx_peer_get_as_path_id(itH);
	bgpstream_as_path_store_path_id_t idxC = bgpview_iter_pfx_peer_get_as_path_id(itC);

	/*
	 * bgpstream_as_path_store_t *store=bgpview_get_as_path_store(viewH);
	 * bgpstream_peer_sig_t *ps=bgpview_iter_peer_get_sig(itC);
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
    }*/


	if(idxH.path_id != idxC.path_id || idxH.path_hash != idxC.path_hash) return 1;

	return 0;
}

static int diff_rows(bgpview_t *viewH, bgpview_iter_t *itH,bgpview_iter_t *itC){

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


/* ==========END SUPPORT FUNCTIONS ========== */

/* ==========START SEND/RECEIVE FUNCTIONS ========== */

static int send_metadata(kafka_data_t dest, bgpview_t *view, kafka_view_data_t *view_data, char *type){

	rd_kafka_t *rk=dest.metadata_rk;
	rd_kafka_topic_t *rkt=dest.metadata_rkt;

  char offset_message[256];
  int nc;

  int publish_partition = dest.pfxs_paths_partition;


  long int peers_offset=view_data->current_peers_offset;
  long int pfxs_paths_offset=view_data->current_pfxs_paths_offset;



  if(strcmp(type,"SYNC")==0)
	  nc= sprintf(offset_message, "SYNC VIEW: %"PRIu32" PUBLISHED IN PARTITION: %d AT OFFSET: %ld. PEERS AT OFFSET, %ld",
		  bgpview_get_time(view), publish_partition,pfxs_paths_offset,peers_offset);
  else if(strcmp(type,"DIFF")==0){

	  int view_sync_id = view_data->sync_view_id;
	  int view_sync_partition=view_data->pfxs_paths_sync_partition;
	  long int view_sync_offset=view_data->pfxs_paths_sync_offset;

	  nc= sprintf(offset_message, "DIFF VIEW: %"PRIu32" PUBLISHED IN PARTITION: %d AT OFFSET: %ld WITH SYNC VIEW: %d IN PARTITION %d AT OFFSET: %ld. PEERS AT OFFSET, %ld",
		  bgpview_get_time(view),
		  publish_partition,
		  pfxs_paths_offset,
		  view_sync_id,
		  view_sync_partition,
		  view_sync_offset,
		  peers_offset);
  }

	  else
	  goto err;

  if(nc>0) offset_message[nc]='\0';


	if(rd_kafka_produce(rkt, dest.metadata_partition,RD_KAFKA_MSG_F_COPY,offset_message,strlen(offset_message),NULL, 0,NULL) == -1) {
		fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
			rd_kafka_topic_name(rkt), dest.metadata_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		//Poll to handle delivery reports
		rd_kafka_poll(rk, 0);
		goto err;
	}

	/* Wait for messages to be delivered */
	while (rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 100);


	return 0;

	err:
	return -1;

}

static int recv_metadata(kafka_data_t *src, kafka_view_data_t *kafka_data){

  rd_kafka_message_t *rkmessage=NULL;

  int i,nf=0;
  long int offset;
  uint32_t view_id;

  offset=get_offset(*src,src->metadata_topic,src->metadata_partition);


  long int history_offset=offset-src->view_frequency-1;

  if(history_offset<0)history_offset=0;

  if(src->metadata_offset!=0) history_offset=src->metadata_offset;

  if(change_consumer_offset_partition(src->metadata_rkt,src->metadata_partition,history_offset)!=0){
	  fprintf(stderr,"Error changing the offset");
	  goto err;
  }

  for(i=0; i<src->view_frequency+2; i++){
	  rkmessage = rd_kafka_consume(src->metadata_rkt, src->metadata_partition, 2000000000);

	  if(rkmessage!=NULL){
		  if(rkmessage->payload == NULL)
		{
		  if(rkmessage->err!=RD_KAFKA_RESP_ERR__PARTITION_EOF)
				  {
				  fprintf(stderr, "Cannot not receive metadata\n");
				  goto err;
				  }
		  if(i==0)
			  rkmessage = rd_kafka_consume(src->metadata_rkt, src->metadata_partition, 2000000000);
		  else
			  break;
		}
		  src->metadata_offset++;
		  view_id=num_elements(rkmessage->payload,2);
		  if(strstr(rkmessage->payload,"DIFF") == NULL){
			  nf=0;
			  kafka_data->pfxs_paths_sync_partition=num_elements(rkmessage->payload,6);
			  kafka_data->pfxs_paths_sync_offset=num_elements(rkmessage->payload,9);
			  kafka_data->sync_view_id=num_elements(rkmessage->payload,2);
			  kafka_data->peers_sync_offset=num_elements(rkmessage->payload,12);
			  kafka_data->num_diffs=0;
		  }
		  else{
			  //metadata->pfxs_paths_sync_partition=num_elements(rkmessage->payload,16);
			  //metadata->pfxs_paths_sync_offset=num_elements(rkmessage->payload,19);
			  //metadata->sync_view_id=num_elements(rkmessage->payload,13);

			  kafka_data->peers_offset[nf]=num_elements(rkmessage->payload,23);

			  kafka_data->pfxs_paths_diffs_partition[nf]=num_elements(rkmessage->payload,6);
			  kafka_data->pfxs_paths_diffs_offset[nf]=num_elements(rkmessage->payload,9);
			  nf++;
			  kafka_data->num_diffs=nf;

		  }
		   rd_kafka_message_destroy (rkmessage);
		 }
		  else{
			  return -1;
		  }
  	  }

   return 0;

   err:

   return -1;

}

static int send_peers(kafka_data_t dest, bgpview_iter_t *it,bgpview_t *view,
					bgpview_io_filter_cb_t *cb)
{


  time_t rawtime;
  struct tm * timeinfo;
  time ( &rawtime );
  timeinfo = localtime ( &rawtime );

  rd_kafka_topic_t *rkt=dest.peers_rkt;
  rd_kafka_t *rk = dest.peers_rk;

  char begin_message[256];

  //protobuf
  void *buf;
  unsigned len=0;
  int peers_tx=0;
  int filter;

  //SEND BEGIN MESSAGE
  int np= bgpview_peer_cnt(view, BGPVIEW_FIELD_ACTIVE);
  int nc = sprintf(begin_message, "BEGIN - PEER: %d",np);
  if(nc>0) begin_message[nc]='\0';
  if(rd_kafka_produce(rkt, dest.peers_partition,RD_KAFKA_MSG_F_COPY,begin_message, strlen(begin_message),NULL, 0,NULL) == -1) {
	fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
		rd_kafka_topic_name(rkt), dest.peers_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		rd_kafka_poll(rk, 0); //Poll to handle delivery reports
  }

  bgpstream_peer_sig_t *ps;
  for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
		  bgpview_iter_has_more_peer(it);
		  bgpview_iter_next_peer(it))
	{
	  if(cb != NULL)
			  {
				/* ask the caller if they want this peer */
		  	  if((filter = cb(it, BGPVIEW_IO_FILTER_PEER)) < 0)
				  {
					goto err;
				  }
				if(filter == 0)
				  {
					continue;
				  }
			  }
        /* past here means this peer is being sent */
		peers_tx++;

		ps = bgpview_iter_peer_get_sig(it);
		assert(ps);

		Peer peer_msg = PEER__INIT;
		peer_msg.peerid_orig=bgpview_iter_peer_get_peer_id(it);
		peer_msg.collector_str=ps->collector_str;
		peer_msg.peer_ip_addr.len=sizeof(bgpstream_addr_storage_t);
		peer_msg.peer_ip_addr.data=(void*)&(ps->peer_ip_addr);
		peer_msg.peer_asnumber=ps->peer_asnumber;

		len = peer__get_packed_size(&peer_msg);
		buf = malloc(len);
		peer__pack(&peer_msg,buf);

		if(rd_kafka_produce(rkt, dest.peers_partition,RD_KAFKA_MSG_F_FREE,buf, len,NULL, 0,NULL) == -1) {
			fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
				rd_kafka_topic_name(rkt), dest.peers_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
			rd_kafka_poll(rk, 0); //Poll to handle delivery reports
			goto err;
		}
		//free(buf); // free the allocated serialized buffer

	}

  assert(peers_tx <= UINT16_MAX);
  nc = sprintf(begin_message, "END PEER: %d",peers_tx);
  if(nc>0) begin_message[nc]='\0';
  if(rd_kafka_produce(rkt, dest.peers_partition,RD_KAFKA_MSG_F_COPY,begin_message, strlen(begin_message),NULL, 0,NULL) == -1) {
	fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
		rd_kafka_topic_name(rkt),dest.peers_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
	rd_kafka_poll(rk, 0);
  }

  while (rd_kafka_outq_len(rk) > 0)
	rd_kafka_poll(rk, 100);

  //printf ( "End writing peers in partition 0. Current local time and date: %s", asctime (timeinfo));

  return 0;
 err:
  return -1;

}

static int send_peer_diffs(kafka_data_t dest, bgpview_iter_t *itH, bgpview_iter_t *itC, uint32_t view_id,uint32_t sync_view_id){

	void *buf;
	unsigned int len=0;


	rd_kafka_t *rk=dest.peers_rk;
	rd_kafka_topic_t *rkt=dest.peers_rkt;
	int partition=dest.peers_partition;

	int total_np=0;

	char message[256];

	len= sprintf(message,"BEGIN DIFF PEERS VIEW %d WITH SYNC VIEW: %d",view_id,sync_view_id);
	message[len]='\0';

	if(rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_COPY,message, strlen(message),NULL, 0,NULL) == -1) {
	fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
		rd_kafka_topic_name(rkt), partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		rd_kafka_poll(rk, 0); //Poll to handle delivery reports
	}

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
			buf = malloc(len);
			peer__pack(&peer_msg,buf);

			if(rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_FREE,buf, len,NULL, 0,NULL) == -1) {
			fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
				rd_kafka_topic_name(rkt), partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
				rd_kafka_poll(rk, 0); //Poll to handle delivery reports
			}
			//free(buf);

			total_np++;
		}
	}

	len= sprintf(message,"END DIFF PEERS VIEW %d WITH SYNC VIEW: %d NEW PEERS %d",view_id,sync_view_id,total_np);
	message[len]='\0';

	if(rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_COPY,message, strlen(message),NULL, 0,NULL) == -1) {
	fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
		rd_kafka_topic_name(rkt), partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		rd_kafka_poll(rk, 0); //Poll to handle delivery reports
	}

	return 0;
}

static int recv_peers(kafka_data_t src, bgpview_iter_t *iter,
							bgpview_io_filter_peer_cb_t *peer_cb,
							bgpstream_peer_id_t *peerid_mapping,
							long int offset)
{

  bgpstream_peer_id_t peerid_orig;
  bgpstream_peer_id_t peerid_new;


  bgpstream_peer_sig_t ps;

  int i;
  int pc=-1;
  int peers_rx = 0;
  int filter;


  rd_kafka_topic_t *rkt=src.peers_rkt;


	if(change_consumer_offset_partition(rkt,src.peers_partition,offset)!=0){
	  fprintf(stderr,"Error changing the offset");
	  goto err;
	}


  Peer *peer_msg;
  for(i=0; i<UINT16_MAX; i++)
    {
	  rd_kafka_message_t *rkmessage;

      /* peerid (or end-of-peers)*/
	  rkmessage = rd_kafka_consume(rkt, src.peers_partition, 1000);
      if(rkmessage->payload == NULL)
	{
          /* end of peers */
    	  if(rkmessage->err!=RD_KAFKA_RESP_ERR__PARTITION_EOF)
    	          {
    	      	  fprintf(stderr, "Cannot not receive peer id\n");
    	      	  goto err;
    	          }
          break;
	}
      peer_msg=NULL;

      peer_msg = peer__unpack(NULL,rkmessage->len,rkmessage->payload); // Deserialize the serialized Peer


      if(peer_msg!=NULL){

		  peerid_orig = (bgpstream_peer_id_t)peer_msg->peerid_orig;

		  assert(peerid_orig <  2048);

		  if(peerid_mapping[peerid_orig] !=0){

			  if(iter == NULL)
				{
				  continue;
				}
			  /*ps="";
				  if(peer_cb != NULL)
						  {
							// ask the caller if they want this peer
							if((filter = peer_cb(&ps)) < 0)
							  {
								goto err;
							  }
							if(filter == 0)
							  {
								continue;
							  }
						  }*/
		  }

		  else{
			  /* collector name */
			  strcpy(ps.collector_str,peer_msg->collector_str);

			  /* peer ip */
			  memcpy(&ps.peer_ip_addr,peer_msg->peer_ip_addr.data,peer_msg->peer_ip_addr.len);

			  /* peer asn */
			  ps.peer_asnumber = peer_msg->peer_asnumber;

			  if(iter == NULL)
				{
				  continue;
				}

			  if(peer_cb != NULL)
					  {
						/* ask the caller if they want this peer */
						if((filter = peer_cb(&ps)) < 0)
						  {
							goto err;
						  }
						if(filter == 0)
						  {
							continue;
						  }
					  }
			  /* all code below here has a valid view */


			  /* now ask the view to add this peer */
			  peerid_new = bgpview_iter_add_peer(iter,
												 ps.collector_str,
												 (bgpstream_ip_addr_t*)&ps.peer_ip_addr,
												 ps.peer_asnumber);
			  bgpview_iter_activate_peer(iter);
			  assert(peerid_new != 0);
			  peerid_mapping[peerid_orig] = peerid_new;
			  peer__free_unpacked(peer_msg, NULL);

			  peers_rx++;

		  }
      }
      else{ //BEGIN || END MESSAGE
    	  if (strstr(rkmessage->payload, "END") != NULL)
    		  break;
    	  if (strstr(rkmessage->payload, "BEGIN") != NULL){
    		  	  if(pc == -1)
    		  	  {
    		  		  pc=num_elements(rkmessage->payload,3);
    		  	  }
    		  	  else{
    				  fprintf(stderr,"Can not read all peers\n");
    				  goto err;
    		  	  }
    	  }
		  if(pc==-1){
			  fprintf(stderr,"No number of peers\n");
			  goto err;
		  }
      }
      rd_kafka_message_destroy (rkmessage);
    }

  assert(pc == peers_rx);


  return 0;

 err:

 return -1;
}

static int send_pfxs_paths(kafka_data_t dest, kafka_performance_t *metrics, bgpview_iter_t *it, bgpview_io_filter_cb_t *cb){

	  time_t rawtime;
	  struct tm * timeinfo;
	  time ( &rawtime );
	  timeinfo = localtime ( &rawtime );


	  rd_kafka_topic_t *rkt=dest.pfxs_paths_rkt;
	  rd_kafka_t *rk = dest.pfxs_paths_rk;

	  int filter,peers_cnt,paths_tx=0,npfx=0;
	  void *buf;
	  unsigned int len=0;

	  char begin_message[256];

	  bgpview_t *view = bgpview_iter_get_view(it);
	  assert(view != NULL);

	  int nc = sprintf(begin_message, "BEGIN - VIEW: %"PRIu32" WITH %d PREFIXES", bgpview_get_time(view),bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE));
	  if(nc>0) begin_message[nc]='\0';
	  while(rd_kafka_produce(rkt, dest.pfxs_paths_partition,RD_KAFKA_MSG_F_COPY,begin_message, strlen(begin_message),NULL, 0,NULL) == -1) {
		fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
			rd_kafka_topic_name(rkt), dest.pfxs_paths_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
			rd_kafka_poll(rk, 0); //Poll to handle delivery reports
	  }

	  int string_size=0;

	  //BGPCell **cells;
	  //uint8_t *path_data;
	  //uint16_t ndata;

	  for(bgpview_iter_first_pfx(it, 0, BGPVIEW_FIELD_ACTIVE);
	      bgpview_iter_has_more_pfx(it);
	      bgpview_iter_next_pfx(it))
	    {
		  string_size=0;
	      if(cb != NULL)
	        {
	          if((filter = cb(it, BGPVIEW_IO_FILTER_PFX)) < 0)
	            {
	              goto err;
	            }
	          if(filter == 0)
	            {
	              continue;
	            }
	        }

		  peers_cnt = bgpview_iter_pfx_get_peer_cnt(it, BGPVIEW_FIELD_ACTIVE);

	      if(peers_cnt == 0)
	        {
	          continue;
	        }

	      buf=row_serialize("A",it,&len);

		  if(rd_kafka_produce(rkt, dest.pfxs_paths_partition,RD_KAFKA_MSG_F_FREE,buf, len,NULL, 0,NULL) == -1) {
			fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
				rd_kafka_topic_name(rkt), dest.pfxs_paths_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
			//Poll to handle delivery reports
			rd_kafka_poll(rk, 0);
		  }
		rd_kafka_poll(rk, 0);

		  //free(buf); // free the allocated serialized buffer
		  npfx++;
		}

	  assert(paths_tx <= UINT32_MAX);

		metrics->add=0;
		metrics->remove=0;
		metrics->change=0;
		metrics->common=0;
		metrics->historical_pfx_cnt=0;
		metrics->current_pfx_cnt=bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE);
		metrics->sync_cnt		=bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE);


	  nc = sprintf(begin_message, "END - VIEW: %"PRIu32" WITH %d PREFIXES AND %d PATHS", bgpview_get_time(view),bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE),paths_tx);
	  if(nc>0) begin_message[nc]='\0';

	  if(rd_kafka_produce(rkt, dest.pfxs_paths_partition,RD_KAFKA_MSG_F_COPY,begin_message, strlen(begin_message),NULL, 0,NULL) == -1) {
		fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
			rd_kafka_topic_name(rkt), dest.pfxs_paths_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		//Poll to handle delivery reports
		rd_kafka_poll(rk, 0);
	  }

	  while (rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 100);


	time ( &rawtime );
	timeinfo = localtime ( &rawtime );

	return 0;

	err:
	return -1;
}

static int send_pfxs_paths_diffs(kafka_data_t dest, kafka_performance_t *metrics, bgpview_t *viewH, bgpview_t *viewC, bgpview_iter_t *itH, bgpview_iter_t *itC, uint32_t view_id,uint32_t sync_view_id){


	int npfxsH = bgpview_pfx_cnt(viewH,BGPVIEW_FIELD_ACTIVE);
	int npfxsC = bgpview_pfx_cnt(viewC,BGPVIEW_FIELD_ACTIVE);

	int total_prefixes=0;
	unsigned int len=0;
	void *buf;

	rd_kafka_t *rk=dest.pfxs_paths_rk;
	rd_kafka_topic_t *rkt=dest.pfxs_paths_rkt;
	int partition=dest.pfxs_paths_partition;


	char message[256];

	len= sprintf(message,"BEGIN DIFF VIEW %d WITH SYNC VIEW: %d",view_id,sync_view_id);
	message[len]='\0';

	if(rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_COPY,message, strlen(message),NULL, 0,NULL) == -1) {
	fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
		rd_kafka_topic_name(rkt), partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		rd_kafka_poll(rk, 0);
	}
	int common=0, change=0;
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

					buf=row_serialize("M",itC,&len);
					total_prefixes++;

					if(rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_FREE,buf, len,NULL, 0,NULL) == -1) {
					fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
						rd_kafka_topic_name(rkt), partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
						rd_kafka_poll(rk, 0);
					}
					//free(buf);
				}
			}
			else{
				buf=row_serialize("A",itC,&len);
				total_prefixes++;
				if(rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_FREE,buf, len,NULL, 0,NULL) == -1) {
				fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
					rd_kafka_topic_name(rkt), partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
					rd_kafka_poll(rk, 0);
				}
				//free(buf);
			}
	    }

	int rm_pfxsH = npfxsH-common;

	fprintf(stderr,"HC %d CC %d common %d remove %d change %d\n",npfxsH,npfxsC,common,rm_pfxsH,change);
	assert(rm_pfxsH>=0);

	metrics->add=(npfxsC-common)+rm_pfxsH;
	if(metrics->add<0)metrics->add=0;
	metrics->remove=rm_pfxsH;
	metrics->change=change;
	metrics->common=common;

	metrics->current_pfx_cnt=metrics->common+metrics->add;
	metrics->historical_pfx_cnt=metrics->common+metrics->remove;
	metrics->sync_cnt=0;

	int tmpn=0;
	if(rm_pfxsH > 0){
		for(bgpview_iter_first_pfx(itH, 0 , BGPVIEW_FIELD_ACTIVE);
			  bgpview_iter_has_more_pfx(itH);
			  bgpview_iter_next_pfx(itH))
			{
			bgpstream_pfx_t *pfx = bgpview_iter_pfx_get_pfx(itH);

			if(bgpview_iter_seek_pfx(itC,pfx,BGPVIEW_FIELD_ACTIVE)==0){
				buf=row_serialize("R",itH,&len);
				rm_pfxsH--;
				total_prefixes++;
				if(rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_FREE,buf, len,NULL, 0,NULL) == -1) {
				fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
					rd_kafka_topic_name(rkt), partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
					rd_kafka_poll(rk, 0);
				}
				//free(buf);
			}
			if(rm_pfxsH==0) break;
			tmpn++;
			}
	}

	len= sprintf(message,"END DIFF VIEW %d WITH SYNC VIEW: %d AND %d PFXS",view_id,sync_view_id,total_prefixes);
	message[len]='\0';
	if(rd_kafka_produce(rkt, partition,RD_KAFKA_MSG_F_COPY,message, strlen(message),NULL, 0,NULL) == -1) {
	fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
		rd_kafka_topic_name(rkt), partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		rd_kafka_poll(rk, 0);
	}

	return 0;
}

static int recv_pfxs_paths(kafka_data_t src, bgpview_iter_t *iter,
							bgpview_io_filter_pfx_cb_t *pfx_cb,
		                    bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
							bgpstream_peer_id_t *peerid_map,int partition, long int offset){


	rd_kafka_topic_t *rkt=src.pfxs_paths_rkt;

	if(change_consumer_offset_partition(rkt,partition,offset)!=0){
	  fprintf(stderr,"Error changing the offset");
	  goto err;
	}


  bgpview_t *view = NULL;

  int i,j;
  int pfx_cnt=0;
  int pfx_rx = 0;

  if(iter != NULL)
    {
      view = bgpview_iter_get_view(iter);
    }



  int recv=0;
  int filter;

  int tor=0;
  int toa=0;
  int tom=0;



  rd_kafka_message_t *rkmessage;

  /* foreach BGPROW:*/
  int n_paths=0;
  for(i=0; i<UINT32_MAX; i++){

	  rkmessage = rd_kafka_consume(rkt, src.pfxs_paths_partition, 1000);


      if(rkmessage->payload == NULL)
      {
    	  if(rkmessage->err!=RD_KAFKA_RESP_ERR__PARTITION_EOF)
    	          {
    	      	  fprintf(stderr, "Cannot receive prefixes and paths\n");
    	      	  goto err;
    	          }
          break;
	}

      BGPRow *row;
      row = bgprow__unpack(NULL,rkmessage->len,rkmessage->payload);


      if(row!=NULL){
    	  pfx_rx++;
    	  bgpstream_pfx_t *pfx = (bgpstream_pfx_t *)row->pfx.data;

          if(pfx_cb != NULL)
            {
              if((filter = pfx_cb((bgpstream_pfx_t*)&pfx)) < 0)
                {
                  goto err;
                }
              if(filter == 0)
                {
                  continue;
                }
            }

          if(strcmp(row->operation,"R")==0 || strcmp(row->operation,"M")==0){
        	  if(strcmp(row->operation,"R")==0)tor++;
        	  else tom++;

        	  if((recv = bgpview_iter_seek_pfx(iter,pfx,BGPVIEW_FIELD_ACTIVE))== 0){
			    	  fprintf(stderr, "Fail to find pfx\n");
			    	  goto err;
				  }

        	  if((recv = bgpview_iter_deactivate_pfx(iter))!= 1){
		    	  fprintf(stderr, "Fail to deactivate pfx\n");
		    	  goto err;
			  }
          }

          if(strcmp(row->operation,"A")==0 || strcmp(row->operation,"M")==0){
        	  if(strcmp(row->operation,"A")==0)toa++;
			  for(j=0;j<row->n_cells;j++){
				  n_paths++;
				  bgpstream_peer_id_t old_peerid=row->cells[j]->peerid;
				  bgpstream_peer_id_t peerid=peerid_map[old_peerid];

				  assert(peerid < 2048);

				  bgpstream_as_path_t *tmp_path = bgpstream_as_path_create();
				  bgpstream_as_path_populate_from_data(tmp_path,row->cells[j]->aspath.data, row->cells[j]->aspath.len);

				  if(iter != NULL)
					  if((recv = bgpview_iter_seek_peer(iter,peerid,BGPVIEW_FIELD_ALL_VALID))== 0){
						  fprintf(stderr, "Peer not existing\n");
						  goto err;
					  }
				  if(pfx_peer_cb != NULL)
					{
					  //TODO: how to check pfx peer as it does not exist now?
					  /*
					   *store_path =
					bgpstream_as_path_store_get_store_path(store,
														   pathid_map[pathidx]);
				  // ask the caller if they want this pfx-peer
				  if((filter = pfx_peer_cb(store_path)) < 0)
					{
					  goto err;
					}
				  if(filter == 0)
					{
					  continue;
					}
					   *
					   */
					//TODO
					}

					  if((recv = bgpview_iter_add_pfx_peer(iter,pfx,peerid,tmp_path))!= 0){
						  fprintf(stderr, "Fail to insert pfx and peer\n");
						  goto err;
					  }
					  bgpview_iter_pfx_activate_peer(iter);
				  bgpstream_as_path_destroy(tmp_path);


			  }
          }
          bgprow__free_unpacked(row, NULL);
      }
      else{
    	  if (strstr(rkmessage->payload, "END") != NULL){
        	  if (strstr(rkmessage->payload, "DIFF") == NULL){
        		  pfx_cnt=num_elements(rkmessage->payload,5);
        	  	  uint32_t view_id=num_elements(rkmessage->payload,3);
    			  if(iter != NULL)
    				  bgpview_set_time(view, view_id);
            	  }
			  else{
        		  pfx_cnt=num_elements(rkmessage->payload,9);
        	  	  uint32_t view_id=num_elements(rkmessage->payload,3);
    			  if(iter != NULL)
    				  bgpview_set_time(view, view_id);
			  }
    		  break;

    	  }
    	  if (strstr(rkmessage->payload, "BEGIN") != NULL)


    	  if(pfx_cnt==-1){
			  fprintf(stderr,"No number of pfxs\n");
			  goto err;
		  }
      }


      rd_kafka_message_destroy (rkmessage);
    }

  fprintf(stderr,"to AMR: %d %d %d\n",toa,tom,tor);

  assert(pfx_rx == pfx_cnt);

  return 0;

  err:

  return -1;

}

static int send_sync_view(kafka_data_t dest, kafka_view_data_t *view_data, kafka_performance_t *metrics, bgpview_t *view,
		bgpview_io_filter_cb_t *cb){

	fprintf(stderr,"Send Sync\n");
	bgpview_iter_t *it = NULL;

	if((it = bgpview_iter_create(view)) == NULL)
	{
	goto err;
	}


	set_sync_view_data(dest,view,view_data);

	set_current_offsets(dest,view_data);

	if(send_peers(dest,it,view,cb)!=0){
	goto err;
	}
	if(send_pfxs_paths(dest,metrics,it,cb)!=0){
	goto err;
	}
	if(send_metadata(dest,
		view,
		view_data,
		"SYNC")==-1){
	fprintf(stderr,"Error on publishing the offset\n");
	goto err;

	}

	bgpview_iter_destroy(it);

	view_data->num_diffs=0;
	return 0;

  err:
 	return -1;

}

static int send_diff_view(kafka_data_t dest, kafka_view_data_t *view_data, kafka_performance_t *metrics, bgpview_t *view,bgpview_io_filter_cb_t *cb){


	fprintf(stderr,"Send Diff %d\n",view_data->num_diffs);
	bgpview_iter_t *itH=NULL, *itC=NULL;
	uint32_t view_id=bgpview_get_time(view);
	uint32_t sync_view_id=view_data->sync_view_id;

	if(((itH = bgpview_iter_create(view_data->viewH)) == NULL) || ((itC = bgpview_iter_create(view)) == NULL) )
	{
	  goto err;
	}

	if(set_current_offsets(dest,view_data)==-1){
		goto err;
	}

	if(send_peer_diffs(dest,itH,itC,view_id,sync_view_id)==-1){
		goto err;
	}

	if(send_pfxs_paths_diffs(dest,metrics,view_data->viewH,view,itH,itC,view_id,sync_view_id)==-1){
		goto err;
	}

	if(send_metadata(dest,
		view,
		view_data,
		"DIFF")==-1){
	fprintf(stderr,"Error on publishing the offset\n");
	goto err;
	}

	view_data->num_diffs++;


	bgpview_iter_destroy(itH);
	bgpview_iter_destroy(itC);

	return 0;

	err:

	bgpview_iter_destroy(itH);
	bgpview_iter_destroy(itC);

	return -1;
}

static int read_sync_view(kafka_data_t src,
		bgpview_t *view,
		kafka_view_data_t *kafka_data,
		bgpview_io_filter_peer_cb_t *peer_cb,
          bgpview_io_filter_pfx_cb_t *pfx_cb,
          bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb){

  bgpview_iter_t *it = NULL;

  if(view != NULL && (it = bgpview_iter_create(view)) == NULL)
	{
	  goto err;
	}

  if(recv_peers(src,it,peer_cb,kafka_data->peerid_map,kafka_data->peers_sync_offset) < 0)
	  {
		fprintf(stderr, "Could not receive peers\n");
		goto err;
	  }
  if(recv_pfxs_paths(src,it,pfx_cb, pfx_peer_cb,kafka_data->peerid_map,kafka_data->pfxs_paths_sync_partition,kafka_data->pfxs_paths_sync_offset) !=0)
	 {
	  fprintf(stderr, "Could not receive prefixes and paths\n");
	  goto err;
	}
  if(it != NULL)
	{
	  bgpview_iter_destroy(it);
	}

  return 0;

 err:
  if(it != NULL)
	{
	  bgpview_iter_destroy(it);
	}

  return -1;

}

static int read_diff_view(kafka_data_t src,
		bgpview_t *view,
		kafka_view_data_t *kafka_data,
		bgpview_io_filter_peer_cb_t *peer_cb,
          bgpview_io_filter_pfx_cb_t *pfx_cb,
          bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb){

	  bgpview_iter_t *it = NULL;

	  if(view != NULL && (it = bgpview_iter_create(view)) == NULL)
		{
		  return -1;
		}

	int i;
	for(i=0;i< kafka_data->num_diffs;i++){
		if(recv_peers(src,it,peer_cb,kafka_data->peerid_map,kafka_data->peers_offset[i]) < 0)
		{
			fprintf(stderr, "Could not receive peers\n");
			return -1;
		}

		if(recv_pfxs_paths(src,it,pfx_cb, pfx_peer_cb,kafka_data->peerid_map,kafka_data->pfxs_paths_diffs_partition[i],kafka_data->pfxs_paths_diffs_offset[i]) !=0)
		 {
		  fprintf(stderr, "Could not receive prefixes and paths\n");
		  goto err;
		}
	}
	int recv=0;
	for(bgpview_iter_first_peer(it, BGPVIEW_FIELD_ACTIVE);
		  bgpview_iter_has_more_peer(it);
		  bgpview_iter_next_peer(it))
	{
		if(bgpview_iter_peer_get_pfx_cnt(it,0,BGPVIEW_FIELD_ACTIVE)==0){

      	  if((recv = bgpview_iter_deactivate_peer(it))!= 1){
		    	  fprintf(stderr, "Fail to deactivate peer\n");
		    	  goto err;
			  }
		}

	}

	  if(it != NULL)
		{
		  bgpview_iter_destroy(it);
		}

	  return 0;

	 err:
	  if(it != NULL)
		{
		  bgpview_iter_destroy(it);
		}

	  return -1;

	return 0;
}

/* ==========END SEND/RECEIVE FUNCTIONS ========== */


/* ========== PROTECTED FUNCTIONS ========== */


int bgpview_io_kafka_send(kafka_data_t dest, kafka_view_data_t *view_data, bgpview_t *view, kafka_performance_t *metrics,
		bgpview_io_filter_cb_t *cb){

	#ifdef DEBUG
	  fprintf(stderr, "DEBUG: Sending view...\n");
	#endif


	time_t rawtime;
	struct tm * timeinfo;
	time(&rawtime);
	timeinfo = localtime(&rawtime);

	time_t start1,start2, end;
	int length;

	time(&start1);

	fprintf(stderr,"#####START sending view at: %s", asctime(timeinfo));


	metrics->arrival_time=(int)time(NULL);

	if(view_data->viewH==NULL || view_data->num_diffs==dest.view_frequency){
		if(send_sync_view(dest,view_data,metrics,view,cb)==-1){
			goto err;
		}
	}
	else{
		if(send_diff_view(dest,view_data,metrics,view,cb)==-1){
				goto err;
		}
	}

	time(&rawtime);
	timeinfo = localtime(&rawtime);
	time(&end);
	length = difftime(end,start1);
	metrics->send_time=length;
	fprintf(stderr,"#####END sending view in %d at: %s",length, asctime(timeinfo));


	time(&rawtime);
	timeinfo = localtime(&rawtime);
	time(&start2);

	fprintf(stderr,"#####START CLONING view at: %s", asctime(timeinfo));

	if(bgpview_clone_view(view,&view_data->viewH)==-1){
		fprintf(stderr,"error cloning the view\n");
		goto err;
	}


	time(&rawtime);
	timeinfo = localtime(&rawtime);
	time(&end);
	length = difftime(end,start2);
	fprintf(stderr,"#####END CLONING view in: %d at: %s",length, asctime(timeinfo));
	metrics->clone_time=length;
	length = difftime(end,start1);
	metrics->total_time=length;

	metrics->processed_time=(int)time(NULL);

	return 0;

  err:
	if(view_data->viewH !=NULL)
		bgpview_destroy(view_data->viewH);

	return -1;
}

int bgpview_io_kafka_recv(kafka_data_t *src,
			kafka_view_data_t *kafka_data,
			bgpview_t *view,
        		bgpview_io_filter_peer_cb_t *peer_cb,
                  bgpview_io_filter_pfx_cb_t *pfx_cb,
                  bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{

	uint8_t first_view=0;

	int i;
	if(bgpview_pfx_cnt(view,BGPVIEW_FIELD_ACTIVE)==0)
		first_view=1;


	if(recv_metadata(src,kafka_data)==-1)
		  {
			fprintf(stderr, "Could not receive view offset\n");
			goto err;
		  }

	if(kafka_data->num_diffs==0 || first_view==1){
		bgpview_clear(view);
		for(i=0;i<2048;i++)
			kafka_data->peerid_map[i]=0;
		read_sync_view(*src,
				view,
				kafka_data,
				peer_cb,
				pfx_cb,
				pfx_peer_cb);

	}

	if(kafka_data->num_diffs>0)
		read_diff_view(*src,
				view,
				kafka_data,
				peer_cb,
				pfx_cb,
				pfx_peer_cb);


return 0;
err:

  return -1;

}

