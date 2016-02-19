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
#include <librdkafka/rdkafka.h>
#include <errno.h>
#include "bgpview_io_kafka.h"
#include "bgpview_io_kafka_row.pb.h"
#include "bgpview_io_kafka_peer.pb.h"

#define BUFFER_LEN 16384
#define BUFFER_1M  1048576

/* because the values of AF_INET* vary from system to system we need to use
   our own encoding for the version */
#define BW_INTERNAL_AF_INET  4
#define BW_INTERNAL_AF_INET6 6

#define END_OF_PEERS 0xffff


/* ========== KAFKA FUNCTIONS ========== */

rd_kafka_topic_t * initialize_consumer_connection(rd_kafka_t **rk,rd_kafka_conf_t **conf,rd_kafka_topic_conf_t **topic_conf,
										char *brokers, char *topic, int partition,int64_t offset){

  rd_kafka_topic_t *rkt;
  char errstr[512];
  //res = rd_kafka_conf_set(conf, name, val,errstr, sizeof(errstr));

  /* Kafka configuration */
  *conf = rd_kafka_conf_new();

  /* Topic configuration */
  *topic_conf = rd_kafka_topic_conf_new();

  /* Create Kafka handle */
  if (!(*rk = rd_kafka_new(RD_KAFKA_CONSUMER, *conf,errstr, sizeof(errstr)))) {
	  fprintf(stderr,"%% Failed to create new consumer: %s\n",errstr);
	  return NULL;
  }

  /* Add brokers */
  if (rd_kafka_brokers_add(*rk, brokers) == 0) {
	  fprintf(stderr, "%% No valid brokers specified\n");
	  return NULL;
  }

  /* Create topic */
  rkt = rd_kafka_topic_new(*rk, topic, *topic_conf);

  /* Start consuming */
  if (rd_kafka_consume_start(rkt, partition, RD_KAFKA_OFFSET_BEGINNING) == -1){
	  fprintf(stderr, "%% Failed to start consuming: %s\n",rd_kafka_err2str(rd_kafka_errno2err(errno)));
	  if (errno == EINVAL)
		  fprintf(stderr,"%% Broker based offset storage requires a group.id, add: -X group.id=yourGroup\n");
	  return NULL;
  }

  return rkt;
}

rd_kafka_topic_t * initialize_producer_connection(rd_kafka_t **rk,rd_kafka_conf_t **conf,
		rd_kafka_topic_conf_t **topic_conf, char *brokers, char *topic, int partition){

  rd_kafka_topic_t *rkt;
  char errstr[512];


  /* Kafka configuration */
   *conf = rd_kafka_conf_new();

   /* Topic configuration */
   *topic_conf = rd_kafka_topic_conf_new();

   //printf("writing in partion %d\n",partition);

   if (rd_kafka_conf_set(*conf, "queue.buffering.max.messages","7000000", errstr, sizeof(errstr)) !=RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%% %s\n", errstr);
		return NULL;
   }
   //Enable compression: gzip | snappy
   //Results for a 1 view all collectors:
   //1 View gzip:     979 MB  ~ 450 sec
   //1 View snappy:   1.7 GB  ~ 150 sec
   //1 View row:      4.8 GB  ~ 135 sec


   if (rd_kafka_conf_set(*conf, "compression.codec", "snappy", errstr, sizeof(errstr)) !=RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%% %s\n", errstr);
		return NULL;
   }

   /* Create Kafka handle */
   if (!(*rk = rd_kafka_new(RD_KAFKA_PRODUCER, *conf,errstr, sizeof(errstr)))) {
 	  fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
 	 return NULL;
   }

   /* Add brokers */
   if (rd_kafka_brokers_add(*rk, brokers) == 0) {
 	fprintf(stderr, "%% No valid brokers specified\n");
 	return NULL;
   }

   /* Create topic */
   rkt = rd_kafka_topic_new(*rk, topic, *topic_conf);

  return rkt;
}


static int num_elements(char* string,int position){


	char* token;
	int j=0;

	if (string != NULL) {
	  while ((token = strsep(&string, " ")) != NULL)
	  {
		  if(j==position){
			return atoi(token);
		  }
			j++;
	  }

	}

	return -1;
}

void prefix_allocation(BGPRow *row, bgpview_iter_t *it){

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



static int get_offset_view(kafka_data_t dest){

	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	int offset;
	rd_kafka_message_t *rkmessage;


	char *brokers=dest.brokers;
	char *topic=dest.pfxs_paths_topic;
	int partition=dest.pfxs_paths_partition;

	/* Kafka configuration */
	conf = rd_kafka_conf_new();

	/* Topic configuration */
	topic_conf = rd_kafka_topic_conf_new();

	/* Create Kafka handle */
	if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,errstr, sizeof(errstr)))) {
		fprintf(stderr,"%% Failed to create new consumer: %s\n",errstr);
		return -1;
	}

	/* Add brokers */
	if (rd_kafka_brokers_add(rk, brokers) == 0) {
		fprintf(stderr, "%% No valid brokers specified\n");
		return -1;
	}

	/* Create topic */
	rkt = rd_kafka_topic_new(rk, topic, topic_conf);

	/* Start consuming */
	if (rd_kafka_consume_start(rkt, partition, RD_KAFKA_OFFSET_END) == -1){
		fprintf(stderr, "%% Failed to start consuming: %s\n",rd_kafka_err2str(rd_kafka_errno2err(errno)));
		if (errno == EINVAL)
			fprintf(stderr,"%% Broker based offset storage requires a group.id, add: -X group.id=yourGroup\n");
		return -1;
	}

	rkmessage = rd_kafka_consume(rkt, partition, 1000);
	if(rkmessage == NULL){
		  rd_kafka_topic_destroy(rkt);
		  rd_kafka_destroy(rk);

		  return -1;
	}

	offset=rkmessage->offset;

  // Destroy topic
  rd_kafka_topic_destroy(rkt);

  // Destroy the handle
  rd_kafka_destroy(rk);

  return offset;
}

static int send_metadata(kafka_data_t dest, bgpview_t *view, int offset, int publish_partition){

	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;

	rkt=initialize_producer_connection(&rk,&conf,&topic_conf,dest.brokers,dest.metadata_topic,dest.metadata_partition);
	  if(rkt==NULL){
	  		   fprintf(stderr,"Error initializing the producer\n");
	  		   goto err;
	  	   }
  char offset_message[256];
  int nc = sprintf(offset_message, "VIEW: %"PRIu32" PUBLISHED IN PARTITION: %d AT OFFSET: %d WITH INTERNAL VIEW ID: TBD", bgpview_get_time(view), publish_partition,offset);
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

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy the handle */
	rd_kafka_destroy(rk);

	return 0;

	err:
	return -1;

}

static int recv_metadata(kafka_data_t src,int interest){

  rd_kafka_t *rk;
  rd_kafka_topic_t *rkt;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;

  int ret_value=-1;

  rkt=initialize_consumer_connection(&rk,&conf,&topic_conf,src.brokers,src.metadata_topic,src.metadata_partition,src.metadata_offset);
  if(rkt==NULL){
  		   fprintf(stderr,"Error initializing the consumer\n");
  		   goto err;
  	   }
  rd_kafka_message_t *rkmessage;

  int i;
  for(i=0; i<UINT32_MAX; i++){
	  rkmessage = rd_kafka_consume(rkt, src.metadata_partition, 1000);
     if(rkmessage->payload == NULL)
	{
   	  if(rkmessage->err!=RD_KAFKA_RESP_ERR__PARTITION_EOF)
   	          {
   	      	  fprintf(stderr, "Could not receive metadata\n");
   	      	  goto err;
   	          }
         break;
	}
       int view_id=num_elements(rkmessage->payload,1);
       if(view_id==interest)
       	 {
		  src.pfxs_paths_partition=num_elements(rkmessage->payload,5);
		  src.pfxs_paths_offset=num_elements(rkmessage->payload,8);

		  rd_kafka_message_destroy (rkmessage);
		  rd_kafka_topic_destroy(rkt);
		  rd_kafka_destroy(rk);
		  return 1;
         }
       if(interest==-1)
       	 {
    	  ret_value=1;
		  src.pfxs_paths_partition=num_elements(rkmessage->payload,5);
		  src.pfxs_paths_offset=num_elements(rkmessage->payload,8);
		 }
       rd_kafka_message_destroy (rkmessage);
     }

   rd_kafka_topic_destroy(rkt);
   rd_kafka_destroy(rk);

   return ret_value;

   err:

   rd_kafka_topic_destroy(rkt);
   rd_kafka_destroy(rk);
   return -1;

}

static int send_peers(kafka_data_t dest, bgpview_iter_t *it,
					bgpview_io_filter_cb_t *cb)
{

	return 0;
  time_t rawtime;
  struct tm * timeinfo;
  time ( &rawtime );
  timeinfo = localtime ( &rawtime );

  printf ( "Start writing peers in partition 0. Current local time and date: %s", asctime (timeinfo));

  //kafka configurations
  rd_kafka_t *rk;
  rd_kafka_topic_t *rkt;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;
  char begin_message[256];

  //protobuf
  void *buf;                     // Buffer to store serialized data
  unsigned len=0;                  // Length of serialized data
  int peers_tx=0;
  int filter;

  bgpview_t *view = bgpview_iter_get_view(it);

  rkt=initialize_producer_connection(&rk,&conf,&topic_conf,dest.brokers,dest.peers_topic,dest.peers_partition);
  if(rkt==NULL){
  		   fprintf(stderr,"Error initializing the producer\n");
  		   goto err;
  	   }
  //SEND BEGIN MESSAGE
  int np= bgpview_peer_cnt(view, BGPVIEW_FIELD_ACTIVE);
  int nc = sprintf(begin_message, "BEGIN - PEER: %d",np);
  if(nc>0) begin_message[nc]='\0';
  if(rd_kafka_produce(rkt, dest.peers_partition,RD_KAFKA_MSG_F_COPY,begin_message, strlen(begin_message),NULL, 0,NULL) == -1) {
	fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
		rd_kafka_topic_name(rkt), dest.peers_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		rd_kafka_poll(rk, 0); //Poll to handle delivery reports
  }

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

		bgpstream_peer_sig_t * ps = bgpview_iter_peer_get_sig(it);
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

		if(rd_kafka_produce(rkt, dest.peers_partition,RD_KAFKA_MSG_F_COPY,buf, len,NULL, 0,NULL) == -1) {
			fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
				rd_kafka_topic_name(rkt), dest.peers_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
			rd_kafka_poll(rk, 0); //Poll to handle delivery reports
			goto err;
		}
		free(buf); // free the allocated serialized buffer

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

  // Destroy topic
  rd_kafka_topic_destroy(rkt);

  // Destroy the handle
  rd_kafka_destroy(rk);


  printf ( "End writing peers in partition 0. Current local time and date: %s", asctime (timeinfo));

  return 0;
 err:
  return -1;

}

static int recv_peers(kafka_data_t src, bgpview_iter_t *iter,
							bgpstream_peer_id_t **peerid_mapping)
{

  bgpstream_peer_id_t peerid_orig;
  bgpstream_peer_id_t peerid_new;
  bgpstream_peer_id_t *idmap = NULL;

  bgpstream_peer_sig_t ps;

  rd_kafka_t *rk;
  rd_kafka_topic_t *rkt;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;

  int i,j;
  int pc=-1;
  int idmap_cnt = 0;
  int peers_rx = 0;

  rkt=initialize_consumer_connection(&rk,&conf,&topic_conf,src.brokers,src.peers_topic,src.peers_partition,src.peers_offset);
  if(rkt==NULL){
  		   fprintf(stderr,"Error initializing the consumer\n");
  		   goto err;
  	   }
  /* foreach peer, recv peerid, collector string, peer ip (version, address),
     peer asn */
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
    	      	  fprintf(stderr, "Could not receive peer id\n");
    	      	  goto err;
    	          }
          break;
	}
      Peer *peer_msg=NULL;

      peer_msg = peer__unpack(NULL,rkmessage->len,rkmessage->payload); // Deserialize the serialized Peer


      if(peer_msg!=NULL){

		  peerid_orig = (bgpstream_peer_id_t)peer_msg->peerid_orig;

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

	      /* ensure we have enough space in the id map */
	      if((peerid_orig+1) > idmap_cnt)
	        {
	          if((idmap =
	              realloc(idmap,
	                      sizeof(bgpstream_peer_id_t) * (peerid_orig+1))) == NULL)
	            {
	              goto err;
	            }

	          /* now set all ids to 0 (reserved) */
	          for(j=idmap_cnt; j<= peerid_orig; j++)
	            {
	              idmap[j] = 0;
	            }
	          idmap_cnt = peerid_orig+1;
	        }

	      /* now ask the view to add this peer */
	      peerid_new = bgpview_iter_add_peer(iter,
	                                         ps.collector_str,
	                                         (bgpstream_ip_addr_t*)&ps.peer_ip_addr,
	                                         ps.peer_asnumber);
	      assert(peerid_new != 0);
	      idmap[peerid_orig] = peerid_new;
	      peer__free_unpacked(peer_msg, NULL);

	      peers_rx++;

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

  // Destroy topic
  rd_kafka_topic_destroy(rkt);

  // Destroy the handle
  rd_kafka_destroy(rk);

  *peerid_mapping = idmap;

  return idmap_cnt;

 err:
  return -1;
}

static int send_pfxs_paths(kafka_data_t dest, bgpview_iter_t *it,
						bgpview_io_filter_cb_t *cb){

	  time_t rawtime;
	  struct tm * timeinfo;
	  time ( &rawtime );
	  timeinfo = localtime ( &rawtime );
	  printf ( "Start writing pfxs paths in partition %d. Current local time and date: %s",dest.pfxs_paths_partition, asctime (timeinfo));

	  //kafka configurations
	  rd_kafka_t *rk;
	  rd_kafka_topic_t *rkt;
	  rd_kafka_conf_t *conf;
	  rd_kafka_topic_conf_t *topic_conf;

	  int i,j,filter,size,peers_cnt,paths_tx=0,npfx=0;
	  void *buf;                     // Buffer to store serialized data
	  unsigned len=0;                  // Length of serialized data

	  char begin_message[256];

	  bgpview_t *view = bgpview_iter_get_view(it);
	  assert(view != NULL);

	   int offset=get_offset_view(dest);
	   printf("offset: %d \n",offset);
	   if(offset==-1){
		   fprintf(stderr,"Error on retrieving the offset\n");
		   goto err;
	   }
	   if(send_metadata(dest,view,offset,dest.pfxs_paths_partition)==-1){
		   fprintf(stderr,"Error on publishing the offset\n");
		   goto err;

	   }

	  rkt=initialize_producer_connection(&rk,&conf,&topic_conf,dest.brokers,dest.pfxs_paths_topic,dest.pfxs_paths_partition);
	  if(rkt==NULL){
	  		   fprintf(stderr,"Error initializing the producer\n");
	  		   goto err;
	  	   }


	  int nc = sprintf(begin_message, "BEGIN - VIEW: %"PRIu32" WITH %d PREFIXES", bgpview_get_time(view),bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE));
	  if(nc>0) begin_message[nc]='\0';
	  while(rd_kafka_produce(rkt, dest.pfxs_paths_partition,RD_KAFKA_MSG_F_COPY,begin_message, strlen(begin_message),NULL, 0,NULL) == -1) {
		fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
			rd_kafka_topic_name(rkt), dest.pfxs_paths_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
			rd_kafka_poll(rk, 0); //Poll to handle delivery reports
	  }


	  for(bgpview_iter_first_pfx(it, 0, BGPVIEW_FIELD_ACTIVE);
	      bgpview_iter_has_more_pfx(it);
	      bgpview_iter_next_pfx(it))
	    {
	      if(cb != NULL)
	        {
	          /* ask the caller if they want this peer */
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

		  /* for a pfx to be sent it must have active peers */
	      if(peers_cnt == 0)
	        {
	          continue;
	        }

		  BGPRow row = BGPROW__INIT;
		  BGPCell **cells;
		  prefix_allocation(&row,it);

		  row.n_cells=peers_cnt;
	      cells =(BGPCell**)malloc(sizeof(BGPCell*)*peers_cnt);

	      i=0;
	      bgpstream_as_path_t* paths[1024];
	      for(bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
	          bgpview_iter_pfx_has_more_peer(it);
	          bgpview_iter_pfx_next_peer(it))
	        {

	    	  bgpstream_peer_id_t peerid=bgpview_iter_peer_get_peer_id(it);

	    	  size = sizeof(bgpstream_peer_id_t);
	    	  cells[i] = malloc (sizeof (BGPCell));
	    	  bgpcell__init(cells[i]);
	      	  cells[i]->peerid=peerid;

	    	  uint8_t *path_data;
	    	  // Optimization to avoid bgpview_iter_pfx_peer_get_as_path i.e., copy of data twice
	    	  bgpstream_as_path_t *path = bgpview_iter_pfx_peer_get_as_path(it);
	    	  assert(path != NULL);

	    	  uint16_t ndata = bgpstream_as_path_get_data(path,&path_data);

	    	  paths[i]=path;
	    	  cells[i]->aspath.len=ndata;
	    	  cells[i]->aspath.data=path_data;

	    	  i++;
	    	  paths_tx++;
			  }

	      	  row.cells=cells;

	    	  len = bgprow__get_packed_size(&row);

	    	  buf = malloc(len);
	    	  bgprow__pack(&row,buf);


	    	  if(rd_kafka_produce(rkt, dest.pfxs_paths_partition,RD_KAFKA_MSG_F_COPY,buf, len,NULL, 0,NULL) == -1) {
	    		fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
	    			rd_kafka_topic_name(rkt), dest.pfxs_paths_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
	    		//Poll to handle delivery reports
	    		rd_kafka_poll(rk, 0);
	    	  }
	  		rd_kafka_poll(rk, 0);

	    	  for(j=0;j<peers_cnt;j++){
	        	  bgpstream_as_path_destroy(paths[j]); //free all created paths
	    	  	  free(cells[j]); //Free all cells
	    	  }
			  free(cells); //free cells vector
			  free(buf); // free the allocated serialized buffer
			  npfx++;
	        }

	  printf("number of active pfx: %d sent %d\n",bgpview_pfx_cnt(view, BGPVIEW_FIELD_ACTIVE),npfx);
	  assert(paths_tx <= UINT32_MAX);

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


	  // Destroy topic
	  rd_kafka_topic_destroy(rkt);

	  // Destroy the handle
	  rd_kafka_destroy(rk);

	  /* destroy the view iterator */
	  bgpview_iter_destroy(it);

	time ( &rawtime );
	timeinfo = localtime ( &rawtime );
	printf ( "End pfxs paths: current local time and date: %s", asctime (timeinfo) );

	   return 0;

	   err:
	   return -1;
}

static int recv_pfxs_paths(kafka_data_t src, bgpview_iter_t *iter,
							bgpview_io_filter_pfx_cb_t *pfx_cb,
		                    bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb,
							bgpstream_peer_id_t *peerid_map,
							int peerid_map_cnt){


  bgpview_t *view = NULL;
  int i,j;
  int pfx_cnt;
  int pfx_rx = 0;

  /* only if we have a valid iterator */
  if(iter != NULL)
    {
      view = bgpview_iter_get_view(iter);
      //check view
    }

  rd_kafka_t *rk;
  rd_kafka_topic_t *rkt;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;

  int recv=0;

  rkt=initialize_consumer_connection(&rk,&conf,&topic_conf,src.brokers,src.pfxs_paths_topic,src.pfxs_paths_partition,src.pfxs_paths_offset);
  if(rkt==NULL){
  		   fprintf(stderr,"Error initializing the consumer\n");
  		   goto err;
  	   }
  rd_kafka_message_t *rkmessage;

  /* foreach BGPROW:*/
  for(i=0; i<UINT32_MAX; i++){

	  rkmessage = rd_kafka_consume(rkt, src.pfxs_paths_partition, 1000);

      if(rkmessage->payload == NULL)
      {
    	  if(rkmessage->err!=RD_KAFKA_RESP_ERR__PARTITION_EOF)
    	          {
    	      	  fprintf(stderr, "Could not receive prefixes and paths\n");
    	      	  goto err;
    	          }
          break;
	}

      BGPRow *row;
      row = bgprow__unpack(NULL,rkmessage->len,rkmessage->payload); // Deserialize the serialized Peer

      if(row!=NULL){
    	  pfx_rx++;

    	  bgpstream_pfx_t *pfx = (bgpstream_pfx_t *)row->pfx.data;

          if(pfx_cb != NULL)
            {
              /* ask the caller if they want this pfx */
              if((filter = pfx_cb((bgpstream_pfx_t*)&pfx)) < 0)
                {
                  goto err;
                }
              if(filter == 0)
                {
                  skip_pfx = 1;
                }
            }
		  for(j=0;j<row->n_cells;j++){

			  bgpstream_peer_id_t old_peerid=row->cells[j]->peerid;
			  bgpstream_peer_id_t peerid=peerid_map[old_peerid];

			  assert(peerid < peerid_map_cnt);

			  bgpstream_as_path_t *tmp_path = bgpstream_as_path_create();
			  bgpstream_as_path_populate_from_data(tmp_path,row->cells[j]->aspath.data, row->cells[j]->aspath.len);

			  if(iter != NULL)
				  if((recv = bgpview_iter_seek_peer(iter,peerid,BGPVIEW_FIELD_ACTIVE))!= 0){
			    	  fprintf(stderr, "Peer not existing\n");
			    	  goto err;
				  }
			  if(pfx_peer_cb != NULL)
			              {
				  printf("tmp\n");
			              }

				  if((recv = bgpview_iter_add_pfx_peer(iter,pfx,peerid,tmp_path))!= 0){
			    	  fprintf(stderr, "Fail to insert pfx and peer\n");
			    	  goto err;
				  }
			  bgpstream_as_path_destroy(tmp_path);

		  }
      }
      else{
    	  if (strstr(rkmessage->payload, "END") != NULL){
    		  break;

    	  }
    	  if (strstr(rkmessage->payload, "BEGIN") != NULL)
    		  pfx_cnt=num_elements(rkmessage->payload,5);
		  if(pfx_cnt==-1){
			  fprintf(stderr,"No number of pfxs\n");
			  goto err;
		  }
      }


      rd_kafka_message_destroy (rkmessage);
    }

  assert(pfx_rx == pfx_cnt);

  return 0;

  err:
   return -1;

}

/* ========== PROTECTED FUNCTIONS ========== */

int bgpview_io_kafka_send(kafka_data_t dest, bgpview_t *view,
		bgpview_io_filter_cb_t *cb){

	printf("\nstart sending\n");

	bgpview_iter_t *it = NULL;

	#ifdef DEBUG
	  fprintf(stderr, "DEBUG: Sending view...\n");
	#endif

	if((it = bgpview_iter_create(view)) == NULL)
	{
		goto err;
	}

	if(send_peers(dest,it,cb)!=0){
		goto err;
	}

	if(send_pfxs_paths(dest,it,cb)!=0){
		goto err;
	}

	bgpview_iter_destroy(it);

	return 0;

  err:
 	return -1;
}

int bgpview_io_kafka_recv(kafka_data_t src, bgpview_t *view, int interest,
        		bgpview_io_filter_peer_cb_t *peer_cb,
                  bgpview_io_filter_pfx_cb_t *pfx_cb,
                  bgpview_io_filter_pfx_peer_cb_t *pfx_peer_cb)
{

  bgpstream_peer_id_t *peerid_map = NULL;
  int peerid_map_cnt = 0;

  bgpview_iter_t *it = NULL;

  if(recv_metadata(src,interest) < 0)
  	  {
		fprintf(stderr, "Could not receive view offset\n");
		goto err;
	  }

  if(view != NULL && (it = bgpview_iter_create(view)) == NULL)
	{
	  goto err;
	}

  if((peerid_map_cnt = recv_peers(src,it,peer_cb,&peerid_map)) < 0)
	  {
		fprintf(stderr, "Could not receive peers\n");
		goto err;
	  }

  if(recv_pfxs_paths(src,it,peerid_map,pfx_cb, pfx_peer_cb,peerid_map_cnt) !=0)
  	 {
	  fprintf(stderr, "Could not receive prefixes and paths\n");
	  goto err;
	}

  if(it != NULL)
	{
	  bgpview_iter_destroy(it);
	}
  free(peerid_map);

  return 0;

 err:
  if(it != NULL)
	{
	  bgpview_iter_destroy(it);
	}
  free(peerid_map);

  return -1;

}

/*int main(){

	// DEFAULT TMP VALUES
	kafka_data_t src;
	src.brokers = "192.172.226.44:9092,192.172.226.46:9092";
	src.pfxs_paths_topic="views";
	src.peers_topic="peers";
	src.metadata_topic="metadata";
	src.pfxs_paths_partition=0;
	src.peers_partition=0;
	src.metadata_partition=0;
	src.pfxs_paths_offset=0;
	src.peers_offset=0;
	src.metadata_offset=0;

	bgpview_t *view = bgpview_create(NULL, NULL, NULL, NULL);
	bgpview_io_kafka_recv(src,view,-1);

	return 0;

}*/
