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
#include "utils.h"
#include "bgpview_consumer_interface.h"
#include "bvc_myviewprocess.h"
#include "bgprow.pb-c.h"
#include "peer.pb-c.h"
#include <errno.h>
#include <time.h>

/*
 *TO REMOVE AFTER INCLUDE CORRECT FILES
 */
typedef enum {

  /** The iterator refers to a peer */
  BGPVIEW_IO_FILTER_PEER = 0,

  /** The iterator refers to a prefix */
  BGPVIEW_IO_FILTER_PFX = 1,

  /** The iterator refers to a prefix-peer */
  BGPVIEW_IO_FILTER_PFX_PEER = 2,

} bgpview_io_filter_type_t;

typedef int (bgpview_io_filter_cb_t)(bgpview_iter_t *iter,
                                     bgpview_io_filter_type_t type);
// END REMOVE


#define NAME "my-view-process"


/* macro to access the current consumer state */
#define STATE					\
  (BVC_GET_STATE(consumer, myviewprocess))

/* macro to access the current chain state, i.e.
 * the state variables shared by other consumers */
#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))


/* our 'class' */
static bvc_t bvc_myviewprocess = {
  BVC_ID_MYVIEWPROCESS,
  NAME,
  BVC_GENERATE_PTRS(myviewprocess)
};



/* our 'instance' */
typedef struct bvc_myviewprocess_state {

  /* count how many views have
   * been processed */
  int view_counter;

  /* count the number of elements (i.e.
   * <pfx,peer> information in the matrix)
   * are present in the current view */
  int current_view_elements;

} bvc_myviewprocess_state_t;

typedef struct kafka_data{

	  /*
	   *
	   */
	  char *brokers;

	  /*
	   *
	   */
	  char *pfxs_paths_topic;
	  char *peers_topic;
	  char *metadata_topic;

	  /*
	   *
	   */
	  int pfxs_paths_partition;
	  int peers_partition;
	  int metadata_partition;

	  /*
	   *
	   */
	  int pfxs_paths_offset;
	  int peers_offset;
	  int metadata_offset;

} kafka_data_t;


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

/* ==================== KAFKA FUNCTIONS ==================== */

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
		exit(1);
   }
   //Enable compression: gzip | snappy
   //Results for a 1 view all collectors:
   //1 View gzip:     979 MB  ~ 450 sec
   //1 View snappy:   1.7 GB  ~ 150 sec
   //1 View row:      4.8 GB  ~ 135 sec


   if (rd_kafka_conf_set(*conf, "compression.codec", "snappy", errstr, sizeof(errstr)) !=RD_KAFKA_CONF_OK) {
		fprintf(stderr, "%% %s\n", errstr);
		exit(1);
   }

   /* Create Kafka handle */
   if (!(*rk = rd_kafka_new(RD_KAFKA_PRODUCER, *conf,errstr, sizeof(errstr)))) {
 	  fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
 	  exit(1);
   }

   /* Add brokers */
   if (rd_kafka_brokers_add(*rk, brokers) == 0) {
 	fprintf(stderr, "%% No valid brokers specified\n");
 	exit(1);
   }

   /* Create topic */
   rkt = rd_kafka_topic_new(*rk, topic, *topic_conf);

  return rkt;
}


/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_myviewprocess_alloc()
{
  return &bvc_myviewprocess;
}


int bvc_myviewprocess_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_myviewprocess_state_t *state = NULL;

  if((state = malloc_zero(sizeof(bvc_myviewprocess_state_t))) == NULL)
    {
      return -1;
    }
  BVC_SET_STATE(consumer, state);


  /* allocate dynamic memory HERE */

  /* set defaults */

  state->view_counter = 0;

  state->current_view_elements = 0;


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


void bvc_myviewprocess_destroy(bvc_t *consumer)
{
  bvc_myviewprocess_state_t *state = STATE;

  if(state == NULL)
    {
      return;
    }

  /* deallocate dynamic memory HERE */

  free(state);

  BVC_SET_STATE(consumer, NULL);
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

int get_offset_view(kafka_data_t dest){

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
		exit(1);
	}

	/* Add brokers */
	if (rd_kafka_brokers_add(rk, brokers) == 0) {
		fprintf(stderr, "%% No valid brokers specified\n");
		exit(1);
	}

	/* Create topic */
	rkt = rd_kafka_topic_new(rk, topic, topic_conf);

	/* Start consuming */
	if (rd_kafka_consume_start(rkt, partition, RD_KAFKA_OFFSET_BEGINNING) == -1){
		fprintf(stderr, "%% Failed to start consuming: %s\n",rd_kafka_err2str(rd_kafka_errno2err(errno)));
		if (errno == EINVAL)
			fprintf(stderr,"%% Broker based offset storage requires a group.id, add: -X group.id=yourGroup\n");
		exit(1);
	}

	rkmessage = rd_kafka_consume(rkt, partition, 1000);
	offset=rkmessage->offset;

  // Destroy topic
  rd_kafka_topic_destroy(rkt);

  // Destroy the handle
  rd_kafka_destroy(rk);

  return offset;
}

void publish_offset(kafka_data_t dest, bgpview_t *view, int offset, int publish_partition){

	rd_kafka_t *rk;
	rd_kafka_topic_t *rkt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;

	rkt=initialize_producer_connection(&rk,&conf,&topic_conf,dest.brokers,dest.metadata_topic,dest.metadata_partition);

  char offset_message[256];
  int nc = sprintf(offset_message, "VIEW: %"PRIu32" PUBLISHED IN PARTITION: %d AT OFFSET: %d WITH INTERNAL VIEW ID: TBD", bgpview_get_time(view), publish_partition,offset);
  if(nc>0) offset_message[nc]='\0';

	if(rd_kafka_produce(rkt, dest.metadata_partition,RD_KAFKA_MSG_F_COPY,offset_message,strlen(offset_message),NULL, 0,NULL) == -1) {
		fprintf(stderr,"%% Failed to produce to topic %s partition %i: %s\n",
			rd_kafka_topic_name(rkt), dest.metadata_partition,rd_kafka_err2str(rd_kafka_errno2err(errno)));
		//Poll to handle delivery reports
		rd_kafka_poll(rk, 0);
	}

	/* Wait for messages to be delivered */
	while (rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 100);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy the handle */
	rd_kafka_destroy(rk);


}
int send_pfxs_paths(kafka_data_t dest, bgpview_iter_t *it,
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
	   publish_offset(dest,view,offset,dest.pfxs_paths_partition);

	  rkt=initialize_producer_connection(&rk,&conf,&topic_conf,dest.brokers,dest.pfxs_paths_topic,dest.pfxs_paths_partition);


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

int send_peers(kafka_data_t dest, bgpview_iter_t *it,
			   bgpview_io_filter_cb_t *cb)
{

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

int bgpview_io_kafka_send(kafka_data_t dest, bgpview_t *view,
						  bgpview_io_filter_cb_t *cb){


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

int bvc_myviewprocess_process_view(bvc_t *consumer, uint8_t interests, bgpview_t *view)
{

	kafka_data_t dest;
	// DEFAULT TMP VALUES

	dest.brokers = "192.172.226.44:9092,192.172.226.46:9092";
	dest.pfxs_paths_topic="views";
	dest.peers_topic="peers";
	dest.metadata_topic="metadata";
	dest.pfxs_paths_partition=0;
	dest.peers_partition=0;
	dest.metadata_partition=0;
	dest.pfxs_paths_offset=0;
	dest.peers_offset=0;
	dest.metadata_offset=0;


	bgpview_io_kafka_send(dest,view,NULL);
	exit(0);
  return 0;
}
