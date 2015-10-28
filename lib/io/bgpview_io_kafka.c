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

#include "config.h"

#include <stdio.h>

#include <czmq.h>

#include "bgpview_io_common_int.h"
#include "bgpview_io.h"

#include <librdkafka/rdkafka.h>
#include <errno.h>
#include "bgprow.pb-c.h"
#include "peer.pb-c.h"

#define BUFFER_LEN 16384
#define BUFFER_1M  1048576

/* because the values of AF_INET* vary from system to system we need to use
   our own encoding for the version */
#define BW_INTERNAL_AF_INET  4
#define BW_INTERNAL_AF_INET6 6

#define END_OF_PEERS 0xffff

#define ASSERT_MORE				\
  if(zsocket_rcvmore(src) == 0)			\
    {						\
      fprintf(stderr, "ERROR: Malformed view message at line %d\n", __LINE__); \
      goto err;					\
    }

/* ========== KAFKA FUNCTIONS ========== */

rd_kafka_topic_t * initialize_connection(rd_kafka_t **rk,rd_kafka_conf_t **conf,rd_kafka_topic_conf_t **topic_conf,
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
	  exit(1);
  }

  /* Add brokers */
  if (rd_kafka_brokers_add(*rk, brokers) == 0) {
	  fprintf(stderr, "%% No valid brokers specified\n");
	  exit(1);
  }

  /* Create topic */
  rkt = rd_kafka_topic_new(*rk, topic, *topic_conf);

  /* Start consuming */
  if (rd_kafka_consume_start(rkt, partition, RD_KAFKA_OFFSET_BEGINNING) == -1){
	  fprintf(stderr, "%% Failed to start consuming: %s\n",rd_kafka_err2str(rd_kafka_errno2err(errno)));
	  if (errno == EINVAL)
		  fprintf(stderr,"%% Broker based offset storage requires a group.id, add: -X group.id=yourGroup\n");
	  exit(1);
  }

  return rkt;
}

static int recv_peers_kafka(bgpview_iter_t *iter, bgpstream_peer_id_t **peerid_mapping)
{

  bgpstream_peer_id_t peerid_orig;
  bgpstream_peer_id_t peerid_new;
  bgpstream_peer_id_t *idmap = NULL;

  bgpstream_peer_sig_t ps;

  rd_kafka_t *rk;
  rd_kafka_topic_t *rkt;
  rd_kafka_conf_t *conf;
  rd_kafka_topic_conf_t *topic_conf;

  char *brokers = "192.172.226.44:9092,192.172.226.46:9092";
  char *topic = "peers";
  int partition = 0;
  int offset=0;

  int i,j;
  uint16_t pc;
  int idmap_cnt = 0;
  int peers_rx = 0;

  rkt=initialize_connection(&rk,&conf,&topic_conf,brokers,topic,partition,offset);

  /* foreach peer, recv peerid, collector string, peer ip (version, address),
     peer asn */
  for(i=0; i<UINT16_MAX; i++)
    {
	  rd_kafka_message_t *rkmessage;

      /* peerid (or end-of-peers)*/
      if((rkmessage = rd_kafka_consume(rkt, partition, 1000)) == NULL)
	{
          /* end of peers */
          break;
	}
      if(rkmessage->err)
        {
    	  fprintf(stderr, "Could not receive peer id\n");
    	  goto err;
        }

      Peer *peer_msg;
      peer_msg = peer__unpack(NULL,rkmessage->len,rkmessage->payload); // Deserialize the serialized Peer

      if(peer_msg!=NULL){

    	  /* by here we have a valid peer to receive */
		  peers_rx++;

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

      }
      else{ //BEGIN || END MESSAGE
    	  pc=0;//(int)rkmessage->payload;
      }
	  peer__free_unpacked(peer_msg, NULL);
      rd_kafka_message_destroy (rkmessage);
    }

  //assert(pc == peers_rx);

  // Destroy topic
  rd_kafka_topic_destroy(rkt);

  // Destroy the handle
  rd_kafka_destroy(rk);

  *peerid_mapping = idmap;

  return idmap_cnt;

 err:
  return -1;
}


static int recv_pfxs_paths_kafka(bgpview_iter_t *iter,bgpstream_peer_id_t *peerid_map,int peerid_map_cnt){


  bgpview_t *view = NULL;
  int i,j;
  //int pfx_cnt;
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

  char *brokers = "192.172.226.44:9092,192.172.226.46:9092";
  char *topic = "views";
  int partition = 0,offset=0;
  int recv;

  rkt=initialize_connection(&rk,&conf,&topic_conf,brokers,topic,partition,offset);

  rd_kafka_message_t *rkmessage;

  /* foreach BGPROW:*/
  for(i=0; i<UINT16_MAX; i++){
	  if((rkmessage = rd_kafka_consume(rkt, partition, 1000)) == NULL){
          /* end of peers */
          break;
	  }
      if(rkmessage->err){
    	  fprintf(stderr, "Could not receive bgprow messages\n");
    	  goto err;
      }

      BGPRow *row;
      row = bgprow__unpack(NULL,rkmessage->len,rkmessage->payload); // Deserialize the serialized Peer

      if(row!=NULL){
    	  pfx_rx++;

    	  bgpstream_pfx_t *pfx = (bgpstream_pfx_t *)row->pfx.data;

		  for(j=0;j<row->n_cells;j++){

			  bgpstream_peer_id_t old_peerid=row->cells[i]->peerid;
			  bgpstream_peer_id_t peerid=peerid_map[old_peerid];

			  assert(peerid < peerid_map_cnt);

			  bgpstream_as_path_t *tmp_path = bgpstream_as_path_create();
			  bgpstream_as_path_populate_from_data(tmp_path,row->cells[i]->aspath.data, row->cells[i]->aspath.len);

			  if(iter != NULL)
				  if((recv = bgpview_iter_seek_peer(iter,peerid,BGPVIEW_FIELD_ACTIVE))!= 0){
			    	  fprintf(stderr, "Peer not existing\n");
			    	  goto err;
				  }

				  if((recv = bgpview_iter_add_pfx_peer(iter,pfx,peerid,tmp_path))!= 0){
			    	  fprintf(stderr, "Fail to insert pfx and peer\n");
			    	  goto err;
				  }
			  bgpstream_as_path_destroy(tmp_path);

		  }
      }
      else{
    	  printf("TODO");
    	  //BEGIN || END MESSAGE
    	  //GET STAMP AND NUMBER OF PXS
    	  /*pfx_cnt=(int)rkmessage->payload;
    	  uint32_t u32;
    	  u32=(int)rkmessage->payload;
    	  bgpview_set_time(view, u32);*/
      }

      bgprow__free_unpacked(row, NULL);
      rd_kafka_message_destroy (rkmessage);
    }

  //assert(pfx_rx == pfx_cnt);

  return 0;

  err:
   return -1;

}


int bgpview_io_recv_kafka(void *src, bgpview_t *view)
{

  printf("Bgpview_io_recv\n");
  return 1;
  int peerid_map_cnt = 0;
  bgpstream_peer_id_t *peerid_map = NULL;

  /* an array of path IDs */

  bgpview_iter_t *it = NULL;
  if(view != NULL && (it = bgpview_iter_create(view)) == NULL)
	{
	  goto err;
	}

  if((peerid_map_cnt = recv_peers_kafka(it,&peerid_map)) < 0)
	  {
		fprintf(stderr, "Could not receive peers\n");
		goto err;
	  }

  /* pfxs */
  if(recv_pfxs_paths_kafka(it,peerid_map,peerid_map_cnt)!=0){
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

int main(){

	bgpview_t *view = bgpview_create(NULL, NULL, NULL, NULL);
	void *src=malloc(sizeof(int));
	bgpview_io_recv_kafka(src,view);

	return 0;

}
