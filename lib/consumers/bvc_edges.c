/*
 * This file is part of bgpstream
 *
 * CAIDA, UC San Diego
 * bgpstream-info@caida.org
 *
 * Copyright (C) 2012 The Regents of the University of California.
 * Authors: Alistair King, Chiara Orsini, Ruwaifa Anwar
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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <czmq.h>

#include "utils.h"
#include "wandio_utils.h"
#include "khash.h"

#include "bgpstream_utils_pfx_set.h"
#include "bgpview_consumer_interface.h"
#include "bvc_edges.h"


#define NAME                         "edges"

#define CONSUMER_METRIC_PREFIX       "edges"

#define OUTPUT_FILE_FORMAT            "%s/"NAME".%"PRIu32".%"PRIu32"s-window.events.gz"

#define OUTPUT_FILE_FORMAT_NEWEDGES   "%s/edges.%"PRIu32".%"PRIu32"s-window.events.gz"

#define METRIC_PREFIX_FORMAT          "%s."CONSUMER_METRIC_PREFIX".%"PRIu32"s-window.%s"

#define META_METRIC_PREFIX_FORMAT     "%s.meta.bgpview.consumer."NAME".%"PRIu32"s-window.%s"


/** Maximum size of the str output buffer */
#define MAX_BUFFER_LEN 1024

/** Maximum number of origin ASns */
#define MAX_UNIQUE_ORIGINS 128

/** Default size of window: 1 week (s) */
#define DEFAULT_WINDOW_SIZE (7*24*3600)

/** Default output folder: current folder */
#define DEFAULT_OUTPUT_FOLDER "./"

/** Default compression level of output file */
#define DEFAULT_COMPRESS_LEVEL 6

#define NEW 1

#define NEWREC 2

#define FINISHED 3
#define ONGOING 4

#define STATE					\
  (BVC_GET_STATE(consumer, edges))

#define CHAIN_STATE                             \
  (BVC_GET_CHAIN_STATE(consumer))


/* our 'class' */
static bvc_t bvc_edges = {
  BVC_ID_EDGES,
  NAME,
  BVC_GENERATE_PTRS(edges)
};


typedef struct edge_info {
  //ASNs involved in the edge
  uint32_t asn1;
  uint32_t asn2;
  //time when edge was seen first
  uint32_t first_seen;
  //time when edge was seen last
  uint32_t last_seen;
  //time when finished
  uint32_t end;
  //time when seen for current occurrence
  uint32_t start;
  //current status
  bool ongoing;
} edge_info_t;

KHASH_INIT(edge_list, uint32_t, edge_info_t, 1, kh_int_hash_func, kh_int_hash_equal);
typedef khash_t(edge_list) edge_list_t;

//contains all the edges
KHASH_INIT(edges_map, uint32_t, edge_list_t*, 1, kh_int_hash_func, kh_int_hash_equal);
typedef khash_t(edges_map) edges_map_t;



/* our 'instance' */
typedef struct bvc_edges_state {
  //window size given by user
  uint32_t window_size;
  //current time stamp
  uint32_t time_now;
  /** output folder */
  char output_folder[MAX_BUFFER_LEN];
  //Khash holding edges
  edges_map_t *edges_map;
  //Output files for edges and triplets
  char filename_newedges[MAX_BUFFER_LEN];
  iow_t *file_newedges;

  /** diff ts when the view arrived */
  uint32_t arrival_delay;
  /** diff ts when the view processing finished */
  uint32_t processed_delay;
  /** processing time */
  uint32_t processing_time;


  /** Timeseries Key Package */
  timeseries_kp_t *kp;

  //Timeseries variables
  //Meta
  int arrival_delay_idx;
  int processed_delay_idx;
  int processing_time_idx;
  //For new edges
  uint32_t new_edges_count;
  int new_edges_count_idx;
  int ongoing_edges_count_idx;
  uint32_t ongoing_edges_count;
  int finished_edges_count_idx;
  uint32_t finished_edges_count;
  int newrec_edges_count_idx;
  uint32_t newrec_edges_count;

} bvc_edges_state_t;


static int output_timeseries(bvc_t *consumer, uint32_t ts)
{
  bvc_edges_state_t *state = STATE;
  timeseries_kp_set(state->kp, state->arrival_delay_idx, state->arrival_delay);
  timeseries_kp_set(state->kp, state->processed_delay_idx, state->processed_delay);
  timeseries_kp_set(state->kp, state->processing_time_idx, state->processing_time);
  timeseries_kp_set(state->kp, state->new_edges_count_idx, state->new_edges_count);
  timeseries_kp_set(state->kp, state->ongoing_edges_count_idx, state->ongoing_edges_count);
  timeseries_kp_set(state->kp, state->finished_edges_count_idx, state->finished_edges_count);
  timeseries_kp_set(state->kp, state->newrec_edges_count_idx, state->newrec_edges_count);
  if(timeseries_kp_flush(state->kp, ts) != 0)
    {
      fprintf(stderr, "Warning: could not flush %s %"PRIu32"\n",
              NAME, ts);
    }

  return 0;
}

static int create_ts_metrics(bvc_t *consumer)
{

  char buffer[MAX_BUFFER_LEN];
  bvc_edges_state_t *state = STATE;



  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "new_edges_count");
  if((state->new_edges_count_idx =
      timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }
  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "finished_edges_count");
  if((state->finished_edges_count_idx =
      timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }
  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "newrec_edges_count");
  if((state->newrec_edges_count_idx =
      timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }
  snprintf(buffer, MAX_BUFFER_LEN, METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "ongoing_edges_count");
  if((state->ongoing_edges_count_idx =
      timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }

    /* Meta metrics */
  snprintf(buffer, MAX_BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "arrival_delay");
  if((state->arrival_delay_idx =
      timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, MAX_BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "processed_delay");
  if((state->processed_delay_idx =
      timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }

  snprintf(buffer, MAX_BUFFER_LEN, META_METRIC_PREFIX_FORMAT,
           CHAIN_STATE->metric_prefix, state->window_size, "processing_time");
  if((state->processing_time_idx =
      timeseries_kp_add_key(state->kp, buffer)) == -1)
    {
      return -1;
    }



  return 0;
}


/** Print usage information to stderr */
static void usage(bvc_t *consumer)
{
  fprintf(stderr,
	  "consumer usage: %s\n"
	  "       -w <window-size>      window size in seconds (default %d)\n"
	  "       -o <output-folder>    output folder (default: %s)\n",
	  consumer->name,
          DEFAULT_WINDOW_SIZE,
          DEFAULT_OUTPUT_FOLDER
          );
}

/** Parse the arguments given to the consumer */
static int parse_args(bvc_t *consumer, int argc, char **argv)
{
  int opt;
  assert(argc > 0 && argv != NULL);

  bvc_edges_state_t *state = STATE;

  /* NB: remember to reset optind to 1 before using getopt! */
  optind = 1;

  /* remember the argv strings DO NOT belong to us */
  while((opt = getopt(argc, argv, ":w:o:?")) >= 0)
    {
      switch(opt)
        {
        case 'w':
          state->window_size = strtoul(optarg, NULL, 10);
          break;
        case 'o':
          strncpy(state->output_folder, optarg, MAX_BUFFER_LEN-1);
          state->output_folder[MAX_BUFFER_LEN-1] = '\0';
          break;
        case '?':
        case ':':
        default:
          usage(consumer);
          return -1;
        }
    }


  /* checking that output_folder is a valid folder */
  struct stat st;
  errno = 0;
  if(stat(state->output_folder, &st) == -1)
    {
      fprintf(stderr, "Error: %s does not exist\n", state->output_folder);
      usage(consumer);
      return -1;
    }
  else
    {
    if(! S_ISDIR(st.st_mode))
      {
      fprintf(stderr, "Error: %s is not a directory \n", state->output_folder);
      usage(consumer);
      return -1;
      }
    }

  return 0;
}


/* ==================== CONSUMER INTERFACE FUNCTIONS ==================== */

bvc_t *bvc_edges_alloc()
{
  return &bvc_edges;
}

int bvc_edges_init(bvc_t *consumer, int argc, char **argv)
{
  bvc_edges_state_t *state = NULL;

  if((state = malloc_zero(sizeof(bvc_edges_state_t))) == NULL)
    {
      return -1;
    }
  BVC_SET_STATE(consumer, state);

  /* defaults here */
  state->window_size = DEFAULT_WINDOW_SIZE;
  strncpy(state->output_folder, DEFAULT_OUTPUT_FOLDER, MAX_BUFFER_LEN);
  state->output_folder[MAX_BUFFER_LEN -1] = '\0';
  state->file_newedges=NULL;

  if((state->edges_map = kh_init(edges_map)) == NULL)
    {
          fprintf(stderr, "Error: could not create edges map\n");
          return -1;
    }
/* parse the command line args */
  if(parse_args(consumer, argc, argv) != 0)
    {
      goto err;
    }

  /* react to args here */
  fprintf(stderr, "INFO: window size: %"PRIu32"\n", state->window_size);
  fprintf(stderr, "INFO: output folder: %s\n", state->output_folder);
  if((state->kp = timeseries_kp_init(BVC_GET_TIMESERIES(consumer), 1)) == NULL)
    {
      fprintf(stderr, "Error: Could not create timeseries key package\n");
      goto err;
    }
  if(create_ts_metrics(consumer) != 0)
    {
      goto err;
    }

  /* init */
  return 0;

 err:
  bvc_edges_destroy(consumer);
  return -1;
}


void bvc_edges_destroy(bvc_t *consumer)
{
  bvc_edges_state_t *state = STATE;
  if(state!=NULL){
    bvc_edges_state_t *state = STATE;
    khint_t k;
    for(k = kh_begin(state->edges_map); k!= kh_end(state->edges_map); k++){
      if(kh_exist(state->edges_map,k)){
        kh_destroy(edge_list,kh_value(state->edges_map,k));
      }
    }
    kh_destroy(edges_map,state->edges_map);
    if(state->kp != NULL)
      {
        timeseries_kp_free(&state->kp);
      }
    free(state);
    BVC_SET_STATE(consumer, NULL);
  }
}

/* Diagnostic function. Prints all edges */
void print_edges(bvc_t *consumer){
  bvc_edges_state_t *state = STATE;
  khint_t j,k;
  printf("PRINGTIN \n");
  for(k = kh_begin(state->edges_map); k!= kh_end(state->edges_map); k++){
    if(kh_exist(state->edges_map,k)){
      printf("for ASN %d : \n", kh_key(state->edges_map,k));
      edge_list_t *edge_list=kh_value(state->edges_map,k);
      for(j = kh_begin(edge_list); j!= kh_end(edge_list); j++){
        if(kh_exist(edge_list,j)){
          printf(" %d ",kh_key(edge_list,j));
        }
      }
    }
    printf( "\n");
  }

}



/* Writes in output file for newedges */
void print_to_file_newedges(bvc_t *consumer, int status, edge_info_t edge_info, bgpstream_pfx_t *pfx){
  bvc_edges_state_t *state = STATE;
  char pfx_str[MAX_BUFFER_LEN];

  if (status==NEW){
    if(wandio_printf(state->file_newedges,"%"PRIu32"|%"PRIu32"-%"PRIu32"|NEW|%"PRIu32"|%"PRIu32"|%"PRIu32"|%s\n",
                  state->time_now,
                  edge_info.asn1,edge_info.asn2,edge_info.first_seen,edge_info.start,edge_info.end,
                  bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN+3, pfx)) == -1)
    {
     fprintf(stderr, "ERROR: Could not write %s file\n",state->filename_newedges);
     return ;
    }
  }

  if (status==NEWREC){
    if(wandio_printf(state->file_newedges,"%"PRIu32"|%"PRIu32"-%"PRIu32"|NEWREC|%"PRIu32"|%"PRIu32"|%"PRIu32"|%s\n",
                  state->time_now,
                  edge_info.asn1,edge_info.asn2,edge_info.first_seen,edge_info.start,edge_info.end,
                  bgpstream_pfx_snprintf(pfx_str, INET6_ADDRSTRLEN+3, pfx)) == -1)
    {
     fprintf(stderr, "ERROR: Could not write %s file\n",state->filename_newedges);
     return ;
    }
  }

  if (status==FINISHED){
    if(wandio_printf(state->file_newedges,"%"PRIu32"|%"PRIu32"-%"PRIu32"|FINISHED|%"PRIu32"|%"PRIu32"|%"PRIu32"\n",
                  state->time_now,
                  edge_info.asn1,edge_info.asn2,edge_info.first_seen,edge_info.start,edge_info.end
                  ) == -1)
    {
     fprintf(stderr, "ERROR: Could not write %s file\n",state->filename_newedges);
     return ;
    }
  }



}





void print_ongoing_newedges(bvc_t* consumer){
  bvc_edges_state_t *state = STATE;
  khint_t k,j;
  for(k = kh_begin(state->edges_map); k!= kh_end(state->edges_map); k++){
    if(kh_exist(state->edges_map,k)){
      edge_list_t *edge_list=kh_value(state->edges_map,k);
      for(j = kh_begin(edge_list); j!= kh_end(edge_list); j++){
        if(kh_exist(edge_list,j)){
          edge_info_t edge_info=kh_value(edge_list,j);
          if(edge_info.ongoing){
            if(wandio_printf(state->file_newedges,"%"PRIu32"|%"PRIu32"-%"PRIu32"|ONGOING|%"PRIu32"|%"PRIu32"|%"PRIu32"\n",
                          state->time_now,
                          edge_info.asn1,edge_info.asn2,edge_info.first_seen,edge_info.start,edge_info.end
                          ) == -1)
            {
             fprintf(stderr, "ERROR: Could not write %s file\n",state->filename_newedges);
             return ;
            }
            state->ongoing_edges_count++;
          }

        }

      }
      kh_value(state->edges_map,k)=edge_list;
    }

  }
}


/* Scans all ongoing edges and remove stale ones */
void remove_stale_link(bvc_t *consumer){
  bvc_edges_state_t *state = STATE;
  khint_t k,j;
  for(k = kh_begin(state->edges_map); k!= kh_end(state->edges_map); k++){
    if(kh_exist(state->edges_map,k)){
      edge_list_t *edge_list=kh_value(state->edges_map,k);
      for(j = kh_begin(edge_list); j!= kh_end(edge_list); j++){
        if(kh_exist(edge_list,j)){
          edge_info_t edge_info=kh_value(edge_list,j);
          /* if last_seen is different from current time then declare it dead */
          if(edge_info.last_seen< state->time_now && edge_info.ongoing){
            edge_info.end=state->time_now;
            edge_info.ongoing=0;
            kh_value(edge_list,j)=edge_info;
            print_to_file_newedges(consumer,FINISHED,edge_info,NULL);
            state->finished_edges_count++;
          }

        }

      }
      kh_value(state->edges_map,k)=edge_list;
    }

  }

}


/* Updates khash and stores new and newrec edges */
void insert_update_edges(bvc_t *consumer, uint32_t asn1, uint32_t asn2, bgpstream_pfx_t *pfx){
  int ret;
  bvc_edges_state_t *state = STATE;
  khint_t k,j,p;
  edge_list_t *edge_list;
  edge_info_t edge_info;
  k=kh_get(edges_map,state->edges_map,asn1);
  /* ASN1 never seen before. New edge seen */
  if(k==kh_end(state->edges_map)){
    edge_list=kh_init(edge_list);
    j=kh_put(edge_list,edge_list,asn2,&ret);
    edge_info.asn1=asn1;
    edge_info.asn2=asn2;
    edge_info.last_seen=state->time_now;
    edge_info.first_seen=state->time_now;
    edge_info.start=state->time_now;
    edge_info.end=0;
    edge_info.ongoing=1;
    kh_value(edge_list,j)=edge_info;
    k=kh_put(edges_map,state->edges_map,asn1,&ret);
    kh_value(state->edges_map,k)=edge_list;
    state->new_edges_count++;
    print_to_file_newedges(consumer,NEW,edge_info,pfx);
    //insert
  }
  //ASN1 seen before
  else{
    edge_list=kh_value(state->edges_map,k);
    j=kh_get(edge_list,edge_list,asn2);
    //seen this edge before. Updating timestamp or checking for new_rec
    if(j!=kh_end(edge_list)){
      edge_info=kh_value(edge_list,j);
      edge_info.last_seen=state->time_now;
      //if ongoing, update last_seen. otherwise check for new or newrec
      if(edge_info.ongoing==0){
        edge_info.ongoing=1;
       //NEWREC if last end is within current window
        if(edge_info.end+state->window_size > state->time_now){
          print_to_file_newedges(consumer,NEWREC,edge_info,pfx);
          state->newrec_edges_count++;
        }
        else{
          print_to_file_newedges(consumer,NEW,edge_info,pfx);
          state->new_edges_count++;
        }
        edge_info.start=state->time_now;
      }
      kh_value(edge_list,j)=edge_info;
    }
    // New edge seen. ASN2 seen first time
    else{
      edge_info.asn1=asn1;
      edge_info.asn2=asn2;
      edge_info.last_seen=state->time_now;
      edge_info.first_seen=state->time_now;
      edge_info.start=state->time_now;
      edge_info.end=0;
      edge_info.ongoing=1;
      p=kh_put(edge_list,edge_list,asn2,&ret);
      kh_value(edge_list,p)=edge_info;
      print_to_file_newedges(consumer,NEW,edge_info,pfx);
      state->new_edges_count++;

    }
    kh_value(state->edges_map,k)=edge_list;
  }
}

int bvc_edges_process_view(bvc_t *consumer, uint8_t interests, bgpview_t *view)
{
  bvc_edges_state_t *state = STATE;
  bgpview_iter_t *it;
  bgpstream_pfx_t *pfx;
  bgpstream_peer_id_t peerid;
  uint32_t time_now = bgpview_get_time(view);
  state->time_now=time_now;
  /* compute arrival delay */
  state->arrival_delay = zclock_time()/1000 - bgpview_get_time(view);

  //Initializing counter for libtimeseries
  state->new_edges_count=0;
  state->ongoing_edges_count=0;
  state->finished_edges_count=0;
  state->newrec_edges_count=0;

  /* Opening file for newedges */
  snprintf(state->filename_newedges, MAX_BUFFER_LEN, OUTPUT_FILE_FORMAT_NEWEDGES,
           state->output_folder, time_now, state->window_size);

  /* open file for writing */
  if((state->file_newedges = wandio_wcreate(state->filename_newedges, wandio_detect_compression_type(state->filename_newedges), DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL)
    {
      fprintf(stderr, "ERROR: Could not open %s for writing\n",state->filename_newedges);
      return -1;
    }

  int ipv_idx,i;

  /* borrowed pointer to a path segment */
  bgpstream_as_path_seg_t *seg;
  uint32_t asn1;
  uint32_t asn2;
  uint32_t asn;
  uint32_t normal_asn;
  uint32_t prev_asn;

  /* prints  ongoing edges */
  print_ongoing_newedges(consumer);

   /* check visibility has been computed */
  if(BVC_GET_CHAIN_STATE(consumer)->visibility_computed == 0)
    {
      fprintf(stderr,
              "ERROR: edges requires the Visibility consumer "
              "to be run first\n");
      return -1;
    }

  /* create view iterator */
  if((it = bgpview_iter_create(view)) == NULL)
    {
      return -1;
    }

  /* iterate through all prefixes */
  for(bgpview_iter_first_pfx(it, 0 /* all versions */, BGPVIEW_FIELD_ACTIVE);
      bgpview_iter_has_more_pfx(it);
      bgpview_iter_next_pfx(it))
    {
      pfx = bgpview_iter_pfx_get_pfx(it);
      ipv_idx = bgpstream_ipv2idx(pfx->address.version);

      for(bgpview_iter_pfx_first_peer(it, BGPVIEW_FIELD_ACTIVE);
          bgpview_iter_pfx_has_more_peer(it);
          bgpview_iter_pfx_next_peer(it))
        {
          /* only consider peers that are full-feed */
          peerid = bgpview_iter_peer_get_peer_id(it);
          if(bgpstream_id_set_exists(BVC_GET_CHAIN_STATE(consumer)->full_feed_peer_ids[ipv_idx], peerid))
            {

              /* initializing asns for each cell */
              i=0;
              asn1=0;
              asn2=0;
              normal_asn=0;
              prev_asn=0;

              bgpview_iter_pfx_peer_as_path_seg_iter_reset(it);

              while((seg = bgpview_iter_pfx_peer_as_path_seg_next(it)) != NULL)
                {
                  i++;

                  /* checking if a segment is a regular asn */
                  if(seg->type == BGPSTREAM_AS_PATH_SEG_ASN){
                      asn=((bgpstream_as_path_seg_asn_t *) seg)->asn;

                      normal_asn=1;
                  }
                  /* Check whether asn is a regular asn */
                  else{
                    normal_asn=0;
                    prev_asn=0;
                    continue;
                  }

                  if(normal_asn){

                    /* Previous asn was a regular one */
                    if(prev_asn!=0){

                      asn1=asn;
                      asn2=prev_asn;

                      /* Continue if ASN prepending is observed */
                      if(asn==prev_asn){
                        continue;
                      }

                      /* Getting the greater than two ASNs. Assuming edges are not directional */
                      if(asn<prev_asn){
                        asn1=prev_asn;
                        asn2=asn;
                      }

                      // Check whether we have seen this edge before or not. Update respective khashes
                      insert_update_edges(consumer,asn1,asn2,pfx);
                    }
                    prev_asn=asn;
                  }

                }
                prev_asn=0;
              }
          }


    }
  /* Loops through all ongoing edges and checks for stale edges */
  remove_stale_link(consumer);
  /* Print the surviving edges */
  bgpview_iter_destroy(it);
  /* Close file I/O */
  wandio_wdestroy(state->file_newedges);

  char filename[MAX_BUFFER_LEN];
  iow_t *done_newedge = NULL;

  /* generate separate .done files for both edges and triplets*/
  snprintf(filename, MAX_BUFFER_LEN, OUTPUT_FILE_FORMAT_NEWEDGES".done",
           state->output_folder, time_now, state->window_size);
  if((done_newedge = wandio_wcreate(filename, wandio_detect_compression_type(filename), DEFAULT_COMPRESS_LEVEL, O_CREAT)) == NULL)
    {
      fprintf(stderr, "ERROR: Could not open %s for writing\n",filename);
      return -1;
    }
  wandio_wdestroy(done_newedge);


  /* compute processed delay */
  state->processed_delay = zclock_time()/1000 - bgpview_get_time(view);
  state->processing_time = state->processed_delay - state->arrival_delay;

  /* Output timeseries */
  if(output_timeseries(consumer, bgpview_get_time(view)) != 0)
  {
    return -1;
  }

  return 0;
}
