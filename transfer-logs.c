/*
 * transfer-logs.c
 *
 * code for importing exported data from a previous toon into a new one.
 *
 * requires at least root access to the new toon.
 * 
 * Limitations: For monthly data, this code will work only when both toons 
 * have had data pushed from Eneco, for the current year. Otherwise, some data
 * will be lost.
 *
 * 20190514, marcelr, alpha release 0.0.11. First working version with all 
 * functionality completed. Final version before beta testing.
 * 
 * 20190310, first draft, with snippets from others (xml stuff, search code and
 * junzip). Credits are given in the functions applying those snippets.
 */

#define VERSION                      "0.1.0"

#define E_DATABASE_DIR_NOT_FOUND     255
#define E_INSUFFICIENT_CL_ARGS       254
#define E_BAD_DL_URL                 253
#define E_INVALID_DATA_DIR           252
#define E_INVALID_END_DATE           251
#define E_NO_DAT_FILES_FOUND         250
#define E_CANNOT_OPEN_DIR            249

#define MAGIC            "hcb_rrd_09082011A"
#define EXIT_FAILURE     -1
#define MAX_LEN          256
#define LINE_LEN         1024

#undef DEBUG

/* 
 * some of the paths used are macro definitions, since they are the same 
 * on all toons
*/

/*
 * Possible locations for config_hcb_rrd.xml and config_happ_pwrusage.xml:
 * /HCBv/config/config_hcb_rrd.xml or config_happ_pwrusage.xml
 * /mnt/data/qmf/config/config_hcb_rrd.xml, with links from /HCBv2 to /qmf
 * and from /qmf/config to /mnt/data/qmf/config
 * 
 * So, one size fits all:
 */

#define HCB_RRD_CFG      "/HCBv2/config/config_hcb_rrd.xml"
#define PWRUSAGE_CFG     "/HCBv2/config/config_happ_pwrusage.xml"

/*
 * Download path for export.zip
 * Downloaded to a ramdisk, a simple reboot will erase all old data ;-). 
 */ 

#define EXPORTS_LOCATION "/var/volatile/tmp/exports/"

/* flags for .dat file structure and rra contents*/

#define N_SUBSETS         4  /* so far, no more than 2 have been detected in 
				any data set*/
#define INTEGER           1
#define DOUBLE            0

#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <curl/curl.h>

#include <zlib.h>
#include "junzip.h"
#include "ezxml.h"

#include <math.h>

#include <time.h>


/* struct for rra file contents definition */

typedef struct dat_sub_s {
  int unk_0;
  int unk_1;
  int unk_2;
  double value;
  double divider;
  int timestamp_0;
  int timestamp_1;
  int minSamplesPerBin;
  int binL_len;
  char *binLength;
  int file_offset;
  int n_samples;
  int unk_3;
  int int_len;
  char *interval;
  int cons_len;
  char *consolidator;
  struct dat_sub_s *next;
} dat_sub_s;

/* struct for data source and type definition */

typedef struct dat_s {
  char magic[17];
  int  devUuid_len;
  char *deviceUuid;
  char *rrd_device_name;
  int  devVar_len;
  char *deviceVar;
  int  devSvc_len;
  char *deviceSvc;
  int  sampleT_len;
  char *sampleType;
  int  n_sets;
  dat_sub_s *subset[N_SUBSETS];
} dat_s;



/* function declarations */

int    read_pwrusage_and_merge( char*, char*, char* );
int    merge_data( char*, char*, dat_s*, int, int );
char*  get_device_name( char*, char* );
void   print_data( dat_s* );
dat_s* read_dat_file( char* );
char*  get_csv_path( char*, dat_s*, int );
char*  get_rra_path( dat_s*, char*, char*, int );

int    search( int*, int, int, int ); 
void*  read_rra_file( char*, dat_s*, int );
void*  read_csv_data( char*, dat_s*, int, int );
int*   create_rra_time( dat_s*, int );
int*   read_csv_time( char*, dat_s*, int, int );
void*  sort_data_for_rra( dat_s*, int, void*, void*, int*, int* );

size_t write_data(void*, size_t, size_t, FILE* );
CURLcode download_export_zip( char*, char* );

int   unzip( char*, char* );
int   process_file( JZFile*, char* );
int   record_callback( JZFile*, int, JZFileHeader*, char*, void* );
void  write_file( char*, char*, void*, long );
int   make_directory( char* );

int   rra_to_csv( char* );
int   download_exports_and_unzip( char* );
int   inject_data( char*, char*, char* );
void  usage( char* );
void  free_dat_s( dat_s* );
int   dir_exist( char* );
char* find_rra_databases( void );
int   unzip_exports( char* );
int   test_host( char* );
int   test_date( char* );
char* create_backups( char * );

char* sprint_xml( ezxml_t, int, char* );
void  print_xml( ezxml_t, int );
char* prettyprint( ezxml_t );



int main ( int argc, char *argv[] ) 
{
  int i;
  int err;
  int dat_cnt;
  char* dl_dir = NULL;
  char* dl_url = NULL;
  char* max_date = NULL;
  int exp_flag = 0;
  int rra_flag = 0;
  int dl_flag  = 0;
  int dir_flag = 0;
  int dat_flag = 0;
  int backup_flag = 0;

  DIR* dir;
  char* rra_location;
  int j, k, l;

  /* post help msg when called without args */

  if ( argc < 2 ) {
    usage( argv[0] );
    exit( 0 );
  }
  /* echo command line to screen (useful when stdout is redirected to a file) */

  for ( i = 0; i < argc; i++ ) {
    printf( "%s ", argv[i] );
  }
  printf( "\n" );

  /* parse command line args */

  for ( i = 1; i < argc; i++ ) {

    if ( !strcmp( "-h", argv[i] ) ) {
	  usage( argv[0] );
	  return 0;
    }

    if( !strcmp( "-d", argv[i] ) ) {
      dl_url = calloc( MAX_LEN, sizeof ( char ) );
      if ( argv[i+1] != NULL ) {
	sprintf( dl_url, "http://%s", argv[i+1] );
	if ( test_host( dl_url ) != 0 ) {
	  fprintf( stderr, "Invalid IP-address supplied with option -d: %s\n",
		   argv[i+1] );
	  usage( argv[0] );
	  return E_BAD_DL_URL;
	} else  {
	  sprintf( dl_url, "http://%s/export.zip", argv[i+1] );
	  exp_flag = 1;
	  dl_flag  = 1;
	  rra_flag = 0;
	  i++;
	} 
      } else {
	printf("Error: option -d requires an IP-address as extra argument\n");
	usage( argv[0] );
	return E_INSUFFICIENT_CL_ARGS;
      }
    }
    
    /* set default dl_dir */

    dl_dir = calloc( MAX_LEN, sizeof ( char ) );
    sprintf( dl_dir,  EXPORTS_LOCATION ); 
 
    /* set dl_dir directory for data to be imported */

    if( !strcmp( "-u", argv[i] ) ) {

      if ( argv[i+1] != NULL ) {
	sprintf( dl_dir, "%s", argv[i+1] );
	if ( ( dir = opendir( dl_dir ) ) != NULL ) { 
	  strcat( dl_dir, "/" );
	  dir_flag = 1;
	  closedir( dir );
	  i++;
	} else {
	  printf("Error: option -u requires a valid path as extra argument\n");
	  usage( argv[0] );
	  return E_INVALID_DATA_DIR;
	}
      }
      dl_flag = 0;
    }

    /* set imports time limit */

    if ( !strcmp( "-L", argv[i] ) ) {
      max_date = calloc( MAX_LEN, sizeof ( char ) );
      if ( argv[i+1] != NULL ) {
	sprintf( max_date, "%s", argv[i+1] );
	if ( ( test_date( max_date ) == 0 ) | ( test_date( max_date ) == 0x7fffffff ) ) { 
	  printf("Error: option -L requires a valid date (YYYY-mm-dd) as extra argument\n");
	  usage( argv[0] );
	  return E_INVALID_END_DATE;
	} else {	 
	  dat_flag = 1;
	  i++;
	}
      }	
    }

    /* read already downloaded exports */
      
    if( !strcmp( "-e", argv[i] ) ) {
      exp_flag = 1;
      rra_flag = 0;
    }

    /* back-ups on */

    if( !strcmp( "-b", argv[i] ) ) {
      backup_flag = 1;
    }

    if( !strcmp( "-r", argv[i] ) ) {
      exp_flag = 0;
      rra_flag = 1;
    }
  }
  
  /* take action according to cl flags */

  if ( ( !( exp_flag | rra_flag ) ) | 
       ( !(  dl_flag | dir_flag ) ) | 
       ( !( exp_flag | dir_flag ) ) ) {
    fprintf( stderr, "Error: Insufficient or invalid command line arguments\n" );
    usage( argv[0] );
    return E_INSUFFICIENT_CL_ARGS;
  } 

  /* check for existence of local database dir */

  if( ( rra_location = find_rra_databases() ) == NULL ) {
    fprintf( stderr, 
	     "find_rra_databases: Cannot find database directory\n" ); 
    return E_DATABASE_DIR_NOT_FOUND;
  } else {    
    printf( "find_rra_databases: rra database location: %s\n", 
	    rra_location );
  }

  /* thou shalt make back-ups! */

  if ( backup_flag ) {
    fprintf( stderr, "Handling back-ups ...\n" );
    create_backups( rra_location );
    fprintf( stderr, "Back-up completed.\n" );
  }

  if ( ( dir_flag & rra_flag ) == 1 ) {

    /* process config_happ_pwrusage.xml if available */

    read_pwrusage_and_merge( dl_dir, PWRUSAGE_CFG, max_date );

    printf( "Converting old .rra files in %s to .csv format\n", dl_dir );

  } else if ( dir_flag & exp_flag ) {
    
    printf( "Processing export.zip file in %s\n", dl_dir );
    err = unzip_exports( dl_dir );
    
  } else if ( dl_flag & exp_flag ) {
    
    printf( "Processing export file: %s\n", dl_url );
    err = download_exports_and_unzip( dl_url );
    free( dl_url );

  } else {
  
    fprintf( stderr, "\nImpossible error. You have reached unreachable code :-)\n\n" );
    fprintf( stderr, "Please report this, along with the program call and all its output\n");
    fprintf( stderr, "to marcelr at the domotica forum (domoticaforum.eu)\n");

  }
    
  if ( dl_flag ) {
    rra_flag = 0;
  }
  

  /* process uploaded .rra databases */
  
  if ( rra_flag & dir_flag ) {

    /* preprocess all old rra databases */

    dat_cnt = rra_to_csv( dl_dir );
    printf("%d old .dat files found \n", dat_cnt );
  }
  
  /* inject old data into .rra databases */
  
  if ( dat_flag ) {
    printf( "Processing data generated until %s, midnight\n", max_date );
  }
  
  dat_cnt = inject_data( rra_location, dl_dir, max_date );
  

  return 0;
}



int read_pwrusage_and_merge( char *pwrusage_path_o, char *pwrusage_path_n, 
			     char *max_date ) {

  FILE* fp_o;
  FILE* fp_n;
  ezxml_t doc_n;
  ezxml_t doc_o;
  ezxml_t doc_w;

  ezxml_t monthInfo_o;
  ezxml_t monthInfo_n;
  ezxml_t monthInfo_i;
  ezxml_t monthInfo_w;
  ezxml_t year_o;
  ezxml_t month_o;
  ezxml_t type_o;
  ezxml_t year_n;
  ezxml_t month_n;
  ezxml_t type_n;

  ezxml_t tag;
  ezxml_t tag_w;

  char path_o[1024];
  char *txt_o;
  char *txt_n;

  char *year_o_s;
  char *year_n_s;
  char *month_o_s;
  char *month_n_s;
  char *month_w_s;
  char *type_o_s;
  char *type_n_s;
  static char *doc_out = 0;

  int write_flag;
  int idx_cnt;
  int cnt;
  int index_w;
  int index_n;
  int index;
  time_t max_time;
  time_t act_time;

  struct tm loc_time = {0};

  strcat( path_o, pwrusage_path_o );
  strcat( path_o, "/config_happ_pwrusage.xml" );

  printf("\nCopying monthly data as stored in config_happ_pwrusage.xml\n\n");

  if ( ( fp_o = fopen( path_o , "r" ) ) > 0 ) {

    if ( ( fp_n = fopen( pwrusage_path_n , "rw" ) ) > 0 ) {

      /* read new data */ 

      if ( ( doc_n = ezxml_parse_file( pwrusage_path_n ) ) == NULL ) {
	fclose( fp_n );
	fprintf( stderr, "ezxml_parse_file returned NULL\n");
	return -4;
      }
      fclose( fp_n );

    } else {
      fprintf( stderr, 
	       "Cannot open new config_happ_pwrusage.xml for reading/writing\n" );
      return -2;
    }

    /* read old data */

      if ( ( doc_o = ezxml_parse_file( path_o ) ) == NULL ) {
	fclose( fp_o );
	fprintf( stderr, "ezxml_parse_file returned NULL\n");
	return -3;
      }
      fclose( fp_o );

 } else {
    fprintf( stderr, 
	     "Cannot open old config_happ_pwrusage.xml for reading\n" );
    return -1;
  }

  /* parse file contents and merge */
 
  /* create new struct for writing data */

  doc_w = ezxml_new( "toFile" );
  index = 0;
   
  for ( monthInfo_o = ezxml_child( doc_o, "monthInfo" ); monthInfo_o; 
	monthInfo_o = monthInfo_o->next ) {

    write_flag = 0;

    month_o = ezxml_child( monthInfo_o, "month" );
    year_o  = ezxml_child( monthInfo_o, "year" );
    type_o  = ezxml_child( monthInfo_o, "type" );

    year_o_s  = ezxml_txt( year_o );
    month_o_s = ezxml_txt( month_o );
    type_o_s  = ezxml_txt( type_o );

    printf("Copying      config_happ_pwrusage.xml (old): year: %d, month: %2d, type: %s\n",  
	   atoi(year_o_s) + 1900, atoi(month_o_s) + 1, type_o_s );

    /* replace existing entries in new data file */

    idx_cnt = 0;

    for ( monthInfo_n = ezxml_child( doc_n, "monthInfo" ); monthInfo_n; 
	  monthInfo_n = monthInfo_n->next ) {

     if ( idx_cnt == 0 ) {
	index_w = (int)monthInfo_n->off;
      }

      idx_cnt++;

      month_n = ezxml_child( monthInfo_n, "month" );
      year_n  = ezxml_child( monthInfo_n, "year" );
      type_n  = ezxml_child( monthInfo_n, "type" );
 
      year_n_s  = ezxml_txt( year_n );
      month_n_s = ezxml_txt( month_n );
      type_n_s  = ezxml_txt( type_n );
            
      if ( !( strcmp( year_o_s,  year_n_s  ) | 
	      strcmp( month_o_s, month_n_s ) |
	      strcmp( type_o_s,  type_n_s  ) ) ) {

	/* check time limit */

	max_time = test_date( max_date );
	loc_time.tm_year = atoi( year_o_s );
	loc_time.tm_mon  = atoi( month_o_s );
	loc_time.tm_mday = 1;

	act_time = mktime( &loc_time );

	if ( ( max_time  > act_time ) ) {
	  printf("Overwriting  config_happ_pwrusage.xml (new): year: %d, month: %2d, type: %s\n\n",  
		 atoi(year_n_s) + 1900, atoi(month_n_s) + 1, type_n_s );

	/* copy data into new xml struct: */

	  month_w_s = ezxml_toxml( monthInfo_o );
	} else {

	  /* keep new entries that weren't available from the old data */

	  printf("Keeping      config_happ_pwrusage.xml (new): year: %d, month: %2d, type: %s\n\n",  
		 atoi(year_n_s) + 1900, atoi(month_n_s) + 1, type_n_s );
	  month_w_s = ezxml_toxml( monthInfo_n );
	}

	monthInfo_w = ezxml_parse_str( month_w_s, strlen( month_w_s ) );
	ezxml_move( monthInfo_w, doc_w, index );
	index++;
	
	write_flag = 1;
      }
    }
    
    if ( write_flag == 0 ) {
 
      /* 
       * Entry wasn't available in new file, so add it, irrespective of a 
       * user-set time limit.
       */

      cnt = 0;

      /* get all children in this tag */

      tag = monthInfo_o->child;
      if ( tag != NULL ) {

	monthInfo_w = ezxml_new("monthInfo");
	printf( "  tag: %32s: ",  tag->name );
	printf( "%s\n", tag->txt );
	tag_w = ezxml_new( tag->name );
	tag_w = ezxml_set_txt( tag_w, tag->txt);
       	ezxml_move( tag_w, monthInfo_w, cnt );

	while ( tag = tag->sibling ) {
	  cnt++;
	  printf( "  tag: %32s: ",  tag->name );
	  printf( "%s\n", tag->txt );
	  tag_w = ezxml_new( tag->name );
	  tag_w = ezxml_set_txt( tag_w, tag->txt);
	  ezxml_move( tag_w, monthInfo_w, cnt );
	} 
      }

      /* insert after the already available data */

	printf("Writing into config_happ_pwrusage.xml (new): year: %d, month: %2d, type: %s\n\n",  atoi(year_o_s) + 1900, atoi(month_o_s) + 1, type_o_s );

      ezxml_move( monthInfo_w, doc_w, index );
      index++;
    }
  } /*  end for ( monthInfo_o = ezxml_child ...  */ 

  /* all data gathered, now replace in the output xml struct */

  while ( ezxml_child( doc_n, "monthInfo" ) ) {
    ezxml_remove( ezxml_child( doc_n, "monthInfo" ) );
  }
  while ( ezxml_child( doc_w, "monthInfo" ) ) {
    ezxml_move( ezxml_child( doc_w, "monthInfo" ), doc_n, index_w );
    index_w++;
  }

  /* write modified data to file */

  doc_out = prettyprint( doc_n );

  //fp_n = fopen( "test1212.xml", "w" );
  fp_n = fopen( pwrusage_path_n, "w" );
  fprintf( fp_n, "%s\n", doc_out );
  fclose( fp_n );

  /* clean up */

  ezxml_free( doc_n );
  ezxml_free( doc_o );
	  
  return 0;
}


char* prettyprint( ezxml_t root ) {
  char* doc;
  doc = calloc( 1, 1 );
  doc = sprint_xml( root, 2, doc );
  return doc;
}


char* sprint_xml( ezxml_t node, int indent_len, char* out ) {
  int cnt;
  int len;
  int len_t;
  char* tmp;

  while( node ) {
 
    tmp = calloc( 256, sizeof( char ) );

    /* print 1st part of opening tag with or without attributes */

    printf( "%*c<%s",  indent_len*2, ' ', node->name );
    sprintf( tmp, "%*c<%s",  indent_len*2, ' ', node->name );
    out = realloc( out, strlen( out ) + strlen( tmp ) + 1 );
    strcat( out, tmp );
    free( tmp );

    /* add attributes if present */

    cnt = 0;
    while ( *(node->attr+cnt) != NULL ) {
     
      printf( " %s=\"%s\"", *(node->attr+cnt), *(node->attr+cnt+1) );
      tmp = calloc( 256, sizeof( char ) );
      sprintf( tmp, " %s=\"%s\"", *(node->attr+cnt), *(node->attr+cnt+1) );
      out = realloc( out, strlen( out ) + strlen( tmp ) + 1 );
      strcat( out, tmp );
      free( tmp );
      
      cnt += 2;
    }
    
    if ( node->child ) {

      /* close opening tag and move on to children */
      printf( ">\n" );
      tmp = calloc( 256, sizeof( char ) );
      sprintf( tmp,  ">\n" );
      out = realloc( out, strlen( out ) + strlen( tmp ) + 1 );
      strcat( out, tmp );
      free( tmp );
      out = sprint_xml( node->child, indent_len + 1, out );
      
      /* print closing tag for parent */

      tmp = calloc( 256, sizeof( char ) );
      printf( "%*c</%s>\n", (indent_len) * 2, ' ', node->name );
      sprintf( tmp, "%*c</%s>\n", (indent_len) * 2, ' ', node->name );
      out = realloc( out, strlen( out ) + strlen( tmp ) + 1 );
      strcat( out, tmp );
      free( tmp );

    } else {

      /* print value and closing tag */
      printf( ">%s</%s>\n", node->txt, node->name );
      tmp = calloc( 256, sizeof( char ) );
      sprintf( tmp,  ">%s</%s>\n", node->txt, node->name );
      out = realloc( out, strlen( out ) + strlen( tmp ) + 1 );
      strcat( out, tmp );
      free( tmp );
      out = sprint_xml( node->child, indent_len + 1, out );
    }
    
    if ( node->sibling ) {
      out = sprint_xml( node->sibling, indent_len, out );
    } 

    node = node->next;

  }
  return out;
}

void print_xml( ezxml_t node, int indent_len ) {
  int cnt;
  int len;
  int len_t;
  char* out;
  char* tmp;

  while( node ) {

    /* print 1st part of opening tag with or without attributes */

    printf( "%*c<%s",  indent_len*2, ' ', node->name );

    /* add attributes if present */

    cnt = 0;
    while ( *(node->attr+cnt) != NULL ) {

      printf( " %s=\"%s\"", *(node->attr+cnt), *(node->attr+cnt+1) );
      cnt += 2;
    }
    
    if ( node->child ) {

      /* close opening tag and move on to children */
      printf( ">\n" );
      print_xml( node->child, indent_len + 1 );
      
      /* print closing tag for parent */

      printf( "%*c</%s>\n", (indent_len) * 2, ' ', node->name );

    } else {

      /* print value and closing tag */
      printf( ">%s</%s>\n", node->txt, node->name );
    }
    
    if ( node->sibling ) {
      print_xml( node->sibling, indent_len );
    } 

    node = node->next;

  }
}


int merge_data( char *csv_path, char *rra_path, dat_s *data, int subset, 
		int max_time ) {

  FILE *fp_csv;
  FILE *fp_csv_out;
  FILE *fp_rra;

  int j;

  int *time_csv;
  int *int_csv;
  double *dble_csv;

  int *time_rra;
  int *int_rra;
  double *dble_rra;

  int data_type;

  char *csv_out_path;
  char *rra_out_path;
  char *ext_ptr;

  int *int_temp;
  double *dble_temp;


  if ( fp_csv = fopen( csv_path, "r" ) ) {

    /*
     * get data type for this file
     */

    if ( !strcmp( data->sampleType, "integer" ) ) {
      data_type = INTEGER;
    } else {
      data_type = DOUBLE;
    }

    /* read data from rra and csv files, store in vectors and sort */

    time_rra = create_rra_time( data, subset );
    time_csv = read_csv_time( csv_path, data, subset, max_time );

    if ( data_type ) {
      int_rra = read_rra_file( rra_path, data, subset );
      int_csv = read_csv_data( csv_path, data, subset, max_time );
      int_temp = sort_data_for_rra( data, subset, int_csv, int_rra, 
				    time_csv, time_rra );
    } else {
      dble_rra = read_rra_file( rra_path, data, subset );
      dble_csv = read_csv_data( csv_path, data, subset, max_time );
      dble_temp = sort_data_for_rra( data, subset, dble_csv, dble_rra, 
				     time_csv, time_rra );
   }

    /* debugging  printout */

#ifdef DEBUG

    if ( data->subset[subset]->n_samples != 0 ) {
      
      csv_out_path = calloc( MAX_LEN, sizeof( char ) );
      strcpy( csv_out_path, csv_path );
      ext_ptr = strstr( csv_out_path, ".csv" );
      sprintf( ext_ptr, "%s", ".CSV" );
      
      printf("csv_out_path    : %s\n", csv_out_path );
      fp_csv_out = fopen( csv_out_path, "w" );
      
      for ( j = 0; j< data->subset[subset]->n_samples; j++ ) {
	if ( data_type ) {
	  fprintf(fp_csv_out, "%d, %d, %d, %d, %d\n", 
		  time_csv[j], int_csv[j], time_rra[j], int_rra[j], 
		  int_temp[j] );
	} else {
	  fprintf(fp_csv_out, "%d, %lf, %d, %lf, %lf\n", 
		  time_csv[j], dble_csv[j], time_rra[j], dble_rra[j], 
		  dble_temp[j] );
	}
      }
      fclose( fp_csv_out );
      free( csv_out_path ) ;
    }

#endif

    /* write to .rra file */

    rra_out_path = malloc( MAX_LEN );
    strcpy( rra_out_path, rra_path );
    ext_ptr = strstr( rra_out_path, ".rra" );
    //sprintf( ext_ptr, "%s", ".RRA" );
      
    printf("rra_out_path    : %s\n", rra_out_path );

    if ( fp_rra = fopen( rra_out_path, "wb" ) ) {
	if ( data_type ) {
	  fwrite( int_temp, sizeof( int ), data->subset[subset]->n_samples,
		  fp_rra );
	} else {
	  fwrite( dble_temp, sizeof( double ), data->subset[subset]->n_samples,
		  fp_rra );
	}
      fclose (fp_rra );
      free( rra_out_path );
   } else {
      printf("merge_data: Cannot open %s for writing\n", rra_out_path );
    }

   /* clean up */
    
    if ( data_type ) {
      free( int_csv );
      free( int_rra );
      free( int_temp );
    } else {
      free( dble_csv );
      free( dble_rra );
      free( dble_temp );
    }
    
    free( time_csv );
    free( time_rra );

    fclose ( fp_csv );
    
  } else {
    printf("merge_data: Cannot open file %s for reading\n", csv_path );
  }
  return 0;
}



char *get_device_name( char *xml_file, char *uuid ) {

  ezxml_t doc_;
  ezxml_t uuid_;
  ezxml_t rrdlogger_;
  ezxml_t name_;
  
  char *device_name;

  device_name = calloc( LINE_LEN,  sizeof( char ) );

  if ( ( doc_ = ezxml_parse_file( xml_file ) ) != NULL ) {

    for ( rrdlogger_ = ezxml_child( doc_, "rrdLogger" ); rrdlogger_; 
	  rrdlogger_ = rrdlogger_->next ) {
      
      uuid_ = ezxml_child( rrdlogger_, "uuid" );
      name_ = ezxml_child( rrdlogger_, "name" );

      if ( !strcmp( uuid,  ezxml_txt( uuid_ ) ) ) {
	strcpy( device_name, ezxml_txt( name_ ) );
	ezxml_free( doc_ );

	return device_name;
      }   
    }
  } else {
    fprintf( stderr, "unable tot open xml file: %s\n", xml_file ); 
  }
  ezxml_free( doc_ );
  free( device_name );

  return NULL;

}



void print_data( dat_s* data ) {

  int j;

  printf( "magic number    : %s\n", data->magic );
  printf( "deviceUuid size : %d\n", data->devUuid_len );
  printf( "deviceUuid      : %s\n", data->deviceUuid );
  printf( "deviceVar size  : %d\n", data->devVar_len );
  printf( "deviceVar       : %s\n", data->deviceVar );
  printf( "device_name     : %s\n", data->rrd_device_name );
  printf( "deviceSvc size  : %d\n", data->devSvc_len );
  printf( "deviceSvc       : %s\n", data->deviceSvc );
  printf( "sampleType size : %d\n", data->sampleT_len );
  printf( "sampleType      : %s\n", data->sampleType );
  printf( "nr of subsets   : %d\n", data->n_sets );
  
  for ( j = 0; j < N_SUBSETS; j++ ) {
    printf( "unk_0           : %d\n", data->subset[j]->unk_0 );
    printf( "unk_1           : %d\n", data->subset[j]->unk_1 );
    printf( "unk_2           : %d\n", data->subset[j]->unk_2 );
    printf( "value           : %.3f\n", data->subset[j]->value );
    printf( "divider         : %.3f\n", data->subset[j]->divider );
    printf( "timestamp_0     : %d\n", data->subset[j]->timestamp_0 );
    printf( "timestamp_1     : %d\n", data->subset[j]->timestamp_1 );
    printf( "minSamplesPerBin: %d\n", data->subset[j]->minSamplesPerBin );
    printf( "binLength size  : %d\n", data->subset[j]->binL_len );
    printf( "binLength       : %s\n", data->subset[j]->binLength );
    printf( "file offset     : %d\n", data->subset[j]->file_offset );
    printf( "n_samples       : %d\n", data->subset[j]->n_samples );
    printf( "unk_3           : %d\n", data->subset[j]->unk_3 );
    printf( "int_len         : %d\n", data->subset[j]->int_len );
    printf( "interval        : %s\n", data->subset[j]->interval );
    printf( "cons_len        : %d\n", data->subset[j]->cons_len );
    printf( "consolidator    : %s\n", data->subset[j]->consolidator );
    printf( "next_subset ptr : 0x%08x\n", data->subset[j]->next );

    if ( data->subset[j]->next == NULL ) {
      break;
      break;
    }
  }
} 


dat_s *read_dat_file( char * dir ) { 

  dat_s *data;
  FILE *fp;

  int done;
  int cnt;
  int j;
  int cnt2;
  char dummy;
  
  data = calloc( 1, sizeof( dat_s ) );
  
  fp = fopen( dir , "r" );
  fread( data->magic, 1, 17, fp ); 
  
  if ( ! strcmp( data->magic, MAGIC ) ) {
    
    /* If the file magic number fits, read on */
    
    fread( &data->devUuid_len, sizeof( int ), 1, fp );
    data->deviceUuid = calloc( data->devUuid_len, sizeof( char ) );
    fread( data->deviceUuid, 1, data->devUuid_len, fp );
    
    /* 
     * read device uuid and other device identifiers.
     */
    
    fread( &data->devVar_len, sizeof( int ), 1, fp );
    data->deviceVar = calloc( data->devVar_len, sizeof( char ) );
    fread( data->deviceVar, 1, data->devVar_len, fp );
    
    fread( &data->devSvc_len, sizeof( int ), 1, fp );
    data->deviceSvc = calloc( data->devSvc_len, sizeof( char ) );
    fread( data->deviceSvc, 1, data->devSvc_len, fp );
    
    fread( &data->sampleT_len, sizeof( int ), 1, fp );
    data->sampleType = calloc( data->sampleT_len, sizeof( char ) );
    fread( data->sampleType, 1, data->sampleT_len, fp );
    
    done = 0;
    j = 0;
    do {
      /* 
       * Workaround for not yet initialized databases. 
       * Uuid is only assigned when there has been contact with the 
       * meter adapter first. Until then, the word "placeholder"
       * is used as uuid. 
       */

      if ( strcmp( data->deviceUuid, "placeholder" ) != 0 ) {      
	
	data->subset[j] = calloc( 1, sizeof( dat_sub_s ) );
	data->n_sets = j + 1; 
	if ( j > 0 ) {
	  data->subset[j-1]->next = data->subset[j];
	}
	
	cnt = 0;
	
	if ( ! strcmp( data->sampleType, "integer" ) ) {
	  fread( &data->subset[j]->unk_0, sizeof( int ), 1, fp );
	  fread( &data->subset[j]->unk_1, sizeof( int ), 1, fp );
	  fread( &data->subset[j]->unk_2, sizeof( int ), 1, fp );
	  data->subset[j]->value   = 0.0;
	  data->subset[j]->divider = 0.0;
	} else {
	  
	  /* double data type */
	  
	  fread( &data->subset[j]->value, sizeof( double ), 1, fp );
	  fread( &data->subset[j]->divider, sizeof( double ), 1, fp );
	  data->subset[j]->unk_0 = 0;
	  data->subset[j]->unk_1 = 0;
	  data->subset[j]->unk_2 = 0;
	}
	
	cnt += fread( &data->subset[j]->timestamp_0, sizeof( int ), 1, fp );
	cnt += fread( &data->subset[j]->timestamp_1, sizeof( int ), 1, fp );
	cnt += fread( &data->subset[j]->minSamplesPerBin, sizeof( int ), 1,
		      fp );
	
	cnt2 = fread( &data->subset[j]->binL_len, sizeof( int ), 1, fp );
	if ( cnt2 )
	  data->subset[j]->binLength = 
	    calloc( data->subset[j]->binL_len, sizeof( char ) );
	cnt += cnt2;
	cnt += fread( data->subset[j]->binLength, 1, data->subset[j]->binL_len,
		      fp );
	
	cnt += fread( &data->subset[j]->file_offset, sizeof( int ), 1, fp );
	cnt += fread( &data->subset[j]->n_samples, sizeof( int ), 1, fp );
	cnt += fread( &data->subset[j]->unk_3, sizeof( int ), 1, fp );
	
	cnt2 = fread( &data->subset[j]->int_len, sizeof( int ), 1, fp );
	
	if ( cnt2 ) 
	  data->subset[j]->interval = 
	    calloc( data->subset[j]->int_len, sizeof( char ) );
	cnt += cnt2;
	cnt += fread( data->subset[j]->interval, 1, data->subset[j]->int_len, 
		      fp );
	
	cnt2 = fread( &data->subset[j]->cons_len, sizeof( int ), 1, fp );
	if ( cnt2 ) 
	  data->subset[j]->consolidator = 
	    calloc( data->subset[j]->cons_len, sizeof( char ) );
	cnt += cnt2;
	cnt += fread( data->subset[j]->consolidator, 1, 
		      data->subset[j]->cons_len, fp );
	
	/* 
	 * Jump out of the loop when .dat file has trailing bytes. 
	 * Workaround for poor .dat file rewriting code.
	 */
	if ( cnt <  (data->subset[j]->cons_len + data->subset[j]->int_len + 
		     data->subset[j]->binL_len + 9 ) ) {
	  printf( "dat file is partly corrupted, continuing ...\n" );

	  data->subset[j-1]->next = NULL;
	  data->n_sets --;
	  done = 1;		  
	  continue;
	}
	
	/* check if we reached EOF */
	
	if ( fread( &dummy, 1, 1, fp ) ) {
	  fseek( fp, -1, SEEK_CUR );
	} else {
	  done = 1;
	}
	
	j++;
	
      } else {
	done = 1;
      } /* end if placeholder */
      
    } while ( done == 0 );
  } else {
    printf( "Bad magic number\n" );
  }
  return data;

}



char *get_csv_path( char* csv_dir, dat_s* data, int subset ) {

  char* csv_path;
  char *csv_name;
  char *csv_name_1;
  char *csv_name_2;

  if ( ( subset >=0 ) & ( subset < data->n_sets ) ) {
    if ( data->rrd_device_name != "" ) {
      
      csv_path = malloc( MAX_LEN );
      csv_name = malloc( MAX_LEN );
      
      csv_name_1 = data->deviceVar;
      csv_name_2 = data->subset[subset]->interval;
      
      if ( strncmp( data->rrd_device_name, "thermstat", 9 ) == 0 ) {
	sprintf( csv_name, "%s_%s.csv", 
		 data->rrd_device_name, csv_name_2 );
      } else {
	sprintf( csv_name, "%s_%s_%s.csv", 
		 data->rrd_device_name, csv_name_1, csv_name_2 );
      }
      
      csv_path = strcpy( csv_path, csv_dir ); 
      csv_path = strcat( csv_path, csv_name ); 
      printf("csv_path        : %s\n", csv_path );
      free( csv_name );
      return csv_path; 
    } else {
      return NULL;
    }
  } else {
    printf("transfer-logs: get_csv_path: subset out of range\n");
    return NULL;
  }
}



char *get_rra_path( dat_s* data, char *uuid, char* loc, int subset ) {

  char *rra_path;
  char *rra_name_2;

  if ( ( subset >=0 ) & ( subset < data->n_sets ) ) {
    if ( data->rrd_device_name != "" ) {
      
      rra_path = malloc( MAX_LEN );
      rra_name_2 = data->subset[subset]->interval;
      
      sprintf( rra_path , "%s%s-%s.rra", loc,
       	       uuid, rra_name_2 );
       printf("rra_path        : %s\n", rra_path );
     return rra_path; 
    } else {
      return NULL;
    }
  } else {
    printf( "get_rra_path: subset out of range\n");
    return NULL;
  }
}


int find_max( int * vec, int n ) {
  int i;
  int max;
  int j;

  max = vec[0];
  j = 0;

  for ( i = 1; i < n; i++ ) {
    if ( vec[i] > max ) {
      max = vec[i]; 
      j = i;
    }
  }
  return j;
}

/*
 * Returns index of key in arr[l..h] if key is present, otherwise returns -1 
 * retrieved from: 
 * https://www.geeksforgeeks.org/search-an-element-in-a-sorted-and-pivoted-array
 * 20190302
 *
 * code suggested by Gaurav Ahirwar
 */

int search( int arr[], int l, int h, int key ) { 
  if ( l > h ) 
    return -1; 
  
  int mid = ( l + h ) / 2; 
  if ( arr[mid] == key ) 
    return mid; 
  
  /* If arr[l...mid] is sorted */
  if ( arr[l] <= arr[mid] ) { 
    /* 
     * As this subarray is sorted, we can quickly check if key lies in half 
     * or other half 
     */
    if ( key >= arr[l] && key <= arr[mid] ) 
      return search( arr, l, mid-1, key ); 
    
    return search( arr, mid+1, h, key ); 
  } 
  
  /* 
   * If arr[l..mid] is not sorted, then arr[mid... r] must be sorted
   */
  if ( key >= arr[mid] && key <= arr[h] ) 
    return search( arr, mid+1, h, key ); 
  
  return search( arr, l, mid-1, key ); 
} 



void* read_rra_file( char* rra_path, dat_s* data, int subset) {
  FILE* fp;
  int data_type;
  int* int_rra;
  double* dble_rra;
  int j;

  if ( fp = fopen( rra_path, "r" ) ) {

    if ( !strcmp( data->sampleType, "integer" ) ) {
      data_type = INTEGER;
      int_rra =  calloc( data->subset[subset]->n_samples, sizeof( int ) );
    } else {
      data_type = DOUBLE;
      dble_rra = calloc( data->subset[subset]->n_samples, sizeof( double ) );
    }
    
    for ( j = 0; j< data->subset[subset]->n_samples; j++ ) {
      if ( data_type ) {
	fread( &int_rra[j],  sizeof( int ), 1, fp );
      } else {
	fread( &dble_rra[j],  sizeof( double ), 1, fp );
      }
    }
    
    if ( data_type ) {
      return int_rra; 
    } else {
      return dble_rra;
    }
    
    fclose( fp );
    
  } else {
  
    printf("read_rra_file: Cannot open file %s for reading\n", rra_path );
    return NULL;

  }

}


void* read_csv_data( char* csv_path, dat_s* data, int subset, int t_max ) {  

  FILE* fp;
  int j;
  int data_type;
  int dum;
  int* int_csv;
  double* dble_csv;
  char* line;
  int t;
  int int_tmp;
  double dble_tmp;

   /* read data from csv file and store in vector */

  if ( fp = fopen( csv_path, "r" ) ) {
    if ( !strcmp( data->sampleType, "integer" ) ) {
      data_type = INTEGER;
      int_csv =  calloc( data->subset[subset]->n_samples, sizeof( int ) );
    } else {
      data_type = DOUBLE;
      dble_csv = calloc( data->subset[subset]->n_samples, sizeof( double ) );
    }

    j = 0;

    line = calloc( LINE_LEN, sizeof( char ) );
    
    while ( fgets( line, LINE_LEN, fp ) != NULL ) {
      if ( data_type ) {
	sscanf( line, "%d,%d\n", &t, &int_tmp );
	if ( t <= t_max )
	  int_csv[j] = int_tmp;
      } else  {
	sscanf( line, "%d,%lf\n", &t, &dble_tmp );
	if ( t <= t_max )
	  dble_csv[j] = dble_tmp;
      }
      if ( t <= t_max )
	j++;
    }

    free( line );

    if ( data_type ) {
      return int_csv; 
    } else {
      return dble_csv;
    }
    
    fclose( fp );

  } else {

    printf("read_csv_data: Cannot open file %s for reading\n", csv_path );
    return NULL;

  }
}



int* create_rra_time( dat_s* data, int subset ) {
 
  int j;
  int* time_rra;
  int interval;

  /* create time vector for rra data */
  
  interval = data->subset[subset]->timestamp_1 - 
    data->subset[subset]->timestamp_0;

  time_rra = calloc( data->subset[subset]->n_samples, sizeof( int ) );
  
  time_rra[data->subset[subset]->file_offset] = 
    data->subset[subset]->timestamp_1;
  
  j = data->subset[subset]->file_offset;
  
  do {
    time_rra[j-1] = time_rra[j] - interval;
    j--;
  } while ( j > 0 );
  
  time_rra[data->subset[subset]->n_samples - 1] = time_rra[0] - interval;
  
  j = data->subset[subset]->n_samples - 1;
  
  do {
    j--;
    time_rra[j] = time_rra[j+1] - interval;
  } while ( j > data->subset[subset]->file_offset+1 );
  
  return time_rra;

}



int* read_csv_time( char* csv_path, dat_s* data, int subset, int max_time ) {  

  FILE* fp;
  int j;
  char* dum;
  int* time_csv;
  char* line;
  int time_tmp;
   /* read data from csv file and store in vector */

  if ( fp = fopen( csv_path, "r" ) ) {

    time_csv = calloc( data->subset[subset]->n_samples, sizeof( int ) );

    line = calloc( LINE_LEN, sizeof( char ) );
    dum  = calloc( LINE_LEN, sizeof( char ) );

    j = 0;
    
    while ( fgets( line, LINE_LEN, fp ) != NULL ) {
      sscanf( line, "%d,%s", &time_tmp, dum );
      if( time_tmp <= max_time ) {
	time_csv[j] = time_tmp;
	//fprintf( stderr,"j = %d\n", j );
	j++;
      }
    }
    
    free( line );
    free( dum );

    return time_csv; 
    
    fclose( fp );

  } else {

    printf("read_csv_time: Cannot open file %s for reading\n", csv_path );
    return NULL;

  }
}



void* sort_data_for_rra( dat_s* data, int subset, 
			 void* data_csv, void* data_rra, 
			 int* time_csv, int* time_rra ) {

  int data_type;
  int index;
  int i;
  int t_min, t_max;

  int* int_tmp;
  int* int_rra;
  int* int_csv;

  double* dble_tmp;
  double* dble_rra;
  double* dble_csv;

  /* 
   * decide on data type and copy newest data into output vectors 
   */

  if ( !strcmp( data->sampleType, "integer" ) ) {
    data_type = INTEGER;
    int_csv = data_csv;
    int_rra = data_rra;
    int_tmp =  calloc( data->subset[subset]->n_samples, sizeof( int ) );

    for ( i = 0; i< data->subset[subset]->n_samples; i++ ) {
      // int_tmp[i] = 0x7fffffff;
      int_tmp[i] = int_rra[i];
    }

  } else {
    data_type = DOUBLE;
    dble_csv = data_csv;
    dble_rra = data_rra;
    dble_tmp = calloc( data->subset[subset]->n_samples, sizeof( double ) );

    for ( i = 0; i< data->subset[subset]->n_samples; i++ ) {
      //dble_tmp[i] = NAN;
      dble_tmp[i] = dble_rra[i];
    }
  }
    
  /* 
   * sort csv data according to time vector 
   */

  t_min = time_rra[data->subset[subset]->file_offset+1];
  t_max = time_rra[data->subset[subset]->file_offset];
  
  for ( i = 0; i < data->subset[subset]->n_samples; i++ ) {
    if ( ( time_csv[i]  >= t_min ) &  (time_csv[i]  <= t_max ) ) {
      index = search( time_csv, 0, data->subset[subset]->n_samples, 
		      time_rra[i] );
      if (index >= 0 ) {
	if ( data_type ) {
	  //int_tmp[i] = int_rra[i];
	  int_tmp[i] = int_csv[index];
	} else {
	  //dble_tmp[i] = dble_rra[i];
	  dble_tmp[i] = dble_csv[index];
	}
      }
    }
  }

  if ( data_type ) {
    return int_tmp;
  } else {
    return dble_tmp;
  }
  
}


CURLcode download_export_zip( char* url, char* exp_file ) {
  CURL* curl;
  FILE* fp;
  CURLcode res;

  size_t write_data( void *ptr, size_t size, size_t nmemb, FILE *stream ) {
    size_t written = fwrite( ptr, size, nmemb, stream );
    return written;
  }

  /* copied from curl examples */

  curl = curl_easy_init();
  if ( curl ) {
    fprintf( stderr, "Downloading %s from %s ...", exp_file, url );

    fp = fopen( exp_file,"wb");

    curl_easy_setopt( curl, CURLOPT_URL, url );
    curl_easy_setopt( curl, CURLOPT_WRITEFUNCTION, write_data );
    curl_easy_setopt( curl, CURLOPT_WRITEDATA, fp );
    curl_easy_setopt( curl, CURLOPT_FAILONERROR, 1 );
    curl_easy_setopt( curl, CURLOPT_TIMEOUT, 10 );

    res = curl_easy_perform( curl );

    curl_easy_cleanup( curl );

    fclose( fp );
    fprintf( stderr, " done\n");

    return res;
  } else {
    return (CURLcode)-1;
  }
}



int unzip( char* file, char* path ) {
  
  FILE* fp;
  JZFile* zip;
  JZEndRecord endRecord;
  int retval;

  char* local_path;
  local_path = calloc( MAX_LEN, sizeof( char ) );

  strcpy( local_path, path );  
  strcat( local_path, file );  

  if ( ( fp = fopen( local_path, "r" ) ) > 0 ) {
  
  zip = jzfile_from_stdio_file( fp );
  
  if ( jzReadEndRecord( zip, &endRecord ) ) {
    printf("unzip: Couldn't read ZIP file end record.");
    retval = -1;
    goto endClose;
  }
  
  if ( jzReadCentralDirectory( zip, &endRecord, record_callback, path ) ) {
    printf("unzip: Couldn't read ZIP file central record.");
    retval = -2;
    goto endClose;
  } 
  retval = 0;
  
 endClose:

  zip->close( zip );
  } else {
    fprintf( stderr, "unzip: Cannot open %s for reading\n", local_path );
    retval = -3;
  }
  free( local_path );
  return retval;
}


int record_callback( JZFile *zip, int idx, JZFileHeader *header, 
		    char *filename, void *user_data ) {
  long offset;
  
  offset = zip->tell( zip ); /* store current position */
  
  if ( zip->seek( zip, header->offset, SEEK_SET ) ) {
    printf( "record_callback: Cannot seek in zip file!" );
    return 0; /* abort */
  }
  
  process_file( zip, (char*)user_data ); /* alters file offset */
  
  zip->seek( zip, offset, SEEK_SET ); /* return to position */
  
  return 1; /* continue */
}


int process_file( JZFile *zip, char* dl_path ) {
  JZFileHeader header;
  char filename[1024];
  unsigned char *data;
  
  if ( jzReadLocalFileHeader( zip, &header, filename, sizeof( filename ) ) ) {
    printf( "process_file: Cannot read local file header of %s\n", filename );
    return -1;
  }
  
  if ( ( data = (unsigned char *)malloc( header.uncompressedSize ) ) == 
       NULL) {
    printf( "process_file: Cannot allocate memory\n" );
    return -1;
  }
  
  if ( jzReadData( zip, &header, data ) != Z_OK ) {
    printf( "Couldn't read file data\n" );
    free( data );
    return -1;
  }

  write_file( filename, dl_path, data, header.uncompressedSize );
  free( data );
  
  return 0;
}


  
void write_file( char *filename, char* dl_path, void *data, long bytes ) {
  FILE *out;
  int i;
  char* path;
  
  path = calloc( 1024, sizeof( char ) );
  strcpy( path, dl_path );
  strcat( path, filename );

  out = fopen( path, "w" );

  if ( out != NULL ) {
    fwrite( data, 1, bytes, out ); /* best effort is enough here */
    fclose( out );
  } else {
    fprintf( stderr, "write_file: Cannot open %s for writing\n", path );
  }
  free( path );
}


int make_directory( char *dir ) {
    printf( "mkdir(%s)\n", dir );
    return 0;
}


int rra_to_csv( char* rra_location ) {

  int i;
  int len_o;
  int dir_len_o;

  char* csv_path;
  char* rra_path;
  DIR *dir_old;
  char *dir_o;
  struct dirent *entry_o;
  char *filename_o;
  char *uuid;
  FILE *fp_o;
  struct dat_s *data;
  int dat_cnt = 0;
  char * cfg_path;

  dir_o =  calloc( 2 * MAX_LEN, sizeof( char ) ); 
  memcpy( dir_o, rra_location, strlen( rra_location ) + 1 );
  dir_len_o = (int)strlen( dir_o );

  /* read old rra data and transform to csv data */

  if ( ( dir_old = opendir( dir_o ) ) != NULL ) {

    cfg_path = calloc( MAX_LEN, sizeof( char ) );
    strcpy( cfg_path, rra_location );
    strcat( cfg_path, "config_hcb_rrd.xml" );

    while ( (entry_o = readdir( dir_old) ) != NULL) {
       
      /* find .dat files */
      
      if ( entry_o->d_type == DT_REG ) {

	filename_o = calloc( MAX_LEN, sizeof( char ) ); 	
	strcpy( filename_o, entry_o->d_name );
	len_o = (int)strlen( filename_o );

	if ( !strcmp( ".dat", filename_o + len_o - 4 ) ) {
	  printf( "\nfound .dat file : %s\n", filename_o );
	  dat_cnt ++;

	  /* open it and read */

	  memcpy( dir_o + dir_len_o, filename_o, len_o + 1 );

	  data = read_dat_file( dir_o );

	  if ( strcmp( data->deviceUuid, "placeholder" ) != 0 ) {
	    
	    uuid = calloc( MAX_LEN , sizeof( char ) ); 
	    memcpy( uuid, filename_o, len_o - 4 );
	    printf( "uuid            : %s\n", uuid );
	    data->rrd_device_name = get_device_name( cfg_path, uuid );
	    
	    print_data( data );
	    
	    /* construct filename for old data set */
	    
	    for ( i = 0; i < data->n_sets; i++ ) {
	      if ( data->rrd_device_name != NULL ) {
		csv_path = get_csv_path( rra_location, data, i );
		rra_path = get_rra_path( data, uuid, rra_location, i );
		
		write_data_to_csv( csv_path, rra_path, data, i );
		
		free( csv_path );
		free( rra_path );
	      }
	    }

	    free( uuid );
	  } else {
	    printf("Corresponding database(s) not yet initialised, continuing ...\n"); 
	  }

	  free_dat_s( data );
	}
	free( filename_o );
      } /* end if DT_REG */
    } /* while ... */
   
    closedir( dir_old );
    
    free( cfg_path );

    if ( dat_cnt == 0 ) {
      fprintf( stderr, "Cannot find any .dat files in %s, exiting\n", 
	       rra_location );
      exit( E_NO_DAT_FILES_FOUND );
    } else {
      free( dir_o );
      return dat_cnt;
    }
  } else {
    /* could not open directory */
    perror ("rra_to_csv: opendir: Can't open directory");
    free( dir_o );
    return EXIT_FAILURE;
  }
}


int download_exports_and_unzip( char * url ) {

  int err;
  char* exp_file   = "export.zip";
  char* therm_file = "thermostat.zip";
  char* usage_file = "usage.zip";
  CURLcode res;

  char path[1024];
  char dl_path[1024];
  char cmd[1024];

  /* recursively create directory to downlod into. */

  sprintf(cmd, "mkdir -m 0755 -p %s\n",  EXPORTS_LOCATION );
  err = system( cmd );

  if ( err ) {
    fprintf( stderr, "%s failed.\n", cmd );
  } else {
    strcpy( path, EXPORTS_LOCATION );
    
    strcpy( dl_path, path );
    strcat( dl_path, "/" );
    strcat( dl_path, exp_file );
    
    /* download zip file and unpack */
    
    if (  ( res = download_export_zip( url, dl_path ) ) != CURLE_OK ) {
      printf("Download failed: error %d\n", res );
      exit( EXIT_FAILURE );
    }
    
    /* open zip files and extract all data to /tmp/exports */
    
    fprintf(stderr, "Uncompressing data ... " );
    if ( unzip( exp_file, EXPORTS_LOCATION ) ) { 
      fprintf( stderr, "Error: Unable to unzip %s\n", 
	       strcat( path, exp_file ) );
      exit( EXIT_FAILURE );
    } 
    if ( unzip( therm_file, EXPORTS_LOCATION ) ) { 
      fprintf( stderr, "Error: Unable to unzip %s\n", 
	       strcat( path, therm_file ) );
      exit( EXIT_FAILURE );
    } 
    if ( unzip( usage_file, EXPORTS_LOCATION ) ) { 
      fprintf( stderr, "Error: Unable to unzip %s\n", 
	       strcat( path, usage_file ) );
      exit( EXIT_FAILURE );
    } 
    fprintf(stderr, "done\n");
  }
  return err;
}



int write_data_to_csv( char *csv_path, char *rra_path, dat_s *data, int subset ) {

  FILE *fp_csv;

  int j;

  int *time_rra;
  int *int_rra;
  double *dble_rra;

  int data_type;

  if ( fp_csv = fopen( csv_path, "w" ) ) {

    /*
     * get data type for this file
     */

    if ( !strcmp( data->sampleType, "integer" ) ) {
      data_type = INTEGER;
    } else {
      data_type = DOUBLE;
    }

    /* read data from rra file, store in vectors */

    time_rra = create_rra_time( data, subset );

    if ( data_type ) {
      int_rra = read_rra_file( rra_path, data, subset );
    } else {
      dble_rra = read_rra_file( rra_path, data, subset );
   }

    /* 
     * write to file, skip not-yet-filled positions
     * ( 0x7FFFFFFF and NaN )
     */

    if ( data->subset[subset]->n_samples != 0 ) {
            
      for ( j = 0; j< data->subset[subset]->n_samples; j++ ) {
	if ( data_type ) {
	  if ( int_rra[j] != 0x7fffffff ) {
	    fprintf( fp_csv, "%d, %d\n", time_rra[j], int_rra[j] );
	  }
	} else {
	  if ( !isnan( dble_rra[j]) ) {
	    fprintf( fp_csv, "%d, %.3lf\n", time_rra[j], dble_rra[j] );
	  }
	}
      }
    }

   /* clean up */
    
    if ( data_type ) {
      free( int_rra );
    } else {
      free( dble_rra );
    }
    
    free( time_rra );
    fclose( fp_csv );
    
  } else {
    printf("write_data_to_csv: Cannot open file %s for writing\n", csv_path );
  }
  return 0;
}


int inject_data( char* rra_location, char* dl_dir, char* max_date ) {
  int i;
  int dat_cnt = 0;
  int len_n;
  int dir_len_n;
  DIR *dir_new;

  char *dir_n;

  struct dirent *entry_n;

  char *filename_n;
  char *uuid;

  FILE *fp_n;

  char *csv_path;
  char *rra_path;
  char* csv_dir;

  int max_time;

  struct dat_s *data;
  
  csv_dir = calloc( MAX_LEN, sizeof( char ) );

  if ( dl_dir == NULL ) {
    /* processing downloaded export.zip */
    strcpy( csv_dir, EXPORTS_LOCATION );
  } else {
    /* processing uploaded data, export.zip or otherwise */
    strcpy( csv_dir, dl_dir );
  }

  max_time = test_date( max_date );

  dir_n =  malloc( 2 * MAX_LEN * sizeof( char ) ); 
  memcpy( dir_n, rra_location, strlen( rra_location ) + 1 );
  dir_len_n = (int)strlen( dir_n );
  
  if ( ( dir_new = opendir( dir_n ) ) != NULL ) {
    while ( (entry_n = readdir( dir_new ) ) != NULL) {
      
      /* find .dat files */
      
      if ( entry_n->d_type == DT_REG ) {

	filename_n = calloc( MAX_LEN, sizeof( char ) ); 	
	strcpy( filename_n, entry_n->d_name );
	len_n = (int)strlen( filename_n );

	if ( !strcmp( ".dat", filename_n + len_n - 4 ) ) {
	  printf( "\nfound .dat file : %s\n", filename_n );
	  dat_cnt ++;

	  /* 
	   * flush all file buffers, to make sure all .rra files are 
	   * up-to-date 
	   */
	  sync();

	  /*
	   * open .dat file and read. Arguably, you can read most of the info
	   * (but not all) from the xml file as well.
	   */

	  memcpy( dir_n + dir_len_n, filename_n, len_n + 1 );

	  data = read_dat_file( dir_n );
	  /* 
	   * Workaround for not yet initialized databases. 
	   * Uuid is only assigned when there has been contact with the 
	   * meter adapter first. Until then, the word "placeholder"
	   * is used as uuid. 
	   */
	  if ( strcmp( data->deviceUuid, "placeholder" ) != 0 ) {

	    /* extract uuid for searching config_hcb_rrd.xml for device name */
	    
	    uuid = calloc( MAX_LEN , sizeof( char ) ); 
	    memcpy( uuid, filename_n, len_n - 4 );
	    printf( "uuid            : %s\n", uuid );
	    
	    data->rrd_device_name = get_device_name( HCB_RRD_CFG, uuid );
	    print_data( data );
	    
	    /* construct filename for old data set */
	    
	    for ( i = 0; i < data->n_sets; i++ ) {
	      csv_path = get_csv_path( csv_dir, data, i );
	      rra_path = get_rra_path( data, uuid, rra_location, i );

	      merge_data( csv_path, rra_path, data, i, max_time );
	      
	      free( csv_path );
	      free( rra_path );
	    }
	    free( uuid );
	  } else {
	    printf("Corresponding database(s) not yet initialised, continuing ...\n"); 
	  }
	  free_dat_s( data );
	}
	free( filename_n );
      } /* end if DT_REG */
    } /* while ... */

    closedir( dir_new );
    
  } else {
    /* could not open directory */
    perror ("inject_data: opendir: Can't open directory");
    exit( E_CANNOT_OPEN_DIR );
  }
  
  free( dir_n );

  /* clean up */

  printf("\n%d .dat files read and processed.\n", dat_cnt );
  free( csv_dir );
  return dat_cnt;
}


void usage ( char* exec_name) {
  printf( "version: %s\n", VERSION );
  printf( "\ncall:\n\n%s [-h] [-d <IP>] [-u <directory>] [-L <date>] -[e] [-r] [-b]\n\n", exec_name ); 
  printf( "options:\n" );
  printf( "    -h              Print this help message and exit.\n" );
  printf( "    -d <IP>         Download data from this IP-address. This option implies -e.\n" );
  printf( "    -u <directory>  Read data from this upload directory. Required for options\n" );
  printf( "                    -e and -r.\n" ); 
  printf( "    -e              Read data from an uploaded export.zip file, as created\n" );

  printf( "                    with the data export function in toons with FW 4.16.10\n" ); 
  printf( "                    or newer.\n" );
  printf( "                    Use this option in combination with -u.\n" );
  printf( "    -r              Read data from .rra files. For this option to work, \n" );
  printf( "                    upload all .rra and corresponding .dat files to the\n" );
  printf( "                    the directory set with option -u.\n" );
  printf( "                    Make sure to put the files config_hcb_rrd.xml and\n" ); 
  printf( "                    config_happ_pwrusage.xml in that same directory as well.\n" );
  printf( "    -L <date>       Process data until (and including) this date, with\n" );
  printf( "                    <date> in the form YYYY-mm-dd, e.g., 2019-03-09.\n" );
  printf( "                    Monthly data will be processed until the last full month\n" );
  printf( "                    before this date, when available.\n" );
  printf( "    -b              Create back-ups of the rra databases and corresponding\n" );
  printf( "                    .dat files. This option also creates a script to restore\n" );
  printf( "                    the back-ups, in case something goes wrong. The script\n" );
  printf( "                    is called restore_logs.sh and is stored with the data in\n" );
  printf( "                    /HCBv2/rra_backups_<POSIX_timestamp>.\n" );
  printf( " \nThis software will only work when your toon has been connected to a meter\nadapter previously. Prior to this first contact, no databases exist on your\ntoon, so there's nothing to write data into.\n" ); 
  printf( "\nPlease note that at least one choice of data files to be imported into the new\ndatabases is mandatory (options -d, -u/-r or -u/-e).\n\n");
  printf( "The new data become available after rebooting your toon.\n\n" );
  printf( "For the best results, apply options -u/-r, with all files uploaded.\nThe toon-generated export.zip file contains far less old data.\n\n" );
  printf( "Especially in the testing phase of this code, option -b is HIGHLY recommended.\n\n" );
}


void free_dat_s( dat_s *data ) {

  /* free dat_s struct memory */

  int i;
  for ( i = 0; i < N_SUBSETS; i++ ) {
    if ( data->subset[i] ) {
      free( data->subset[i]->consolidator );
      free( data->subset[i]->interval );
      free( data->subset[i]->binLength );
      free( data->subset[i] );
    }
  }
  free( data->sampleType );
  free( data->deviceSvc );
  free( data->deviceVar );
  free( data->rrd_device_name );
  free( data->deviceUuid );
  free( data );
}


char* find_rra_databases( void ) {

  char* rra_location;

  rra_location = calloc( MAX_LEN, sizeof (char ) );

  /* 
   * Possible rra locations: 
   *
   * /HCBv2/data/hcb_rrd/
   * /qmf/var/hcb_rrd/ 
   *
   * This means that paths need to be generated on the fly.
   */

 
  if ( !dir_exist( "/HCBv2/data/hcb_rrd/" ) ) { 
    /* FW 1.9.10 - 4.4.21 (?) */
    strcpy( rra_location, "/HCBv2/data/hcb_rrd/" );
    return rra_location;
  } else if ( !dir_exist( "/qmf/var/hcb_rrd/" ) ) {
    /* later versions */
    strcpy( rra_location, "/qmf/var/hcb_rrd/" );
    return rra_location;
  } else {
    free( rra_location );
    return NULL;
  }
}


int dir_exist( char* path ) {

  DIR* dir = opendir( path );

  if ( dir ) {
    /* Directory exists. */
    closedir( dir );
    return 0;
  } else if ( ENOENT == errno ) {
    /* Directory does not exist. */
    return -1;
  } else {
    /* opendir() failed for some other reason. */
    return -2;
  }
}



int unzip_exports( char* path ) {

  int err;
  char* file;

  file = calloc( MAX_LEN, sizeof( char ) );
  strcpy( file, path );
  /* open zip files and extract all data to path */
  
  fprintf(stderr, "Uncompressing data ... " );

  if ( unzip( "export.zip", path ) ) { 
    fprintf( stderr, "Error: Unable to unzip %s%s\n", path, "export.zip" );
    exit( EXIT_FAILURE );
  } 

  strcpy( file, path );
  if ( unzip( "thermostat.zip", path ) ) { 
    fprintf( stderr, "Error: Unable to unzip %s%s\n", path, "thermostat.zip" );
    exit( EXIT_FAILURE );
  } 

  strcpy( file, path );
  if ( unzip( "usage.zip", path ) ) { 
    fprintf( stderr, "Error: Unable to unzip %s%s\n", path, "usage.zip" );
    exit( EXIT_FAILURE );
  }
 
  fprintf( stderr, "done\n" );
  free( file );
  return err;
}


int test_host( char* url ) {
  CURL *curl;
  CURLcode res = CURLE_OK;

  /* taken from curl examples */

  curl = curl_easy_init();

  if ( curl ) {
    curl_easy_setopt( curl, CURLOPT_URL, url );

    /* Do not do the transfer - only connect to host */

    curl_easy_setopt( curl, CURLOPT_CONNECT_ONLY, 1L );
    res = curl_easy_perform( curl );

    curl_easy_cleanup( curl );
  }

   return (int)res;
}


int test_date( char* date ) {
  struct tm tm = { 0, 0, 0, 0, 0, 0, 0, 0, 0 };
  int t = 0x7fffffff;

  /* return input date as POSIX timestamp, cast to int */

  if ( date != NULL ) {
    if ( strptime( date, "%Y-%m-%d", &tm ) != 0 ) {
      t = (int)mktime( &tm );
      t += 86400;
    } 
  }
  return t;
}



char* create_backups( char* rra_location ) {

  char* backup_dir = "/HCBv2/rra_backups";
  char* cmd;
  char* dir;
  char* restore_script;
  int err;

  FILE* fp;

  struct timeval *tv;

  tv = calloc( 1, sizeof( struct timeval ) );

  gettimeofday( tv, NULL );

  cmd = calloc( MAX_LEN, sizeof( char ) );
  dir = calloc( MAX_LEN, sizeof( char ) );
  sprintf( dir, "%s_%d/", backup_dir, tv->tv_sec ); 
  sprintf( cmd, "cp %s/* %s", rra_location, dir );

  err = mkdir( dir, 0755 );

  printf( "Creating database backups and restoration script in %s\n", dir );
  printf( "Executing: %s\n", cmd );  
  system( cmd );
  sprintf( cmd, "cp %s %s ", HCB_RRD_CFG, dir );
  printf( "Executing: %s\n", cmd );  
  system( cmd );
  sprintf( cmd, "cp %s %s ", PWRUSAGE_CFG, dir );
  printf( "Executing: %s\n", cmd );  
  system( cmd );

  /* write restore script */

  restore_script = calloc( MAX_LEN, sizeof( char ) );

  sprintf( restore_script, "%s/restore_logs.sh", dir );
  fp = fopen( restore_script, "w" );
  fprintf( fp, "#! /bin/sh\n#\n# Script for backup restoration. Generated by transfer-logs\n" );
  fprintf( fp, "cp %s/*.rra %s\n", dir, rra_location );
  fprintf( fp, "cp %s/*.dat %s\n", dir, rra_location );
  fprintf( fp, "cp %s/config_hcb_rrd.xml /HCBv2/config/\n", dir );
  fprintf( fp, "cp %s/config_happ_pwrusage.xml /HCBv2/config/\n", dir );
  fclose( fp );

  chmod( restore_script, S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP );
  free( cmd );
  free( dir );
  free( tv );
  free( restore_script );
}

