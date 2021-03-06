#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>
#include <alloca.h>

#include "timer.h"
#include "kmeans.h"

#include <regex.h>

#include<sys/ioctl.h>
#include<arpa/inet.h>
#include<sys/socket.h>
#include<linux/if.h>
#include<net/ethernet.h>
#include<netpacket/packet.h>
#include<netinet/if_ether.h>
#include<netinet/ip.h>
#include<netinet/ip6.h>
#include<netinet/ip_icmp.h>
#include<netinet/icmp6.h>
#include<netinet/tcp.h>
#include<netinet/udp.h>
//#include<linux/inet.h>
//#include <arpa/inet.h>
// #include<linux/types.h>

#include <map>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <bitset>

#include <cstdio>
#include <cctype>
#include <iostream>

#include <algorithm>
#include <cstdlib>
// #include "util.h"
#include "timer.h" 

#include "tbb/task_scheduler_init.h"
#include "tbb/concurrent_hash_map.h"
#include "tbb/blocked_range.h"
#include "tbb/parallel_for.h"

using namespace std;
using namespace tbb;

#define N 1
#define WORKER_THREAD_NUM N
#define MAX_QUEUE_NUM N
#define END_MARK_FNAME   "///"
#define END_MARK_FLENGTH 3

#define DISP_FREQ 100000

/* srcIP, destIP */
typedef struct _addrpair {
  map<string, string> m;
  pthread_mutex_t mutex;
} addrpair_t;
addrpair_t addrpair;

/*
struct HashCompare {
  static size_t hash( std::string x ) {
    return (size_t)x.c_str();
    }
  static bool equal( std::string x, std::string y ) {
        return x==y;
    }
};
*/

struct HashCompare {
  static size_t hash( unsigned long long x ) {
    return (size_t)x;
    }
  static bool equal( unsigned long long x, unsigned long long y ) {
        return x==y;
    }
};

// typedef concurrent_hash_map<std::string, std::string, HashCompare> CharTable;
// typedef concurrent_hash_map<std::string, int, HashCompare> CharTable;

typedef concurrent_hash_map<std::string, std::string> CharTable;
static CharTable table;

typedef concurrent_hash_map<unsigned long long, int, HashCompare> CharTable2;
static CharTable2 table2;

typedef concurrent_hash_map<unsigned long long, int, HashCompare> CharTable3;
static CharTable3 table3;

typedef concurrent_hash_map<unsigned long long, long, HashCompare> CharTable4;
static CharTable4 table4;

/* reduced */
typedef struct _reduced {
  map<unsigned long long, int> count;
  map<unsigned long long, int> tlen;
  map<unsigned long long, int> ttl;
  map<unsigned long long, int> sport;
  map<unsigned long long, int> dport;
  pthread_mutex_t mutex;
} reduced_t;
reduced_t reduced;

static int chksum = 0;
static int display_counter = 0;

struct pseudo_ip{
        struct in_addr  ip_src;
        struct in_addr  ip_dst;
        unsigned char   dummy;
        unsigned char   ip_p;
        unsigned short  ip_len;
};

struct pseudo_ip6_hdr{
        struct in6_addr src;
        struct in6_addr dst;
        unsigned long   plen;
        unsigned short  dmy1;
        unsigned char   dmy2;
        unsigned char   nxt;
};

u_int16_t checksum(u_char *data,int len)
{
register u_int32_t       sum;
register u_int16_t       *ptr;
register int     c;

        sum=0;
        ptr=(u_int16_t *)data;

        for(c=len;c>1;c-=2){
                sum+=(*ptr);
                if(sum&0x80000000){
                        sum=(sum&0xFFFF)+(sum>>16);
                }
                ptr++;
        }
        if(c==1){
                u_int16_t       val;
                val=0;
                memcpy(&val,ptr,sizeof(u_int8_t));
                sum+=val;
        }

        while(sum>>16){
                sum=(sum&0xFFFF)+(sum>>16);
        }

	return(~sum);
}

u_int16_t checksum2(u_char *data1,int len1,u_char *data2,int len2)
{
register u_int32_t       sum;
register u_int16_t       *ptr;
register int     c;

        sum=0;
        ptr=(u_int16_t *)data1;
        for(c=len1;c>1;c-=2){
                sum+=(*ptr);
                if(sum&0x80000000){
                        sum=(sum&0xFFFF)+(sum>>16);
                }
                ptr++;
        }
        if(c==1){
                u_int16_t       val;
                val=((*ptr)<<8)+(*data2);
                sum+=val;
                if(sum&0x80000000){
                        sum=(sum&0xFFFF)+(sum>>16);
                }
                ptr=(u_int16_t *)(data2+1);
                len2--;
        }
        else{
                ptr=(u_int16_t *)data2;
        }
        for(c=len2;c>1;c-=2){
                sum+=(*ptr);
                if(sum&0x80000000){
                        sum=(sum&0xFFFF)+(sum>>16);
                }
                ptr++;
        }
        if(c==1){
                u_int16_t       val;
                val=0;
                memcpy(&val,ptr,sizeof(u_int8_t));
                sum+=val;
        }

        while(sum>>16){
                sum=(sum&0xFFFF)+(sum>>16);
        }

	return(~sum);
}

int checkIPchecksum(struct iphdr *iphdr,u_char *option,int optionLen)
{
  unsigned short	sum;

	if(optionLen==0){
		sum=checksum((u_char *)iphdr,sizeof(struct iphdr));
		if(sum==0||sum==0xFFFF){
			return(1);
		}
		else{
			return(0);
		}
	}
	else{
		sum=checksum2((u_char *)iphdr,sizeof(struct iphdr),option,optionLen);
		if(sum==0||sum==0xFFFF){
			return(1);
		}
		else{
			return(0);
		}
	}
}

int checkIPDATAchecksum(struct iphdr *iphdr,unsigned char *data,int len)
{
  struct pseudo_ip        p_ip;
  unsigned short  sum;

        memset(&p_ip,0,sizeof(struct pseudo_ip));
        p_ip.ip_src.s_addr=iphdr->saddr;
        p_ip.ip_dst.s_addr=iphdr->daddr;
        p_ip.ip_p=iphdr->protocol;
        p_ip.ip_len=htons(len);

        sum=checksum2((unsigned char *)&p_ip,sizeof(struct pseudo_ip),data,len);
        if(sum==0||sum==0xFFFF){
                return(1);
        }
        else{
                return(0);
        }
}

int checkIP6DATAchecksum(struct ip6_hdr *ip,unsigned char *data,int len)
{
  struct pseudo_ip6_hdr   p_ip;
  unsigned short  sum;

        memset(&p_ip,0,sizeof(struct pseudo_ip6_hdr));

        memcpy(&p_ip.src,&ip->ip6_src,sizeof(struct in6_addr));
        memcpy(&p_ip.dst,&ip->ip6_dst,sizeof(struct in6_addr));
        p_ip.plen=ip->ip6_plen;
        p_ip.nxt=ip->ip6_nxt;

	sum=checksum2((unsigned char *)&p_ip,sizeof(struct pseudo_ip6_hdr),data,len);
	if(sum==0||sum==0xFFFF){
		return(1);
	}
	else{
		return(0);
	}
}

typedef struct _result {
    int num;
    char* fname;
    pthread_mutex_t mutex;    
} result_t;
result_t result;

typedef struct _queue {
    char* fname[MAX_QUEUE_NUM];
    int flength[MAX_QUEUE_NUM];
    int rp, wp;
    int remain;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} queue_t;

typedef struct _thread_arg {
    int id;
    int cpuid;
    queue_t* q;
    char* srchstr;
    char* dirname;
    int filenum;
} thread_arg_t;

char *ip_ip2str(u_int32_t ip,char *buf,socklen_t size)
{
  struct in_addr*addr;

  addr=(struct in_addr *)&ip;
  inet_ntop(AF_INET,addr,buf,size);

  return(buf);
}

std::vector<std::string> split_string_2(std::string str, char del) {
  int first = 0;
  int last = str.find_first_of(del);

  std::vector<std::string> result;

  while (first < str.size()) {
    std::string subStr(str, first, last - first);

    result.push_back(subStr);

    first = last + 1;
    last = str.find_first_of(del, first);

    if (last == std::string::npos) {
      last = str.size();
    }
  }

  return result;
}

int ProcIpHeader(struct iphdr *iphdr,u_char *option,int optionLen,FILE *fp,u_char*ptr)
{
  int i;
  char buf[16];
  
  char src[80];
  char dst[80];

  string saddr;
  string daddr;

  int tlen;

  struct tcphdr *tcp;
  struct udphdr *udp;

  // u_char*ptr;
  // ptr = iphadr;

  /*
  fprintf(fp,"protocol=%u, ",iphdr->protocol);
  fprintf(fp,"saddr=%s,",ip_ip2str(iphdr->saddr,buf,sizeof(buf)));
  fprintf(fp,"daddr=%s\n",ip_ip2str(iphdr->daddr,buf,sizeof(buf)));
  fprintf(fp,"length=%u\n",ntohs(iphdr->tot_len));
  */

  fprintf(fp,"saddr=%pI4 \n",&iphdr->saddr);
  
  /*
  char source[16];
  in4_pton(source, -1, &iphdr->saddr, '\0', NULL);
  */

  /*
  char source[16];
  snprintf(source, 16, "%pI4", &iphdr->saddr); 
  std::cout << source << std::endl;
  */  

  sprintf(src, ip_ip2str(iphdr->saddr,buf,sizeof(buf)));
  sprintf(dst, ip_ip2str(iphdr->daddr,buf,sizeof(buf)));
  
  tlen = (signed int)ntohs(iphdr->tot_len);
  
  saddr = string(src);
  daddr = string(dst);

  // std::cout << src << std::endl;
  
  int sport = 0;
  int dport = 0;
  
  char del = '.';

  std::string stringIP;
  std::string IPstring;
	    	    
  stringIP = saddr;	    
  for (const auto subStr : split_string_2(stringIP, del)) {
    unsigned long ipaddr_src;
    ipaddr_src = atoi(subStr.c_str());
    std::bitset<8> trans =  std::bitset<8>(ipaddr_src);
    std::string trans_string = trans.to_string();
    IPstring = IPstring + trans_string;
  }
	    
  stringIP = daddr;
  for (const auto subStr : split_string_2(stringIP, del)) {
    unsigned long ipaddr_src;
    ipaddr_src = atoi(subStr.c_str());
    std::bitset<8> trans =  std::bitset<8>(ipaddr_src);
    std::string trans_string = trans.to_string();
    IPstring = IPstring + trans_string;
  }
	  
  // CharTable::accessor a;
  // table.insert(a, saddr);
  // table.insert(a, IPstring);
  // table.insert(a, n);
  // a->second = daddr;
  // a->second += 1;

  unsigned long long n = bitset<64>(IPstring).to_ullong();    
  
  CharTable2::accessor a2;
  table2.insert(a2, n);
  a2->second = +tlen;     

  CharTable3::accessor a3;
  table3.insert(a3, n);
  a3->second += 1;     

  if(iphdr->protocol == IPPROTO_TCP)
    {
      tcp = (struct tcphdr *) ptr;
      ptr += ((int) (tcp->th_off) << 2);   

      sport = ntohs(tcp->th_sport);
      dport = ntohs(tcp->th_dport);

      CharTable4::accessor a4;
      table4.insert(a4, n);
      a4->second += sport;
      
      // display_counter++;
    }
      
  return(0);
}

int AnalyzeIp(u_char *data,int size)
{
  u_char*ptr;
  int lest;
  struct iphdr*iphdr;
  u_char*option;
  int optionLen,len;
  unsigned short  sum;

  struct tcphdr *tcp;
  struct udphdr *udp;
  
  ptr=data;
  lest=size;

  if(lest<sizeof(struct iphdr)){
    fprintf(stderr,"lest(%d)<sizeof(struct iphdr)\n",lest);
    return(-1);
  }
  iphdr=(struct iphdr *)ptr;
  ptr+=sizeof(struct iphdr);
  lest-=sizeof(struct iphdr);

  optionLen=iphdr->ihl*4-sizeof(struct iphdr);
  if(optionLen>0){
    if(optionLen>=1500){
      fprintf(stderr,"IP optionLen(%d):too big\n",optionLen);
      // return(-1);
    }
    option=ptr;
    ptr+=optionLen;
    lest-=optionLen;
  }

  if(checkIPchecksum(iphdr,option,optionLen)==0){
    chksum++;
  }
  
  ProcIpHeader(iphdr,option,optionLen,stdout,ptr);
  
  return(0);
}

int traverse_file(char* filename, char* srchstr, int thread_id) {
    char buf[256];
    int n = 0, sumn = 0;

    FILE    *fp;
    int     data;	
    char    tmp[256];
    
    char    *s1 = "08";
    char    *s2 = "00";
    char    *s3 = "45";

    int j;
    
    u_char *ptr;
	
    int counter = 0;
    int disp_counter = 0;
	
    fp = fopen(filename, "rb");
    if(fp == NULL){
      printf("file cannot be opened. \n");
      return 0;
    }

    for(;;){
      
      if((data = getc(fp) ) == EOF){
	fclose(fp); return 1;}
      
      /* init */
      for(j=0;j<256;j++)
	tmp[j] = 0;
      for(j=0;j<256;j++)
	buf[j] = 0;
	  
      sprintf(tmp, "%02X", data);
      if(strcmp(tmp, s1)==0)
	{
	  data = getc(fp);	
	  sprintf(tmp, "%02X", data);
	  
	  if(strcmp(tmp, s2)==0)
	    {
	      data = getc(fp);
	      sprintf(tmp, "%02X", data);

	      sprintf(tmp, "%02X", data);
	      if(strcmp(tmp, s3)==0)
		{
		  //if(thread_id % WORKER_THREAD_NUM == 0)
		  if(counter % DISP_FREQ == 0)
		    printf("threadID:%d:filename:%s IP sig 080045:# of packets processed:%d\n", thread_id, filename, counter);
		  
		  fseek(fp,-1.5L,SEEK_CUR);
		  if (fgets(buf, sizeof(struct iphdr)+8, fp) != NULL)
		    {
		      ptr = buf;
		      AnalyzeIp(ptr,sizeof(struct iphdr));
		      counter = counter + 1;
		    }
		  fseek(fp,-3L,SEEK_CUR);
		}
	      
	    } // if(strcmp(tmp,s2)
	  disp_counter++;
	} // if(strcmp(tmp,s1) *s1 = "08";
      
    } // for (;;)

    fclose(fp);
    return sumn;
}

void initqueue(queue_t* q) {
    int i;
    q->rp = q->wp = q->remain= 0;
    for (i = 0; i < MAX_QUEUE_NUM; ++i) q->fname[i] = NULL;
    pthread_mutex_init(&q->mutex,    NULL);
    pthread_cond_init(&q->not_full,  NULL);
    pthread_cond_init(&q->not_empty, NULL);
    return;
}

void enqueue(queue_t* q, char* path, int size) {
  
    pthread_mutex_lock(&q->mutex);
    while (q->remain == MAX_QUEUE_NUM) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }
    char** fname = (char**)&q->fname[q->wp];
    if (*fname != NULL) free(*fname);
    *fname = (char*)malloc(size);
    strcpy(*fname, path);
    q->flength[q->wp] = size;
    q->wp++; q->remain++;

    if (q->wp == MAX_QUEUE_NUM) q->wp = 0;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return;
}

void dequeue(queue_t* q, char** fname, int* flen) {

    pthread_mutex_lock(&q->mutex);
    while (q->remain == 0) 
        pthread_cond_wait(&q->not_empty, &q->mutex);

    *flen  = q->flength[q->rp];
    if (*fname != NULL) free(*fname);
    *fname = (char*)malloc(*flen);
    strcpy(*fname, q->fname[q->rp]);
    q->rp++; q->remain--;
    if (q->rp == MAX_QUEUE_NUM) q->rp = 0;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    if (strcmp(*fname,"")==0) printf("rp=%d\n", q->rp-1);
    return;
}

int traverse_dir_thread(queue_t* q, char* dirname) {
    static int cnt = 0;
    struct dirent* dent;
    DIR* dd = opendir(dirname);

    if (dd == NULL) {
        printf("Could not open the directory %s\n", dirname); return 0;
    }

    while ((dent = readdir(dd)) != NULL) {
        if (strncmp(dent->d_name, ".",  2) == 0) continue;
        if (strncmp(dent->d_name, "..", 3) == 0) continue;	

        int size = strlen(dirname) + strlen(dent->d_name) + 2;
#if 0
        char* path = (char*)malloc(size);
        sprintf(path, "%s/%s", dirname, dent->d_name);

        struct stat fs;
        if (stat(path, &fs) < 0)
            continue;
        else {
            if (S_ISDIR(fs.st_mode))
                traverse_dir_thread(q, path);
            else if (S_ISREG(fs.st_mode)) {
                enqueue(q, path, size);
                cnt++;
            }
        }
#else
        {
            char* path = (char*)alloca(size);
            sprintf(path, "%s/%s", dirname, dent->d_name);

            struct stat fs;
            if (stat(path, &fs) < 0)
                continue;
            else {
                if (S_ISDIR(fs.st_mode))
                    traverse_dir_thread(q, path);
                else if (S_ISREG(fs.st_mode)) {
                    enqueue(q, path, size);
                    cnt++;
                }
            }
        }
#endif
    }
    closedir(dd);
    return cnt;
}

void master_func(thread_arg_t* arg) {
    queue_t* q = arg->q;
    int i;
    arg->filenum = traverse_dir_thread(q, arg->dirname);

    /* enqueue END_MARK */
    for (i = 0; i < WORKER_THREAD_NUM; ++i) 
        enqueue(q, END_MARK_FNAME, END_MARK_FLENGTH);
    return;
}

void worker_func(thread_arg_t* arg) {
    int flen;
    char* fname = NULL;
    queue_t* q = arg->q;
    char* srchstr = arg->srchstr;

    int thread_id = arg->id;

    printf("worker func %d launched \n", thread_id);
    
#ifdef __CPU_SET
    cpu_set_t mask;    
    __CPU_ZERO(&mask);
    __CPU_SET(arg->cpuid, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
        printf("WARNING: faild to set CPU affinity...\n");
#endif

#if 0
    while (1) {
        int n;

        dequeue(q, &fname, &flen));

        if (strncmp(fname, END_MARK_FNAME, END_MARK_FLENGTH + 1) == 0)
            break;

        n = traverse_file(fname, srchstr, thread_id);
        pthread_mutex_lock(&result.mutex);

        if (n > result.num) {
            result.num = n;
            if (result.fname != NULL) free(result.fname);
            result.fname = (char*)malloc(flen);
            strcpy(result.fname, fname);
        }
        pthread_mutex_unlock(&result.mutex);
    }
#else
    char* my_result_fname;
    int my_result_num = 0;
    int my_result_len = 0;
    while (1) {
        int n;

        dequeue(q, &fname, &flen);

        if (strncmp(fname, END_MARK_FNAME, END_MARK_FLENGTH + 1) == 0)
            break;

        n = traverse_file(fname, srchstr, thread_id);

        if (n > my_result_num) {
            my_result_num = n;
            my_result_len = flen;
            my_result_fname = (char*)alloca(flen);
            strcpy(my_result_fname, fname);
        }
    }
    pthread_mutex_lock(&result.mutex);
    if (my_result_num > result.num) {
        result.num = my_result_num;
	if (result.fname != NULL) free(result.fname);
        result.fname = (char*)malloc(my_result_len);
        strcpy(result.fname, my_result_fname);	
    }
    pthread_mutex_unlock(&result.mutex);
#endif
    return;
}

void print_result(thread_arg_t* arg) {
    if (result.num) {
        printf("Total %d files\n", arg->filenum);
        printf("Max include file: %s[include %d]\n", result.fname, result.num);	
    }
    return;
}

int main(int argc, char* argv[]) {
    int i;
    int thread_num = 1 + WORKER_THREAD_NUM;
    unsigned int t, travdirtime;
    queue_t q;
    thread_arg_t targ[thread_num];
    pthread_t master;
    pthread_t worker[thread_num];
    pthread_t worker2[thread_num];
    int cpu_num;

    if (argc != 2) {
        printf("Usage: ./asura [DIR] \n"); return 0;
    }
    cpu_num = sysconf(_SC_NPROCESSORS_CONF);

    initqueue(&q);

    for (i = 0; i < thread_num; ++i) {
        targ[i].q = &q;
        targ[i].dirname = argv[1];
        targ[i].filenum = 0;
        targ[i].cpuid = i%cpu_num;
    }
    result.fname = NULL;

    start_timer(&t);

    pthread_mutex_init(&result.mutex, NULL);

    /* first scatter */
    
    pthread_create(&master, NULL, (void*)master_func, (void*)&targ[0]);
    for (i = 1; i < thread_num; ++i)
      { 
        targ[i].id = i;
        pthread_create(&worker[i], NULL, (void*)worker_func, (void*)&targ[i]);
      }
    for (i = 1; i < thread_num; ++i) 
        pthread_join(worker[i], NULL);

    travdirtime = stop_timer(&t);
    print_timer(travdirtime);
    
    int counter = 0;
    std::cout << "table3:" << table3.size() << endl;

    std::remove("reduced");
    ofstream outputfile3("reduced");

    std::vector<unsigned long long> s_vec_1;
    std::vector<float> s_vec_2;
    std::vector<float> s_vec_3;
    std::vector<float> s_vec_4;
    
    counter = 0;
    for( CharTable3::iterator i=table3.begin(); i!=table3.end(); ++i )
    {
      if(counter < table3.size() && (int)i->first > 0)
	{
	  s_vec_1.push_back((unsigned long long)i->first);
	  s_vec_2.push_back((int)i->second);
	}
      
      counter = counter + 1;
    }

    counter = 0;
    for( CharTable2::iterator i=table2.begin(); i!=table2.end(); ++i )
    {
      if(counter < table2.size() && (int)i->first > 0)
	{
	  //std::cout << i->first << "," << i->second << endl;       
	  s_vec_3.push_back((unsigned long long)i->first);
	  s_vec_4.push_back((int)i->second);
	  
	}
	  
      counter = counter + 1;
    }

    for(int i = 0; i < s_vec_1.size(); i++)
      {
	double tmp_div;
	if (s_vec_4[i] == 0)
	  tmp_div = 0;
	else if (s_vec_4[i] > 0)
	  tmp_div =  (double)s_vec_2[i] / (double)s_vec_4[i];
	
	outputfile3 << s_vec_1[i] << "," << s_vec_2[i] << "," << s_vec_4[i] << endl;
      }

    outputfile3.close();

    const size_t SQRT_K = 4;
    const size_t K = SQRT_K*SQRT_K;
    const size_t M = s_vec_1.size();
    
    point* points;
    points = (struct point *)malloc(M*sizeof(struct point));

    point* centroid;
    centroid = (struct point *)malloc(M*sizeof(struct point));

    cluster_id* id;
    id = (unsigned short *)malloc(M*sizeof(unsigned short));

    std::vector<string> pair; 
    
    for (int i = 0; i < M; i++) {

      point& p = points[i];

      p.x = (float)s_vec_2[i];
      p.y = (float)s_vec_4[i];
      pair.push_back(to_string(s_vec_1[i]));
    }

    start_timer(&t);
    tbb_asura::do_k_means( M, points, K, id, centroid );
    travdirtime = stop_timer(&t);
    print_timer(travdirtime);
    
#if 1
    int* counts;
    counts = (int *)malloc(K*sizeof(int));

    for( size_t i=0; i<M; ++i ) {
      counts[id[i]] = 0;
    }
    
    for( size_t i=0; i<M; ++i ) {
      counts[id[i]]++;
    }

    std::remove("clustered");
    ofstream outputfile4("clustered");
    
    for( size_t i=0; i<M; ++i ) {

	    unsigned long pair_long = std::stoul(pair[i]);
      
	    std::bitset<64> bset_pair = std::bitset<64>(pair_long);   
	    string bset_pair_string = bset_pair.to_string();
	    
	    std::string ip1 = bset_pair_string.substr(0, 8);
	    std::string ip2 = bset_pair_string.substr(8, 8);
	    std::string ip3 = bset_pair_string.substr(16, 8);
	    std::string ip4 = bset_pair_string.substr(24, 8);

	    std::bitset<8> ip1_bset = std::bitset<8>(ip1);
	    std::bitset<8> ip2_bset = std::bitset<8>(ip2);
	    std::bitset<8> ip3_bset = std::bitset<8>(ip3);
	    std::bitset<8> ip4_bset = std::bitset<8>(ip4);

	    outputfile4 << ip1_bset.to_ulong() << "." << ip2_bset.to_ulong() << "." << ip3_bset.to_ulong() << "." << ip4_bset.to_ulong() << ",";

	    std::string ip5 = bset_pair_string.substr(32, 8);
	    std::string ip6 = bset_pair_string.substr(40, 8);
	    std::string ip7 = bset_pair_string.substr(48, 8);
	    std::string ip8 = bset_pair_string.substr(56, 8);

	    std::bitset<8> ip5_bset = std::bitset<8>(ip5);
	    std::bitset<8> ip6_bset = std::bitset<8>(ip6);
	    std::bitset<8> ip7_bset = std::bitset<8>(ip7);
	    std::bitset<8> ip8_bset = std::bitset<8>(ip8);

	    outputfile4 << ip5_bset.to_ulong() << "." << ip6_bset.to_ulong() << "." << ip7_bset.to_ulong() << "." << ip8_bset.to_ulong() << " -> ";

	    float percent = (float)counts[id[i]]/(float)M;

	    outputfile4 << id[i] << " (" << points[i].x << "," << points[i].y << ")," << counts[id[i]] << "," << M << "," << percent << endl;
	    	
    }

    outputfile4.close();

#endif
    
#if 0
    printf("centroids = ");
    for( size_t j=0; j<K; ++j ) {
        printf("(%g %g)",centroid[j].x,centroid[j].y);
    }
    printf("\n");
#endif

    return 0;
}

