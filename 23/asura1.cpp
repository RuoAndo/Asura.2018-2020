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

struct HashCompare {
  static size_t hash( unsigned long long x ) {
    return (size_t)x;
    }
  static bool equal( unsigned long long x, unsigned long long y ) {
        return x==y;
    }
};

typedef concurrent_hash_map<std::string, std::string> CharTable;
static CharTable table;

typedef concurrent_hash_map<unsigned long long, int, HashCompare> CharTable2;
static CharTable2 table2;

typedef concurrent_hash_map<unsigned long long, int, HashCompare> CharTable3;
static CharTable3 table3;

typedef concurrent_hash_map<unsigned long long, long, HashCompare> CharTable4;
static CharTable4 table4;

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

int traverse_file(char* filename, char* srchstr, int thread_id) {

return 0;
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

