#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

void *threadFunc(void *arg) {
    int n = (int)arg;
    int i;

    for(i = 0; i < n; i++) {
        printf("threadFunc id %d\n", i);
        sleep(1);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    pthread_t thread;
    int n,i;

    if(argc > 1)
        n = atoi(argv[1]);
    else
        n = 1;
    if(pthread_create(&thread, NULL, threadFunc, (void *)n) != 0) {
        printf("Error: Failed to create new thread.\n");
        exit(1);
    }

    for(i = 0; i < 5; i++) {
        printf("main: %d\n", i);
        sleep(1);
    }

    if(pthread_join(thread, NULL) != 0) {
        printf("Error: Failed to wait for the thread termination.\n");
        exit(1);
    }

    printf("Bye.\n");

    return 0;
}
