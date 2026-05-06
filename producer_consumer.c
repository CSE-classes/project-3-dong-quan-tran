#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define BUFFER_SIZE 5   // queue size required by the assignment

char buffer[BUFFER_SIZE];
int in = 0;     // next position to write into
int out = 0;    // next position to read from
int count = 0;  // number of items currently in buffer
int done = 0;   // producer sets this when file reading is finished

pthread_mutex_t mutex;
pthread_cond_t not_full;
pthread_cond_t not_empty;

void *producer(void *arg)
{
    FILE *fp;
    int ch;

    fp = fopen("message.txt", "r");
    if (fp == NULL) {
        printf("ERROR: can't open message.txt!\n");
        pthread_exit(NULL);
    }

    // read one character at a time and place it into the circular queue
    while ((ch = fgetc(fp)) != EOF) {
        pthread_mutex_lock(&mutex);

        // if buffer is full, wait until consumer removes something
        while (count == BUFFER_SIZE) {
            pthread_cond_wait(&not_full, &mutex);
        }

        buffer[in] = (char)ch;
        in = (in + 1) % BUFFER_SIZE;
        count++;

        // tell consumer there is something available now
        pthread_cond_signal(&not_empty);
        pthread_mutex_unlock(&mutex);
    }

    fclose(fp);

    // mark that producer is done so consumer knows when to stop
    pthread_mutex_lock(&mutex);
    done = 1;
    pthread_cond_signal(&not_empty);
    pthread_mutex_unlock(&mutex);

    pthread_exit(NULL);
}

void *consumer(void *arg)
{
    char ch;

    while (1) {
        pthread_mutex_lock(&mutex);

        // if buffer is empty, wait unless producer already finished
        while (count == 0 && !done) {
            pthread_cond_wait(&not_empty, &mutex);
        }

        // if producer is done and nothing is left, exit cleanly
        if (count == 0 && done) {
            pthread_mutex_unlock(&mutex);
            break;
        }

        ch = buffer[out];
        out = (out + 1) % BUFFER_SIZE;
        count--;

        // tell producer there is room in the buffer again
        pthread_cond_signal(&not_full);
        pthread_mutex_unlock(&mutex);

        // print in the same order characters were produced
        printf("%c", ch);
        fflush(stdout);
    }

    printf("\n");
    pthread_exit(NULL);
}

int main()
{
    pthread_t prod, cons;

    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&not_full, NULL);
    pthread_cond_init(&not_empty, NULL);

    // create one producer and one consumer
    pthread_create(&prod, NULL, producer, NULL);
    pthread_create(&cons, NULL, consumer, NULL);

    pthread_join(prod, NULL);
    pthread_join(cons, NULL);

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&not_full);
    pthread_cond_destroy(&not_empty);

    return 0;
}