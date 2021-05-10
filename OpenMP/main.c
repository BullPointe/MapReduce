#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <stdbool.h>
#include <unistd.h>
#include "main.h"


#define HASHARRAYSIZE 65536
#define NUMPROCESS 16
#define BUFFERSIZE 50
#define NUM_MAPPERS 5
#define NUM_READERS 5
#define NUM_REDUCERS 5


int hashCode(char* word){
    //XOR characters to form 16 bit hash code
    int i;
    if(strlen(word) <= 1) {
        return (int)word[0];
    }
    int hash = ((int)word[0] << 8) + (int)word[1];
    for(i = 2; i < strlen(word) - 1; i+=2){
        hash = hash ^ ((int)word[i] << 8) + (int)word[i+1];
    }
    if(strlen(word) % 2){
        hash = hash ^ (int)word[strlen(word)-1];
    }
    return hash;
}

void initHashMap(HashMap * hashMap){
    int i;
    for(i = 0; i < HASHARRAYSIZE; i++){
        hashMap[i].head = NULL;
        hashMap[i].tail = NULL;
    }
}

void initMapperQueues(MapperWorkQueue * mapperQ){
    int i;
    for(i = 0; i < NUM_MAPPERS; i++){
        mapperQ[i].head = NULL;
        mapperQ[i].tail = NULL;
        mapperQ->len = 0;
    }
}

void addToMapperQueue(MapperWorkQueue* mapperQ,MapperWorkElement* wordElement, int index) {
    MapperWorkElement* temp = mapperQ[index].head;
    if(temp == NULL) {
        mapperQ[index].head = wordElement;
        mapperQ[index].tail = wordElement;
        mapperQ[index].len = 1;
    }
    else {
       mapperQ[index].tail ->next = wordElement;
       mapperQ[index].tail = wordElement;
       mapperQ[index].len += 1;
    }
}

void printMapperQueue(MapperWorkQueue* mapperQ) {
    printf("Printing Mapper Qs\n");
    int i;
    for(i = 0; i < NUM_MAPPERS; i++) {
        MapperWorkElement* temp = mapperQ[i].head;
        while(temp != NULL) {
            printf("%d: %s\n",i,temp->word);
            temp = temp -> next;
        }
    }
    
}

void initReductionQueues(ReductionWorkQueue * reductionQ){
    int i;
    for(i = 0; i < NUM_REDUCERS; i++){
        reductionQ[i].head = NULL;
        reductionQ[i].tail = NULL;
        reductionQ->len = 0;
    }
}

void addToReductionQueue(ReductionWorkQueue* reductionQ,ReductionWorkElement* reductionElement, int index) {
    ReductionWorkElement* temp = reductionQ[index].head;
    if(temp == NULL) {
        reductionQ[index].head = reductionElement;
        reductionQ[index].tail = reductionElement;
        reductionQ[index].len = 1;
    }
    else {
       reductionQ[index].tail ->next = reductionElement;
       reductionQ[index].tail = reductionElement;
       reductionQ[index].len += 1;
    }
}

void printReductionQueue(ReductionWorkQueue* reductionQ) {
    printf("Printing Reducer Qs\n");
    int i;
    for(i = 0; i < NUM_REDUCERS; i++) {
        ReductionWorkElement* temp = reductionQ[i].head;
        while(temp != NULL) {
            printf("%d: %s:%d\n",i,temp->wordPair->word,temp->wordPair->count);
            temp = temp -> next;
        }
    }
    
}


int addToHashMap(HashMap *hashMap, char *buffer){
    int hashNum = hashCode(buffer);
    if(hashMap[hashNum].head == NULL){
        WordPair *wordPair = malloc(sizeof(*wordPair));
        wordPair -> word = malloc(sizeof(char) * strlen(buffer));
        strcpy(wordPair -> word, buffer);
        wordPair -> count = 1;
        wordPair -> next = NULL;
        hashMap[hashNum].head = wordPair;
        hashMap[hashNum].tail = wordPair;
        return 0;
    } else{
        WordPair *temp = hashMap[hashNum].head;
        while(temp != NULL){
            if(strcmp(temp -> word, buffer) == 0){
                temp -> count++;
                return 2; 
            }
            temp = temp -> next;
        }
        WordPair *wordPair = malloc(sizeof(*wordPair));
        wordPair -> word = malloc(sizeof(char) * strlen(buffer));
        strcpy(wordPair -> word, buffer);
        wordPair -> count = 1;
        wordPair -> next = NULL;
        temp = wordPair;
        hashMap[hashNum].tail -> next = temp;
        hashMap[hashNum].tail = temp;
        return 1;
    }
}

void printHashMap(HashMap *hashMap){
    printf("PRINTING HASH MAP\n");
    int i;
    for(i = 0; i < HASHARRAYSIZE; i++){
        if(hashMap[i].head != NULL){
            printf("HASHCODE: %d \n", i);
            WordPair *temp = hashMap[i].head;
            while(temp != NULL){
                printf("(%s, %d) \n", temp -> word, temp -> count);
                temp = temp -> next;
            }
        }
    }
}

void mergeMapperQueues(MapperWorkQueue* mapperQLocal, MapperWorkQueue* mapperQ) {

    int i;
    for(i = 0; i < NUM_MAPPERS; i++) {
        MapperWorkElement* head = mapperQLocal[i].head;
        while(head != NULL) {
            addToMapperQueue(mapperQ,head,i);
            head = head ->next;
        }
    }
}


void parser(FILE *f, HashMap *hashMap,MapperWorkQueue* mapperQ, omp_lock_t* lck){
    char* buffer = malloc(sizeof(*buffer) * BUFFERSIZE);
    int hashNum;
    MapperWorkQueue* mapperQLocal = malloc(sizeof(MapperWorkQueue) * NUM_MAPPERS);
    while(fscanf(f, "%s", buffer) != EOF){
        //Distribute to the Buffers and the other threads will deal with mapping
        MapperWorkElement* wordElement = malloc(sizeof(*wordElement));
        wordElement->next = NULL;
        wordElement->word =  malloc(sizeof(char) * strlen(buffer));
        strcpy(wordElement -> word, buffer);

        //Figure out which Queue to put into (Balancing)
        int minIndex = 0;
        int minLength = __INT_MAX__;
        int i;
        for(i = 0; i < NUM_MAPPERS; i++) {
            if(mapperQ[i].len <= minLength) {
                minIndex = i;
                minLength = mapperQ[i].len;
            }
        }
        //Add to Q
        // omp_set_lock(lck);
        addToMapperQueue(mapperQLocal,wordElement,minIndex);
        // omp_unset_lock(lck);
    }

    omp_set_lock(lck);
    mergeMapperQueues(mapperQLocal,mapperQ);
    omp_unset_lock(lck);


}

void mapper(MapperWorkQueue* mapperQ, ReductionWorkQueue* reductionQ, int index, omp_lock_t* parserlck) {
    omp_set_lock(parserlck);
    MapperWorkQueue* currQ  = &mapperQ[index];
    while(currQ -> head != NULL) {
        MapperWorkQueue* currQ  = &mapperQ[index];
        MapperWorkElement* temp = currQ -> head;
        while(temp != NULL) {
            int hashNum = hashCode(temp->word);
            int reducerIndex = hashNum%NUM_REDUCERS;
            WordPair* wordPair = malloc(sizeof(*wordPair));
            wordPair ->next = NULL;
            wordPair ->count = 1;
            wordPair ->word = temp->word;
            ReductionWorkElement* reductionElement = malloc(sizeof(*reductionElement));
            reductionElement ->wordPair = wordPair;
            reductionElement ->next = NULL;
            // printf("%d: %s:%d\n",reducerIndex,reductionElement->wordPair->word,reductionElement->wordPair->count);
            addToReductionQueue(reductionQ,reductionElement,reducerIndex);
            currQ -> head = temp -> next;
            temp = temp ->next;
        }
    }
    omp_unset_lock(parserlck);
}

void reducer(ReductionWorkQueue* reductionQ, HashMap* hashMap, int index, omp_lock_t* lck) {
    omp_set_lock(lck);
    ReductionWorkElement* temp = reductionQ[index].head;
    while (temp != NULL) {
        addToHashMap(hashMap,temp->wordPair->word);
        temp = temp -> next;
    }
    omp_unset_lock(lck);
}

// 16 Threads
// 1 Thread - Master 
// 5 Threads - Readers 
    // Reads Each Word
    // Places it into a mapper work queue
// 5 Threads - Mappers
    // Takes in a word from its Q ... creates a wordpair
    // Places it Reducer Queue based on hashnum
// 5 Threads - Reducers 
    // Takes in Word Pairs from its Q (Corresponding based on the hashnum)
    // Counts them
    // We call reduce on the array since the indexes are the same

//Final array of the entire hashmap
//userdefined reduction

int main(int argc, char** argv) {
    HashMap* hashMap = malloc(sizeof(HashMap) * HASHARRAYSIZE);
    MapperWorkQueue* mapperQ = malloc(sizeof(MapperWorkQueue) * NUM_MAPPERS);
    ReductionWorkQueue* reductionQ = malloc(sizeof(*reductionQ) * NUM_REDUCERS);

    initHashMap(hashMap);
    initMapperQueues(mapperQ);
    initReductionQueues(reductionQ); 

    //Step 1: Map Step
    //SHARED VARIABLES: MAPPER WORK QUEUES + REDUCTION WORK QUEUES + ReaderIsDone
    int readerIsDone = NUM_READERS;
    int mapperIsDone = NUM_MAPPERS;
    //omp parallel {} NOT paralllel FOR!
    // for(tid = 1; tid < NUMPROCESS; tid++){
    omp_lock_t parserlck,reducerlck;
    omp_init_lock(&parserlck);
    omp_init_lock(&reducerlck);

    double startTimeTotal = omp_get_wtime();

    
    omp_set_num_threads(NUMPROCESS);
    #pragma omp parallel shared(readerIsDone,mapperIsDone,mapperQ,reductionQ,hashMap)
    {   
        double startTime = omp_get_wtime();
        //Split this based on TID e.g if tid == 1 -5 do reading, 6-10 do mapping, etc..
        int tid = omp_get_thread_num();
        if(tid > 0 && tid <= NUM_READERS) {
            //READER
            double temp = omp_get_wtime();
            FILE * input = fopen(argv[tid], "r");
            if(input == NULL){
                printf("Unable to open file");
            }
            else {
                parser(input, hashMap,mapperQ, &parserlck);
                readerIsDone -= 1;
            }
            // printf("Total Time for reader,%d: %lf\n",tid,omp_get_wtime() - temp);
        }
        else if(tid > NUM_READERS && tid <= NUM_READERS + NUM_MAPPERS) {
            //MAPPER
            double temp = omp_get_wtime();
            while(readerIsDone >0) {
                asm("");
                mapper(mapperQ,reductionQ,tid - (NUM_MAPPERS + 1),&parserlck);
            }
            // printf("Total Time for mapper,%d: %lf\n",tid,omp_get_wtime() - temp);
            // mapper(mapperQ,reductionQ,tid - 6,&readerIsDone,&parserlck,&mapperlck);
            mapperIsDone -= 1;
        }
        else if(tid > NUM_READERS + NUM_MAPPERS && tid <= NUMPROCESS){
            //REDUCER
            while(mapperIsDone >0) {               
                asm("");
                // sleep(.1);
            }
            double temp = omp_get_wtime();
            reducer(reductionQ,hashMap,tid-(NUM_READERS + NUM_MAPPERS + 1),&reducerlck);
            // printf("Total Time for reducer,%d: %lf\n",tid,omp_get_wtime() - temp);
        }

        // printf("Total Time for thread,%d: %lf\n",tid,omp_get_wtime() - startTime);
    }

    
    omp_destroy_lock(&parserlck);
    omp_destroy_lock(&reducerlck);
    printHashMap(hashMap);

    // printf("Total Time OPen MP: %lf",omp_get_wtime() - startTimeTotal);
    
    // printMapperQueue(mapperQ);
    // printReductionQueue(reductionQ);
    return 0;
}
