#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <mpi.h>
#include <stdbool.h>
#include <unistd.h>
#include "main.h"

#define HASHARRAYSIZE 65536
#define NUMPROCESS 16
#define BUFFERSIZE 50
#define NUM_MAPPERS 5
#define NUM_READERS 5
#define NUM_REDUCERS 5
#define NUMP 2
#define TOTAL_REDUCERS (NUM_REDUCERS * NUMP)

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
    for(i = 0; i < TOTAL_REDUCERS; i++){
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
    for(i = 0; i < TOTAL_REDUCERS; i++) {
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

void printHashMap(HashMap *hashMap, FILE *outFile){
    int i;
    for(i = 0; i < HASHARRAYSIZE; i++){
        if(hashMap[i].head != NULL){
            WordPair *temp = hashMap[i].head;
            while(temp != NULL){
                fprintf(outFile, "(%s, %d) \n", temp -> word, temp -> count);
                temp = temp -> next;
            }
        }
    }
}


void parser(FILE *f, HashMap *hashMap,MapperWorkQueue* mapperQ, omp_lock_t* lck){
    char* buffer = malloc(sizeof(*buffer) * BUFFERSIZE);
    int hashNum;
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
        omp_set_lock(lck);
        addToMapperQueue(mapperQ,wordElement,minIndex);
        omp_unset_lock(lck);
    }
}

void mapper(MapperWorkQueue* mapperQ, ReductionWorkQueue* reductionQ, int index, omp_lock_t* parserlck) {
    omp_set_lock(parserlck);
    MapperWorkQueue* currQ  = &mapperQ[index];
    while(currQ -> head != NULL) {
        MapperWorkQueue* currQ  = &mapperQ[index];
        MapperWorkElement* temp = currQ -> head;
        while(temp != NULL) {
            int hashNum = hashCode(temp->word);
            int reducerIndex = hashNum%TOTAL_REDUCERS;
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

int reductionQToArray(ReductionWorkQueue* reductionQ, char ** reductionArr){
    int totalLength = 0;
    int i;
    for(i = 0; i < TOTAL_REDUCERS; i++){
        if(reductionQ[i].head != NULL){
            totalLength += reductionQ[i].len;
        }
    }
    *reductionArr = malloc(sizeof(char) * totalLength * BUFFERSIZE);
    int index = 0;
    ReductionWorkElement* temp;
    for(i = 0; i < TOTAL_REDUCERS; i++){
        temp = reductionQ[i].head;
        while(temp != NULL){
            strcpy(&((*reductionArr)[index]), temp -> wordPair -> word);
            temp = temp -> next;
            index += BUFFERSIZE;
        }
    }
    return totalLength;
}

void createHeaderStruct(ReductionWorkQueue* reductionQ, int ** headerArr){
    *headerArr = malloc(sizeof(int) * TOTAL_REDUCERS); 
    int i;
    for(i = 0; i < TOTAL_REDUCERS; i++){
        if(reductionQ[i].head == NULL){
            (*headerArr)[i] = 0;
        } else {
            (*headerArr)[i] = reductionQ[i].len;    
        }
    }
}

void bufferToReductionQ(char * buffer, ReductionWorkQueue* finalReductionQ, int *countFromEachProcessesArr){
    int i,j,buffIndex;
    buffIndex = 0;
    for(i = 0; i < TOTAL_REDUCERS; i++){
        for(j = 0; j < countFromEachProcessesArr[i]; j++){
            WordPair* wordPair = malloc(sizeof(*wordPair));
            wordPair ->next = NULL;
            wordPair ->count = 1;
            wordPair ->word = malloc(sizeof(char) * BUFFERSIZE);
            memcpy(wordPair -> word, &(buffer[BUFFERSIZE * buffIndex]), BUFFERSIZE);
            ReductionWorkElement* reductionElement = malloc(sizeof(*reductionElement));
            reductionElement ->wordPair = wordPair;
            reductionElement ->next = NULL;
            addToReductionQueue(finalReductionQ,reductionElement,i%NUM_REDUCERS);
            buffIndex += 1;
        }
    }
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

int main(int argc, char** argv) {
    //MPI Initialization
    int pid, numP;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numP);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    
    HashMap* hashMap = malloc(sizeof(HashMap) * HASHARRAYSIZE);
    MapperWorkQueue* mapperQ = malloc(sizeof(MapperWorkQueue) * NUM_MAPPERS);
    ReductionWorkQueue* reductionQ = malloc(sizeof(*reductionQ) * NUM_REDUCERS * numP);
    ReductionWorkQueue* finalReductionQ = malloc(sizeof(*finalReductionQ) * NUM_REDUCERS * numP);

    initHashMap(hashMap);
    initMapperQueues(mapperQ);
    initReductionQueues(reductionQ); 
    initReductionQueues(finalReductionQ); 

    //Step 1: Map Step
    int readerIsDone = NUM_READERS;
    int mapperIsDone = NUM_MAPPERS;
    int commIsDone = 0;

    omp_lock_t parserlck,reducerlck;
    omp_init_lock(&parserlck);
    omp_init_lock(&reducerlck);
    omp_set_num_threads(NUMPROCESS);
    #pragma omp parallel shared(readerIsDone,mapperIsDone,commIsDone,mapperQ,reductionQ,hashMap,finalReductionQ)
    {
        //Split this based on TID e.g if tid == 1 -5 do reading, 6-10 do mapping, etc..
        int tid = omp_get_thread_num();
        if(tid > 0 && tid <= NUM_READERS) {
            //READER
            FILE * input = fopen(argv[pid*NUM_READERS + tid], "r");
            if(input == NULL){
                printf("Unable to open file");
            }
            else {
                parser(input, hashMap, mapperQ, &parserlck);
                readerIsDone -= 1;
            }
        }
        else if(tid > NUM_READERS && tid <= NUM_READERS + NUM_MAPPERS) {
            //MAPPER
            while(readerIsDone >0) {
                asm("");
                mapper(mapperQ,reductionQ,tid - (NUM_MAPPERS + 1),&parserlck);
            }
            mapperIsDone -= 1;
        }
        else if(tid == 0 || (tid > NUM_READERS + NUM_MAPPERS && tid <= NUMPROCESS)){
            //REDUCER
            while(mapperIsDone >0) {               
                asm("");
            }
            if(tid == 0){
                MPI_Barrier(MPI_COMM_WORLD);
                char * reductionArr;
                int * headerArr;
                int totalLength = reductionQToArray(reductionQ, &reductionArr);
                createHeaderStruct(reductionQ, &headerArr);
                int i,j,k = 0;
                char * bufferCharArr;
                int * countFromEachProcessesArr = malloc(sizeof(int) * TOTAL_REDUCERS);
                MPI_Alltoall(headerArr, NUM_REDUCERS, MPI_INT, countFromEachProcessesArr, NUM_REDUCERS, MPI_INT, MPI_COMM_WORLD);
                MPI_Barrier(MPI_COMM_WORLD);
                int totalCountToRecv = 0;
                int totalCountToSend = 0; 
                int localCountRecv = 0;
                int localCountSend = 0;
                int localDisplacementRecv = 0;
                int localDisplacementSend = 0;
                for(i = 0; i < TOTAL_REDUCERS; i++){
                    totalCountToRecv += countFromEachProcessesArr[i];
                    totalCountToSend += headerArr[i];
                }
                int * countsToSend = malloc(sizeof(int) * numP);
                int * displacementsToSend = malloc(sizeof(int) * numP);
                char * bufferToRecv = malloc(sizeof(char) * BUFFERSIZE * totalCountToRecv);
                int * countsToRecv = malloc(sizeof(int) * numP); 
                int * displacementsToRecv = malloc(sizeof(int) * numP);
                for(i = 0; i < numP; i++){
                    localCountRecv = 0; 
                    localCountSend = 0;
                    localDisplacementSend = 0;
                    localDisplacementRecv = 0; 
                    for(j = i*NUM_REDUCERS; j < i*NUM_REDUCERS + NUM_REDUCERS; j++){
                        localCountRecv += countFromEachProcessesArr[j];
                        localCountSend += headerArr[j];
                    }
                    countsToSend[i] = BUFFERSIZE * localCountSend;
                    countsToRecv[i] = BUFFERSIZE * localCountRecv;
                    for(k = i*NUM_REDUCERS - 1; k >= 0; k--){
                        localDisplacementSend += headerArr[k];
                        localDisplacementRecv += countFromEachProcessesArr[k];
                    }
                    displacementsToSend[i] = BUFFERSIZE * localDisplacementSend;
                    displacementsToRecv[i] = BUFFERSIZE * localDisplacementRecv;
                }
                // if(pid == 3){
                //     for(i = 0; i < TOTAL_REDUCERS; i++){
                //         printf("%d, ", countFromEachProcessesArr[i]);
                //     }
                //     for(i = 0; i < numP; i++){
                //         printf("%d, %d, %d, %d \n", countsToSend[i], countsToRecv[i], displacementsToSend[i], displacementsToRecv[i]);
                //     }
                // }
                MPI_Alltoallv(reductionArr,countsToSend,displacementsToSend,MPI_CHAR, bufferToRecv,countsToRecv,displacementsToRecv,MPI_CHAR, MPI_COMM_WORLD);
                MPI_Barrier(MPI_COMM_WORLD);
                
                bufferToReductionQ(bufferToRecv, finalReductionQ, countFromEachProcessesArr);
                commIsDone = 1;
            } else {
                while(!commIsDone){
                    asm("");
                }
                reducer(finalReductionQ,hashMap,tid-(NUM_READERS + NUM_MAPPERS + 1),&reducerlck);
            }
        }
    }
    omp_destroy_lock(&parserlck);
    omp_destroy_lock(&reducerlck);
    // char buf[10];
    // sprintf(buf, "%d", pid);
    // char *filename = strcat(buf,".txtout");
    // FILE *outFile = fopen(filename, "w");
    printHashMap(hashMap, stdout);
    MPI_Finalize();
    // printf("MAP REDUCE COMPLETE");
    return 0; 
}