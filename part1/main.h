
typedef struct WordPair {
    char* word;
    int count;
    struct WordPair *next;
} WordPair;

typedef struct HashMap{
    WordPair *head;
    WordPair *tail;
} HashMap;

typedef struct MapperWorkElement {
    char* word;
    struct MapperWorkElement* next;
} MapperWorkElement;

typedef struct MapperWorkQueue {
    MapperWorkElement* head;
    MapperWorkElement* tail;
    int len;
} MapperWorkQueue;

typedef struct ReductionWorkElement {
    WordPair* wordPair;
    struct ReductionWorkElement* next;
} ReductionWorkElement;

typedef struct ReductionWorkQueue {
    ReductionWorkElement* head;
    ReductionWorkElement* tail;
    int len;
} ReductionWorkQueue;

int hashCode(char* word);
void initHashMap(HashMap * hashMap);
void initMapperQueues(MapperWorkQueue * mapperQ);
void addToMapperQueue(MapperWorkQueue* mapperQ,MapperWorkElement* wordElement, int index);
void initReductionQueues(ReductionWorkQueue * reductionQ);
void parser(FILE *f, HashMap *hashMap,MapperWorkQueue* mapperQ, omp_lock_t *lck);
void mapper(MapperWorkQueue* mapperQ, ReductionWorkQueue* reductionQ, int index, int *readerIsDone, omp_lock_t *parserlck ,omp_lock_t *mapperlck);
void reducer(ReductionWorkQueue* reductionQ, HashMap* hashMap, int index, omp_lock_t* lck);
int addToHashMap(HashMap *hashMap, char *buffer);
void printHashMap(HashMap *hashMap);
