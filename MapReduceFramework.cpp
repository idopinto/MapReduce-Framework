

#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <map>
#include <Barrier.h>
#include <cstdio>
#include <iostream>
#include <bits/stdc++.h>
#include <semaphore.h>
#include <sys/unistd.h>

#define DONE 0x7FFFFFFF
#define TOTAL 31
#define STAGE_BITS 62
#define GET_ALREADY_PROCESSED(X) (X & 0x7FFFFFFF)
#define GET_TOTAL_TO_PROCESS(X) ((X >> 31) & (0x7FFFFFFF))
bool compareIfKeysEqual(IntermediatePair p1,IntermediatePair p2);
void printCounterBits(void* job);
void printMidVecMap(IntermediateVec *vec);
void printOutputVecMap(OutputVec *vec);
float calcPercentage(void* job);
void updateJobState(void* job);
void shuffle(void* job);
bool compareKeys(IntermediatePair p1,IntermediatePair p2);
void* startRoutine(void* job);
void reduce (void *job);

class VString : public V1 {
public:
    VString(std::string content) : content(content) { }
    std::string content;
};
class KChar : public K2, public K3{
public:
    KChar(char c) : c(c) { }
    virtual bool operator<(const K2 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    virtual bool operator<(const K3 &other) const {
        return c < static_cast<const KChar&>(other).c;
    }
    char c;
};
class VCount : public V2, public V3{
public:
    VCount(int count) : count(count) { }
    int count;
};

pthread_mutex_t mutexPrints = PTHREAD_MUTEX_INITIALIZER;

typedef struct JobContext{

    const MapReduceClient *client;
    InputVec  inputVec;
    OutputVec outputVec;
    int  numOfThreads;
    JobState jobState;
    pthread_t *threads;
    IntermediateVec **intermediateVectors;
    std::map<pthread_t,int> tMap;

//    std::map<int,IntermediateVec*> midVecMap;
    std::deque<IntermediateVec> shuffledQueue;

    std::atomic<uint64_t> *jobStateCounter;
    Barrier barrier;

    uint32_t inputVecCounter=0;
    uint32_t intermediateVecCounter=0;
    uint32_t outputVecCounter=0;

    pthread_mutex_t mutexBinary = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexEmit = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexReduce =  PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexInsert =  PTHREAD_MUTEX_INITIALIZER;

//    sem_t semShuffle;

}JobContext;

/**
 * The function starts running the MapReduce algorithm (with several threads)
 *
 * @param client The task that the framework should run
 * @param inputVec A vector of type vector<pair<K1*,V1*>, the input elements
 * @param outputVec A vector of type vector<pair<K3*,V3*>, the output elements, will be added before returning.
 * @param multiThreadLevel The number of threads to be used for running the algorithm
 * Assumption: the outputVec is not empty
 * @return JobHandle
 */
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel){
   pthread_t threads[multiThreadLevel];
   IntermediateVec* interVectors[multiThreadLevel];
    std::atomic<uint64_t> atomicCounter( 0);
    //inputVec.size()<<31
    Barrier barrier = Barrier(multiThreadLevel);
    std::map<pthread_t,int> tMap;
    std::map<int,IntermediateVec*> midVecMap;
    std::deque<IntermediateVec> shuffledQueue;

    JobContext *jobContext = new JobContext {.client=&client,
                                             .inputVec = inputVec,
                                             .outputVec=outputVec,
                                             .numOfThreads=multiThreadLevel,
                                             .jobState={UNDEFINED_STAGE,0.0},
                                             .threads = threads,
                                             .intermediateVectors=interVectors,
                                             .tMap = tMap,
                                             .jobStateCounter = &atomicCounter,
                                             .barrier = barrier
                                             };

    // switched to MAP_STAGE
    (*(jobContext->jobStateCounter)) += (uint64_t)1 << 62;
    jobContext->jobState.stage = MAP_STAGE;

    /* Create the threads with startRoutine as entry point with the jobContext*/
  for (int i = 0; i < multiThreadLevel; ++i)
    {

        if(pthread_create (jobContext->threads + i,NULL,startRoutine,jobContext)!= 0){
          fprintf(stderr,"ERROR");
          exit(1);
        }

      auto *interVec = new IntermediateVec();
      jobContext->intermediateVectors[i] = interVec;
        // insert
      jobContext->tMap[*(jobContext->threads + i)] = i;
    }


//    for (int i = 0; i < multiThreadLevel; ++i) {
//        pthread_t thr;
//        pthread_t* pntr = &thr;//        pthread_mutex_lock (&jobContext->mutexInsert);
//        jobContext->tMap.insert({*pntr,i}); // critical section
//        jobContext->midVecMap.insert({i,new IntermediateVec()});
////        pthread_mutex_unlock (&jobContext->mutexInsert);
//
////        pthread_create(&thread, NULL, startRoutine, jobContext);
//    }
//  for (auto &pair: jobContext->tMap){
//      auto* thread = const_cast<pthread_t *>(&pair.first);
//      pthread_create(thread, NULL, startRoutine, jobContext);
//    }

    return static_cast<JobHandle>(jobContext);
}

/**
 *a function gets JobHandle returned by startMapReduceFramework and waits
 * until it is finished.
 * @param job
*/
void waitForJob(JobHandle job){
//
//    for (auto& tid_pair:static_cast<JobContext*>(job)->tMap) {
//        pthread_join(tid_pair.first,NULL);
//    }
}

/**
 * this function gets a JobHandle and updates the state of the job into the given
 * JobState struct
 * @param job
 * @param state
 */
void getJobState(JobHandle job, JobState* state){
    auto* j = static_cast<JobContext*>(job);
    state->stage = j->jobState.stage;
    state->percentage = j->jobState.percentage;
}
/**
 * – Releasing all resources of a job. You should prevent releasing resources
 * before the job finished. After this function is called the job handle will be invalid.
 * @param job
 */
void closeJobHandle(JobHandle job){

}

int get_thread_index(void* job) { // note: critical section
  auto *jc = static_cast<JobContext *>(job);
  if (jc->tMap.find(pthread_self()) == jc->tMap.end()) return -1;
  return jc->tMap[pthread_self()];
}


/**
 * The function receives as input intermediary element (K2, V2) and context
 * which contains data structure of the thread that created the intermediary
 * element. The function saves the
 * intermediary element in the context data structures.
 * In addition, the function updates the
 * number of intermediary elements using atomic counter.
 * Please pay attention that emit2 is called from the client's map function
 * and the context is passed from the framework to the client's map function
 * as parameter.
 * @param key
 * @param value
 * @param context
 */

void emit2 (K2* key, V2* value, void* context){
  auto jc = static_cast<JobContext*>(context);

//  pthread_mutex_lock(&jc->mutexEmit);
  auto id = get_thread_index (context);
  if(id == -1){
    fprintf(stderr,"error");
    exit(1);
  }
  jc->intermediateVectors[id]->emplace_back (key,value);
  jc->intermediateVecCounter++; // not atomic but there is mutex
//  pthread_mutex_unlock(&jc->mutexEmit);
}

/**
 * The function receives as input output element (K3, V3) and context which contains data
structure of the thread that created the output element. The function saves the output
element in the context data structures (output vector). In addition, the function updates the
number of output elements using atomic counter.
Please pay attention that emit3 is called from the client's map function and the context is
passed from the framework to the client's map function as parameter.
 * @param key
 * @param value
 * @param context
 */
void emit3 (K3* key, V3* value, void* context){
  auto jc = static_cast<JobContext*>(context);

//  pthread_mutex_lock(&jc->mutexEmit);
    auto id = get_thread_index (context);
    if(id == -1){
    fprintf(stderr,"error");
    exit(1);
    }
  jc->outputVec.emplace_back(key,value);
  jc->outputVecCounter++; // not atomic but there is mutex
//  pthread_mutex_unlock(&jc->mutexEmit);
}



void* startRoutine(void* job)
{
  auto *jc = static_cast<JobContext *>(job);

  int id = get_thread_index (job); // critical section
  if (id == -1){exit (1);}
//  auto midVec = jc->midVecMap.at (id); // critical section
  auto limit = jc->inputVec.size ();
  /* Map*/
  while (jc->inputVecCounter < limit)
    {
//      auto current_pair = jc->inputVec.at (jc->inputVecCounter); // critical section

     ++(*(jc->jobStateCounter)); // atomic operation

      pthread_mutex_lock (&jc->mutexBinary);
      auto current_pair = jc->inputVec.at (jc->inputVecCounter); // critical section
      jc->inputVecCounter = jc->jobStateCounter->load () & DONE;
      pthread_mutex_unlock (&jc->mutexBinary);
      pthread_mutex_lock (&mutexPrints);
      std::cout << dynamic_cast<const VString *>(current_pair.second)->content
      << " by: " << id << std::endl;
    printf("limit: %lu, counter: %d\n",limit,jc->inputVecCounter);
    pthread_mutex_unlock (&mutexPrints);

      jc->client->map (current_pair.first, current_pair.second, job);

    }
  /* Sort Stage*/
  std::sort (jc->intermediateVectors[id]->begin (), jc->intermediateVectors[id]->end (), compareKeys);
  printMidVecMap(jc->intermediateVectors[id]);

  /*wait until all threads reach. then only thread 0 goes to shuffle and the rest are waiting for him*/
  jc->barrier.barrier ();
  auto afterBarrierId = get_thread_index(job);
  if(afterBarrierId == -1){
      exit(1);
  }
  if(afterBarrierId == 0){
    shuffle(job);
  }
  jc->barrier.barrier();
  reduce(job);
//  if (afterBarrierThread->second == 0)
//    { shuffle (job); }
//  jc->barrier.barrier ();
//  reduce (job);
  return nullptr;
}



  //wait
//  sem_wait(&jc->semShuffle);
////  pthread_mutex_lock(&mutexPrints);
////  printf("\nEntered..\n");
////  pthread_mutex_unlock(&mutexPrints);
//  //critical section
//  if(jc->tMap[pthread_self()] == 0)
//    {
//      shuffle (job);
//    }
//  //signal
//  sem_post(&jc->semShuffle);

//


  /*Reduce*/
//  reduce(job);



void reduce (void *job)
{
  auto* jc =  static_cast<JobContext*>(job);


  while(!jc->shuffledQueue.empty()){
    pthread_mutex_lock (&jc->mutexReduce);
    IntermediateVec currentVector = jc->shuffledQueue.back();
    jc->shuffledQueue.pop_back();
    pthread_mutex_lock (&mutexPrints);
//    printMidVecMap(&currentVector);
    std::cout<<"reduced  by: "<<jc->tMap[pthread_self()]<<std::endl;
    pthread_mutex_unlock (&mutexPrints);
    pthread_mutex_unlock (&jc->mutexReduce);
    jc->client->reduce (&currentVector,job);
  }

  printOutputVecMap(&jc->outputVec);

}


float calcPercentage(void* job){
  auto* jc = (JobContext *) job;
  float total = GET_TOTAL_TO_PROCESS(jc->jobStateCounter->load());
  float done = GET_ALREADY_PROCESSED(jc->jobStateCounter->load());
  return (total == 0) ? 0: (100 * (done/total));
}

void updateJobState(void* job){
  auto* jc = (JobContext *) job;
  auto p = calcPercentage(job);
  if(p == 0){
      std::cerr<<"Error division by zero"<<std::endl;
      exit(1);
    }
//    std::cout << "new percentage: " << p <<std::endl;
  jc->jobState.percentage = p;

//    if(jc->jobState.percentage == 100.0){
//        (*(jc->jobStateCounter)) += (uint64_t)1 << STAGE_BITS;
//        jc->jobState.stage = (stage_t)(jc->jobStateCounter->load() >> STAGE_BITS);
//        (*jc->jobStateCounter) = (*jc->jobStateCounter) & 0xC000000000000000 | (uint64_t)jc->uniqueKeys->size() <<TOTAL;
//    }

}


int findFirstNotEmptyVector(void *job){
  auto* jc =  static_cast<JobContext*>(job);
  for(int i=0;i<jc->numOfThreads;i++){
      if(!jc->intermediateVectors[i]->empty()){
          return i;
      }
  }

    return -1;
}

IntermediatePair popMaxKey(void* job,int index){
  auto* jc =  static_cast<JobContext*>(job);
  auto length = jc->intermediateVectors[index]->size();
  IntermediatePair maxPair = jc->intermediateVectors[index]->at(length-1);
//  IntermediatePair maxPair = jc->midVecMap.at(index)->at (length-1);
   for (int i=0;i <jc->numOfThreads;i++ ) {
        length = jc->intermediateVectors[i]->size();
        if(length == 0){ continue;}
        auto curKey =  jc->intermediateVectors[i]->at(length-1);
        if(maxPair.first < curKey.first){
        maxPair= curKey;
        index = i;
        }

   }
   jc->intermediateVectors[index]->pop_back();
   return maxPair;

}

void appendToShuffleQ(void* job ,IntermediatePair pair){
  auto* jc =  static_cast<JobContext*>(job);
  bool success = false;
  for (auto& vec:jc->shuffledQueue){
    if ((!vec.empty())&&(compareIfKeysEqual(pair,vec.at (0)))){
      vec.push_back(pair);
      success = true;
      break;
    }
  }

  if(!success){
    IntermediateVec newKeyVec;
    newKeyVec.push_back(pair);
    jc->shuffledQueue.push_front(newKeyVec);
    jc->intermediateVecCounter++;
  }

}

void shuffle(void* job){
  auto* jc =  static_cast<JobContext*>(job);

  pthread_mutex_lock (&mutexPrints);
    printf("%d in shuffle..\n",jc->tMap[pthread_self()]);
  pthread_mutex_unlock (&mutexPrints);

  int index = findFirstNotEmptyVector (job);
  while(index != -1)
    {
      appendToShuffleQ (job,popMaxKey(job,index));
      index = findFirstNotEmptyVector (job);
    }

  for (auto& vec:jc->shuffledQueue){
      printMidVecMap(&vec);
  }
}

bool compareKeys(IntermediatePair p1,IntermediatePair p2)
{
  return *p1.first < *p2.first;
}
bool compareIfKeysEqual(IntermediatePair p1,IntermediatePair p2)
{
  return !(*p1.first < *p2.first || *p2.first < *p1.first) ;
}

void printCounterBits(void* job){
  auto* jc = (JobContext *) job;
  printf("~~~~~~\n");
  printf("already processed: (first 31 bit): %lu\n", jc->jobStateCounter->load() & 0x7FFFFFFF);
  printf("total elements to process: (next 31 bit): %lu\n", jc->jobStateCounter->load() >> 31 & 0x7FFFFFFF);
  printf("Stage: last 2 bit: %lu\n", jc->jobStateCounter->load() >> 62);
}

void printMidVecMap(IntermediateVec *vec){
  pthread_mutex_lock(&mutexPrints);

  for (auto& p : *vec) {
      std::cout <<"( "<< ((const KChar*)p.first)->c<< ", "<<
                ((const VCount*)p.second)->count<<" )";
    }
  std::cout<<" --> \n";
  pthread_mutex_unlock(&mutexPrints);

}

void printOutputVecMap(OutputVec *vec){
  pthread_mutex_lock(&mutexPrints);
//  std::cout <<"reduce done!"<<std::endl;

  for (auto& p : *vec) {
      std::cout <<"( "<< ((const KChar*)p.first)->c<< ", "<<
                ((const VCount*)p.second)->count<<" )";
    }
  std::cout<<" --> \n";
//  std::cout <<"reduce done!2"<<std::endl;

  pthread_mutex_unlock(&mutexPrints);

}

//struct ThreadContext{
//    ThreadContext() : id(0), tid(0), has_waited(false), jobContext(nullptr){}
//    unsigned int id;
//    pthread_t tid;
//    std::atomic<bool> has_waited;
//    JobContext *jobContext;
//};

//bool isContainsKey(void* job,K2* key){
//    auto* jc = (JobContext *) job;
//    for (auto& k: *jc->uniqueKeys) {
//        if(!(key < k | k < key)){
//            return true;
//        }
//    }
//    return false;
//}
