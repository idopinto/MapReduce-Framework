//
// Created by idopinto12 on 26/04/2022.
//

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
pthread_mutex_t mutexPrints = PTHREAD_MUTEX_INITIALIZER;

void printInterVectors(IntermediateVec *vec);
bool compareKeys(IntermediatePair p1,IntermediatePair p2);
bool compareIfKeysEqual(IntermediatePair p1,IntermediatePair p2);
void shuffle(void* mainThread);
void reduce (void *thread);
class JobContext;

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



typedef struct ThreadContext{
    int id;
    IntermediateVec *intermediateVector;
    JobContext* job;
}ThreadContext;

typedef struct JobContext{

    const MapReduceClient *client;
    InputVec inputVec;
    OutputVec *outputVec;
    int  numOfThreads;
    JobState jobState;
    pthread_t* threads;
    ThreadContext* threadContexts;
    std::deque<IntermediateVec> shuffledQueue;
    std::atomic<uint64_t> *jobStateCounter;
    Barrier *barrier;
    uint32_t indexToProcessMap=0;
    uint64_t totalNumOfPairs=0;
//    uint32_t shuffleReduceCounter=0;

    pthread_mutex_t mutexMap = PTHREAD_MUTEX_INITIALIZER;

    pthread_mutex_t mutexEmit = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexReduce =  PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexGetJobState =  PTHREAD_MUTEX_INITIALIZER;
}JobContext;


void emit2 (K2* key, V2* value, void* context){
    ThreadContext* tc = (ThreadContext*)context;
    pthread_mutex_lock(&tc->job->mutexEmit);
    tc->intermediateVector->emplace_back(key,value);
    pthread_mutex_unlock(&tc->job->mutexEmit);

}
void emit3 (K3* key, V3* value, void* context){
    ThreadContext* tc= (ThreadContext*)context;
    pthread_mutex_lock(&tc->job->mutexEmit);
    tc->job->outputVec->emplace_back(key,value);
    pthread_mutex_unlock(&tc->job->mutexEmit);
}
void* threadStartFunc(void* thread){
    ThreadContext* tc = (ThreadContext*)thread;
    auto limit = tc->job->inputVec.size ();
    /* Map stage */

    while(tc->job->indexToProcessMap < limit)
    {
        pthread_mutex_lock(&tc->job->mutexMap);
        auto current_pair = tc->job->inputVec[tc->job->indexToProcessMap]; // critical section
        (*tc->job->jobStateCounter)++;
        tc->job->indexToProcessMap = (*tc->job->jobStateCounter) & 31;
        pthread_mutex_unlock(&tc->job->mutexMap);
        tc->job->client->map (current_pair.first, current_pair.second, thread);
    }
//    do{
//        pthread_mutex_lock(&tc->job->mutexMap);
//        auto current_pair = tc->job->inputVec[tc->job->indexToProcessMap]; // critical section
//        (*tc->job->jobStateCounter)++;
//        tc->job->indexToProcessMap = (*tc->job->jobStateCounter) & 31;
//        pthread_mutex_unlock(&tc->job->mutexMap);
//        tc->job->client->map (current_pair.first, current_pair.second, thread);
//
//    }while(tc->job->indexToProcessMap < limit);
    /* Sort */

    std::sort (tc->intermediateVector->begin (), tc->intermediateVector->end (), compareKeys);
    /*Map stage is over*/
    tc->job->barrier->barrier();
//    printInterVectors(tc->intermediateVector);
    if(tc->id == 0){shuffle(thread);}
    tc->job->barrier->barrier();
    reduce(thread);

    return nullptr;
}

int findFirstNotEmptyVector(void *job){
    JobContext* jc =  (JobContext*)job;
    for(int i=0;i<jc->numOfThreads;i++){
        if(!jc->threadContexts[i].intermediateVector->empty()){
            return i;
        }
    }
    return -1;
}

IntermediatePair popMaxKey(void* job,int index){
    JobContext* jc =  (JobContext*)job;
    auto length = jc->threadContexts[index].intermediateVector->size();

    IntermediatePair maxPair = jc->threadContexts[index].intermediateVector->at(length-1);

    for (int i=0;i <jc->numOfThreads;i++ ) {
        length = jc->threadContexts[i].intermediateVector->size();
        if(length == 0){ continue;}
        auto curKey =  jc->threadContexts[i].intermediateVector->at(length-1);
        if(maxPair.first < curKey.first){
            maxPair= curKey;
            index = i;
        }
    }
    jc->threadContexts[index].intermediateVector->pop_back();
    return maxPair;
}

void appendToShuffleQ(void* job ,IntermediatePair pair){
    JobContext* jc =  (JobContext*)job;
    bool success = false;
    for (auto& vec:jc->shuffledQueue){
        if ((!vec.empty())&&(compareIfKeysEqual(pair,vec.at (0)))){
            vec.push_back(pair);
//            jc->jobStateCounter++;
            success = true;
            break;
        }
    }

    if(!success){
        IntermediateVec newKeyVec;
        newKeyVec.push_back(pair);
        jc->shuffledQueue.push_front(newKeyVec);
//        jc->intermediateVecCounter++;
    }
}

void shuffle(void* mainThread){
    ThreadContext* tc = (ThreadContext*)mainThread;

    for (int i=0;i<tc->job->numOfThreads;i++) {
        tc->job->totalNumOfPairs += tc->job->threadContexts[i].intermediateVector->size();
    }
    (*tc->job->jobStateCounter) += (uint64_t)1 << 62;
    auto mask = tc->job->totalNumOfPairs<<31;
    (*tc->job->jobStateCounter) &=  0xC000000000000000;
    (*tc->job->jobStateCounter) |= mask;
    tc->job->jobState.stage = SHUFFLE_STAGE;
//    pthread_mutex_lock(&mutexPrints);
//    printf("total number of pairs: %lu\n",tc->job->totalNumOfPairs);
//    printf("actual number of pairs: %lu\n",((tc->job->jobStateCounter->load() >> 31) & (0x7FFFFFFF)));
//    pthread_mutex_unlock(&mutexPrints);
//    tc->job->jobState.stage = SHUFFLE_STAGE;
    int index = findFirstNotEmptyVector (tc->job);
    while(index != -1)
    {
        appendToShuffleQ (tc->job,popMaxKey(tc->job,index));
        (*tc->job->jobStateCounter)++;
//        printf("shuffle counter: %lu\n",tc->job->jobStateCounter->load()&31);
        index = findFirstNotEmptyVector (tc->job);
    }


//    pthread_mutex_lock(&mutexPrints);
//    printf("total number of pairs: %lu\n",tc->job->totalNumOfPairs);
//    printf("actual number of pairs: %lu\n",((tc->job->jobStateCounter->load() >> 31) & (0x7FFFFFFF)));
//    pthread_mutex_unlock(&mutexPrints);
//      for (auto& vec:tc->job->shuffledQueue){
//          printInterVectors(&vec);
//      }
}

void reduce (void *thread){
    ThreadContext* tc = (ThreadContext*)thread;
    if(tc->job->jobState.stage == SHUFFLE_STAGE){
        (*tc->job->jobStateCounter) += (uint64_t)1 << 62;
        auto mask = tc->job->totalNumOfPairs<<31;
        (*tc->job->jobStateCounter) &=  0xC000000000000000;
        (*tc->job->jobStateCounter) |= mask;
        tc->job->jobState.stage = REDUCE_STAGE;
    }
    while(!tc->job->shuffledQueue.empty()){
        pthread_mutex_lock (&tc->job->mutexReduce);
        IntermediateVec currentVector = tc->job->shuffledQueue.back();
        tc->job->shuffledQueue.pop_back();
        pthread_mutex_unlock (&tc->job->mutexReduce);
        tc->job->client->reduce (&currentVector,thread);
        (*tc->job->jobStateCounter) += currentVector.size();
//        printf("output counter: %lu\n",tc->job->jobStateCounter->load()&31);

    }
//    pthread_mutex_lock(&mutexPrints);
//    printf("Output vector: \n");
//
//    for (auto& p : tc->job->outputVec) {
//        std::cout <<"( "<< ((const KChar*)p.first)->c<< ", "<<
//        ((const VCount*)p.second)->count<<" )";
//    }
//    printf("\n");
//
//    pthread_mutex_unlock(&mutexPrints);
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

    // Initialize job context struct
    JobContext *jc = new JobContext{.client=&client,
                                    .inputVec = inputVec,
                                    .outputVec=&outputVec,
                                    .numOfThreads=multiThreadLevel,
                                    .jobState={UNDEFINED_STAGE,0.0},
                                    .threads = new pthread_t[multiThreadLevel],
                                    .threadContexts = new ThreadContext[multiThreadLevel],
                                    .shuffledQueue = std::deque<IntermediateVec>(),
                                    .jobStateCounter = new std::atomic<uint64_t>(0),
                                    .barrier = new Barrier(multiThreadLevel)
    };

    // update the job state to be in map stage
    (*(jc->jobStateCounter)) |= (uint64_t)1 << 62;
    (*(jc->jobStateCounter)) |= jc->inputVec.size() << 31;
    jc->jobState.stage = MAP_STAGE;

    // create threads and their contexts
    for (int i = 0; i < multiThreadLevel; ++i) {
        jc->threadContexts[i] = {.id=i,.intermediateVector=new IntermediateVec(),.job=jc};
        if(pthread_create (jc->threads + i,NULL,threadStartFunc,jc->threadContexts + i)!= 0){
            fprintf(stderr,"ERROR");
            exit(1);
        }
    }
    return static_cast<JobHandle>(jc);
}

void waitForJob(JobHandle job){

}
void getJobState(JobHandle job, JobState* state){
    auto jc = (JobContext *) job;
    uint64_t curCounter = *jc->jobStateCounter;  // receive current details

    // manipulation to get correct percentage
    uint64_t total = ((curCounter >> 31) & (0x7FFFFFFF));
    if(total == 0){
        printf("Division by zero\n ");
        exit(1);
    }
    uint64_t alreadyProcessed = curCounter & (0x7FFFFFFF);
//    printf("already processed: %lu\n",alreadyProcessed);
    jc->jobState.percentage = (100 * (float)alreadyProcessed/(float)total);
//    jc->jobState.stage =(stage_t) (curCounter << 62);
    state->stage = jc->jobState.stage;
    state->percentage = jc->jobState.percentage;
}
void closeJobHandle(JobHandle job){

}


void printInterVectors(IntermediateVec *vec){
    pthread_mutex_lock(&mutexPrints);
    for (auto& p : *vec) {
        if(vec->empty()){std::cout<< "empty"<<std::endl;}
        std::cout <<"( "<< ((const KChar*)p.first)->c<< ", "<<((const VCount*)p.second)->count<<" )";
    }
    printf("\n");
    pthread_mutex_unlock(&mutexPrints);

}
bool compareKeys(IntermediatePair p1,IntermediatePair p2)
{
    return *p1.first < *p2.first;
}
bool compareIfKeysEqual(IntermediatePair p1,IntermediatePair p2)
{
    return !(*p1.first < *p2.first || *p2.first < *p1.first) ;
}
