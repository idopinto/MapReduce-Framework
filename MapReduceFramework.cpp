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

#define MAIN_THREAD_ID 0
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
    IntermediateVec intermediateVector;
    JobContext* job;
    bool hasWaited;

    ~ThreadContext(){
//        delete intermediateVector;
    }
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
    Barrier *barrier; //
    uint32_t indexToProcessMap=0;
    uint64_t totalNumOfPairs=0;
    pthread_mutex_t mutexMap = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexEmit = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexReduce =  PTHREAD_MUTEX_INITIALIZER;
//    pthread_mutex_t mutexGetJobState =  PTHREAD_MUTEX_INITIALIZER;

    ~JobContext(){
        delete[] threadContexts;

        delete[] threads;
        delete jobStateCounter;
        delete barrier;

        if (pthread_mutex_destroy(&this->mutexMap) != 0)
        {
            printf("system error: error on pthread_mutex_destroy");
        }

        if (pthread_mutex_destroy(&this->mutexEmit) != 0)
        {
            printf("system error: error on pthread_mutex_destroy");
        }

        if (pthread_mutex_destroy(&this->mutexReduce) != 0)
        {
            printf("system error: error on pthread_mutex_destroy");
        }

    }
}JobContext;


void emit2 (K2* key, V2* value, void* context){
    ThreadContext* tc = (ThreadContext*)context;
    pthread_mutex_lock(&tc->job->mutexEmit);
    tc->intermediateVector.emplace_back(key,value);
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
    while(tc->job->indexToProcessMap < limit){
        pthread_mutex_lock(&tc->job->mutexMap);
        if(tc->job->indexToProcessMap >= limit){break;}
        auto current_pair = tc->job->inputVec[tc->job->indexToProcessMap]; // critical section
        (*tc->job->jobStateCounter)++;
        tc->job->indexToProcessMap = (*tc->job->jobStateCounter) & 31;
//        printf("stage 1: %lu\n",tc->job->jobStateCounter->load()&31);
        pthread_mutex_unlock(&tc->job->mutexMap);
        tc->job->client->map (current_pair.first, current_pair.second, thread);
    }
    /* Sort */
    std::sort (tc->intermediateVector.begin (), tc->intermediateVector.end (), compareKeys);

    /*Map stage is over*/
    tc->job->barrier->barrier();
    if(tc->id == MAIN_THREAD_ID){shuffle(thread);}
    tc->job->barrier->barrier();
    reduce(thread);
    pthread_exit(NULL);
}

int findFirstNotEmptyVector(void *job){
    JobContext* jc =  (JobContext*)job;
    for(int i=0;i<jc->numOfThreads;i++){
        if(!jc->threadContexts[i].intermediateVector.empty()){
            return i;
        }
    }
    return -1;
}

IntermediatePair popMaxKey(void* job,int index){
    JobContext* jc =  (JobContext*)job;
    auto length = jc->threadContexts[index].intermediateVector.size();

    IntermediatePair maxPair = jc->threadContexts[index].intermediateVector.at(length-1);

    for (int i=0;i <jc->numOfThreads;i++ ) {
        length = jc->threadContexts[i].intermediateVector.size();
        if(length == 0){ continue;}
        auto curKey =  jc->threadContexts[i].intermediateVector.at(length-1);
        if(maxPair.first < curKey.first){
            maxPair= curKey;
            index = i;
        }
    }
    jc->threadContexts[index].intermediateVector.pop_back();
    return maxPair;
}

void appendToShuffleQ(void* job ,IntermediatePair pair){
    JobContext* jc =  (JobContext*)job;
    bool success = false;
    for (auto& vec:jc->shuffledQueue){
        if ((!vec.empty())&&(compareIfKeysEqual(pair,vec.at (0)))){
            vec.push_back(pair);
            success = true;
            break;
        }
    }
    if(!success){
        IntermediateVec newKeyVec; // allocation?
        newKeyVec.push_back(pair);
        jc->shuffledQueue.push_front(newKeyVec);
    }
}

void shuffle(void* mainThread){
    ThreadContext* tc = (ThreadContext*)mainThread;

    for (int i=0;i<tc->job->numOfThreads;i++) {
        tc->job->totalNumOfPairs += tc->job->threadContexts[i].intermediateVector.size();
    }
//    updateStage(tc->job,MAP_STAGE);
//    (*tc->job->jobStateCounter) += (uint64_t)1 << 62;
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
    for (auto& vec:tc->job->shuffledQueue) {
        printInterVectors(&vec);
    }

}

void reduce (void *thread){
    ThreadContext* tc = (ThreadContext*)thread;

//    updateStage(tc->job,SHUFFLE_STAGE); // critical section
    if(tc->job->jobState.stage == SHUFFLE_STAGE){
        (*tc->job->jobStateCounter) += (uint64_t)1 << 62;//TODO:MUTEX?
        auto mask = tc->job->totalNumOfPairs<<31;
        (*tc->job->jobStateCounter) &=  0xC000000000000000;
        (*tc->job->jobStateCounter) |= mask;
        tc->job->jobState.stage = REDUCE_STAGE;
    }

    while(!tc->job->shuffledQueue.empty()){
//        printf("output counter: %lu\n",tc->job->jobStateCounter->load()&31);
        pthread_mutex_lock (&tc->job->mutexReduce);
        if (tc->job->shuffledQueue.empty())
        {
            return;
        }
        IntermediateVec currentVector = tc->job->shuffledQueue.back();
        tc->job->shuffledQueue.pop_back();
        pthread_mutex_unlock (&tc->job->mutexReduce);
        tc->job->client->reduce (&currentVector,thread);
        (*tc->job->jobStateCounter) += currentVector.size();//TODO:MUTEX?

//        printf("output counter: %lu\n",tc->job->jobStateCounter->load()&31);

    }
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
//    updateStage(jc,UNDEFINED_STAGE);
    (*(jc->jobStateCounter)) |= (uint64_t)1 << 62;
    (*(jc->jobStateCounter)) |= jc->inputVec.size() << 31;
    jc->jobState.stage = MAP_STAGE;

    // create threads and their contexts
    for (int i = 0; i < multiThreadLevel; ++i) {
        jc->threadContexts[i] = {.id=i,.intermediateVector=IntermediateVec(),.job=jc,.hasWaited=false};
        if(pthread_create (jc->threads + i,NULL,threadStartFunc,jc->threadContexts + i)!= 0){
            fprintf(stderr,"ERROR");
            exit(1);
        }
    }
    return static_cast<JobHandle>(jc);
}

void waitForJob(JobHandle job){

    JobContext* jc = (JobContext *)(job);
    for (unsigned int i = 0; i < jc->numOfThreads; i++)
    {
        if (jc->threadContexts[i].hasWaited){continue;}
        jc->threadContexts[i].hasWaited= true;
        if (pthread_join(jc->threads[i], nullptr) != 0)
        {
            printf("system error: pthread_join error\n");
        }
    }
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
    jc->jobState.percentage = (100 * (float)alreadyProcessed/(float)total);
//    jc->jobState.stage =(stage_t) (curCounter << 62);
    state->stage = jc->jobState.stage;
    state->percentage = jc->jobState.percentage;
}
void closeJobHandle(JobHandle job){

    auto jc = (JobContext *) job;
    waitForJob(job);
    delete jc;
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
