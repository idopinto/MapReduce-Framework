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
struct JobContext;

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
}ThreadContext;

typedef struct JobContext{
    const MapReduceClient *client;
    InputVec inputVec;
    OutputVec *outputVec;
    int  numOfThreads;
    bool hasWaitCalledOnJob;
    JobState jobState{};
    pthread_t* threads;
    ThreadContext* threadContexts;
    std::deque<IntermediateVec> shuffledQueue;
    std::atomic<uint64_t> *jobStateCounter;
    Barrier *barrier;


    uint64_t indexToProcessMap=0;
    uint64_t totalNumOfPairs=0;
    pthread_mutex_t mutexMap = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexEmit = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexReduce =  PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexGetJob =  PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutexWait =  PTHREAD_MUTEX_INITIALIZER;

    JobContext(const MapReduceClient& client,const InputVec &inputVec,OutputVec* outputVec,int numOfThreads){
        this->client = &client;
        this->inputVec = inputVec;
        this->outputVec = outputVec;
        this->numOfThreads = numOfThreads;
        this->hasWaitCalledOnJob = false;
        this->jobState = {.stage= UNDEFINED_STAGE,.percentage = 0.0};
        this->threads = new (std::nothrow)pthread_t[numOfThreads];
        if(this->threads == nullptr){
            printf("memory allocation failure\n");
        }
        this->threadContexts = new (std::nothrow) ThreadContext[numOfThreads];
        if(this->threadContexts == nullptr){
            printf("memory allocation failure\n");
        }
        this->jobStateCounter = new (std::nothrow) std::atomic<uint64_t>(0);
        if(this->jobStateCounter == nullptr){
            printf("memory allocation failure\n");
        }
        this->barrier = new (std::nothrow) Barrier(numOfThreads);
        if(this->barrier == nullptr){
            printf("memory allocation failure\n");
        }
    }
    ~JobContext(){
        delete[] threadContexts;
        delete[] threads;
        delete jobStateCounter;
        delete barrier;

        auto retVal = pthread_mutex_destroy(&this->mutexMap) ;
        if (retVal == EBUSY)
        {
            printf("system error: (MAP) error on pthread_mutex_destroy\nThe mutex is currently owned by another thread\n");
        }
        else if(retVal == EINVAL)
        {
            printf("system error: (MAP) error on pthread_mutex_destroy\nThe value specified for the argument is not correct\n");

        }

//        if (pthread_mutex_destroy(&this->mutexEmit) != 0)
//        {
//            printf("system error: error on pthread_mutex_destroy\n");
//        }
        auto retVal3 = pthread_mutex_destroy(&this->mutexReduce) ;
        auto retVal2 = pthread_mutex_destroy(&this->mutexEmit) ;
        if (retVal2 == EBUSY)
        {
            printf("system error: (EMIT) error on pthread_mutex_destroy\nThe mutex is currently owned by another thread\n");
        }
        else if(retVal2 == EINVAL)
        {
            printf("system error: (EMIT) error on pthread_mutex_destroy\nThe value specified for the argument is not correct\n");

        }
        if (retVal3 == EBUSY)
        {
            printf("system error: (REDUCE) error on pthread_mutex_destroy\nThe mutex is currently owned by another thread\n");
        }
        else if(retVal3 == EINVAL)
        {
            printf("system error: (REDUCE) error on pthread_mutex_destroy\nThe value specified for the argument is not correct\n");

        }
        if (pthread_mutex_destroy(&this->mutexWait) != 0)
        {
            printf("system error: error on pthread_mutex_destroy\n");
        }
        if (pthread_mutex_destroy(&this->mutexGetJob) != 0)
        {
            printf("system error: error on pthread_mutex_destroy\n");
        }

    }
}JobContext;


void emit2 (K2* key, V2* value, void* context){
    ThreadContext* tc = (ThreadContext*)context;
    tc->intermediateVector.emplace_back(key,value);
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
//    tc->job->indexToProcessMap = tc->job->jobStateCounter->load()&(0x7FFFFFFF);
//    (*tc->job->jobStateCounter)++;
    /* Map stage */
    while(tc->job->indexToProcessMap < limit){

        pthread_mutex_lock(&tc->job->mutexMap);
        if(tc->job->indexToProcessMap >= limit){
            pthread_mutex_unlock(&tc->job->mutexMap);
            break;}
        auto current_pair = tc->job->inputVec[tc->job->indexToProcessMap]; // critical section
        (*tc->job->jobStateCounter)++;
        tc->job->indexToProcessMap = tc->job->jobStateCounter->load()&(0x7FFFFFFF);
        pthread_mutex_unlock(&tc->job->mutexMap);
        tc->job->client->map (current_pair.first, current_pair.second, thread);
        (*tc->job->jobStateCounter) += (uint64_t)1 << 31;


    }
//    printf("percentage map %f\n",tc->job->jobState.percentage);

    /* Sort */
    std::sort (tc->intermediateVector.begin (), tc->intermediateVector.end (), compareKeys);

    /*Map stage is over*/
    tc->job->barrier->barrier();
    if(tc->id == MAIN_THREAD_ID){
        for (int i=0;i<tc->job->numOfThreads;i++) {
            tc->job->totalNumOfPairs += tc->job->threadContexts[i].intermediateVector.size();
        }
        shuffle(thread);
    }
    tc->job->barrier->barrier();
    reduce(thread);
    return nullptr;
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
    (*tc->job->jobStateCounter) = (tc->job->totalNumOfPairs<<31 | (uint64_t) SHUFFLE_STAGE << 62);
//    tc->job->jobState.percentage = 0.0;
    tc->job->jobState.stage = SHUFFLE_STAGE;
    int index = findFirstNotEmptyVector (tc->job);

    while(index != -1){
//        printf("percentage shuffle %f\n",tc->job->jobState.percentage);
        (*tc->job->jobStateCounter)++;
        appendToShuffleQ (tc->job,popMaxKey(tc->job,index));
        index = findFirstNotEmptyVector (tc->job);

    }
//    printf("percentage %f\n",tc->job->jobState.percentage);
//    printf("Done\n");
//    for (auto& vec:tc->job->shuffledQueue) {
//        printInterVectors(&vec);
//    }

}

void reduce (void *thread){
    ThreadContext* tc = (ThreadContext*)thread;

    if(tc->job->jobState.stage == SHUFFLE_STAGE){
        (*tc->job->jobStateCounter) = (tc->job->totalNumOfPairs<<31 | (uint64_t) REDUCE_STAGE << 62);
        tc->job->jobState.stage = REDUCE_STAGE;
    }

    while(!tc->job->shuffledQueue.empty()){
        pthread_mutex_lock (&tc->job->mutexReduce);
        if (tc->job->shuffledQueue.empty()){
            pthread_mutex_unlock (&tc->job->mutexReduce);
            return;}
        IntermediateVec currentVector = tc->job->shuffledQueue.back();
        tc->job->shuffledQueue.pop_back();
        pthread_mutex_unlock (&tc->job->mutexReduce);
        tc->job->client->reduce (&currentVector,thread);
        (*tc->job->jobStateCounter) += currentVector.size();//TODO:MUTEX?
    }
}


JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    // Initialize job context struct
    JobContext *jc = new JobContext(client,inputVec,&outputVec,multiThreadLevel);
    // update the job state to be in map stage
    (*(jc->jobStateCounter)) = (uint64_t)1 << 62;
//    (*(jc->jobStateCounter)) |= jc->inputVec.size() << 31;
    jc->jobState.stage = MAP_STAGE;
    // create threads and their contexts
    for (int i = 0; i < multiThreadLevel; ++i) {
//        printf("counter : %lu\n",jc->jobStateCounter->load());

        jc->threadContexts[i] = {.id=i,.intermediateVector=IntermediateVec(),.job=jc,.hasWaited=false};
//        printf("counter : %lu\n",jc->jobStateCounter->load());
        if(pthread_create (jc->threads + i,NULL,threadStartFunc,jc->threadContexts + i)!= 0){
            fprintf(stderr,"ERROR");
            exit(1);
        }
    }
    return static_cast<JobHandle>(jc);
}

void waitForJob(JobHandle job){

    JobContext* jc = (JobContext *)(job);
    pthread_mutex_lock(&jc->mutexWait);
    if(jc->hasWaitCalledOnJob){
        printf("calling waitForJob twice from the same process");
        pthread_mutex_unlock(&jc->mutexWait);
        return;
    }
    else{jc->hasWaitCalledOnJob = true;}
    pthread_mutex_unlock(&jc->mutexWait);
    for (int i = 0; i < jc->numOfThreads; ++i) {
        if (pthread_join(jc->threads[i], nullptr) != 0)
        {
            printf("system error: pthread_join error\n");
            exit(1);
        }
    }
}


//    for (int i = 0; i < jc->numOfThreads; i++)
//    {
//        if (jc->threadContexts[i].hasWaited){
//
//            continue;}
//        jc->threadContexts[i].hasWaited= true;
//        if (pthread_join(jc->threads[i], nullptr) != 0)
//        {
//            printf("system error: pthread_join error\n");
//            exit(1);
//        }
//    }

void getJobState(JobHandle job, JobState* state){
    auto jc = (JobContext *) job;
    pthread_mutex_lock (&jc->mutexGetJob);
    uint64_t curCounter = *jc->jobStateCounter;  // receive current details
    uint64_t total, alreadyProcessed;
    // manipulation to get correct percentage
    if(jc->jobState.stage == MAP_STAGE){
        total = jc->inputVec.size();
        alreadyProcessed = ((curCounter >> 31) & (0x7FFFFFFF));
    }
    else{
        total = ((curCounter >> 31) & (0x7FFFFFFF));
        alreadyProcessed = curCounter & (0x7FFFFFFF);
    }
    if(total == 0){
        pthread_mutex_unlock (&jc->mutexGetJob);
        printf("Division by zero\n ");
        return;}
    jc->jobState.percentage = (100 * (float)alreadyProcessed/(float)total);
    state->stage = jc->jobState.stage;
    state->percentage = jc->jobState.percentage;
    pthread_mutex_unlock (&jc->mutexGetJob);
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
