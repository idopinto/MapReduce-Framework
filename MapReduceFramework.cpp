

#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <map>
#include <Barrier.h>
#include <cstdio>
#include <iostream>
#include <bits/stdc++.h>
#include <semaphore.h>

#define DONE 0x7FFFFFFF
#define TOTAL 31
#define STAGE_BITS 62
#define GET_ALREADY_PROCESSED(X) (X & 0x7FFFFFFF)
#define GET_TOTAL_TO_PROCESS(X) ((X >> 31) & (0x7FFFFFFF))

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


typedef struct JobContext{
    const MapReduceClient *client;
    InputVec  inputVec;
    OutputVec outputVec;
    std::map<pthread_t,int> tMap;
    std::atomic<uint64_t> *atomic_counter;
    JobState jobState;
    std::map<int,IntermediateVec*> midVecMap;
    Barrier barrier;
    uint64_t *inputVecCounter=0;
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_emit2 = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_emit3 =  PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t mutex_reduce =  PTHREAD_MUTEX_INITIALIZER;

}JobContext;

struct ThreadContext{
    ThreadContext() : id(0), tid(0), has_waited(false), jobContext(nullptr){}
    unsigned int id;
    pthread_t tid;
    std::atomic<bool> has_waited;
    JobContext *jobContext;
};

void printMidVecMap(int tid ,IntermediateVec *vec){
    for (auto& p : *vec) {
        std::cout <<"( "<< ((const KChar*)p.first)->c<< ", "<<((const VCount*)p.second)->count<<" )";
    }
    std::cout<<" --> \n";
}

bool compareKeys(IntermediatePair p1,IntermediatePair p2)
{
    return *p1.first < *p2.first;
}
/**
 * The function receives as input intermediary element (K2, V2) and context which contains
data structure of the thread that created the intermediary element. The function saves the
intermediary element in the context data structures. In addition, the function updates the
number of intermediary elements using atomic counter.
Please pay attention that emit2 is called from the client's map function and the context is
passed from the framework to the client's map function as parameter.

 * @param key
 * @param value
 * @param context
 */
//bool isContainsKey(void* job,K2* key){
//    auto* jc = (JobContext *) job;
//    for (auto& k: *jc->uniqueKeys) {
//        if(!(key < k | k < key)){
//            return true;
//        }
//    }
//    return false;
//}

void emit2 (K2* key, V2* value, void* context){
    auto jc = static_cast<JobContext*>(context);

    pthread_mutex_lock(&jc->mutex_emit2);
    auto tid_iter = jc->tMap.find(pthread_self());
    if(tid_iter == jc->tMap.end()){
        std::cerr<<"ERROR2 thread not found"<<std::endl;
    }

//    std::cout<< "emit 2 tid:" <<tid<<std::endl;
//    std::cout<<"atomic counter done is:"<<GET_ALREADY_PROCESSED(jc->atomic_counter->load()) <<std::endl;
//    std::cout<<"atomic counter total is:"<<GET_TOTAL_TO_PROCESS(jc->atomic_counter->load()) <<std::endl;
    jc->midVecMap.at(tid_iter->second)->emplace_back(key,value);
    pthread_mutex_unlock(&jc->mutex_emit2);
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

}
void printCounterBits(void* job){
    auto* jc = (JobContext *) job;
    printf("~~~~~~\n");
    printf("already processed: (first 31 bit): %lu\n", jc->atomic_counter->load() & 0x7FFFFFFF);
    printf("total elements to process: (next 31 bit): %lu\n", jc->atomic_counter->load() >> 31 & 0x7FFFFFFF);
    printf("Stage: last 2 bit: %lu\n", jc->atomic_counter->load() >> 62);
}

float calcPercentage(void* job){
    auto* jc = (JobContext *) job;
    float total = GET_TOTAL_TO_PROCESS(jc->atomic_counter->load());
    float done = GET_ALREADY_PROCESSED(jc->atomic_counter->load());
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
//        (*(jc->atomic_counter)) += (uint64_t)1 << STAGE_BITS;
//        jc->jobState.stage = (stage_t)(jc->atomic_counter->load() >> STAGE_BITS);
//        (*jc->atomic_counter) = (*jc->atomic_counter) & 0xC000000000000000 | (uint64_t)jc->uniqueKeys->size() <<TOTAL;
//    }

}

void shuffle(void* JobContext){

    printf("hello");

}

void* startRoutine(void* jobContext){
    auto* jc = (JobContext *) jobContext;

    auto tid_iter = jc->tMap.find(pthread_self());
    if(tid_iter == jc->tMap.end()){
        std::cerr<<"ERROR1 thread not found"<<std::endl;
        return nullptr;
    }
    int tid = tid_iter->second;

    auto midVec = jc->midVecMap.at(tid);
    std::cout << "Thread "<<tid << " starts map stage..."<<std::endl;

    /* Map*/
    while(*jc->inputVecCounter < jc->inputVec.size())
    {
        auto current_pair = jc->inputVec.at(*jc->inputVecCounter);
        ++(*(jc->atomic_counter));
        *jc->inputVecCounter = jc->atomic_counter->load() & 0x7fffffff;
        std::cout<<dynamic_cast<const VString*>(current_pair.second)->content<<" is being processed now by: "<<tid<<std::endl;
        jc->client->map(current_pair.first,current_pair.second,jobContext);
    }

    /* Sort Stage*/
//    pthread_mutex_lock(&jc->mutex);
//    std::cout<<"Thread "<< tid <<" Sorting now.."<<std::endl;
//    pthread_mutex_unlock(&jc->mutex);
    std::sort(midVec->begin(),midVec->end(), compareKeys);

//    pthread_mutex_lock(&jc->mutex);
//    printMidVecMap(tid,midVec);
//    pthread_mutex_unlock(&jc->mutex);

    /*wait until all threads reach. then only thread 0 goes to shuffle and the rest are waiting for him*/
    jc->barrier.barrier(tid,&shuffle,jobContext);

    /*Reduce*/
    return nullptr;

}

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
JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    std::atomic<uint64_t> atomic_counter( inputVec.size()<<31);
    uint64_t x =0;
    Barrier barrier = Barrier(multiThreadLevel);
    std::map<pthread_t,int> tMap;
    std::map<int,IntermediateVec*> midVecMap;
    JobContext *jobContext = new JobContext {.client=&client,
                                             .inputVec = inputVec,
                                             .outputVec=outputVec,
                                             .tMap = tMap,
                                             .atomic_counter = &atomic_counter,
                                             .jobState={UNDEFINED_STAGE,0.0},
                                             .midVecMap=midVecMap,
                                             .barrier=barrier,
                                             .inputVecCounter= &x,
                                             };

    if(jobContext->atomic_counter->load() >> 62 == 0){
        (*(jobContext->atomic_counter)) += (uint64_t)1 << 62;
        jobContext->jobState.stage = MAP_STAGE;
    }
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_t thread;
        pthread_create(&thread, NULL, startRoutine, jobContext);
        jobContext->tMap.insert({thread,i});
        jobContext->midVecMap.insert({i,new IntermediateVec()});
    }
//    pthread_mutex_lock(&jobContext->mutex);
////    std::cout<<"atomic counter size is:"<<GET_TOTAL_TO_PROCESS(jobContext->atomic_counter->load()) <<std::endl;
//    pthread_mutex_unlock(&jobContext->mutex);
    return static_cast<JobHandle>(jobContext);
}

/**
 *a function gets JobHandle returned by startMapReduceFramework and waits
 * until it is finished.
 * @param job
 */    jc->barrier.barrier(tid,&shuffle,jobContext);

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