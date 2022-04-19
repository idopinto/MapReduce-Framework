

#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <map>
#include <Barrier.h>
#include <cstdio>
#include <iostream>
#include <bits/stdc++.h>

unsigned long oldValue = 0;
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
    pthread_mutex_t mutex;
//    unsigned long *old_val;

}JobContext;

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

void printMidVecMap(int tid ,IntermediateVec *vec){
    for (auto& p : *vec) {
        std::cout <<"( "<< ((const KChar*)p.first)->c<< ", "<<((const VCount*)p.second)->count<<" )";
    }
    std::cout<<" --> \n";
}
void emit2 (K2* key, V2* value, void* context){
    auto* jc = (JobContext *) context;
    auto tid_iter = jc->tMap.find(pthread_self());
    if(tid_iter == jc->tMap.end()){
        std::cerr<<"ERROR2"<<std::endl;
    }
    int tid = tid_iter->second;
    auto midVec = jc->midVecMap.at(tid);
    midVec->push_back({key,value});
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


void* start_routine(void* jobContext){

    auto* jc = (JobContext *) jobContext;
    pthread_mutex_lock(&jc->mutex);

    auto tid_iter = jc->tMap.find(pthread_self());
    if(tid_iter == jc->tMap.end()){
        std::cerr<<"ERROR1"<<std::endl;
        return nullptr;
    }
    int tid = tid_iter->second;
    auto midVec = jc->midVecMap.at(tid);
//    std::cout << "Thread #"<<tid << " starts map stage..."<<std::endl;
    if(oldValue < jc->inputVec.size())
    {
        std::cout<<"-------------Thread " << tid <<" Map ---------------"<<std::endl;
        auto current_pair = jc->inputVec.at(oldValue);
        jc->client->map(current_pair.first,current_pair.second,jobContext);
        printMidVecMap(tid,jc->midVecMap.at(tid));
        oldValue = ++(*(jc->atomic_counter));
        std::cout<<"The pair (nullptr, "<<dynamic_cast<const VString*>(current_pair.second)->content<<") has processed now."<<std::endl;
        std::cout<<"Total # pairs: "<<jc->midVecMap.at(tid)->size()<<std::endl;
        std::cout<<"Thread #"<<tid << " finished map stage..."<<std::endl;
    }
    printf("current counter = %lu\n",jc->atomic_counter->load() &(0xffffff));
    std::cout<<"-------------Thread " << tid <<" Sort ---------------"<<std::endl;
    std::sort(midVec->begin(),midVec->end());
    printMidVecMap(tid,midVec);
    pthread_mutex_unlock(&jc->mutex);

    jc->barrier.barrier();
    if(jc->tMap[pthread_self()] == 0){
        std::cout << "Map phase is done"<<std::endl;
        //TODO SHUFFLE
    }
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
    pthread_t threads[multiThreadLevel];
    std::cout<<"input vector size is:"<<inputVec.size()<<std::endl;
    std::atomic<uint64_t> atomic_counter(0);
//    atomic_counter += inputVec.size()<<31;
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
                                             .mutex = PTHREAD_MUTEX_INITIALIZER,};

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_t thread;
        pthread_create(&thread,NULL, start_routine,jobContext);
        jobContext->tMap.insert({thread,i});
        jobContext->midVecMap.insert({i,new IntermediateVec()});
        threads[i] = thread;
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_join(threads[i], NULL);
    }

    auto job = static_cast<JobHandle>(jobContext);
    return job;
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