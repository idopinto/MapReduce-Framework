//
// Created by ilana on 18/04/2022.
//

#include "MapReduceFramework.h"
#include <pthread.h>
#include <atomic>
#include <map>
#include <Barrier.h>

typedef struct JobContext{
    const MapReduceClient *client;
    InputVec  inputVec;
    OutputVec outputVec;
    std::map<pthread_t,int> tMap;
    std::atomic<uint64_t> *atomic_counter;
    JobState jobState;
    std::map<int,IntermediateVec> midVecMap;
    Barrier barrier;
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

void emit2 (K2* key, V2* value, void* context){
    JobContext* jc = (JobContext *) context;
    int tid = jc->tMap[pthread_self()];
    jc->midVecMap.at(tid).push_back({key,value});
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
    JobContext* jc = (JobContext *) jobContext;

    int tid = jc->tMap[pthread_self()];
    printf("Thread %d starts map stage...\n",tid);
    auto current_pair = jc->inputVec.at(jc->atomic_counter->load());
    jc->client->map(current_pair.first,current_pair.second,jobContext);
    (*(jc->atomic_counter))++;
    printf("current counter = %d\n",jc->atomic_counter->load());

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

    std::atomic<uint64_t> atomic_counter(0);
    Barrier barrier = Barrier(multiThreadLevel);
    std::map<pthread_t,int> tMap;
    std::map<int,IntermediateVec> midVecMap;

    JobContext *jobContext = new JobContext {.client=&client,
                                             .inputVec = inputVec,
                                             .outputVec=outputVec,
                                             .tMap = tMap,
                                             .atomic_counter = &atomic_counter,
                                             .jobState={UNDEFINED_STAGE,0.0},
                                             .midVecMap=midVecMap,
                                             .barrier=barrier};

    for (int i = 0; i < multiThreadLevel; ++i) {
        jobContext->tMap.insert({(pthread_t)(threads+i),i});
        pthread_create(threads+i,NULL, start_routine,jobContext);
    }
    auto job = static_cast<JobHandle>(jobContext);
//    waitForJob(job);
    return job;
}

/**
 *a function gets JobHandle returned by startMapReduceFramework and waits
 * until it is finished.
 * @param job
 */
void waitForJob(JobHandle job){

    for (auto& tid_pair:static_cast<JobContext*>(job)->tMap) {
        pthread_join(tid_pair.first,NULL);
    }
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