#ifndef MAPREDUCEFRAMEWORK_H
#define MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"

/**
 *  An identifier of a running job
 */
typedef void* JobHandle;

enum stage_t {UNDEFINED_STAGE=0, MAP_STAGE=1, SHUFFLE_STAGE=2, REDUCE_STAGE=3};

/**
 *  a struct which quantizes the state of a job
 */
typedef struct {
	stage_t stage; // an enum
	float percentage; // Job progress of current stage. completed tasks / all tasks in the current stage
} JobState;

void emit2 (K2* key, V2* value, void* context);
void emit3 (K3* key, V3* value, void* context);

JobHandle startMapReduceJob(const MapReduceClient& client,
	const InputVec& inputVec, OutputVec& outputVec,
	int multiThreadLevel);

void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state);
void closeJobHandle(JobHandle job);
	
	
#endif //MAPREDUCEFRAMEWORK_H
