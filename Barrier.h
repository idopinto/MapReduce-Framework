#ifndef BARRIER_H
#define BARRIER_H
#include <pthread.h>

// a multiple use barrier

class Barrier {
 public:
  Barrier(int numThreads);
  ~Barrier();
  void barrier();
//  void barrier2(int tid,void (func)(void *), void *func_arg);

  void wakeUp();

 private:
  pthread_mutex_t mutex;
  pthread_cond_t cv;
  pthread_cond_t cv2;

  int count;
  int numThreads;
};

#endif //BARRIER_H
