#include "Barrier.h"
#include <cstdlib>
#include <cstdio>
#include <Barrier.h>


Barrier::Barrier(int numThreads)
    : mutex(PTHREAD_MUTEX_INITIALIZER)
    , cv(PTHREAD_COND_INITIALIZER)
    , count(0)
    , numThreads(numThreads)
{ }


Barrier::~Barrier()
{
  if (pthread_mutex_destroy(&mutex) != 0) {
      fprintf(stderr, "[[Barrier]] error on pthread_mutex_destroy");
      exit(1);
    }
  if (pthread_cond_destroy(&cv) != 0){
      fprintf(stderr, "[[Barrier]] error on pthread_cond_destroy");
      exit(1);
    }
}


void Barrier::barrier()
{
  if (pthread_mutex_lock(&mutex) != 0){
      fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
      exit(1);
    }
  if (++count < numThreads) {
//      printf("count is = %d\n",count);
      if (pthread_cond_wait(&cv, &mutex) != 0){
          fprintf(stderr, "[[Barrier]] error on pthread_cond_wait");
          exit(1);
        }
    } else {
      count = 0;
      if (pthread_cond_broadcast(&cv) != 0) {
          fprintf(stderr, "[[Barrier]] error on pthread_cond_broadcast");
          exit(1);
        }
    }
  if (pthread_mutex_unlock(&mutex) != 0) {
      fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
      exit(1);
    }
}


//void Barrier::barrier2(int tid,void (func)(void *), void *func_arg)
//{
//
//  if (pthread_mutex_lock(&mutex) != 0){
//      fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
//      exit(1);
//    }
//
//    if(++count < numThreads)
//      {
//        if(tid != 0)
//          {
//            if (pthread_cond_wait (&cv2, &mutex) != 0)
//              {
//                fprintf (stderr, "[[Barrier]] error on pthread_cond_wait");
//                exit (1);
//              }
//          }else{
//            func(func_arg);
//          }
//      }else{
//        func(func_arg);
//        count = 0;
//        if (pthread_cond_broadcast(&cv2) != 0) {
//            fprintf(stderr, "[[Barrier]] error on pthread_cond_broadcast");
//            exit(1);
//          }
//      }
//
//    }
//  if (pthread_mutex_unlock(&mutex) != 0) {
//      fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
//      exit(1);
//    }
//}

//void Barrier::wakeUp(){
//
//  if(count == numThreads - 1 )
//    {
//      count = 0;
//      if (pthread_cond_broadcast(&cv) != 0) {
//          fprintf(stderr, "[[Barrier]] error on pthread_cond_broadcast");
//          exit(1);
//        }
//    }

//  printf("wakeup\n");

