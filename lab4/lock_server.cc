// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
  granted.clear();
  
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&mutex);

  if(granted.count(lid) > 0){
    while(granted[lid])
      pthread_cond_wait(&cond, &mutex);
  }
  granted[lid] = true;

  pthread_mutex_unlock(&mutex);
  printf("acquire %llu\n",lid);
  return lock_protocol::OK;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  pthread_mutex_lock(&mutex);
  
  if(granted.count(lid) == 0 || granted[lid] == false){
    pthread_mutex_unlock(&mutex);
    return lock_protocol::NOENT;
  }  
  granted[lid] = false;
  pthread_cond_signal(&cond);
  
  pthread_mutex_unlock(&mutex);
  printf("release %llu\n",lid);
  return lock_protocol::OK;
}
