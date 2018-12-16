/// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <algorithm>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"


lock_server_cache::lock_server_cache():
  nacquire (0)
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);
  std::cout <<"acquire lid:"<< lid << " id:"<< id <<std::endl;
  std::cout.flush();
  if(lock[lid].empty()){
    lock[lid] = id;
  }
  else if(wait_set[lid].count(id)){
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  else{
    wait_q[lid].push(id);
    wait_set[lid].insert(id);
    if(wait_set[lid].size() > 1){//has others
      ret = lock_protocol::RETRY;
    }
    else{//revoke the holder and return OK
      handle h(lock[lid]);
      rpcc* cl = h.safebind();
      int revoke = 0;
      pthread_mutex_unlock(&mutex);
      revoke = cl->call(rlock_protocol::revoke, lid, r);
      pthread_mutex_lock(&mutex);
      if(revoke == lock_protocol::RETRY){
        ret = lock_protocol::RETRY;
      }
      else{
        ret = rlock_protocol::REVOKE;
      }
      pthread_mutex_unlock(&mutex);
      return ret;
    }
  }
  printf("wait: %d\n",wait_set[lid].size());
  pthread_mutex_unlock(&mutex);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);
  wait_set[lid].erase(id);
  if(lock[lid] == id){
    std::cout <<"release lid:"<< lid << " id:"<< id <<std::endl;
    std::cout.flush();
  }
  else{
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  if(wait_set[lid].size()){
    std::string next = wait_q[lid].front();
    while(!wait_set[lid].count(next)){
      wait_q[lid].pop();
      next = wait_q[lid].front();
    }
    // printf("retry next: %s\n", next.c_str());
    handle h(next);
    rpcc *cl = h.safebind();
    pthread_mutex_unlock(&mutex);
    cl->call(rlock_protocol::retry, lid, r);
    pthread_mutex_lock(&mutex);
    lock[lid] = next;
    wait_q[lid].pop();
    wait_set[lid].erase(next);
    if(wait_set[lid].size() > 0){
      // printf("revoke %s still wait %d\n",lock[lid].c_str(), wait_set1[lid].size());
      pthread_mutex_unlock(&mutex);
      cl->call(rlock_protocol::revoke, lid, r);
      return ret;
    }
    pthread_mutex_unlock(&mutex);
    return ret;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

