// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <unistd.h>



int lock_client_cache::last_port = 0;
enum {none, free_, locked, acquiring, releasing} state;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  pthread_mutex_init(&mutex, NULL);
  lock.clear();
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  char hname[100];
  VERIFY(gethostname(hname, 100) == 0);
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{ 
  pthread_mutex_lock(&mutex);
  printf("acq %s %lld\n",id.c_str(),lid);
  thread[lid].insert(pthread_self());
  if(conds.find(lid) == conds.end()){
    pthread_cond_init(&conds[lid], NULL);
  }
  while(lock[lid] == releasing)
    pthread_cond_wait(&conds[lid], &mutex);
  if(lock[lid] == none){
    int r = 0;
    lock[lid] = acquiring;
    pthread_mutex_unlock(&mutex);    
    int ret = cl->call(lock_protocol::acquire, lid, id, r);
    pthread_mutex_lock(&mutex);
    if(lock[lid] == locked){
      printf("acq retry early than RETRY\n");
    }
    else if(ret == lock_protocol::RETRY){
        pthread_cond_wait(&conds[lid], &mutex);
    }
    else if(ret == rlock_protocol::REVOKE){
      printf("acq wait REVOKE %s\n",id.c_str());
    }
    else if(lock[lid] == acquiring){
      printf("acq locked %s\n", id.c_str());
      lock[lid] = locked;      
    } 
  }
  else if(lock[lid] == free_){
    printf("acq free to locked %s\n", id.c_str());
    lock[lid] = locked;
  }
  else{//locked acquiring
    printf("acq wait %s\n",id.c_str());
    pthread_cond_wait(&conds[lid], &mutex);
  }
  std::cout<<"acq "<<lid<<" succ:"<<time(NULL)<<std::endl;
  pthread_mutex_unlock(&mutex);
  return lock_protocol::OK;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int r = 0;
  pthread_mutex_lock(&mutex);
  printf("release %s\n", id.c_str());
  thread[lid].erase(pthread_self());
  if(thread[lid].size() > 0){
    pthread_cond_signal(&conds[lid]);
  }
  else{//no other threads 
    if(revoke[lid] == 1){ //give back to the server
      pthread_cond_init(&conds[lid], NULL);
      lock[lid] = releasing;
      pthread_mutex_unlock(&mutex);
      cl->call(lock_protocol::release, lid, id, r);
      pthread_mutex_lock(&mutex);
      lock[lid] = none;
      revoke[lid] = 0;
      printf("release none\n");
    }
    else{
      lock[lid] = free_;
      printf("release free\n");
    }
    std::cout<<lid<<std::endl;
  }
 
  pthread_mutex_unlock(&mutex);
  return lock_protocol::OK;
}

//tells the client that it should send the lock 
//back to the server when it releases the lock 
//or right now if no thread on the client 
//is holding the lock.

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if(lock[lid] == free_){
    printf("revoke give back %lld %d\n", lid, thread[lid].size());
    lock[lid] = none;
    int r = 0;
    pthread_mutex_unlock(&mutex);
    cl->call(lock_protocol::release, lid, id, r);
    return ret;
  }
  else{ //locked acquiring
    printf("revoke to %s %d %d\n", id.c_str(), lock[lid],thread[lid].size());
    if(thread[lid].size() && lock[lid] == acquiring){
      pthread_cond_signal(&conds[lid]);
    }
    revoke[lid] = 1;
    ret = lock_protocol::RETRY;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                        int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if(lock[lid] == acquiring){
    pthread_cond_signal(&conds[lid]);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}



