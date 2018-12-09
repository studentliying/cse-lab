// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
    srand(time(NULL)^last_port);
    rlock_port = ((rand()%32000) | (0x1 << 10));
    const char *hname;
    // VERIFY(gethostname(hname, 100) == 0);
    hname = "127.0.0.1";
    std::ostringstream host;
    host << hname << ":" << rlock_port;
    id = host.str();
    last_port = rlock_port;
    rpcs *rlsrpc = new rpcs(rlock_port);
    rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
    rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  
    pthread_mutex_init(&mutex, NULL) == 0;
    pthread_cond_init(&cv, NULL) == 0;
}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  tid thread = pthread_self();
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);

  if(lockmap.find(lid) == lockmap.end())
  {
    //init a new lock
    printf("lock_client_cache: thread %ld acquire lock %lld which does not exist.\n", thread, lid);
    lockmap[lid].state = NONE;
    lockmap[lid].owner = 0;
    lockmap[lid].is_revoked = false;
  }

  if(lockmap[lid].state == FREE)
  {
    printf("lock_client_cache: thread %ld acquire lock %lld which state is FREE.\n", thread, lid);
    lockmap[lid].state = LOCKED;
    lockmap[lid].owner = thread;
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  else if(lockmap[lid].state == NONE || lockmap[lid].state == RELEASING)
  {
    printf("lock_client_cache: thread %ld acquire lock %lld which state is NONE.\n", thread, lid);
    lockmap[lid].state = ACQUIRING;
    lockmap[lid].waiting_threads.push(thread);
    pthread_mutex_unlock(&mutex);
    int r;
    ret = cl->call(lock_protocol::acquire, lid, id, r);
    VERIFY(ret == lock_protocol::OK || ret == lock_protocol::RETRY);
    pthread_mutex_lock(&mutex);
    //we may receive a RETRY before OK returned because sending rpc is not locked
    if(ret == lock_protocol::OK)
    {
      tprintf("lock_client_cache: thread %ld acquire lock %lld, receive RPC response OK.\n", thread, lid);     
      lockmap[lid].state = LOCKED;
      lockmap[lid].owner = lockmap[lid].waiting_threads.front();
      lockmap[lid].waiting_threads.pop();
      printf("%llu get lock, waiting client size:%d", lockmap[lid].owner, lockmap[lid].waiting_threads.size());              
      pthread_mutex_unlock(&mutex);
      return ret;
    }

    else   //receive RETRY
    {
      printf("lock_client_cache: thread %ld acquire lock %lld, receive RPC response RETRY.\n", thread, lid);
      /*while (lockmap[lid].owner != thread)
        pthread_cond_wait(&cv, &mutex);
      pthread_mutex_unlock(&mutex);
      ret = lock_protocol::OK;*/
      pthread_mutex_unlock(&mutex);
      int r;
      while(ret != lock_protocol::OK)
      {
        if(lockmap[lid].state == FREE)
          break;
        if(ret != lock_protocol::RETRY)
          ret = cl->call(lock_protocol::acquire, lid, id, r);
      }
      pthread_mutex_lock(&mutex);
      lockmap[lid].state = LOCKED;
      lockmap[lid].waiting_threads.pop();
      pthread_mutex_unlock(&mutex);
      return ret;
    }
  }
  else
  {
    //state is LOCKED or ACQUIRING 
    printf("lock_client_cache: thread %ld acquire lock %lld which state is %d.\n", thread, lid, lockmap[lid].state);
    lockmap[lid].waiting_threads.push(thread);
    //wait until thread keeps the lock
    while (lockmap[lid].owner != thread){
      pthread_cond_wait(&cv, &mutex);
    }
    pthread_mutex_unlock(&mutex);
    return ret;
  }
}


lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  int r;
  tid thread = pthread_self();
  pthread_mutex_lock(&mutex);

  if(lockmap.find(lid) == lockmap.end())
  {
    printf("lock_client_cache: cannot find the lock %lld to release.\n", lid);
    ret = lock_protocol::NOENT;
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  if(lockmap[lid].is_revoked)
  {
    //first set lock state to RELEASING and then deal with it according to its state
    printf("lock_client_cache: set lock %llu FREE but doesn't return back to server.\n", lid);
    lockmap[lid].state = RELEASING;
    lockmap[lid].is_revoked = false;
  }
  
  
  if(lockmap[lid].state == LOCKED)
  {
    printf("lock_client_cache: release lock %llu which state is LOCKED.\n", lid);
    if(lockmap[lid].waiting_threads.empty())  //server not revoke & no thread wants the clock
    {
      printf("a lock is free and wait to be acquired\n");
      lockmap[lid].state = FREE;
      lockmap[lid].owner = 0;
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    else //server not revoke & there are threads waiting for the clock
    {
      tid next = lockmap[lid].waiting_threads.front();
      lockmap[lid].owner = next;
      lockmap[lid].waiting_threads.pop(); //TODO
      printf("grant lock to next waiting client %llu\n", next);
      pthread_cond_broadcast(&cv);
      pthread_mutex_unlock(&mutex);
      return ret;
    }
  }  

  else if(lockmap[lid].state == RELEASING)
  {
    printf("lock_client_cache: release lock %llu which state is RELEASING.\n", lid);
    
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);
    if(ret = lock_protocol::OK)
    {
      //if(lockmap[lid].waiting_threads.empty())
      if(lockmap[lid].state == RELEASING)
        lockmap[lid].state = NONE;
      //else   
        //lockmap[lid].state = ACQUIRING;
      lockmap[lid].owner = 0;
    
      if(lockmap[lid].waiting_threads.empty())
      {
        pthread_mutex_unlock(&mutex);
        return ret;
      }
      else
      {
        pthread_mutex_unlock(&mutex);
        pthread_cond_broadcast(&cv);
        return ret;
      }
    }
    else{
      printf("release failed.\n");
      pthread_mutex_unlock(&mutex);
      return ret;
    }
  }
  else
  {
    //printf("release failed.\n");
    pthread_mutex_unlock(&mutex);
    return ret;
  }
}



rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (lockmap.find(lid) == lockmap.end()) { // impossible
      printf("lock_client_cache: revoke lock %llu which doesn't exist.\n", lid);
      lockmap[lid].state = NONE;
      lockmap[lid].is_revoked = false;
      lockmap[lid].owner = 0;
    }

  if(lockmap[lid].state == FREE)
  {
    printf("lock_client_cache: revoke lock %llu which state is FREE.\n", lid);
    lockmap[lid].state = RELEASING;
    pthread_mutex_unlock(&mutex);
    int tmp;
    ret = cl->call(lock_protocol::release, lid, id, tmp);
    if(ret == lock_protocol::OK && lockmap[lid].state == RELEASING)
      lockmap[lid].state = NONE;
  }
  else if (lockmap[lid].state == LOCKED || lockmap[lid].state == ACQUIRING)
  {
    printf("lock_client_cache: revoke lock %llu which state is LOCKED.\n", lid);
    lockmap[lid].is_revoked = true;
    pthread_mutex_unlock(&mutex);
  }
  else // impossible
  {
    printf("lock_client_cache: revoke lock %llu which state is unexpected.\n", lid);
    pthread_mutex_unlock(&mutex);
    ret = rlock_protocol::RPCERR;
  }
  return ret;
}


rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, bool to_revoke,
                                 int &)
{
  printf("receive retry!\n");
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if (lockmap.find(lid) == lockmap.end())
  {
    printf("lock_client_cache: retry lock %llu which state is unexpected\n", lid);
    lockmap[lid].state = NONE;
    lockmap[lid].is_revoked = false;
    lockmap[lid].owner = 0;
  }
  /*if (lockmap[lid].state != NONE) 
  {
    // LOCKED/FREE/ACQURING/RELEASING
    printf("lock_client_cache: retry lock %llu which status is %d.\n", lid, lockmap[lid].state);
  }
  pthread_mutex_unlock(&mutex);
  int r;
  r = cl->call(lock_protocol::acquire, lid, id, r);
  pthread_mutex_lock(&mutex);*/

  
  if(lockmap[lid].state == ACQUIRING)
  {
    lockmap[lid].state = FREE;
    if(to_revoke)
      lockmap[lid].is_revoked = true;
  }
  else
  {
    printf("lock_client_cache: retry lock %llu which state is unexpected\n", lid);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}



