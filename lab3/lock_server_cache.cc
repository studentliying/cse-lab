// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

using namespace std;
lock_server_cache::lock_server_cache()
{
  VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
  VERIFY(pthread_cond_init(&cv, 0) == 0);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);
  if(lockmap.find(lid) == lockmap.end() || lockmap[lid].state == FREE)
  {
    //init a new lock
    printf("lock_server_cache: client %s acquire lock %lld which is free or never be acquired.\n", id.c_str(), lid);
    lockmap[lid].owner = id;
    lockmap[lid].state = LOCKED;
    pthread_mutex_unlock(&mutex);
    ret = lock_protocol::OK;
    r = (++nacquire);
    return ret;
  }
  
  else
  //lock state is LOCKED
  {
    if(!lockmap[lid].waiting_clients.empty())
    {
      lockmap[lid].waiting_clients.push(id);
      pthread_mutex_unlock(&mutex);
      ret = lock_protocol::RETRY;
      r = (++nacquire);
      return ret;
    }
    else  //waiting_clients is empty
    {
      lockmap[lid].waiting_clients.push(id);
      string cid = lockmap[lid].owner;
      pthread_mutex_unlock(&mutex);

      rlock_protocol::status rret = rlock_protocol::OK;
      handle h(cid);
      if (h.safebind()) {
        int tmp;
        rret =  h.safebind()->call(rlock_protocol::revoke, lid, tmp);
        printf("lock_server_cache: acquire() RPC to clt waiting, server sent revoke to holder!\n");
      }
      if (!h.safebind() || rret != rlock_protocol::OK) {
        printf("lock_server_cache: acquire() RPC to clt failed!\n");
        assert(true);
      }
      ret = lock_protocol::RETRY;
      r = (++nacquire);
      return ret;
    }  
  }
}


int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&mutex);

  if (lockmap.find(lid) == lockmap.end())
  {
    printf("lock_server_cache: lock %lld not found.\n", lid);
    ret = lock_protocol::NOENT;
    r = (--nacquire);
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  if(lockmap[lid].owner != id)
  {
    printf("lock_server_cache: release lock %lld whose owner is not %s\n", lid, id.c_str());
    ret = lock_protocol::RPCERR;
    r =  (--nacquire);
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  if(lockmap[lid].state != LOCKED)
  {
    printf("lock_server_cache: release lock %lld whose state is not LOCKED\n", lid);
    ret = lock_protocol::RPCERR;
    r =  (--nacquire);
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  if(lockmap[lid].waiting_clients.empty())
  {
    printf("lock_server_cache: there is no client waiting for lock %lld\n", lid);
    lockmap[lid].state = FREE;
    lockmap[lid].owner = "";
    ret = lock_protocol::OK;
    r =  (--nacquire);
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  //if there are other clients waiting for the lock
  string next_client = lockmap[lid].waiting_clients.front();
  lockmap[lid].owner = next_client;
  printf("next select client:%s\n", next_client.c_str());
  lockmap[lid].waiting_clients.pop();
  bool to_revoke = false;
  if(!lockmap[lid].waiting_clients.empty())
    to_revoke = true;
  pthread_mutex_unlock(&mutex);

  rlock_protocol::status rret;
  int tmp;
  handle h(next_client);
  if (h.safebind()){
    printf("send a retry\n");
    rret = h.safebind()->call(rlock_protocol::retry, lid, to_revoke, tmp);
  }
  else{
    printf("bind error!!\n");
  }

  if(rret == rlock_protocol::OK)
  {
    ret = lock_protocol::OK;
    r = (--nacquire);
    printf("lock_server_cache: %s release lock %lld success.\n", id.c_str(), lid);
  }
  else
  {
    printf("lock_server_cache: %s release lock %lld fails.\n", id.c_str(), lid);
    r = (--nacquire);
    return release(lid, next_client, r);
  }

  return ret;
}


lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  printf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}


