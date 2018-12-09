#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <queue>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  enum xxstate{
       FREE, LOCKED
    };
  struct lock_info{
      xxstate state;
      std::string owner;
      std::queue<std::string> waiting_clients;
  };
  pthread_mutex_t mutex;
  pthread_cond_t cv;

 public:
  lock_server_cache();
  std::map<lock_protocol::lockid_t, lock_info> lockmap;
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
