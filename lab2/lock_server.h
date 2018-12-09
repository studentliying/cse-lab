// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <map>
#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"
#include "pthread.h"

class lock_server {

 protected:
  int nacquire;
 

 private:
  struct lockState{
  	bool isFree;
	pthread_cond_t cond;
	lockState(){
		isFree = true;
		pthread_cond_init(&cond, NULL);
	}
  };

  pthread_mutex_t mutex;
  std::map<lock_protocol::lockid_t, lockState> states_map;


 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 







