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
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from client %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  	lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
	pthread_mutex_lock(&mutex);
  	if (states_map.find(lid) == states_map.end()) //create a new lock
  	{	
   		states_map[lid].isFree = false;
		pthread_mutex_unlock(&mutex);
  		VERIFY(ret == lock_protocol::OK);
		return ret;
	}
	else //lock exists
	{
		while(states_map[lid].isFree == false) //wait until lock is free
			pthread_cond_wait(&states_map[lid].cond, &mutex);
		states_map[lid].isFree = false;
		pthread_mutex_unlock(&mutex);
		VERIFY(ret == lock_protocol::OK);
		return ret;
	}
	VERIFY(ret == lock_protocol::OK);
  	return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  	lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
	pthread_mutex_lock(&mutex);
	if (states_map.find(lid) == states_map.end())
		return lock_protocol::IOERR;
	states_map[lid].isFree = true;
	pthread_mutex_unlock(&mutex);
	pthread_cond_signal(&states_map[lid].cond);
 	return ret;
}
