#include "datanode.h"
#include <arpa/inet.h>
#include "extent_client.h"
#include <unistd.h>
#include <algorithm>
#include "threader.h"

using namespace std;

int DataNode::init(const string &extent_dst, const string &namenode, const struct sockaddr_in *bindaddr) {
  ec = new extent_client(extent_dst);

  // Generate ID based on listen address
  id.set_ipaddr(inet_ntoa(bindaddr->sin_addr));
  id.set_hostname(GetHostname());
  id.set_datanodeuuid(GenerateUUID());
  id.set_xferport(ntohs(bindaddr->sin_port));
  id.set_infoport(0);
  id.set_ipcport(0);

  // Save namenode address and connect
  make_sockaddr(namenode.c_str(), &namenode_addr);
  if (!ConnectToNN()) {
    delete ec;
    ec = NULL;
    return -1;
  }

  // Register on namenode
  if (!RegisterOnNamenode()) {
    delete ec;
    ec = NULL;
    close(namenode_conn);
    namenode_conn = -1;
    return -1;
  }

  /* Add your initialization here */

  //Create a thread in datanode to send heartbeat periodically 
  NewThread(this, &DataNode::heartbeat);
  
  return 0;
}

void DataNode::heartbeat(){
  while(1){
    SendHeartbeat();
    sleep(1);
  }
}

bool DataNode::ReadBlock(blockid_t bid, uint64_t offset, uint64_t len, string &buf) {
  /* Your lab4 part 2 code */
  string temp;
  ec->read_block(bid, temp);
  if(offset > temp.size() || len == 0)
     buf = "";
  else
    buf = temp.substr(offset, len);
  // cout<<"read block:" << bid << " offset:" << offset << " len:" << len << " result: "<<buf<<endl;
  // cout.flush();
  return true;
}

bool DataNode::WriteBlock(blockid_t bid, uint64_t offset, uint64_t len, const string &buf) {
  /* Your lab4 part 2 code */
  if(!len)
    return true;
  string wbuf;
  ec->read_block(bid, wbuf);
  if(wbuf.empty()){
    wbuf = buf.substr(0, len);
  }
  else if(offset == 0){
    wbuf = buf + wbuf.substr(len);
  }
  else{
    wbuf = wbuf.substr(0, offset) + buf + wbuf.substr(offset + len);
  }
  ec->write_block(bid, wbuf);
  // cout<< "write block:" << bid << " offset:" << offset << " len:" << len << "  write result: "<<wbuf<<endl;
  cout.flush();
  return true;
}

