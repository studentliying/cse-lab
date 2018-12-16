#include "namenode.h"
#include "extent_client.h"
#include "lock_client.h"
#include <sys/stat.h>
#include <unistd.h>
#include "threader.h"

using namespace std;

void NameNode::init(const string &extent_dst, const string &lock_dst) {
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst);
  yfs = new yfs_client(ec, lc);

  /* Add your init logic here */
  heart = 0;
  NewThread(this, &NameNode::addBeat);
}

void NameNode::addBeat(){
  while(1){
    this->heart++;
    sleep(1);
  }
}

list<NameNode::LocatedBlock> NameNode::GetBlockLocations(yfs_client::inum ino) {
  //Call get_block_ids and convert block ids to LocatedBlocks.
  list<blockid_t> blockids;
  list<LocatedBlock> Blocks;
  ec->get_block_ids(ino, blockids);
  extent_protocol::attr attr;
  ec->getattr(ino, attr);
  list<blockid_t>::iterator it;
  unsigned int i = 1;
  long long size = 0;

  for(it = blockids.begin(); it != blockids.end(); ++it, ++i){
    LocatedBlock lb(*it, size, (i == blockids.size()) ? attr.size - size : BLOCK_SIZE, GetDatanodes());
    Blocks.push_back(lb);
    size += BLOCK_SIZE;
  }
  return Blocks;
}

bool NameNode::Complete(yfs_client::inum ino, uint32_t new_size) {
  // Call complete and unlock the file.
  bool ret = !ec->complete(ino, new_size);
  if(ret)
    lc->release(ino);
  return ret;
}

NameNode::LocatedBlock NameNode::AppendBlock(yfs_client::inum ino) {
  // Call append_block and convert block id to LocatedBlock.
  blockid_t id;
  extent_protocol::attr attr;
  ec->getattr(ino, attr);
  ec->append_block(ino, id);
  modified_blocks.insert(id);
  int size = attr.size % BLOCK_SIZE;
  LocatedBlock lb(id, attr.size, size ? size : BLOCK_SIZE, GetDatanodes());
  return lb;
}

bool NameNode::Rename(yfs_client::inum src_dir_ino, string src_name, yfs_client::inum dst_dir_ino, string dst_name) {
  //Move a directory entry. Note that src_name/dst_name is entry name, not full path.
  string buf;
  ec->get(src_dir_ino, buf);
  list<yfs_client::dirent> src_dir;
  bool found = false;
  yfs_client::inum ino;

  yfs->lookup(src_dir_ino, src_name.c_str(), found, ino);
  if(found){
    yfs->readdir(src_dir_ino, src_dir);
    ostringstream ost;
    for(list<yfs_client::dirent>::iterator it = src_dir.begin(); it != src_dir.end(); ++it){
      if(it->name.compare(src_name)){
        ost << it->name;
        ost.put('\0');
        ost << it->inum;    
      }
    }
    ec->put(src_dir_ino, ost.str());
    yfs->add_entry(dst_dir_ino, dst_name.c_str(), ino);
  }
  return found;
}

bool NameNode::Mkdir(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  bool ret = !yfs->mkdir(parent, name.c_str(), mode, ino_out);
  return ret;
}

bool NameNode::Create(yfs_client::inum parent, string name, mode_t mode, yfs_client::inum &ino_out) {
  bool ret = !yfs->create(parent, name.c_str(), mode, ino_out);
  if(ret)
    lc->acquire(ino_out);
  return ret;
}

bool NameNode::Isfile(yfs_client::inum ino) {
  extent_protocol::attr a;
  ec->getattr(ino, a);
  if (a.type == extent_protocol::T_FILE){
      return true;
  }
  return false;
}

bool NameNode::Isdir(yfs_client::inum ino) {
  extent_protocol::attr a;
  ec->getattr(ino, a);
  if(a.type == extent_protocol::T_DIR){
    return true;
  }
  return false;
}

bool NameNode::Getfile(yfs_client::inum ino, yfs_client::fileinfo &info) {
  extent_protocol::attr a;
  if (ec->getattr(ino, a) != extent_protocol::OK) {
    return false;
  }
  info.atime = a.atime;
  info.mtime = a.mtime;
  info.ctime = a.ctime;
  info.size = a.size;
  return true;
}

bool NameNode::Getdir(yfs_client::inum ino, yfs_client::dirinfo &info) {
  extent_protocol::attr a;
  if (ec->getattr(ino, a) != extent_protocol::OK) {
    return false;
  }
  info.atime = a.atime;
  info.mtime = a.mtime;
  info.ctime = a.ctime;
  return true;
}

bool NameNode::Readdir(yfs_client::inum ino, std::list<yfs_client::dirent> &dir) {
  yfs->readdir(ino, dir);
  return true;
}

bool NameNode::Unlink(yfs_client::inum parent, string name, yfs_client::inum ino) {
  list<yfs_client::dirent> dir;
  yfs_client::dirent entry;
  string buf;
  ec->get(parent, buf);
  istringstream ist(buf);
  ostringstream ost;
  bool found = false;
  while(getline(ist, entry.name, '\0')){  //read src dir
    ist >> entry.inum;
    if(entry.name == name){
      found = true;
      ino = entry.inum;                   //find and not add to list
    }
    else
      dir.push_back(entry);
  }
  if(!found)
    return false;
  for(list<yfs_client::dirent>::iterator it = dir.begin(); it != dir.end(); ++it){
    ost << it->name;
    ost.put('\0');
    ost << it->inum;
  }
  ec->remove(ino);
  ec->put(parent, ost.str());
  // cout<<ost.str()<<endl;
  // cout.flush();
  return true;
}

void NameNode::DatanodeHeartbeat(DatanodeIDProto id) {
  datanodes[id] = this->heart;
}

void NameNode::RegisterDatanode(DatanodeIDProto id) {
  // cout<<datanodes.size()<<" heart: "<<this->heart<<" register!!\n";
  // cout.flush();
  if(this->heart > 5){
    cout<<"recovery\n";
    for(auto b : modified_blocks){
      ReplicateBlock(b, master_datanode, id);
    }
  }
  datanodes.insert(pair<DatanodeIDProto, int>(id, this->heart));
}

list<DatanodeIDProto> NameNode::GetDatanodes() {
  list<DatanodeIDProto> l;
  map<DatanodeIDProto, int>::iterator it = datanodes.begin();
  for(; it != datanodes.end(); ++it){
    if(it->second >=  this->heart - 4){
      l.push_back(it->first);
    }
  }
  // cout<<"get node size:"<<l.size()<<endl;
  return l;

}
