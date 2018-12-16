// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst){
    ec = new extent_client(extent_dst);
    // lc = new lock_client(lock_dst);
    lc = new lock_client_cache(lock_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

yfs_client::yfs_client(extent_client * nec, lock_client* nlc){
    ec = nec;
    lc = nlc;
}

yfs_client::inum yfs_client::n2i(std::string n){
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string yfs_client::filename(inum inum){
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool yfs_client::isfile(inum inum){
    lc->acquire(inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    if (a.type == extent_protocol::T_FILE){
        lc->release(inum);
        return true;
    }
    lc->release(inum);
    return false;
}

bool yfs_client::isdir(inum inum){
    lc->acquire(inum);
    extent_protocol::attr a;
    if(ec->getattr(inum, a) != extent_protocol::OK){
        printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    if(a.type == extent_protocol::T_DIR){
//      printf("isdir: %lld is a dir\n", inum);
        lc->release(inum);
        return true;
    }
    lc->release(inum);
    return false;

}

int yfs_client::getfile(inum inum, fileinfo &fin){
    int r = OK;
    lc->acquire(inum); 

    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
//  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    lc->release(inum);
    return r;
}

int yfs_client::getdir(inum inum, dirinfo &din){
    int r = OK;
    lc->acquire(inum);

    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    lc->release(inum);
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

/*
 * Only support set size of attr
 * get the content of inode ino, and modify its content
 * according to the size (< = >)content length
 */
int yfs_client::setattr(inum ino, size_t size){
    lc->acquire(ino);
    std::string content;
    if(ec->get(ino, content) != extent_protocol::OK){
        lc->release(ino);
        return IOERR;
    }
    if(content.size() == size){
        lc->release(ino);
        return OK;
    }
    content.resize(size);
    if(ec->put(ino, content) != extent_protocol::OK){
        lc->release(ino);
        return IOERR;
    }
    lc->release(ino);
    return OK;
}

int yfs_client::add_entry(inum parent, const char *name, inum ino){
    std::string content;
    if(ec->get(parent, content) != extent_protocol::OK){
        return IOERR;
    }
    content.append(name);
    content.push_back('\0');
    content.append(filename(ino));
    if(ec->put(parent, content) != extent_protocol::OK){
        return IOERR;
    }
    // std::cout<<"dir:"<<parent<<"\n"<<content<<std::endl;
    return OK;
}

int yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out){
    lc->acquire(parent);
    bool found = false;
    int r = OK;
    lookup(parent, name, found, ino_out);
    if(found){
        lc->release(parent);
        return EXIST;
    }
    if(ec->create(extent_protocol::T_FILE, ino_out) != OK){
        printf("CREATE fail\n");
        lc->release(parent);
        return IOERR;
    }
    printf("CREATE dir: %0lld file name:%s, ino:%0lld\n",parent, name, ino_out);
    r = add_entry(parent, name, ino_out);
    lc->release(parent);
    return r;
}

int yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out){
    lc->acquire(parent);
    bool found = false;
    int r = OK;
    r = lookup(parent, name, found, ino_out);
    if(found){
        lc->release(parent);
        return EXIST;
    }
    if(ec->create(extent_protocol::T_DIR, ino_out) != OK){
        lc->release(parent);
        return IOERR;
    }
    printf("CREATE dir name:%s, ino:%0lld\n", name, ino_out);
    fflush(stdout);
    r = add_entry(parent, name, ino_out);
    lc->release(parent);
    return r;
}

int yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out){
    std::list<dirent> list;
    std::cout<<"yfs lookup "<<parent <<" "<<name <<std::endl; 
    std::cout.flush();
    if(readdir(parent, list) != OK){
        printf("LOOKUP error- inum: %016llx\n", parent);
        return IOERR;
    }
    for(std::list<dirent>::iterator it = list.begin(); it != list.end(); ++it){
        printf("-- %s -- %d\n",it->name.c_str(),it->inum);
        fflush(stdout);
        if(it->name == name){
            found = true;
            ino_out = it->inum;
            return OK;
        }
    }
    return OK;
}

int yfs_client::readdir(inum dir, std::list<dirent> &list){
    std::string content;
    if(ec->get(dir, content) != extent_protocol::OK){
        printf("READDIR fail\n");
        return IOERR;
    }
    std::istringstream ist(content);
    dirent entry;
    list.clear();
    while(getline(ist, entry.name, '\0')){
        ist >> entry.inum;
        list.push_back(entry);
    }
    return OK;
}

int yfs_client::read(inum ino, size_t size, off_t off, std::string &data){
    if(size < 0 || off < 0 || ino <= 0){
        return IOERR;
    }

    lc->acquire(ino);
    std::cout<<"yfs read "<<ino<<" "<<size<<" "<<off<<std::endl;
    std::cout.flush();
    if(ec->get(ino, data) != OK){
        lc->release(ino);
        return IOERR;
    }
    if(off >= (unsigned)data.size())
       data = "";
    else
        data = data.substr(off);
    if(data.size() > size)
        data.resize(size);
    lc->release(ino);
    return OK;
}

/*
 * write: when off > length of original file, fill the holes with '\0'
 */
int yfs_client::write(inum ino, size_t size, off_t off, const char *data, size_t &bytes_written){
    lc->acquire(ino);
    std::string content;
    std::string temp(data, size);
    int r = OK;
    r = ec->get(ino, content);
    bytes_written = 0;
    if(content.size() < off){
        bytes_written += off - content.size();
        content.append(off - content.size(), 0);
        content += temp;
    }
    else
        content.replace(off, size, temp);
    bytes_written += size;
    if(ec->put(ino, content) != extent_protocol::OK){
        lc->release(ino);
        return IOERR;
    }
    std::cout<<"yfs write "<<ino<<" size"<<size<<" "<<data<<std::endl;
    lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent, const char *name){
    lc->acquire(parent);
    std::list<dirent> old_entries;
    if(readdir(parent, old_entries) != extent_protocol::OK){
        lc->release(parent);
        return IOERR;
    }
    std::ostringstream ost;
    inum ino;
    bool found = false;
    for(std::list<dirent>::iterator it = old_entries.begin(); it != old_entries.end(); ++it){
        if(it->name != name){
            ost << it->name;
            ost.put('\0');
            ost << it->inum;
        }
        else{
            ino = it->inum;
            found = true;
        }
    }
    if(!found || ec->remove(ino) != extent_protocol::OK){
        lc->release(parent);
        return IOERR;
    }
    if(ec->put(parent, ost.str()) != extent_protocol::OK){
        lc->release(parent);
        return IOERR;
    }
    lc->release(parent);
    return OK;
}

int yfs_client::symlink(inum parent, const char *name, const char *path,  inum &ino_out){
    if(parent <= 0)
        return IOERR;
    // printf("SYMLINK-----\nparent:%0lld name:%s path:%s\n",parent, name, path);
    lc->acquire(parent);
    if(ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK){
        lc->release(parent);
        return IOERR;
    }
    if(ec->put(ino_out, path) != extent_protocol::OK){
        lc->release(parent);
        return IOERR;
    }
    if(add_entry(parent, name, ino_out) != extent_protocol::OK){
        lc->release(parent);
        return IOERR;
    }
    lc->release(parent);
    return OK;
}

int yfs_client::readlink(inum ino, std::string &path){
    lc->acquire(ino);
    if(ec->get(ino, path) != extent_protocol::OK){
        lc->release(ino);
        return IOERR;  
    }
    lc->release(ino);
    return OK;
}
