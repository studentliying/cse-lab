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

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  	ec = new extent_client(extent_dst);
  	lc = new lock_client(lock_dst);
	//lc -> acquire(0);
  	if (ec->put(1, "") != extent_protocol::OK)
      	printf("error init root dir\n"); // XYB: init root dir
	//lc -> release(0);
}


yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}


std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;
	//lc -> acquire(inum);

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
	//	lc -> release(inum);
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
	//	lc -> release(inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
	//lc -> release(inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
	extent_protocol::attr a;
	//lc -> acquire(inum);
	if (ec->getattr(inum, a) != extent_protocol::OK)
	{
		printf("get attr error");
	//	lc -> release(inum);
		return false;
	}
	if (a.type == extent_protocol::T_DIR)
	{
		printf("isdir: %lld is a dir\n", inum);
	//	lc -> release(inum);
		return true;
	}
	//lc -> release(inum);
	return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;
	lc -> acquire(inum);
    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
	lc -> release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;
	lc -> acquire(inum);
    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
	lc -> release(inum);
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    if (ino < 0 || ino > INODE_NUM)
		return IOERR;
	lc -> acquire(ino);
	std:: string buf;
	EXT_RPC(ec->get(ino, buf));
	buf.resize(size);
	EXT_RPC(ec->put(ino, buf));

release:
	lc -> release(ino);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	lc -> acquire(parent);
	bool found = false;
	dirent new_entry;
	std::list<dirent> dir_list;

	EXT_RPC(lookup(parent, name, found, ino_out));
	if(found)
	{
		printf("file %lld exists", ino_out);
		lc -> release(parent);
		return EXIST;
	}
	EXT_RPC(ec->create(extent_protocol::T_FILE, ino_out));
	printf("create file %s success\n", name);
	EXT_RPC(readdir_nolock(parent, dir_list));
	new_entry.inum = ino_out;
	new_entry.name = name;
	dir_list.push_back(new_entry);
	if(writedir_nolock(parent, dir_list) != OK)
	{
		lc -> release(parent);
		return IOERR;
	}
	printf("add new entry when create file %s success\n", name);

release:
	lc -> release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
	lc -> acquire(parent);
	bool found = false;
	dirent new_entry;
	std::list<dirent> dir_list;

	EXT_RPC(lookup(parent, name, found, ino_out));
	if(found)
	{
		printf("mkdir: dir %s exists\n", name);
		lc -> release(parent);
		return EXIST;
	}
	EXT_RPC(ec->create(extent_protocol::T_DIR, ino_out));
	printf("mkdir:create dir %s success\n", name);
	EXT_RPC(readdir_nolock(parent, dir_list));
	new_entry.inum = ino_out;
	new_entry.name = name;
	dir_list.push_back(new_entry);
	if(writedir_nolock(parent, dir_list) != OK)
	{
		lc -> release(parent);	
		return IOERR;
	}
release:
	lc -> release(parent);
    return r;

}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
	if(!isdir(parent))
	{
		printf("lookup: %lld is not a dir\n", parent);
   		return IOERR;
	}
	std::list<dirent> dir_list;
	EXT_RPC(readdir_nolock(parent, dir_list));
	found = false;
	for (std::list<dirent>::iterator it = dir_list.begin(); it != dir_list.end(); ++it)
		if(it->name == name)
		{
			ino_out = it -> inum;
			found = true;
			break;
		}
	//printf("yfs::lookup is over!");
release:
	return r;
}

int 
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
	lc -> acquire(dir);
	int ret = readdir_nolock(dir, list);
	lc -> release(dir);
	return ret;
}

int
yfs_client::readdir_nolock(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
	//lc -> acquire(dir);
	list.clear();
	std::string buf;
	dirent entry;

	if(ec->get(dir, buf) != extent_protocol::OK)
	{
		printf("readdir: error with get\n");
		//lc -> release(dir);
		return IOERR;
	}
	std::istringstream ist(buf);
	//EXT_RPC(ec->get(dir, buf));
	while(std::getline(ist, entry.name, '\0'))
	{
		ist >> entry.inum;
		list.push_back(entry);
	}
	//printf("yfs:readdir is over!");
    //lc -> release(dir);
	return r;
}

int yfs_client::writedir(inum dir, std::list<dirent>& dir_list)
{
	lc->acquire(dir);
	int ret = writedir_nolock(dir, dir_list);
	lc->release(dir);
	return ret;
}


int yfs_client::writedir_nolock(inum dir, std::list<dirent>& dir_list) {

	int r = OK;
    std::ostringstream ost;
	//lc -> acquire(dir);
    for (std::list<dirent>::iterator it = dir_list.begin(); it != dir_list.end(); ++it) 
	{
        ost << it->name;
        ost.put('\0');
        ost << it->inum;
    }
   	EXT_RPC(ec->put(dir, ost.str()));

release:
	//lc -> release(dir);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
	if (ino < 0 || ino > INODE_NUM || off < 0)	
		return IOERR;
		
	lc -> acquire(ino);
	extent_protocol::attr a;
	std::string buf;
	
	EXT_RPC(ec->getattr(ino, a));
	if (off >= a.size)
	{
		lc -> release(ino);
		return IOERR;
	}
	EXT_RPC(ec->get(ino, buf));
	data = buf.substr(off, size);
release:
	lc -> release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
	if (ino < 0 || ino > INODE_NUM || off < 0)
		return IOERR;
	lc -> acquire(ino);
	std::string buf;
	EXT_RPC(ec->get(ino, buf));

	if(buf.size() < (unsigned)off)
	{
		buf.resize(off);
		buf.append(data, size);
	}
	else
	{
		if(buf.size() >= (unsigned)(off + (int)size))
			buf.replace(off, size, std::string(data, size));
		else
		{
			buf.resize(off);
			buf.append(data, size);
		}
	}
	bytes_written = size;
	EXT_RPC(ec->put(ino, buf));

release:
	lc -> release(ino);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
	lc->acquire(parent);
	if(!isdir(parent))
	{
		printf("unlink: dir %s is not a dir\n", name);
		return IOERR;
	}

	std::list<dirent> dir_list;
	std::list<dirent>::iterator it;
	bool found = false;

	EXT_RPC(readdir_nolock(parent, dir_list));
	
	for (it = dir_list.begin(); it != dir_list.end(); ++it)
		if(it->name == name)
		{
			found = true;
			break;
		}
	if (!found)
	{
		printf("no such file or directory!");
		lc->release(parent);
		return IOERR;
	}
	if (!isfile(it->inum))
	{
		printf("%s is not a file\n", name);
		lc->release(parent);
		return IOERR;
	}
	lc->acquire(it->inum);
	if ((ec->remove(it->inum)) != OK)
	{
		printf("remove file %lld fail\n", it->inum);
		lc->release(it->inum);
		lc->release(parent);
		return IOERR;
	}
	dir_list.erase(it);
	lc->release(it->inum);

	if(writedir_nolock(parent, dir_list) != OK)
	{
		printf("writedir error!");
		lc->release(parent);
		return IOERR;
	}
release:
	lc->release(parent);
    return r;
}

int yfs_client::symlink(inum parent, const char *link, const char *name, inum& ino_out)
{
	int r= OK;
	if (!isdir(parent))
	{
		printf("symlink: dir %s is not a dir\n", name);
		return IOERR;
	}
	lc -> acquire(parent);
	bool found;
	dirent new_entry;
	std::list<dirent> dir_list;
	EXT_RPC(lookup(parent, name, found, ino_out));
	if(found)
	{
		lc->release(parent);
		return EXIST;
	}
	EXT_RPC(ec->create(extent_protocol::T_SLINK, ino_out));
	EXT_RPC(ec->put(ino_out, link));
	EXT_RPC(readdir_nolock(parent, dir_list));	
	new_entry.inum = ino_out;
	new_entry.name = name;
	dir_list.push_back(new_entry);
	if(writedir_nolock(parent, dir_list) != OK)
	{
		lc->release(parent);		
		return IOERR;
	}

release:
	lc->release(parent);
	return r;
}

int yfs_client::readlink(inum ino, std::string& buf)
{
	int r = OK;
	lc->acquire(ino);
	if(ino < 0)
	{
		lc->release(ino);
		return IOERR;
	}
	extent_protocol::attr a;
	
	EXT_RPC(ec->getattr(ino, a));
	if(a.type != extent_protocol::T_SLINK)
	{
		lc->release(ino);	
		return IOERR;
	}
	EXT_RPC(ec->get(ino, buf));

release: 
	lc->release(ino);
	return r;
}


int yfs_client::getslink(inum inum, slinkinfo &sinfo)
{
	int r = OK;
	lc -> acquire(inum);
	extent_protocol::attr a;
	EXT_RPC(ec->getattr(inum, a));
	sinfo.atime = a.atime;
	sinfo.mtime = a.mtime;
	sinfo.ctime = a.ctime;
	sinfo.size = a.size;
	printf("getslink %016llx -> sz %llu\n", inum, sinfo.size);
release:
	lc -> release(inum);
	return r;
}


