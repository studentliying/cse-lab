#include "inode_manager.h"
#include <time.h>
#define MIN(a,b) ((a)<(b) ? (a) : (b))

// disk layer -----------------------------------------

disk::disk()
{
    bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
    if(id <= 0 || id > BLOCK_NUM || buf == NULL){
        // printf("invalid blockid_t %d when read_block\n", id);
        return;
    }
    memcpy(buf, blocks[id - 1], BLOCK_SIZE);
}

void
disk::write_block(blockid_t id, const char *buf)
{   
    if(id <= 0 || id > BLOCK_NUM || buf == NULL){
        // printf("invalid blockid_t %d when write_block\n", id);
        return;
    }
    memcpy(blocks[id - 1], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
    uint32_t id = IBLOCK(INODE_NUM, sb.nblocks) + 1;    //start after inode table 
    char bitmap[BLOCK_SIZE];
    for(; id < BLOCK_NUM; id += BPB){
        d->read_block(BBLOCK(id), bitmap);
        for(int i = 0; i < BLOCK_SIZE; i++){
            if(~bitmap[i] != 0){
                for(int offset = 0; offset < 8; offset++){
                    int mask = 1 << (7 - offset);
                    if((bitmap[i] & mask) == 0){
                        bitmap[i] |= mask;
                        d->write_block(BBLOCK(id), bitmap);
                        return id + 8*i + offset;
                    }
                }
            }
        }
    }
    return 0;
}

void
block_manager::free_block(uint32_t id)
{
    if(id <= 0 || id >= BLOCK_NUM || id <= BBLOCK(BLOCK_NUM))//superblock?
        return;
    char bitmap[BLOCK_SIZE];
    read_block(BBLOCK(id), bitmap);
    uint32_t bit = id % BPB;
    bitmap[bit/8] &= ~(1 << (7 - bit%8));
    d->write_block(BBLOCK(id), bitmap);
    return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
    bm = new block_manager();
    uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
    if (root_dir != 1) {
        // printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
        exit(0);
    }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
    if(type < 0){
        printf("wrong type:%d when alloc_inode\n",type); 
        return 0;
    }
    struct inode* ino;
    char buf[BLOCK_SIZE];
    uint32_t inum;
    //begins from 2nd inode block, so inum starts from 1
    for(inum = 1; inum < INODE_NUM; inum++){
        bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
        ino = (struct inode*)buf + (inum - 1) % IPB;
        if(ino->type == 0){
            ino->atime = time(NULL);
            ino->mtime = time(NULL);
            ino->ctime = time(NULL);
            ino->type = type;
            ino->size = 0;
            bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
            return inum;
        }
    }
    return 0;
}

void
inode_manager::free_inode(uint32_t inum)
{
    struct inode *ino = get_inode(inum);
    if(ino == NULL)
        return;
    ino->type = 0;
    ino->size = 0;
    ino->atime = 0;
    ino->mtime = 0;
    ino->ctime = 0;
    put_inode(inum, ino);
    free(ino);
    return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];
//  printf("\tim: get_inode %d\n", inum);

  if (inum <= 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }
  
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

//  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

uint32_t inode_manager::get_block(struct inode *ino, uint32_t id){
    if(id < NDIRECT)
        return ino->blocks[id];
    else{
        uint32_t indirect[BLOCK_SIZE / sizeof(uint32_t)];
        bm->read_block(ino->blocks[NDIRECT], (char *)indirect);
        return indirect[id - NDIRECT];
    }
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
    if(inum <= 0 || inum >= INODE_NUM || size == NULL || buf_out == NULL)
        return;
    struct inode* ino = get_inode(inum);
    if(ino->type == 0 ||ino == NULL || ino->size <= 0)
        return;
    char *buf = (char *)malloc(ino->size);
    char temp[BLOCK_SIZE];
    unsigned int bnum = (ino->size + BLOCK_SIZE - 1)/BLOCK_SIZE;
    for(uint32_t i = 0; i < bnum; i++){
        if(i != bnum - 1)
            bm->read_block(get_block(ino, i), buf + i*BLOCK_SIZE);
        else{
            bm->read_block(get_block(ino, i), temp);
            memcpy(buf + i*BLOCK_SIZE, temp, ino->size - i*BLOCK_SIZE);
        }
    }
    ino->atime = time(NULL);
    ino->ctime = time(NULL);
    *size = ino->size;
    *buf_out = buf;
    put_inode(inum, ino);
    free(ino);
    return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
    if((inum > INODE_NUM)|| (inum <= 0)){
        printf("write file: inum out of range\n");
        return;
    }
    if((buf == NULL) || (size < 0) || ((uint32_t)size > MAXFILE * BLOCK_SIZE)){
        printf("write file: invalid buf or size\n");
        return;
    }
    struct inode* ino = get_inode(inum);
    if(ino->type == 0 || ino == NULL){
        printf("write_file:ino type == 0\n");
        return;
    }

    int avai_bnum = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE; //avoid bnum < 0
    int need_bnum = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    //need to free blocks
    if(avai_bnum >= need_bnum){
        for(int u = need_bnum; u < avai_bnum; u++)
            bm->free_block(get_block(ino, u));
        if(need_bnum <= NDIRECT)
            bm->free_block(ino->blocks[NDIRECT]);
    }
    //need to alloc
    else{
        for(int i = avai_bnum; (avai_bnum < NDIRECT) && (i < MIN(need_bnum, NDIRECT)); i++)
            ino->blocks[i] = bm->alloc_block();
        if(need_bnum > NDIRECT){
            if(avai_bnum <= NDIRECT)
                ino->blocks[NDIRECT] = bm->alloc_block();   
            uint32_t indirect[BLOCK_SIZE / sizeof(uint32_t)];
            bm->read_block(ino->blocks[NDIRECT], (char *)indirect);
            for(int i = avai_bnum < NDIRECT ? 0 : avai_bnum - NDIRECT; i < (need_bnum - NDIRECT); i++)
                indirect[i] = bm->alloc_block();
            bm->write_block(ino->blocks[NDIRECT], (char *)indirect);
        }
    }
    //write 
    for(int i = 0; i < need_bnum; i++){
        char temp[BLOCK_SIZE];
	    if(i != need_bnum - 1)
            bm->write_block(get_block(ino, i), buf + i*BLOCK_SIZE);
        else{
            bzero(temp, BLOCK_SIZE);
            memcpy(temp, buf + i*BLOCK_SIZE, size - i*BLOCK_SIZE);
            bm->write_block(get_block(ino, i), temp);
       }
    }
    ino->mtime = time(NULL);
    ino->ctime = time(NULL);
    ino->size = size;
    put_inode(inum, ino);
    free(ino);
    return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
    if((inum <= 0) || (inum > INODE_NUM)){
        printf("invalid inum:%d when get attr\n",inum);
        return;
    }
    struct inode* ino = get_inode(inum);
    if(ino == NULL){
        printf("get NULL inode:%d when get attr\n",inum);
        return;
    }
    a.type = ino->type;
    a.atime = ino->atime;
    a.mtime = ino->mtime;
    a.ctime = ino->ctime;
    a.size = ino->size;
    put_inode(inum, ino);
    free(ino);
    return;
}

void
inode_manager::remove_file(uint32_t inum)
{
    if(inum <= 0 || inum > INODE_NUM)
        return;
    struct inode *ino = get_inode(inum);
    uint32_t bnum = (ino->size + BLOCK_SIZE - 1)/BLOCK_SIZE;
    free_inode(inum);
    for(uint32_t id = 0; id < bnum; id++)
        bm->free_block(get_block(ino, id));
    if(bnum >= NDIRECT)
        bm->free_block(ino->blocks[NDIRECT]);
    free(ino);
    std::cout<<"remove file "<<inum<<" "<<time(NULL)<<std::endl;
    std::cout.flush();
}

void
inode_manager::append_block(uint32_t inum, blockid_t &bid)
{
  /*
   * your code goes here.
   */
    if((inum > INODE_NUM)|| (inum <= 0)){
        printf("write file: inum out of range\n");
        return;
    }
    struct inode* ino = get_inode(inum);
    int avai_bnum = ino->size / BLOCK_SIZE + (ino->size % BLOCK_SIZE); 
    bid = bm->alloc_block();
    if(avai_bnum < NDIRECT){
        ino->blocks[avai_bnum] = bid;
    }
    else{
        uint32_t indirect[BLOCK_SIZE / sizeof(uint32_t)];
        bm->read_block(ino->blocks[NDIRECT], (char *)indirect);
        indirect[avai_bnum-NDIRECT] = bid;
        bm->write_block(ino->blocks[NDIRECT], (char *)indirect);
    }
    ino->size += BLOCK_SIZE;
    put_inode(inum, ino);
}

void
inode_manager::get_block_ids(uint32_t inum, std::list<blockid_t> &block_ids)
{
  /*
   * your code goes here.
   */
    struct inode* ino = get_inode(inum);
    int bnum = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    for(int i = 0; i < bnum; i++){
        block_ids.push_back(get_block(ino, i));
        std::cout<<"inode"<<inum<<" "<<get_block(ino, i)<<std::endl;
        std::cout.flush();
    }
    return;
}

void
inode_manager::read_block(blockid_t id, char buf[BLOCK_SIZE])
{
  /*
   * your code goes here.
   */
    // std::cout<<"inode readblock "<<id<<"\n";
    bm->read_block(id, (char *)buf);
}

void
inode_manager::write_block(blockid_t id, const char buf[BLOCK_SIZE])
{
  /*
   * your code goes here.
   */
    // std::cout<<"inode writeblock "<<buf<<"\n";
    // std::cout.flush();
    bm->write_block(id, (char *)buf);
}

void
inode_manager::complete(uint32_t inum, uint32_t size)
{
  /*
   * your code goes here.
   */
    struct inode* ino = get_inode(inum);
    ino->mtime = time(NULL);
    ino->size = size;
    put_inode(inum, ino);
    free(ino);
}
