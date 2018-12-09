#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
	//printf("read block starts!");
	if( id >= 0 && id < BLOCK_NUM && buf != NULL)
	{
		memcpy(buf, blocks[id], BLOCK_SIZE);
	//	printf("read block over!");
		return;
	}
}

void
disk::write_block(blockid_t id, const char *buf)
{
	if( id >= 0 && id < BLOCK_NUM && buf != NULL )
	{
		memcpy(blocks[id], buf, BLOCK_SIZE);
		return;
	}
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
	uint32_t first_data_block = BLOCK_NUM / BPB + INODE_NUM / IPB + 2;
  	for (uint32_t i = first_data_block; i < BLOCK_NUM; i++)
	{
   		if (using_blocks.find(i) == using_blocks.end())
		{
			using_blocks[i] = 0;
   			return i;
    	}
  	}
	exit(0);

}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
 	std::map<uint32_t, int>::iterator it = using_blocks.find(id);
  	if(it != using_blocks.end())
    	using_blocks.erase(it);
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
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
 	char buf[BLOCK_SIZE];
	uint32_t tmp = 1;
	while (tmp <= INODE_NUM)
	{
		bm->read_block(IBLOCK(tmp, BLOCK_NUM), buf);
		for (int i=0; i<IPB; i++)
		{
			inode_t* ino = (inode_t*)buf + i;
			if(ino->type == 0)
			{
				ino->type = type;
				ino->size = 0;
				ino->atime = time(0);
				ino->mtime = time(0);
				ino->ctime = time(0);
				bm->write_block(IBLOCK(tmp, BLOCK_NUM), buf);
				return tmp;
			}
			tmp ++;
		}
	}
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
	inode_t* ino = get_inode(inum);
	if(ino == NULL)
		return;
	ino -> type = 0;
	ino -> size = 0;
	ino -> atime = time(0);
	ino -> mtime = time(0);
	put_inode(inum, ino);
	free(ino);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
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

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
	inode_t* ino = get_inode(inum);
	ino->atime = time(0);
	int block_total = (ino->size % BLOCK_SIZE == 0) ? (ino->size/BLOCK_SIZE):(ino->size/BLOCK_SIZE+1);
    *buf_out = (char*)malloc(block_total * BLOCK_SIZE * sizeof(char));
	if (block_total <= NDIRECT)
	{
		for(int i=0; i<block_total; i++)
		    bm->read_block(ino->blocks[i], *buf_out + i*BLOCK_SIZE);
	}
	else
	{
		for(int i=0; i<NDIRECT; i++)
            bm->read_block(ino->blocks[i], *buf_out + i*BLOCK_SIZE);
		blockid_t indir_blocks[NINDIRECT];
		bm->read_block(ino->blocks[NDIRECT], (char*)(indir_blocks));
		int indir_num = block_total - NDIRECT;
		for (int i=0; i<indir_num; i++)
		    bm->read_block(indir_blocks[i], *buf_out + (i+NDIRECT)*BLOCK_SIZE);	
	}

	put_inode(inum, ino);
	*size = ino->size;
	free(ino); 
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  
	if (inum<0 || inum>=INODE_NUM)
		return;

    inode_t* ino = get_inode(inum);
	unsigned int block_old_num = ((ino->size%BLOCK_SIZE == 0) ? (ino->size/BLOCK_SIZE) : (ino->size/BLOCK_SIZE+1));
	unsigned int block_new_num = ((size%BLOCK_SIZE == 0) ? (size/BLOCK_SIZE) : (size/BLOCK_SIZE+1));
	char temp[BLOCK_SIZE];
	if(block_new_num < block_old_num) //write a smaller file
	{
		if(block_old_num <= NDIRECT) //block_new_num <= block_old_num <=NDIRECT
		{
			for(unsigned int i=0; i<block_old_num; i++)
			{
				if(i<block_new_num)
				{
					memset(temp,0,BLOCK_SIZE);
         			int cpylen = MIN(size-i*BLOCK_SIZE,BLOCK_SIZE);
         			memcpy(temp, buf+i*BLOCK_SIZE, cpylen);
					bm->write_block(ino->blocks[i], temp);
				}
				else
					bm->free_block(ino->blocks[i]);
			}
		}
		else if(block_new_num <= NDIRECT && block_old_num > NDIRECT) //block_new_num <= NDIRECT < block_old_num
		{
			for (unsigned int i=0; i<NDIRECT; i++)
			{
				if(i<block_new_num)
				{
					memset(temp,0,BLOCK_SIZE);
        			int cpylen = MIN(size-i*BLOCK_SIZE, BLOCK_SIZE);
        			memcpy(temp, buf+i*BLOCK_SIZE, cpylen);
        			bm->write_block(ino->blocks[i], temp);
				}
				else
					bm->free_block(ino->blocks[i]);
			}
			blockid_t indir_blocks[NINDIRECT];
			bm->read_block(ino->blocks[NDIRECT], (char*)indir_blocks);
			unsigned int indir_num = block_old_num - NDIRECT;
			for (unsigned int i=0; i<indir_num; i++)
				bm->free_block(indir_blocks[i]);
			bm->free_block(ino->blocks[NDIRECT]); //finally free the last block of inode 
		}

		else if(block_new_num > NDIRECT) //NDIRECT < block_new_num <= block_old_num
		{
			for(unsigned int i = 0; i < NDIRECT; i++)
			{
				memset(temp,0,BLOCK_SIZE);
        		memcpy(temp, buf+i*BLOCK_SIZE, BLOCK_SIZE);
        		bm->write_block(ino->blocks[i], temp);
			}
       		blockid_t indir_blocks[NINDIRECT];
			bm->read_block(ino->blocks[NDIRECT], (char*)indir_blocks);
			unsigned int indir_free_num = block_old_num - block_new_num;
			unsigned int indir_write_num = block_new_num - NDIRECT;
			for(unsigned int i = 0; i < indir_write_num; i++)
			{
				memset(temp,0,BLOCK_SIZE);
        		int cpylen = MIN(size-(i+NDIRECT)*BLOCK_SIZE,BLOCK_SIZE);
        		memcpy(temp, buf+(i+NDIRECT)*BLOCK_SIZE, cpylen);
        		bm->write_block(indir_blocks[i], temp);
			}
       		for(unsigned int i = 0; i < indir_free_num; i++)
          		bm->free_block(indir_blocks[i+indir_write_num]);
       		bm->write_block(ino->blocks[NDIRECT], (char*)indir_blocks);
		}
	}
	else //write a larger file 
	{
		if(block_new_num <= NDIRECT) //block_old_num < block_new_num <= NDIRECT
		{
			for(unsigned int i=0; i<block_new_num; i++)
			{
				if (i>=block_old_num)
					ino->blocks[i] = bm->alloc_block();
				memset(temp,0,BLOCK_SIZE);
        		int cpylen = MIN(size-i*BLOCK_SIZE,BLOCK_SIZE);
        		memcpy(temp, buf+i*BLOCK_SIZE, cpylen);
        		bm->write_block(ino->blocks[i], temp);
			}
		}
		else if(block_old_num <= NDIRECT && block_new_num > NDIRECT) //block_old_num <= NDIRECT < block_new_num
		{
			for(unsigned int i =0; i<NDIRECT; i++)
			{
				if (i>=block_old_num)
					ino->blocks[i] = bm->alloc_block();
				memset(temp,0,BLOCK_SIZE);
        		memcpy(temp, buf+i*BLOCK_SIZE, BLOCK_SIZE);
        		bm->write_block(ino->blocks[i], temp);
			}
			ino->blocks[NDIRECT] = bm->alloc_block();
			blockid_t indir_blocks[NINDIRECT];
			unsigned int indir_num = block_new_num - NDIRECT;
			for(unsigned int i=0; i<indir_num; i++)
			{
				indir_blocks[i] = bm->alloc_block();
				memset(temp,0,BLOCK_SIZE);
        		int cpylen = MIN(size-(i+NDIRECT)*BLOCK_SIZE,BLOCK_SIZE);
        		memcpy(temp, buf+(i+NDIRECT)*BLOCK_SIZE, cpylen);
        		bm->write_block(indir_blocks[i], temp);
			}		
			bm->write_block(ino->blocks[NDIRECT], (char *)indir_blocks);
		}
		else if (block_old_num > NDIRECT) //NDIRECT < block_olde_num < block_new_num
		{
			for(unsigned int i=0; i<NDIRECT; i++)
			{
				memset(temp,0,BLOCK_SIZE);
        		memcpy(temp, buf+i*BLOCK_SIZE, BLOCK_SIZE);
        		bm->write_block(ino->blocks[i], temp);
			}
			blockid_t indir_blocks[NINDIRECT];
			bm->read_block(ino->blocks[NDIRECT], (char*)indir_blocks);
			unsigned int indir_alloc_num = block_new_num - block_old_num;
			for(unsigned int i=0; i<indir_alloc_num; i++)
				indir_blocks[i+block_old_num-NDIRECT] = bm->alloc_block();
			unsigned int indir_num = block_new_num - NDIRECT;
			for(unsigned int i=0; i<indir_num; i++)
			{
				memset(temp,0,BLOCK_SIZE);
        		int cpylen = MIN(size-(i+NDIRECT)*BLOCK_SIZE,BLOCK_SIZE);
        		memcpy(temp, buf+(i+NDIRECT)*BLOCK_SIZE, cpylen);
        		bm->write_block(indir_blocks[i], temp);
			}
			bm->write_block(ino->blocks[NDIRECT], (char*)indir_blocks);
		}
	}
	ino -> size = size;
	ino -> mtime = time(0); 
    ino -> ctime = time(0);
	put_inode(inum, ino);
	free(ino);
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
 	inode* ino = get_inode(inum);
 	if (ino != NULL)
	{
		a.type = ino -> type;
		a.atime = ino -> atime;
		a.mtime = ino -> mtime;
		a.ctime = ino -> ctime;
		a.size = ino -> size;
		free(ino);
	} 
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
	inode_t* ino = get_inode(inum);
	if(ino == NULL)
			return;
	int block_total = (ino->size%BLOCK_SIZE == 0) ? (ino->size/BLOCK_SIZE):(ino->size/BLOCK_SIZE+1);
	if(block_total <= NDIRECT)
	{
		for( int i=0; i<block_total; i++)
			bm->free_block(ino->blocks[i]);
	}
	else
	{
		for( int i=0; i<NDIRECT; i++)
			bm->free_block(ino->blocks[i]);
		char buf[BLOCK_SIZE];
		bm->read_block(ino->blocks[NDIRECT], buf);
		int indir_num = block_total - NDIRECT;
		for(int i=0; i<indir_num; i++)
				bm->free_block(buf[i]);
		bm->free_block(ino->blocks[NDIRECT]);
	}
 	free_inode(inum);
    free(ino);	
  
}
