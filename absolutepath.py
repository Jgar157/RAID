import fsconfig
import logging
from block import *
from inode import *
from inodenumber import *
from filename import *

## This class implements methods for absolute path layer

class AbsolutePathName():
  def __init__(self, FileNameObject: FileName):
    self.FileNameObject = FileNameObject

  def PathToInodeNumber(self, path, dir):

    logging.debug("AbsolutePathName::PathToInodeNumber: path: " + str(path) + ", dir: " + str(dir))
    # print(path)
    if "/" in path:
      split_path = path.split("/")
      first = split_path[0]
      del split_path[0]
      rest = "/".join(split_path)
      logging.debug("AbsolutePathName::PathToInodeNumber: first: " + str(first) + ", rest: " + str(rest))
      d = self.FileNameObject.Lookup(first, dir)
      # print('first',d, first, dir)
      if d == -1:
        return -1
      
      inode = InodeNumber(d)
      inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
      
      # print('inode', inode.inode.type, inode.inode_number)
      if inode.inode.type == fsconfig.INODE_TYPE_SYM:
        inode_contents = inode.inode.block_numbers[0]

        block = self.FileNameObject.RawBlocks.Get(inode_contents)
        target_path = block.rstrip('\x00')
        # print('target_path', target_path)
        return self.PathToInodeNumber(target_path, d)

      return self.PathToInodeNumber(rest, d)
    else:
      d = self.FileNameObject.Lookup(path, dir)
      # print('second',d, path)

      inode = InodeNumber(d)
      inode.InodeNumberToInode(self.FileNameObject.RawBlocks)

      if inode.inode.type == fsconfig.INODE_TYPE_SYM:
        inode_contents = inode.inode.block_numbers[0]

        block = self.FileNameObject.RawBlocks.Get(inode_contents)
        target_path = block.decode("utf-8").rstrip('\x00')
        # print('second target_path', target_path)

        d = self.GeneralPathToInodeNumber(target_path, dir)
      return d


  def GeneralPathToInodeNumber(self, path, cwd):

    logging.debug ("AbsolutePathName::GeneralPathToInodeNumber: path: " + str(path) + ", cwd: " + str(cwd))

    if path[0] == "/":
      if len(path) == 1: # special case: root
        logging.debug ("AbsolutePathName::GeneralPathToInodeNumber: returning root inode 0")
        return 0
      cut_path = path[1:len(path)]
      logging.debug ("AbsolutePathName::GeneralPathToInodeNumber: cut_path: " + str(cut_path))
      return self.PathToInodeNumber(cut_path,0)
    else:
      return self.PathToInodeNumber(path,cwd)

  def Link(self, target, name, cwd):
    target_inode = self.GeneralPathToInodeNumber(target, cwd)
    parent_path = target.split("/")
    parent_inode = None

    if len(parent_path) > 1:
      if parent_path[0] == "":
        parent_path = "/"
      else:
        parent_path = parent_path[:-1]
        parent_path = "/".join(parent_path)
      parent_inode = self.GeneralPathToInodeNumber(parent_path, cwd)
    else:
      parent_inode = cwd

    if target_inode == -1:
      return "ERROR_LINK_TARGET_DOESNOT_EXIST"
    
    if parent_inode == -1:
      return "ERROR_LINK_NOT_DIRECTORY"

    if self.FileNameObject.FindAvailableFileEntry(parent_inode) == -1:
      return "ERROR_LINK_DATA_BLOCK_NOT_AVAILABLE"
    
    if self.FileNameObject.Lookup(name, cwd) != -1:
      return "ERROR_LINK_ALREADY_EXISTS"
    
    # Get the inode from the inode number
    parent_inode = InodeNumber(parent_inode)
    parent_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)

    target_inode = InodeNumber(target_inode)
    target_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
    
    if target_inode.inode.type != fsconfig.INODE_TYPE_FILE:
      return "ERROR_LINK_TARGET_NOT_FILE"

    cwd_inode = InodeNumber(cwd)
    cwd_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
    target_inode.inode.refcnt += 1
    cwd_inode.inode.refcnt += 1

    target_inode.StoreInode(self.FileNameObject.RawBlocks)

    self.FileNameObject.InsertFilenameInodeNumber(
      cwd_inode, name, target_inode.inode_number
    )

    return "SUCCESS"

  def Symlink(self, target, name, cwd):
    name_size = len(name.encode('utf-8'))
    if name_size > fsconfig.BLOCK_SIZE:
      return "ERROR_SYMLINK_TARGET_EXCEEDS_BLOCK_SIZE"

    target_inode = self.GeneralPathToInodeNumber(target, cwd)
    parent_path = target.split("/")
    parent_inode = None

    if len(parent_path) > 1:
      if parent_path[0] == "":
        parent_path = "/"
      else:
        parent_path = parent_path[:-1]
        parent_path = "/".join(parent_path)
      parent_inode = self.GeneralPathToInodeNumber(parent_path, cwd)
    else:
      parent_inode = cwd

    if target_inode == -1:
      return "ERROR_SYMLINK_TARGET_DOESNOT_EXIST"
    
    if parent_inode == -1:
      return "ERROR_SYMLINK_NOT_DIRECTORY"

    if self.FileNameObject.FindAvailableFileEntry(parent_inode) == -1:
      return "ERROR_SYMLINK_DATA_BLOCK_NOT_AVAILABLE"
    
    if self.FileNameObject.Lookup(name, cwd) != -1:
      return "ERROR_SYMLINK_ALREADY_EXISTS"
    
    # Get the inode from the inode number
    parent_inode = InodeNumber(parent_inode)
    parent_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)

    target_inode = InodeNumber(target_inode)
    target_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)

    available_inode = self.FileNameObject.FindAvailableInode()
    if available_inode == -1:
      # There is no more free inodes to use to create the symlink
      return "ERROR_SYMLINK_INODE_NOT_AVAILABLE"

    # Aquire the current working directory inode
    cwd_inode = InodeNumber(cwd)
    cwd_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)

    # Make a new inode for the symlink
    new_inode = InodeNumber(available_inode)
    new_inode.InodeNumberToInode(self.FileNameObject.RawBlocks)
    new_inode.inode.type = fsconfig.INODE_TYPE_SYM

    # Add 'name' to the symlink in block 0
    new_block = self.FileNameObject.AllocateDataBlock()

    new_inode.inode.block_numbers[0] = new_block
    block_number = new_block

    # Read the block from raw storage
    block = self.FileNameObject.RawBlocks.Get(block_number)

    # Write the name to the symlink block[0]
    # print("AbsolutePathName::Symlink: name: " + str(name) + ", name_size: " + str(name_size))
    stringbyte = bytearray(target.replace('\n', ''),"utf-8")
    block[0:len(target)] = stringbyte


    # Write modified block back to disk
    self.FileNameObject.RawBlocks.Put(block_number, block)
    new_inode.inode.size = name_size
    new_inode.inode.refcnt = 1

    # Store the new inode
    new_inode.StoreInode(self.FileNameObject.RawBlocks)
    cwd_inode.inode.refcnt += 1

    self.FileNameObject.InsertFilenameInodeNumber(
      cwd_inode, name, new_inode.inode_number
    )

    return "SUCCESS"
