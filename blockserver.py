import pickle, logging
import argparse
import time
import fsconfig

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)

class DiskBlocks():
  def __init__(self, total_num_blocks, block_size, delayat):
    # This class stores the raw block array
    self.block = []
    # initialize request counter
    self.counter = 0
    self.delayat = delayat
    # Initialize raw blocks
    for i in range (0, total_num_blocks):
      putdata = bytearray(block_size)
      self.block.insert(i,putdata)

  def Sleep(self):
    self.counter += 1
    if (self.counter % self.delayat) == 0:
      time.sleep(10)

if __name__ == "__main__":

  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-port', '--port', type=int, help='an integer value')
  ap.add_argument('-delayat', '--delayat', type=int, help='an integer value')
  ap.add_argument('-cblk', type=int, help='an integer value')

  args = ap.parse_args()

  if args.total_num_blocks:
    TOTAL_NUM_BLOCKS = args.total_num_blocks
  else:
    print('Must specify total number of blocks')
    quit()

  if args.block_size:
    BLOCK_SIZE = args.block_size
  else:
    print('Must specify block size')
    quit()

  if args.port:
    PORT = args.port
  else:
    print('Must specify port number')
    quit()

  if args.cblk:
    CBLK = args.cblk
  else:
    CBLK = None

  if args.delayat:
    delayat = args.delayat
  else:
    # initialize delayat with artificially large number
    delayat = 1000000000

  # initialize blocks
  RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE, delayat)

  # Create server
  server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler)

  # Checksum
  checksum = [0 for i in range(TOTAL_NUM_BLOCKS)]

  def GetChecksum():
    return checksum
  
  def calculateChecksum(block_number):
    final_num = 0
    block = RawBlocks.block[block_number]
    for byte in block:
      final_num ^= int(byte)
    return final_num

  def Get(block_number):
    result = RawBlocks.block[block_number]

    curr_checksum = calculateChecksum(block_number)
    # print("Checksum for block", block_number, "is", curr_checksum)
    # print("Checksum for block", block_number, "should be", checksum[block_number])
    if curr_checksum != checksum[block_number]:
      return "CORRUPT"
    
    RawBlocks.Sleep()
    return result

  server.register_function(Get)

  def Put(block_number, data):
    RawBlocks.block[block_number] = data.data
    RawBlocks.Sleep()
    # Update checksum for this block
    checksum[block_number] = calculateChecksum(block_number)
    # print("Just calculated: Checksum for block", block_number, "is", checksum[block_number])

    # If corrupt block enabled, corrupt the block
    if CBLK is not None and block_number == CBLK:
      RawBlocks.block[block_number] = bytearray(b'\x01') * BLOCK_SIZE
      # print("Corrupting block", block_number, "with", RawBlocks.block[block_number])

    return 0

  server.register_function(Put)

  def RSM(block_number):
    RSM_LOCKED = bytearray(b'\x01') * 1
    # print("RSM", block_number, RawBlocks.block[block_number])
    result = RawBlocks.block[block_number]
    # RawBlocks.block[block_number] = RSM_LOCKED
    RawBlocks.block[block_number] = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01'))
    RawBlocks.Sleep()
    return result

  server.register_function(RSM)

  # Run the server's main loop
  logging.error("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " on port " + str(PORT))
  print ("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " on port " + str(PORT))
  server.serve_forever()