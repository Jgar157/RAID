import pickle, logging
import fsconfig
import xmlrpc.client, socket, time

#### BLOCK LAYER

# global TOTAL_NUM_BLOCKS, BLOCK_SIZE, INODE_SIZE, MAX_NUM_INODES, MAX_FILENAME, INODE_NUMBER_DIRENTRY_SIZE

class DiskBlocks():
    def __init__(self, numServers=1, startingPort=8000):

        # initialize clientID
        if fsconfig.CID >= 0 and fsconfig.CID < fsconfig.MAX_CLIENTS:
            self.clientID = fsconfig.CID
        else:
            print('Must specify valid cid')
            quit()

        # initialize XMLRPC client connection to raw block server
        if fsconfig.PORT:
            PORT = fsconfig.PORT
        else:
            print('Must specify port number')
            quit()

        # Create a list of servers based on numServers and startingPort
        self.servers = []
        for server in range(numServers):
            server_url = 'http://' + fsconfig.SERVER_ADDRESS + ':' + str(startingPort + server)
            self.servers.append(xmlrpc.client.ServerProxy(server_url, use_builtin_types=True))

        print('Connected to servers: ' + str(self.servers))

        socket.setdefaulttimeout(fsconfig.SOCKET_TIMEOUT)
        self.num_servers = numServers

        # initialize block cache empty
        self.blockcache = {}

        # parity server is always last server
        self.parity_server = self.servers[-1]
        self.last_writer_server = self.servers[0]
        # servers should not include parity
        self.servers = self.servers[:-1]
        self.num_servers = numServers - 1


    ## Put: interface to write a raw block of data to the block indexed by block number
    ## Blocks are padded with zeroes up to BLOCK_SIZE

    def SinglePut(self, server, block_number, block_data):

        logging.debug(
            'Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # ljust does the padding with zeros
            putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))
            # Write block
            # commenting this out as the request now goes to the server
            # self.block[block_number] = putdata
            # call Put() method on the server; code currently quits on any server failure
        
            try:
                ret = server.Put(block_number, putdata)
            except:
                print("SERVER_TIMED_OUT SinglePut", server, block_number)
                ret = -1

            # update block cache
            if fsconfig.CACHE_DEBUG:
                print('CACHE_WRITE_THROUGH ' + str(block_number))

            self.blockcache[block_number] = putdata
            # flag this is the last writer
            # unless this is a release - which doesn't flag last writer
            if block_number != fsconfig.TOTAL_NUM_BLOCKS-1:
                LAST_WRITER_SERVER, VIRTUAL_BLOCK = self.SelectServer(fsconfig.TOTAL_NUM_BLOCKS-1)
                updated_block = bytearray(fsconfig.BLOCK_SIZE)
                updated_block[0] = fsconfig.CID

                try:
                    LAST_WRITER_SERVER.Put(VIRTUAL_BLOCK, updated_block)
                except:
                    print("SERVER_TIMED_OUT 2!!")
                    # SERVER_DISCONNECTED operation block_number
                    print("SERVER_DISCONNECTED Put " + str(VIRTUAL_BLOCK))
                    ret = -1

            if ret == -1:
                logging.error('Put: Server returns error')
                return -1
            
            return 0
        else:
            logging.error('Put: Block out of range: ' + str(block_number))
            quit()


    ## Get: interface to read a raw block of data from block indexed by block number
    ## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

    def SingleGet(self, server, block_number):

        logging.debug('Get: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # logging.debug ('\n' + str((self.block[block_number]).hex()))
            # commenting this out as the request now goes to the server
            # return self.block[block_number]
            # call Get() method on the server
            # don't look up cache for last two blocks
            if (block_number < fsconfig.TOTAL_NUM_BLOCKS-2) and (block_number in self.blockcache):

                if fsconfig.CACHE_DEBUG:
                    print('CACHE_HIT '+ str(block_number))

                data = self.blockcache[block_number]

            else:

                if fsconfig.CACHE_DEBUG:
                    print('CACHE_MISS ' + str(block_number))

                try:
                    data = server.Get(block_number)
                except:
                    print("SERVER_TIMED_OUT SingleGet")
                    print("SERVER_DISCONNECTED Get " + str(block_number))
                    return -1
                
                # Only update cache if not corrupt or bad output
                if data != "CORRUPT" and data != -1:
                    self.blockcache[block_number] = data

            if data == "CORRUPT":
                return "CORRUPT"
            
            return bytearray(data)

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

    # Raid-4 version
    def Put(self, block_number, block_data):
        target_server, virtual_block_number = self.SelectServer(block_number)

        # Put onto target server and parity server
        res = self.SinglePut(target_server, virtual_block_number, block_data)

        # Update the parity server, always even if a server fails
        try:
            self.parity_server.Put(virtual_block_number, block_data)
        except:
            print("SERVER_TIMED_OUT Put")
            print("SERVER_DISCONNECTED Put " + str(block_number))
            return -1
        
        return 0 # Only 1 server ever fails, so this always succeeds
        
    # Raid-4 version
    def Get(self, block_number):

        target_server, virtual_block_number = self.SelectServer(block_number)
        server_read = -1
        data = -1

        # Only read from the target server
        try:
            data = self.SingleGet(target_server, virtual_block_number)
            server_read = target_server

        except:
            print("SERVER_TIMED_OUT Get")
            print("SERVER_DISCONNECTED Get " + str(block_number))
            
           
        # Recover from parity and other servers XOR
        # Either on crash or corrupt ^^^
        if data == "CORRUPT":
            print("Server " + str(server_read) + " returned corrupt block " + str(block_number))
            print("Recovering block " + str(block_number) + " from following servers")

            # Recover from parity
            data = self.Recover(target_server, block_number)

        else:
            # print('Success reading block ' + str(block_number) + ' from server ' + str(server_read))
            pass

        return data
    
    def SelectServer(self, block_number):
        # Calculate the target server index and select the server
        target_server_index = block_number % self.num_servers
        target_server = self.servers[target_server_index]

        # Calculate the virtual block number
        virtual_block_number = block_number // self.num_servers
        print(f'\nSelected server {target_server} for block {block_number} (virtual block {virtual_block_number})')
        return target_server, virtual_block_number

## Recover: recovers a block from parity and other servers XOR
## Either on crash or corrupt

    def Recover(self, failed_server, block_number):
        # Recover from parity
        parity_data = self.SingleGet(self.parity_server, block_number)

        for server in self.servers:
            if server != failed_server:
                data = self.SingleGet(server, block_number)
                parity_data ^= (parity_data, data)

        # Recovered data is the XOR of all data
        return parity_data

## RSM: read and set memory equivalent
    def RSM(self, block_number):
        logging.debug('RSM: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):

            # Always use the first server for RSM, this server is always up
            first_server = self.servers[0]
            data = first_server.RSM(block_number)

            return bytearray(data)

        logging.error('RSM: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

        ## Acquire and Release using a disk block lock

    def Acquire(self):
        logging.debug('Acquire')
        RSM_BLOCK = (fsconfig.TOTAL_NUM_BLOCKS // self.num_servers) - 1
        lockvalue = self.RSM(RSM_BLOCK);
        logging.debug("RSM_BLOCK Lock value: " + str(lockvalue))
        while lockvalue[0] == 1:  # test just first byte of block to check if RSM_LOCKED
            logging.debug("Acquire: spinning...")
            lockvalue = self.RSM(RSM_BLOCK);
        # once the lock is acquired, check if need to invalidate cache
        self.CheckAndInvalidateCache()
        return 0

    # Raid-4 version, server 0
    def Release(self):
        logging.debug('Release')
        RSM_BLOCK = (fsconfig.TOTAL_NUM_BLOCKS // self.num_servers) - 1
        # Put()s a zero-filled block to release lock

        # Put onto rsm server
        first_server = self.servers[0]
        self.SinglePut(first_server, RSM_BLOCK,bytearray(fsconfig.RSM_UNLOCKED.ljust(fsconfig.BLOCK_SIZE, b'\x00')))

        return 0

    # Use RSM server, or the first server to store the last writer
    def CheckAndInvalidateCache(self):
        LAST_WRITER_SERVER, VIRTUAL_BLOCK = self.SelectServer(fsconfig.TOTAL_NUM_BLOCKS-1)

        last_writer = self.SingleGet(LAST_WRITER_SERVER, VIRTUAL_BLOCK)

        # if ID of last writer is not self, invalidate and update
        if last_writer[0] != fsconfig.CID:

            if fsconfig.CACHE_DEBUG:
                print("CACHE_INVALIDATED")

            self.blockcache = {}
            updated_block = bytearray(fsconfig.BLOCK_SIZE)
            updated_block[0] = fsconfig.CID

            self.SinglePut(LAST_WRITER_SERVER, VIRTUAL_BLOCK, updated_block)

    ## Serializes and saves the DiskBlocks block[] data structure to a "dump" file on your disk

    def DumpToDisk(self, filename):

        logging.info("DiskBlocks::DumpToDisk: Dumping pickled blocks to file " + filename)
        file = open(filename,'wb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)
        pickle.dump(file_system_constants, file)
        pickle.dump(self.block, file)

        file.close()

    ## Loads DiskBlocks block[] data structure from a "dump" file on your disk

    def LoadFromDump(self, filename):

        logging.info("DiskBlocks::LoadFromDump: Reading blocks from pickled file " + filename)
        file = open(filename,'rb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)

        try:
            read_file_system_constants = pickle.load(file)
            if file_system_constants != read_file_system_constants:
                print('DiskBlocks::LoadFromDump Error: File System constants of File :' + read_file_system_constants + ' do not match with current file system constants :' + file_system_constants)
                return -1
            block = pickle.load(file)
            for i in range(0, fsconfig.TOTAL_NUM_BLOCKS):
                self.Put(i,block[i])
            return 0
        except TypeError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered type error ")
            return -1
        except EOFError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered EOFError error ")
            return -1
        finally:
            file.close()


## Prints to screen block contents, from min to max

    def PrintBlocks(self,tag,min,max):
        print ('#### Raw disk blocks: ' + tag)
        for i in range(min,max):
            print ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))
