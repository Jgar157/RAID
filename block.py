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
        self.starting_port = startingPort

        # initialize block cache empty
        self.blockcache = {}

        # parity server is always last server
        # self.parity_server = self.servers[-1]
        self.last_writer_server = self.servers[0]
        # servers should not include parity
        # servers = self.servers[:-1]
        self.num_servers = numServers


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

            try:
                data = server.Get(block_number)
            except:
                print("SERVER_TIMED_OUT SingleGet")
                print("SERVER_DISCONNECTED Get " + str(block_number))
                return -1

            if data == "CORRUPT":
                return "CORRUPT"
            
            return bytearray(data)

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

    # Raid-4 version
    def Put(self, block_number, block_data):
        parity_server = self.GetParityServer(block_number)
        target_server, virtual_block_number = self.SelectServer(block_number)

        # update block cache here due to having the real block number
        if fsconfig.CACHE_DEBUG:
            print('CACHE_WRITE_THROUGH ' + str(block_number))

        putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))

        # Update the parity server, always even if a server fails
        try:
            new_parity = self.UpdateParity(block_number, virtual_block_number, target_server, putdata)
        except Exception as e:
            print("SERVER_TIMED_OUT")
            print("SERVER_DISCONNECTED " + str(block_number))

            # Recover due to parity, don't crash or exit
        
        # Put onto target server and parity server
        # Only put after parity is updated
        res = self.SinglePut(target_server, virtual_block_number, putdata)
        self.blockcache[block_number] = putdata
        
        return 0 # Only 1 server ever fails, so this always succeeds
        
    # Raid-4 version
    def Get(self, block_number):

        target_server, virtual_block_number = self.SelectServer(block_number)
        server_read = -1
        data = -1

        if (block_number < fsconfig.TOTAL_NUM_BLOCKS-2) and (block_number in self.blockcache): 
            
            if fsconfig.CACHE_DEBUG:
                print('CACHE_HIT '+ str(block_number) + ' ' + str())

            data = self.blockcache[block_number]

        else: # Cache miss or last two blocks

            if fsconfig.CACHE_DEBUG:
                print('CACHE_MISS ' + str(block_number))

            # Only read from the target server
            try:
                data = self.SingleGet(target_server, virtual_block_number)
                server_read = target_server

            except:
                print("SERVER_TIMED_OUT")
                print("SERVER_DISCONNECTED" + str(block_number))
            
           
        # Recover from parity and other servers XOR
        # Either on crash or corrupt ^^^
        if data == "CORRUPT" or data == -1:
            print("Server " + str(server_read) + " returned corrupt block " + str(block_number))
            print("Recovering block " + str(block_number) + " from following servers")

            # Recover from parity
            data = self.Recover(target_server, block_number, virtual_block_number)
            # print("Recovered Data", data)

        # This resolves the caching issue by using the correct block number
        # and not the virtual block number which was destroying
        # the cache and causing the ls to display the wrong type
        if data != "CORRUPT" and data != -1:
            self.blockcache[block_number] = data

        else:
            # print('Success reading block ' + str(block_number) + ' from server ' + str(server_read))
            pass

        return bytearray(data)
    
    def SelectServer(self, block_number):
        # Calculate the target server index and select the server
        target_server_index = block_number % self.num_servers
        target_server = self.servers[target_server_index]

        # Calculate the virtual block number
        virtual_block_number = block_number // self.num_servers
        # print(f'Selected server {target_server} for block {block_number} (virtual block {virtual_block_number})')
        return target_server, virtual_block_number
    
    def UpdateParity(self, original_block_number, virtual_block_number, target_server, block_data):
        # Get the block from every server except the parity server
        # print('\nCalculating parity for block ' + str(block_number))

        parity_server = self.GetParityServer(original_block_number)
        parity_data = self.SingleGet(parity_server, virtual_block_number)

        # print("SERVER_TIMED_OUT")
        # parity_data = bytearray(fsconfig.BLOCK_SIZE)
        # print('parity_data: ' + str(parity_data))
        
        # If parity data is corrupt, we can't write to it so just drop action
        if parity_data == "CORRUPT" or parity_data == -1:
            print("SERVER_TIMED_OUT")
            print("SERVER_DISCONNECTED " + str(original_block_number))
            return -1

        parity_data_int = int.from_bytes(parity_data, byteorder='big')

        # print('parity_data_int: ' + parity_data_int.to_bytes(len(parity_data), byteorder='big').decode('utf-8'))

        # Get the old data from target_server
        old_data = self.SingleGet(target_server, virtual_block_number)

        if old_data == "CORRUPT" or old_data == -1:
            old_data = self.Recover(target_server, original_block_number, virtual_block_number)

        # print('old_data: ' + str(old_data))
        old_data_int = int.from_bytes(old_data, byteorder='big')
        new_data_int = int.from_bytes(block_data, byteorder='big')

        # ('new_data: ' + str(block_data))
        # print('new_data_int: ' + new_data_int.to_bytes(len(block_data), byteorder='big').decode('utf-8'))

        parity_data_int ^= old_data_int ^ new_data_int
        parity_data = parity_data_int.to_bytes(len(block_data), byteorder='big')
    
## Get the Parity Server based on the current block number
## block number must be the original block number, not the virtual block number
    def GetParityServer(self, block_number):
        # Calculate the parity server index and select the server
        parity_server_index = (block_number + 1) % self.num_servers
        parity_server = self.servers[parity_server_index]
        return parity_server

## Recover: recovers a block from parity and other servers XOR
## Either on crash or corrupt

    def Recover(self, failed_server, original_block_number, virtual_block_number):
        # Recover from parity

        # The idea of a parity server does not exist here,
        # we just need to get the parity data from all the servers
        # except the failed server and XOR them together

        # Create the empty parity data
        parity_data = bytearray(fsconfig.BLOCK_SIZE)
        parity_data_int = int.from_bytes(parity_data, byteorder='big')
        recovered_data_int = parity_data_int
        print('Recovering due to failed server ' + str(failed_server))
        for server in self.servers:
            if server != failed_server:
                data = self.SingleGet(server, virtual_block_number)

                # print('parity_data: ' + str(parity_data))

                data_int = int.from_bytes(data, byteorder='big')
                # data_str = data.decode('utf-8')
                # print(str(server) + 'data_int: ' + data_str)

                # print('data_int: ' + str(data))
                recovered_data_int ^= data_int
                
                
        recovered_data = recovered_data_int.to_bytes(len(data), byteorder='big')
        # Recovered data is the XOR of all data

        recovered_data_str = recovered_data.decode('utf-8')

        print('Recovery from server ' + str(failed_server) + ' for block ' + str(original_block_number))
        print('Recovered data: ' + recovered_data_str)
        return recovered_data
    
## Repair: repairs entire server using parity
## Either on crash or corrupt

    def Repair(self, failed_server):
        # Reconnect to the failed server
        failed_server_id = failed_server
        failed_server = self.servers[failed_server]
        
        server_url = 'http://' + fsconfig.SERVER_ADDRESS + ':' + str(self.starting_port + failed_server_id)
        self.servers[failed_server_id] = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)

        failed_server = self.servers[failed_server_id]

        # Iterate through all blocks
        blocks_per_server = fsconfig.TOTAL_NUM_BLOCKS // self.num_servers
        for block_number in range(0, blocks_per_server):

            # if the block is on the failed server
            if block_number % self.num_servers == failed_server:
                recovered_data = self.Recover(failed_server, block_number)
                failed_server.put(block_number, recovered_data)

        print('Successfully repaired server ' + str(failed_server))


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
        if  last_writer != -1 and last_writer[0] != fsconfig.CID:

            if fsconfig.CACHE_DEBUG:
                print("CACHE_INVALIDATED")

            self.blockcache = {}
            updated_block = bytearray(fsconfig.BLOCK_SIZE)
            updated_block[0] = fsconfig.CID

            # new_parity = self.UpdateParity(fsconfig.TOTAL_NUM_BLOCKS-1, VIRTUAL_BLOCK, LAST_WRITER_SERVER, updated_block)
            
            # # update parity
            # parity_server = self.GetParityServer(fsconfig.TOTAL_NUM_BLOCKS-1)
            # parity_server.Put(VIRTUAL_BLOCK, new_parity)

            # update parity
            self.UpdateParity(fsconfig.TOTAL_NUM_BLOCKS-1, VIRTUAL_BLOCK, LAST_WRITER_SERVER, updated_block)

            self.SinglePut(LAST_WRITER_SERVER, VIRTUAL_BLOCK, updated_block)

        elif last_writer == -1: # Even if the last server is dead, 

            # Invalidate cache too
            self.blockcache = {}
            updated_block = bytearray(fsconfig.BLOCK_SIZE)
            updated_block[0] = fsconfig.CID

            # new_parity = self.UpdateParity(fsconfig.TOTAL_NUM_BLOCKS-1, VIRTUAL_BLOCK, LAST_WRITER_SERVER, updated_block)
            
            # # update parity
            # parity_server = self.GetParityServer(fsconfig.TOTAL_NUM_BLOCKS-1)
            # parity_server.Put(VIRTUAL_BLOCK, new_parity)
            self.UpdateParity(fsconfig.TOTAL_NUM_BLOCKS-1, VIRTUAL_BLOCK, LAST_WRITER_SERVER, updated_block)
            
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
        for pi in range(min,max):
            print ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))
