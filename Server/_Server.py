from Server.ServerNodes import BootstrapNode

server1 = BootstrapNode.BootstrapNode("server1", "10.30.8.123", 50010)
server1.start()
# Easiest way to prevent uncaught 'KeyboardInterrupt' errors
try:
    server1.join()
except KeyboardInterrupt:
    pass


"""
Note: Issue regarding breakpoints in debugger causes incorrect timing of connections and packets.
Explanation of the distributed system flow can be found below.

Server Nodes:
- Prime Node (BootstrapNode)
    - Main connection and communication handler between all other nodes.
    - Spawns other server nodes.
    
- Auth Node (AuthNode)
    - Handles authentication of client and token/cookie generation and management
    
- Content Node (ContentNode)
    - Handles music file streaming with authenticated clients
    
Client Nodes:
- Client Node (ClientNode)
    - Main client interface for interacting with the server nodes inside the distributed system
    
Primary Network Flow:
- Prime node is created
- Prime node spawns Auth node and Content node
- Auth node and Content node are registered to the Prime node
- Client node is created
- Client node connects to the Prime node
- Client node requests Prime node for available Auth node
- Prime node searches for available Auth node
- Prime node responds to Client node with available Auth node IP and Port
- Client node connects to Auth node
- Auth node requests Client node for login
- Client node responds to Auth node with login (user input)
- Auth node validates login and generates unique token/cookie
- Auth node stores generated token/cookie
- Auth nodes responds to Client node with token/cookie
- Client node disconnects from Auth node
- Client node requests Prime node for available Content node
- Prime node searches for available Content node
- Prime node responds to Client node with available Content node IP and Port
- Client node connects to Content node
- Content node requests Client node with token/cookie
- Client node responds to Content node with token/cookie
- Content node responds to Client node with awaiting validation message
- Content node requests Prime node for available Auth node
- Prime node searches for available Auth node
- Prime node responds to Content node with available Auth node IP and Port
- Content node connects to Auth node
- Auth node requests Content node for token/cookie
- Content node responds to Auth node with token/cookie
- Auth node validates token/cookie
- Auth node responds to Content node with validation message
- Content node disconnects from Auth node

- --- REPEATING SECTION ---
- Content node generates list of available music files to stream
- Content node responds to Client node with music file list
- Client node requests Content node with name of music file
- Content node validates name of music file
- Content node requests Client node to enter streaming mode
- Client node enters streaming mode
- Client node responds to Content node with streaming mode entered
- Content mode enters streaming mode
- Content mode sends all streaming data to Client node
- Client node receives streaming data from Content node
- Client node executes streaming data
- Client node exits streaming mode
- Client responds to Content node with streaming mode left
- Content node exist streaming mode
- Goto REPEATING SECTION start
"""