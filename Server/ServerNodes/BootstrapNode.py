import selectors
import threading
import time
from Server.ServerNodes import AuthNode
from Server.ServerNodes import ContentNode
from SharedNodes import Node

thread_manager = Node.NodeThreadManager()


class BootstrapNodeThread(threading.Thread):
    """
    Incoming connection thread to the primary node.
    Each thread has a 'conn_type' that defines the type of connection (CLIENT, AUTH, CONTENT).
    """
    def __init__(self, node_name, prime_host, prime_port, sock, addr, handshake_args):
        """
        Instantiates Object 'BootstrapNodeThread'.
        :param node_name: String
        :param sock: Socket
        :param addr: Tuple(String, Int)
        :param handshake_args: Array[2 or 4]
        """
        threading.Thread.__init__(self)

        self.node_type = "PRIME"
        self.node_name = node_name
        self.node_host = None
        self.node_port = None

        self.prime_host = prime_host
        self.prime_port = prime_port

        self.conn_type = handshake_args[0]
        self.conn_name = handshake_args[1]
        self.conn_host = handshake_args[2]
        self.conn_port = handshake_args[3]

        self.sock = sock
        self.addr = addr
        self.selector = selectors.DefaultSelector()

        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.selector.register(self.sock, events, data=None)

        self.load = 1
        thread_manager.register(self)

        self.write_handler = Node.NodeWriteHandler(self)

    def run(self):
        """
        Main loop to handle read/write availability.
        :return: Nothing
        """
        print(f"\nPRIME-THREAD ({self.conn_name}): entered run on {self.sock.getsockname()}")
        try:
            while self.write_handler.running:
                events = self.selector.select(timeout=1)

                for key, mask in events:
                    if mask & selectors.EVENT_READ:
                        self.read(key)

                    if mask & selectors.EVENT_WRITE and not self.write_handler.out_buffer.empty():
                        self.write_handler.write(key)

                if not self.selector.get_map():
                    break

        except (KeyboardInterrupt, ConnectionResetError):
            pass

        finally:
            thread_manager.unregister(self)
            self.selector.close()

    def read(self, key):
        """
        Read incoming messages from Socket 'key.fileobj'.
        :param key: Instance(selectors.SelectorKey)
        :return: Nothing
        """
        print(f"\nPRIME-THREAD ({self.conn_name}): entered read on {key.fileobj.getsockname()}")

        # Get message packet
        recv_data = key.fileobj.recv(1024).decode()

        if recv_data:
            print(f"\nPRIME-THREAD ({self.conn_name}): received {recv_data} from connection "
                  f"{key.fileobj.getpeername()}")

            # Split message packet into separate messages
            messages = recv_data.split('|')

            # Set self.load to the new node load
            if messages[0] == "NODE_LOAD_UPDATED":
                self.load = int(messages[1])

            # Process node requests using thread manager and load balancer
            elif messages[0] == "NODE_REQUEST":
                self.handle_node_request(messages)

            # Undefined message packet
            else:
                pass

        # No message packets received
        if not recv_data:
            print(f"\nPRIME-THREAD ({self.conn_name}): closing connection {key.fileobj.getpeername()}")
            thread_manager.unregister(self)
            self.selector.unregister(key.fileobj)
            key.fileobj.close()

    def handle_node_request(self, messages):
        """
        Handles requests for Server Nodes by type
        :param messages: Array[]
        :return: Nothing
        """
        while True:
            # Get node using load balancer
            node = thread_manager.load_balancer(messages[2])

            # Has found a node instance available for connection
            if node is None:
                # No nodes found, spawn a new node based on conn_type
                if messages[2] == "AUTH":
                    new_node = AuthNode.AuthNode("auth1", self.prime_host, self.prime_port)

                elif messages[2] == "CONTENT":
                    new_node = ContentNode.ContentNode("content1", self.prime_host, self.prime_port)

                # Will not go here unless some unknown issue occurs (i.e. messages[2] is outside of scope)
                else:
                    return

                # Register the new node and start it
                thread_manager.register(new_node)
                new_node.start()
                time.sleep(0.1)
            else:
                break

        # Send node information to requester
        self.write_handler.push_message(f"NODE_FIND_SUCCESS|{messages[1]}|{messages[2]}|{node.conn_host}|"
                                        f"{node.conn_port}")


class BootstrapNode(threading.Thread):
    """
    Handles incoming connections from other nodes.
    Creates an instance of Object 'BootstrapNodeThread' for each incoming connection.
    """
    def __init__(self, node_name="prime1", node_host="127.0.0.1", node_port=12345):
        """
        Instantiates Object 'BootstrapNode'.
        :param node_name: String
        :param node_host: String
        :param node_port: Int
        """
        threading.Thread.__init__(self)

        self.node_name = node_name
        self.node_type = "PRIME"
        self.node_host = node_host
        self.node_port = node_port
        self.conn_name = node_name

        self.selector = selectors.DefaultSelector()
        self.connection_handler = Node.NodeConnectionHandler(self)

    def shutdown(self):
        """
        Closes all instances of Object 'BootstrapNodeThreads' then closes itself.
        :return: Nothing
        """
        thread_manager.close()
        self.selector.close()

    def run(self):
        """
        Main loop to handle reading incoming connections. Also spawns nodes 'AuthNode' and 'ContentNode'.
        :return: Nothing
        """
        print(f"\nPRIME: entered run")

        # Configure BootstrapNode server
        self.connection_handler.configure_static_server()
        self.connection_handler.start_server()

        try:
            while True:
                events = self.selector.select(timeout=None)

                for key, mask in events:
                    if key.data is None:
                        new_conn = self.connection_handler.accept_wrapper(key.fileobj)
                        module = BootstrapNodeThread(self.node_name, self.node_host, self.node_port,
                                                     new_conn[0], new_conn[1], new_conn[2])
                        module.start()

                    else:
                        pass

        except KeyboardInterrupt:
            print("\nPRIME: caught keyboard interrupt, exiting")

        finally:
            self.shutdown()
