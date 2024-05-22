import errno
import queue
import socket
import selectors
import threading
import time

"""
Moved some common code between nodes to generic Node.CLASS_NAME classes.
Each generic Node class runs on the 'parent_node' thread and uses locks to prevent update/access conflicts.

Might not be the most efficient method due to reference loops being possible:
    i.e. self.parent_node.connection_handler.parent_node.connection_handler... etc etc.
    
Will remain as a solution to reduce redundant/repeated code and reduce the overall code size of each specific Node file.

Note: 'Instance(Node)' refers to an instance of one of the following objects:
    BootstrapNode, ClientNode, AuthNode, ContentNode (or their Thread counterparts).
"""


class NodeThreadManager:
    """
    Storage of all threads connected to the parent node (acts as a Management Node).
    Each thread is an incoming connection to the parent node.
    """
    def __init__(self):
        """
        Instantiates Object 'NodeThreadManager'
        """
        self.node_threads = []
        self.lock = threading.Lock()

    def register(self, node):
        """
        Stores a node in Array[] 'self.node_threads[]'.
        :param node: Instance(Node)
        :return: Nothing
        """
        with self.lock:
            self.node_threads.append(node)
            return

    def unregister(self, node):
        """
        Removes Instance 'node' in Array[] 'self.node_threads[]'.
        :param node: Instance(Node)
        :return: Nothing
        """
        with self.lock:
            if node in self.node_threads:
                self.node_threads.remove(node)
                return

    def get_nodes_by_type(self, conn_type):
        """
        Gets all instances of nodes found in Array[] 'self.node_threads[]' where 'node.conn_type == conn_type'.
        :param conn_type: String
        :return: Array[]
        """
        return [node for node in self.node_threads if node.conn_type == conn_type]

    def load_balancer(self, conn_type):
        """
        Gets the requested node of conn_type based on the load of each node, returning the least loaded node
        :param conn_type: String
        :return: Instance(Node)
        """
        with self.lock:
            # Filter all handler nodes by conn_type
            nodes = self.get_nodes_by_type(conn_type)

            if nodes:
                # Fetch the node with the minimum load
                least_load_node = min(nodes, key=lambda node: node.load)
                # Increment the load
                least_load_node.load += 1
                return least_load_node

            return None

    def update_main_load(self):
        """
        Updates the load of the parent_node based on all node threads for that parent_node
        :return: Int
        """
        load = 0
        for node in self.node_threads:
            load += node.load
        return load

    def close(self):
        """
        Kills all nodes in Array[] 'self.node_threads[]' and clears Array[] 'self.node_threads[]'.
        :return: Nothing
        """
        with self.lock:
            for node in self.node_threads:
                node.kill_connection()
                node.selector.unregister(node.sock)
                node.sock.close()
                node.join()
            self.node_threads = []


class NodeConnectionHandler:
    """
    Generic Node class for handling incoming and outgoing connections
    """
    def __init__(self, parent_node):
        """
        Instantiates Object 'NodeConnectionHandler'
        :param parent_node: Instance(Node)
        """
        self.parent_node = parent_node
        self.listening_sock = None

    def configure_static_server(self):
        """
        Create a server Socket 'self.listening_sock' using predefined host and port from Node 'self.parent_node'.
        :return: Nothing
        """
        self.listening_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listening_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Bind to parent_node's node_host and node_port
        self.listening_sock.bind((self.parent_node.node_host, self.parent_node.node_port))

    def configure_dynamic_server(self):
        """
        Create a server Socket 'self.listening_sock' using dynamically generated host and port
        :return: String, Int
        """
        self.listening_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind to any available host and port on the network
        self.listening_sock.bind(('0.0.0.0', 0))
        self.listening_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _, port = self.listening_sock.getsockname()
        return socket.gethostbyname(socket.gethostname()), port

    def start_server(self):
        """
        Start listening on Socket 'self.listening_sock' for any incoming connections from other nodes.
        :return: Nothing
        """
        # Start listening on the socket
        self.listening_sock.listen()
        print(f"\n{self.parent_node.node_type}: listening on", (self.parent_node.node_host, self.parent_node.node_port))
        self.listening_sock.setblocking(False)
        self.parent_node.selector.register(self.listening_sock, selectors.EVENT_READ, data=None)

    def accept_wrapper(self, sock):
        """
        Accepts incoming connections from Socket 'sock'.
        :param sock: Socket
        :return: Array[Socket, Tuple(String, Int), Array[4]]
        """
        # Accept the incoming connection
        conn, addr = sock.accept()
        print(f"\n{self.parent_node.node_type}: accepted connection from", addr)
        conn.setblocking(False)

        # Receive handshake to establish conn_name, conn_type, conn_host, conn_port
        while True:
            try:
                handshake_message = conn.recv(1024).decode()

                if handshake_message:
                    break

            except BlockingIOError:
                pass

        handshake_args = handshake_message.split('|')
        return [conn, addr, handshake_args]

    def connect_to_node(self, conn_host, conn_port):
        """
        Connects to other nodes
        :param conn_host: String
        :param conn_port: Int
        :return: Socket
        """
        # Pre-defined connection attempt limit
        try_limit = 6
        new_sock = None
        handshake_selector = selectors.DefaultSelector()

        # Attempt connection to the node
        for attempt in range(try_limit):
            print(f"{self.parent_node.node_type}-THREAD ({self.parent_node.conn_name}): "
                  f"Connection attempt ({attempt + 1} / {try_limit}) for {(conn_host, conn_port)}")

            # Try to connect to the given (host, port)
            try:
                new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_sock.setblocking(False)
                new_sock.connect_ex((conn_host, conn_port))

            # Connection could not be made for given attempt
            except Exception as e:
                print(f"{self.parent_node.node_type}-THREAD ({self.parent_node.conn_name}): "
                      f"Error during connection attempt: {e}")
                new_sock.close()
                continue

            # Wait for socket to be writable
            handshake_selector.register(new_sock, selectors.EVENT_WRITE, data=None)

            # Check if connection is ready
            try:
                while True:
                    events = handshake_selector.select()

                    # Connection is ready, unregister and continue
                    if events:
                        handshake_selector.unregister(new_sock)
                        break

            # Connection is terminated prematurely
            except Exception as e:
                print(f"{self.parent_node.node_type}-THREAD ({self.parent_node.conn_name}): "
                      f"Error while connecting: {e}")
                new_sock.close()
                return None

            # Sending handshake message
            try:
                new_sock.send(f"{self.parent_node.node_type}|{self.parent_node.node_name}|{self.parent_node.node_host}|"
                              f"{self.parent_node.node_port}".encode())
                events = selectors.EVENT_READ | selectors.EVENT_WRITE
                self.parent_node.selector.register(new_sock, events, data=None)
                return new_sock

            # Handshake message could not be sent/received by connection socket
            except OSError:
                print(f"{self.parent_node.node_type}-THREAD ({self.parent_node.conn_name}): "
                      f"Error during connection attempt: Could not send handshake.")
                new_sock.close()
                continue

        # Attempt limit has been reached, no connection could be made
        return None


class NodeWriteHandler:
    """
    Generic Node class for handling all outgoing messages to connections.
    """
    def __init__(self, parent_node):
        """
        Instantiates Object 'NodeWriteHandler'
        :param parent_node: Instance(Node)
        """
        self.parent_node = parent_node
        self.running = True
        self.out_buffer = queue.Queue()

    def kill_connection(self):
        """
        Kills the node
        :return: Nothing
        """
        self.running = False

    def write(self, key):
        """
        Write outgoing messages from Queue 'self.out_buffer' to Socket 'key.fileobj'.
        :param key:
        :return: Nothing
        """
        print(f"\n{self.parent_node.node_type} ({self.parent_node.conn_name}): "
              f"entered write on {key.fileobj.getsockname()}")
        # Get message from queue
        try:
            message = self.out_buffer.get_nowait()

        # No message in queue
        except queue.Empty:
            message = None

        # Does the socket no longer exist?
        if key.fileobj.fileno() == -1:
            print(f"\n{self.parent_node.node_type} ({self.parent_node.conn_name}): socket is closed")
            self.parent_node.kill_connection()
            return

        # Send the message
        if message:
            key.fileobj.send(message.encode())

    def push_message(self, message):
        """
        Pushes String 'message' onto Queue 'self.out_buffer'.
        :param message: String
        :return: Nothing
        """
        self.out_buffer.put(message)
