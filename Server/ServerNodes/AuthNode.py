import selectors
import threading
import time
import random
from SharedNodes import Node

thread_manager = Node.NodeThreadManager()


class AuthNodeCookieManager:
    """
    Generation and storage handling of all client cookies in Array[] 'cls._client_cookies[]'.
    """
    _client_cookies = []
    _lock = threading.Lock()
    _chars = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')

    @classmethod
    def is_authenticated(cls, cookie):
        """
        Check if String 'cookie' is stored in Array[] 'cls._client_cookies[]'.
        :param cookie: String
        :return: Bool
        """
        return cookie in cls._client_cookies

    @classmethod
    def generate_cookie(cls):
        """
        Generates a unique String 'new_cookie' and stores it in Array[] 'cls._client_cookies[]'.
        :return: String
        """
        with cls._lock:
            while True:
                new_cookie = ''.join(random.choice(cls._chars) for _ in range(len(cls._chars)))
                if not cls.is_authenticated(new_cookie):
                    cls._client_cookies.append(new_cookie)
                    return new_cookie

    @classmethod
    def remove_cookie(cls, cookie):
        """
        Removes String 'cookie' from Array[] 'cls._client_cookies[]'.
        :param cookie: String
        :return: Nothing
        """
        with cls._lock:
            if cls.is_authenticated(cookie):
                cls._client_cookies.remove(cookie)

    @classmethod
    def clear_cookies(cls):
        """
        Clears Array[] 'cls._client_cookies[]'.
        :return: Nothing
        """
        with cls._lock:
            cls._client_cookies = []


class AuthNodeThread(threading.Thread):
    """
    Incoming connection thread to the authentication node.
    """
    def __init__(self, node_name, sock, addr, handshake_args):
        """
        Instantiates Object 'AuthNodeThread'
        :param node_name: String
        :param sock: Socket
        :param addr: Tuple(String, Int)
        :param handshake_args: Array[2 or 4]
        """
        threading.Thread.__init__(self)

        self.node_type = "AUTH"
        self.node_name = node_name
        self.node_host = None
        self.node_port = None

        self.conn_type = handshake_args[0]
        self.conn_name = handshake_args[1]
        self.conn_host = handshake_args[2]
        self.conn_port = handshake_args[3]

        self.sock = sock
        self.addr = addr

        self.selector = selectors.DefaultSelector()

        self.load = 1
        thread_manager.register(self)

        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.selector.register(self.sock, events, data=None)
        self.write_handler = Node.NodeWriteHandler(self)

    def run(self):
        """
        Main loop to handle read/write availability of connected nodes.
        :return: Nothing
        """
        print(f"\nAUTH-THREAD ({self.conn_name}): entered run on {self.sock.getsockname()}")

        # Initial messages to connected node based on 'self.conn_type'
        if self.conn_type == "CLIENT":
            self.write_handler.push_message("LOGIN_REQUEST")
        elif self.conn_type == "CONTENT":
            self.write_handler.push_message("COOKIE_CHECK")

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
        print(f"\nAUTH-THREAD ({self.conn_name}): entered read on {key.fileobj.getsockname()}")

        # Get message packet
        recv_data = key.fileobj.recv(1024).decode()

        if recv_data:
            print(f"\nAUTH-THREAD ({self.conn_name}): received {recv_data} from connection {key.fileobj.getpeername()}")

            # Split message packet into separate messages
            messages = recv_data.split('|')

            # Client node communication
            if self.conn_type == "CLIENT":
                self.handle_client(key, messages)

            # Content node communication
            elif self.conn_type == "CONTENT":
                self.handle_content(key, messages)

            # Undefined message packet source
            else:
                pass

        # No message packets received
        if not recv_data:
            print(f"\nAUTH-THREAD ({self.conn_name}): closing connection {key.fileobj.getpeername()}")
            thread_manager.unregister(self)
            self.selector.unregister(key.fileobj)
            key.fileobj.close()

    def handle_client(self, key, messages):
        # Client login data
        if messages[0] == "LOGIN_DATA":
            new_cookie = AuthNodeCookieManager.generate_cookie()
            self.write_handler.push_message(f"LOGIN_SUCCESS|{new_cookie}")

        # Handle disconnect from ClientNode
        elif messages[0] == "DISCONNECT":
            print(f"\nAUTH-THREAD ({self.conn_name}): closing connection {key.fileobj.getpeername()}")
            self.write_handler.kill_connection()
            return

        # Undefined message packet
        else:
            pass

    def handle_content(self, key, messages):
        # Validate ContentNode's client authentication cookie
        if messages[0] == "AUTH_COOKIE":
            # Cookie is valid
            if AuthNodeCookieManager.is_authenticated(messages[1]):
                self.write_handler.push_message("COOKIE_VALID")

            # Cookie is invalid
            else:
                self.write_handler.push_message("COOKIE_INVALID")

        # Handle disconnect from ContentNode
        elif messages[0] == "DISCONNECT":
            print(f"\nAUTH-THREAD ({self.conn_name}): closing connection {key.fileobj.getpeername()}")
            self.write_handler.kill_connection()
            return

        # Undefined message packet
        else:
            pass


class AuthNode(threading.Thread):
    """
    Handles incoming connections from other nodes.
    Creates an instance of Object 'AuthNodeThread' for each incoming connection.
    """
    def __init__(self, node_name="AUTH", prime_host="127.0.0.1", prime_port=12345):
        """
        Instantiates Object 'AuthNode'.
        :param node_name: String
        :param prime_host: String
        :param prime_port: Int
        """
        threading.Thread.__init__(self)
        random.seed()

        self.node_name = node_name
        self.node_type = "AUTH"
        self.node_host = None
        self.node_port = None

        self.conn_name = node_name
        self.conn_type = self.node_type
        self.conn_host = self.node_host
        self.conn_port = self.node_port

        self.prime_host = prime_host
        self.prime_port = prime_port
        self.prime_sock = None

        self.selector = selectors.DefaultSelector()

        self.load = 0

        self.cookie_manager = AuthNodeCookieManager()
        self.connection_handler = Node.NodeConnectionHandler(self)
        self.write_handler = Node.NodeWriteHandler(self)

    def shutdown(self):
        """
        Closes all instances of Object 'AuthNodeThread' then closes itself.
        :return: Nothing
        """
        thread_manager.close()
        self.write_handler.kill_connection()

    def run(self):
        """
        Main loop to handle reading incoming connections.
        :return: Nothing
        """
        print(f"\nAUTH: entered run on {self.node_name}")

        # Configure AuthNode server
        self.node_host, self.node_port = self.connection_handler.configure_dynamic_server()
        self.conn_host, self.conn_port = self.node_host, self.node_port
        self.connection_handler.start_server()

        # Connect to Prime node
        self.prime_sock = self.connection_handler.connect_to_node(self.prime_host, self.prime_port)

        # Check if connection established with Prime node
        if self.prime_sock is None:
            print("\nAUTH: Prime node cannot be reached, closing.")
            self.selector.close()
            return

        try:
            while self.write_handler.running:
                events = self.selector.select(timeout=None)

                for key, mask in events:
                    # Accept connections that aren't the Prime node
                    if key.data is None and key.fileobj != self.prime_sock:
                        new_conn = self.connection_handler.accept_wrapper(key.fileobj)
                        module = AuthNodeThread(self.node_name, new_conn[0], new_conn[1], new_conn[2])
                        module.start()

                    # Read/write to Prime node for AuthNode request and retrieval
                    if key.fileobj == self.prime_sock:
                        # Check for difference in load
                        new_load = thread_manager.update_main_load()

                        # Update load difference locally and on the Prime node
                        if self.load != new_load:
                            self.load = new_load
                            self.write_handler.push_message(f"NODE_LOAD_UPDATED|{self.load}")

                        # Reading from Prime node on Auth node not required for any functionality
                        if mask & selectors.EVENT_READ:
                            pass

                        if mask & selectors.EVENT_WRITE and not self.write_handler.out_buffer.empty():
                            self.write_handler.write(key)

                    else:
                        pass

                if not self.selector.get_map():
                    break

        except KeyboardInterrupt:
            print("\nAUTH: caught keyboard interrupt, exiting")

        finally:
            self.selector.close()
