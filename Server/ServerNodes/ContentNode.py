import selectors
import threading
import time
import os
import wave
import struct
import hashlib
from SharedNodes import Node

thread_manager = Node.NodeThreadManager()


class ContentNodeAuthManager:
    """
    Storage of instances of Object 'AuthNodeThread' available for connection to authenticate ClientNode(s).
    """
    _available_auth_nodes = []
    _lock = threading.Lock()

    @classmethod
    def add_auth_node(cls, auth_host, auth_port):
        """
        Stores the connection information Tuple() '(auth_host, auth_port)' of an instance of
        Object 'AuthNodeThread' in Array[] 'cls._available_auth_nodes[]'.
        :param auth_host: String
        :param auth_port: Int
        :return: Nothing
        """
        with cls._lock:
            # Already in cls._available_auth_nodes
            if (auth_host, auth_port) in cls._available_auth_nodes:
                return

            # Append the new address
            cls._available_auth_nodes.append((auth_host, auth_port))

    @classmethod
    def remove_auth_node(cls, auth_host, auth_port):
        """
        Removes the connection information Tuple() '(auth_host, auth_port)' of an instance of
        Object 'AuthNodeThread' from Array[] 'cls._available_auth_nodes[]'.
        :param auth_host: String
        :param auth_port: Int
        :return:
        """
        with cls._lock:
            # If address in cls._available_auth_nodes
            if (auth_host, auth_port) in cls._available_auth_nodes:
                # Remove from the array
                cls._available_auth_nodes.remove((auth_host, auth_port))

    @classmethod
    def get_auth_nodes(cls):
        """
        Gets Array[] 'cls._available_auth_nodes'.
        :return: Array[]
        """
        # Prevent race condition by waiting until the parent node thread gets Auth node connection data
        while len(cls._available_auth_nodes) < 1:
            time.sleep(0)

        # Return the list of available Auth nodes
        return cls._available_auth_nodes


class ContentNodeThread(threading.Thread):
    """
    Incoming connection thread to the content node.
    """
    def __init__(self, node_name, sock, addr, handshake_args):
        """
        Instantiates Object 'ContentNodeThread'.
        :param node_name: String
        :param sock: Socket
        :param addr: Tuple(String, Int)
        :param handshake_args: Array[2 or 4]
        """
        threading.Thread.__init__(self)

        self.node_type = "CONTENT"
        self.node_name = node_name
        self.node_host = None
        self.node_port = None

        self.conn_type = handshake_args[0]
        self.conn_name = handshake_args[1]
        self.conn_host = handshake_args[2]
        self.conn_port = handshake_args[3]

        self.sock = sock
        self.addr = addr

        self.client_cookie = None

        # Music streaming data
        self.path_to_music_files = os.path.join(os.path.dirname(__file__), 'Music')
        self.music_files = set()
        self.file_to_stream = None
        self.stream_mode = False

        self.auth_host = None
        self.auth_port = None
        self.auth_sock = None

        self.selector = selectors.DefaultSelector()

        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.selector.register(self.sock, events, data=None)

        self.load = 1
        thread_manager.register(self)

        self.connection_handler = Node.NodeConnectionHandler(self)
        self.write_handler = Node.NodeWriteHandler(self)

    def disconnect_from_auth_node(self):
        # Disconnect from auth node and cleanup
        self.write_handler.push_message(f"DISCONNECT|{self.auth_host}|{self.auth_port}")
        self.auth_host = None
        self.auth_port = None
        self.auth_sock = None
        self.load -= 1
        time.sleep(0)

    def get_music_files(self):
        # Fetch updated music files
        files = os.listdir(self.path_to_music_files)

        # Any music files in directory?
        if not files:
            self.write_handler.push_message("NO_FILES_AVAILABLE")
            return False

        self.music_files = {os.path.splitext(file)[0].lower() for file in files if file.lower().endswith(".wav")}
        return True

    def process_file_request(self):
        if not self.get_music_files():
            return

        response = '|'.join(self.music_files)
        self.write_handler.push_message(f"FILES_AVAILABLE|{response}")

    def run(self):
        """
        Main loop to handle read/write availability of connected nodes.
        :return: Nothing
        """
        print(f"\nCONTENT-THREAD ({self.conn_name}): entered run on {self.sock.getsockname()}")

        # Initial messages to the connected node based on 'self.conn_type'
        if self.conn_type == "CLIENT":
            self.write_handler.push_message("COOKIE_REQUEST")

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
        print(f"\nCONTENT-THREAD ({self.conn_name}): entered read on {key.fileobj.getsockname()}")

        # Get message packet
        recv_data = key.fileobj.recv(1024).decode()

        if recv_data:
            print(f"\nCONTENT-THREAD ({self.conn_name}): received {recv_data} from connection "
                  f"{key.fileobj.getpeername()}")

            # Split message packet into separate messages
            messages = recv_data.split('|')

            # Client node communication
            if self.conn_type == "CLIENT" and key.fileobj == self.sock:
                self.handle_client(key, messages)

            # Auth node communication
            elif self.conn_type == "CLIENT" and key.fileobj == self.auth_sock:
                self.handle_auth(messages)

            # Undefined message packet source
            else:
                pass

        # No message packets received
        if not recv_data:
            print(f"\nCONTENT-THREAD ({self.conn_name}): closing connection {key.fileobj.getpeername()}")
            thread_manager.unregister(self)
            self.selector.unregister(key.fileobj)
            key.fileobj.close()

    def handle_client(self, key, messages):
        # Receiving client authentication cookie
        if messages[0] == "CLIENT_COOKIE":
            # Load balancer not required as Prime node automatically handles this before this stage
            self.client_cookie = messages[1]
            auth_node = ContentNodeAuthManager.get_auth_nodes()[0]
            self.auth_host, self.auth_port = auth_node
            self.auth_sock = self.connection_handler.connect_to_node(self.auth_host, self.auth_port)
            self.load += 1
            self.write_handler.push_message("CHECKING_COOKIE")

        # Client has requested all available music files to stream
        elif messages[0] == "FILE_REQUEST":
            self.process_file_request()

        # Name of file to stream
        elif messages[0] == "FILE_NAME":
            if not self.get_music_files():
                return

            # File name found in 'self.music_files'
            if messages[1] in self.music_files:
                formatted_file = f"{messages[1]}.wav"
                self.file_to_stream = next(file for file in os.listdir(self.path_to_music_files)
                                           if file.lower() == formatted_file)
                response = "STREAM_MODE_ENTER|"

                # Open music file for streaming
                with wave.open(os.path.join(self.path_to_music_files, self.file_to_stream), 'rb') as waveform:
                    rate = waveform.getframerate()

                    # Incase the file sample rate is small (11025)
                    if rate == 22050:
                        rate = rate / 2

                    # Client requires sample rate to ensure file is played correctly
                    response += str(rate)

                self.stream_mode = True
                self.write_handler.push_message(response)

            # File name not found in 'self.music_files'
            else:
                self.write_handler.push_message("FILE_NAME_INVALID")

        # Client has entered stream mode
        elif messages[0] == "CLIENT_STREAM_MODE_ENTER" and self.stream_mode:
            # Write checking
            write_selector = selectors.DefaultSelector()
            write_selector.register(key.fileobj, selectors.EVENT_WRITE, data=None)

            # Open the file to stream
            with wave.open(os.path.join(self.path_to_music_files, self.file_to_stream), 'rb') as waveform:
                chunk_size = 1024

                while True:
                    chunk_data = waveform.readframes(chunk_size)

                    if not chunk_data:
                        break

                    md5_checksum = hashlib.md5(chunk_data).digest()

                    try:
                        events = write_selector.select()

                        if not events:
                            if key.fileobj.fileno() == -1:
                                break
                            continue

                        key.fileobj.sendall(md5_checksum + struct.pack("I", len(chunk_data)) + chunk_data)

                    except Exception as e:
                        print(f"CONTENT-THREAD ({self.conn_name}): Error in streaming loop: {e}")
                        break

        # Client has left stream mode
        elif messages[0] == "CLIENT_STREAM_MODE_LEAVE" and self.stream_mode:
            self.stream_mode = False
            self.process_file_request()

        # Handle disconnect from ClientNode
        elif messages[0] == "DISCONNECT":
            print(f"\nCONTENT-THREAD ({self.conn_name}): closing connection {key.fileobj.getpeername()}")
            self.write_handler.kill_connection()
            return

        # Undefined message packet
        else:
            pass

    def handle_auth(self, messages):
        # Request for client authentication cookie
        if messages[0] == "COOKIE_CHECK":
            self.write_handler.push_message(f"AUTH_COOKIE|{self.client_cookie}")

        # Cookie is valid
        elif messages[0] == "COOKIE_VALID":
            # Disconnect from auth node and cleanup
            self.disconnect_from_auth_node()
            self.write_handler.push_message("COOKIE_CHECK_SUCCESS")

        # Cookie is invalid
        elif messages[0] == "COOKIE_INVALID":
            self.disconnect_from_auth_node()
            self.write_handler.push_message("COOKIE_CHECK_FAIL")

        # Undefined message packet
        else:
            pass


class ContentNode(threading.Thread):
    """
    Handles incoming connections from other nodes.
    Creates an instance of Object 'ContentNodeThread' for each incoming connection.
    """
    def __init__(self, node_name="CONTENT", prime_host="127.0.0.1", prime_port=12345):
        """
        Instantiates Object 'ContentNode'.
        :param node_name: String
        :param prime_host: String
        :param prime_port: Int
        """
        threading.Thread.__init__(self)

        self.node_name = node_name
        self.node_type = "CONTENT"
        self.node_host = None
        self.node_port = None

        # Ensures values of packets are consistent by referencing universal 'conn_' prefix rather than 'node_' prefix
        self.conn_name = node_name
        self.conn_type = self.node_type
        self.conn_host = None
        self.conn_port = None

        self.prime_host = prime_host
        self.prime_port = prime_port
        self.prime_sock = None

        self.selector = selectors.DefaultSelector()

        self.load = 0

        self.connection_handler = Node.NodeConnectionHandler(self)
        self.write_handler = Node.NodeWriteHandler(self)

    def shutdown(self):
        """
        Closes all instances of Object 'ContentNodeThread' then closes itself.
        :return: Nothing
        """
        thread_manager.close()
        self.write_handler.kill_connection()

    def run(self):
        """
        Main loop to handle reading incoming connections.
        :return: Nothing
        """
        print(f"\nCONTENT: entered run on {self.node_name}")

        # Configure ContentNode server
        self.node_host, self.node_port = self.connection_handler.configure_dynamic_server()
        self.conn_host, self.conn_port = self.node_host, self.node_port
        self.connection_handler.start_server()

        # Connect to Prime node
        self.prime_sock = self.connection_handler.connect_to_node(self.prime_host, self.prime_port)

        # Check if connection established with Prime node
        if self.prime_sock is None:
            print("\nCONTENT: Prime node cannot be reached, closing.")
            self.selector.close()
            return

        # Request Auth node from Prime node
        time.sleep(0.1)
        self.write_handler.push_message(f"NODE_REQUEST|CONTENT|AUTH")

        try:
            while self.write_handler.running:
                events = self.selector.select(timeout=None)

                for key, mask in events:
                    # Accept connections that aren't the Prime node
                    if key.data is None and key.fileobj != self.prime_sock:
                        new_conn = self.connection_handler.accept_wrapper(key.fileobj)
                        module = ContentNodeThread(self.node_name, new_conn[0], new_conn[1], new_conn[2])
                        module.start()

                    # Read/write to Prime node for AuthNode request and retrieval
                    if key.fileobj == self.prime_sock:
                        # Check for difference in load
                        new_load = thread_manager.update_main_load()

                        # Update load difference locally and on the Prime node
                        if self.load != new_load:
                            self.load = new_load
                            self.write_handler.push_message(f"NODE_LOAD_UPDATED|{self.load}")

                        if mask & selectors.EVENT_READ:
                            self.read(key)

                        if mask & selectors.EVENT_WRITE and not self.write_handler.out_buffer.empty():
                            self.write_handler.write(key)

                    else:
                        pass

                if not self.selector.get_map():
                    break

        except KeyboardInterrupt:
            print("\nCONTENT: caught keyboard interrupt, exiting")

        finally:
            self.selector.close()

    def read(self, key):
        """
        Read incoming messages from Socket 'key.fileobj'.
        :param key: Instance(selectors.SelectorKey)
        :return: Nothing
        """
        print(f"\nCONTENT-THREAD ({self.node_name}): entered read on {key.fileobj.getsockname()}")

        # Get message packet
        recv_data = key.fileobj.recv(1024).decode()

        if recv_data:
            print(f"\nCONTENT: received {recv_data} from connection {key.fileobj.getpeername()}")

            # Split message packet into separate messages
            messages = recv_data.split('|')

            # Prime node communication
            if key.fileobj.getpeername() == (self.prime_host, self.prime_port):
                # Ensure message is directed to the ContentNode
                if messages[1] == "CONTENT":
                    # Has found a node instance available for connection
                    if messages[0] == "NODE_FIND_SUCCESS":
                        ContentNodeAuthManager.add_auth_node(messages[3], int(messages[4]))

                    # Undefined message packet
                    else:
                        pass

                # Undefined message packet
                else:
                    pass

            # Undefined message packet source
            else:
                pass

        # No message packets received
        if not recv_data:
            print(f"\nCONTENT: closing connection {key.fileobj.getpeername()}")
            self.selector.unregister(key.fileobj)
            key.fileobj.close()
