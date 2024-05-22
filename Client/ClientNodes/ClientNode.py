import selectors
from threading import Thread
import time
import pyaudio
import struct
import hashlib
from SharedNodes import Node


class ClientNode(Thread):
    """
    Client node for outgoing connections to nodes (PRIME, AUTH, CONTENT).
    """
    def __init__(self, node_name="client1", prime_host="127.0.0.1", prime_port=12345,
                 node_host=None, node_port=None):
        """
        Instantiates Object 'ClientNode'.
        :param node_name: String
        :param prime_host: String
        :param prime_port: Int
        :param node_host: String ? None
        :param node_port: Int ? None
        """
        Thread.__init__(self)

        self.node_name = node_name
        self.node_type = "CLIENT"
        self.node_host = node_host
        self.node_port = node_port
        self.conn_name = node_name

        self.cookie = None

        self.prime_host = prime_host
        self.prime_port = prime_port
        self.prime_sock = None

        self.auth_host = None
        self.auth_port = None
        self.auth_sock = None

        self.content_host = None
        self.content_port = None
        self.content_sock = None

        self.stream_mode = False
        self.music_rate = 44100  # Default value for now

        self.selector = selectors.DefaultSelector()
        self.connection_handler = Node.NodeConnectionHandler(self)
        self.write_handler = Node.NodeWriteHandler(self)

    def run(self):
        """
        Main loop to handle read/write availability of connected nodes.
        :return: Nothing
        """
        print(f"\nCLIENT: entered run on {self.node_name}")

        # Initial request to Prime node
        self.prime_sock = self.connection_handler.connect_to_node(self.prime_host, self.prime_port)

        if self.prime_sock is None:
            print("\nCLIENT: Prime node cannot be reached, closing.")
            self.selector.close()
            return

        time.sleep(0.1)
        self.write_handler.push_message(f"NODE_REQUEST|CLIENT|AUTH")

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

            # self.write_handler.push_message(f"DISCONNECT|{self.prime_host}|{self.prime_port}")

        except (KeyboardInterrupt, ConnectionResetError):
            pass

        finally:
            self.selector.close()

    def read(self, key):
        """
        Read incoming messages from Socket 'key.fileobj'.
        :param key: Instance(selectors.SelectorKey)
        :return: Nothing
        """
        print(f"\nCLIENT ({self.node_name}): entered read on {key.fileobj.getsockname()}")

        # Streaming music files
        if key.fileobj.getpeername() == (self.content_host, self.content_port) and self.stream_mode:
            self.handle_stream_mode(key)

        # Regular communication to other nodes
        else:
            # Get message packet
            recv_data = key.fileobj.recv(1024).decode()

            if recv_data:
                print(f"\nCLIENT: received {recv_data} from connection {key.fileobj.getpeername()}")

                # Split message packet into separate messages
                messages = recv_data.split('|')

                # Prime node communication
                if key.fileobj.getpeername() == (self.prime_host, self.prime_port):
                    self.handle_prime(messages)

                # Auth node communication
                elif key.fileobj.getpeername() == (self.auth_host, self.auth_port):
                    self.handle_auth(key, messages)

                # Content node communication
                elif key.fileobj.getpeername() == (self.content_host, self.content_port):
                    self.handle_content(key, messages)

                # Undefined message packet source
                else:
                    pass

            # No message packets received
            if not recv_data:
                print(f"\nCLIENT: closing connection {key.fileobj.getpeername()}")
                self.selector.unregister(key.fileobj)
                key.fileobj.close()

    def handle_stream_mode(self, key):
        """
        Handles streaming music file data from a Socket of Object Instance 'ContentNodeThread'
        :param key: Instance(selectors.SelectorKey)
        :return: Nothing
        """
        chunk_size = 1024

        # Audio stream
        audio = pyaudio.PyAudio()
        stream = audio.open(
            format=audio.get_format_from_width(2),
            channels=2,
            rate=self.music_rate,
            output=True,
            frames_per_buffer=chunk_size
        )

        # Packet data for music file streaming
        header_size = struct.calcsize("I")
        data = b""

        # Attempt to get the audio stream
        try:
            while True:
                # Get the header of the packet
                while len(data) < header_size + 16:
                    packet = key.fileobj.recv(4 * chunk_size)  # 4K

                    # Packet hasn't been sent by the ContentNode
                    if not packet:
                        raise RuntimeError(f"CLIENT ({self.node_name}): Connection closed unexpectedly")

                    data += packet

                # Get checksum of the packet before audio streaming/processing
                md5_checksum = data[:16]
                data = data[16:]

                # Size of the audio stream packet bytes
                packet_msg_size = data[:header_size]
                data = data[header_size:]
                msg_size = struct.unpack("I", packet_msg_size)[0]

                # Loop through to receive all packet bytes
                while len(data) < msg_size:
                    try:
                        data += key.fileobj.recv(4 * chunk_size)
                    # Prevent non-blocking port timing issues
                    except BlockingIOError:
                        pass

                # Checksum after audio streaming/processing
                recv_checksum = hashlib.md5(data[:msg_size]).digest()

                # Checksums don't align
                if recv_checksum != md5_checksum:
                    print(f"CLIENT ({self.node_name}): Audio data may be corrupted")
                    break

                # Write the data of the audio stream to be played
                stream.write(data[:msg_size])
                data = data[msg_size:]

        # Handle any exception (primarily for when the stream ends)
        except Exception as e:
            print(f"CLIENT ({self.node_name}): Error in streaming loop: {e}")

        # Cleanup audio stream
        finally:
            stream.stop_stream()
            stream.close()
            audio.terminate()
            self.stream_mode = False
            self.write_handler.push_message("CLIENT_STREAM_MODE_LEAVE")

    def handle_prime(self, messages):
        """
        Handles incoming communication packets from a Socket of Object Instance 'BootstrapNodeThread'
        :param messages: Array[]
        :return: Nothing
        """
        # Ensure message is directed to the ClientNode
        if messages[1] == "CLIENT":
            # Has found a node instance available for connection
            if messages[0] == "NODE_FIND_SUCCESS":
                new_sock = self.connection_handler.connect_to_node(messages[3], int(messages[4]))

                if messages[2] == "AUTH":
                    self.auth_sock = new_sock
                    self.auth_host = messages[3]
                    self.auth_port = int(messages[4])

                elif messages[2] == "CONTENT":
                    self.content_sock = new_sock
                    self.content_host = messages[3]
                    self.content_port = int(messages[4])

            # Undefined message packet
            else:
                pass

        # Undefined message packet
        else:
            pass

    def handle_auth(self, key, messages):
        """
        Handles incoming communication packets from a Socket of Object Instance 'AuthNodeThread'
        :param key: Instance(selectors.SelectorKey)
        :param messages: Array[]
        :return: Nothing
        """
        # Request for login
        if messages[0] == "LOGIN_REQUEST":
            # Attempt user input
            try:
                response = input("Enter login information: ").strip()
                self.write_handler.push_message(f"LOGIN_DATA|{response}")

            # Connection has been terminated
            except UnicodeDecodeError:
                print(f"\nCLIENT: closing connection {key.fileobj.getpeername()}")
                self.selector.unregister(key.fileobj)
                key.fileobj.close()

            # Client has closed the program
            except KeyboardInterrupt:
                print(f"\nCLIENT: caught keyboard interrupt, exiting")

            finally:
                pass

        # Login successful
        elif messages[0] == "LOGIN_SUCCESS":
            self.cookie = messages[1]
            self.write_handler.push_message(f"DISCONNECT|{self.auth_host}|{self.auth_port}")
            self.write_handler.push_message(f"NODE_REQUEST|CLIENT|CONTENT")

        # Login unsuccessful
        elif messages[0] == "LOGIN_FAIL":
            self.write_handler.push_message(f"DISCONNECT|{self.auth_host}|{self.auth_port}")
            return

        # Undefined message packet
        else:
            pass

    def handle_content(self, key, messages):
        """
        Handles incoming communication packets from a Socket of Object Instance 'ContentNodeThread'
        :param key: Instance(selectors.SelectorKey)
        :param messages: Array[]
        :return: Nothing
        """
        # Request for authorisation cookie
        if messages[0] == "COOKIE_REQUEST":
            self.write_handler.push_message(f"CLIENT_COOKIE|{self.cookie}")

        # Response for waiting to validate
        elif messages[0] == "CHECKING_COOKIE":
            pass

        # Cookie validation successful
        elif messages[0] == "COOKIE_CHECK_SUCCESS":
            self.write_handler.push_message("FILE_REQUEST")

        # Cookie validation unsuccessful
        elif messages[0] == "COOKIE_CHECK_FAIL":
            self.write_handler.push_message(f"DISCONNECT|{self.content_host}|{self.content_port}")
            return

        # Files available for streaming
        elif messages[0] == "FILES_AVAILABLE":
            try:
                print('\n'.join(messages[1:]))
                response = input("Enter music file name: ").strip()
                self.write_handler.push_message(f"FILE_NAME|{response.lower()}")

            # Connection has been terminated
            except UnicodeDecodeError:
                print(f"\nCLIENT: closing connection {key.fileobj.getpeername()}")
                self.selector.unregister(key.fileobj)
                key.fileobj.close()

            # Client has closed the program
            except KeyboardInterrupt:
                print(f"\nCLIENT: caught keyboard interrupt, exiting")

            finally:
                pass

        # No files available
        elif messages[0] == "NO_FILES_AVAILABLE":
            time.sleep(1)
            self.write_handler.push_message("FILE_REQUEST")

        elif messages[0] == "FILE_NAME_INVALID":
            self.write_handler.push_message("FILE_REQUEST")

        # Enter streaming music mode (repeated packets)
        elif messages[0] == "STREAM_MODE_ENTER":
            self.stream_mode = True
            self.music_rate = int(float(messages[1]))
            self.write_handler.push_message("CLIENT_STREAM_MODE_ENTER")

        # Undefined message packet
        else:
            pass
