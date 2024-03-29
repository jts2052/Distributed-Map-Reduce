import socket as sock
import time
import threading
import logging

import config

# Logger configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class MapperNode:
    def __init__(self, id, port, master_port, map_fn, receive_size=2048):
        self.id = id
        self.port = port
        self.master_port = master_port
        self.map_fn = map_fn
        self.input_data = None
        self.receive_size = receive_size

    def start(self, multi):
        try:
            # Reopen socket to listen for input data
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.bind((config.network["SERVER_NAME"], self.port))
            s.listen()
            logging.info(f"Mapper {self.id} started")

            conn, addr = s.accept()
            data = conn.recv(self.receive_size).decode()
            msg_parts = data.split(":")

            if multi: # If multi True, indicate which document the map function is processing
                tup = eval(msg_parts[1])[0]
                doc_id = tup[0]
                self.input_data = tup[1]
                map_result = self.map_fn(self.input_data, doc_id)
            else:
                self.input_data = msg_parts[1]
                map_result = self.map_fn(self.input_data)

            s.close()

            self.send_map_output(map_result)

        except Exception as e:
            logging.error(f"An error occurred in start in Mapper {self.id}: {e}")
        finally:
            s.close()

    def stop(self):
        logging.info(f"Stopping Mapper {self.id}")

    def send_map_output(self, output):
        try:
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.connect((config.network["SERVER_NAME"], self.master_port))
            s.sendall(f"{self.port}:{output}".encode())
        except Exception as e:
            logging.error(f"Failed to send map output from Mapper {self.id}: {e}")
        finally:
            s.close()

    def handle_msg(self, msg):
        msg_parts = msg.split(":")
        msg_id = msg_parts[0]
        msg_type = msg_parts[1]
        if msg_id == str(self.port):
            match msg_type:
                case "START":
                    self.start(multi=False)
                case "MULTI":
                    self.start(multi=True)
                case "STOP":
                    self.stop()
                case "WAIT":
                    pass
                case _:
                    logging.warning("Unknown message")

    def run(self):
        try:
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.setblocking(False)  # Set socket to non-blocking mode
            s.bind((config.network["SERVER_NAME"], self.port))
            s.listen()

            while True:
                try:
                    conn, addr = s.accept()
                    with conn:
                        data = conn.recv(self.receive_size)
                        logging.info(f"Received data in mapper {self.id}: {data}")
                        if not data:
                            break
                        data = data.decode()
                        s.close()
                        self.handle_msg(data)
                        break
                except sock.error as e:
                    if (
                        e.errno == sock.errno.EAGAIN
                        or e.errno == sock.errno.EWOULDBLOCK
                    ):
                        time.sleep(0.1)
                    else:
                        logging.error("Socket error:", e)
                        break
        finally:
            s.close()
