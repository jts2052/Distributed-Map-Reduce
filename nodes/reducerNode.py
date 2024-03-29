import time
import socket as sock
import logging

import config

# Logger configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

class ReducerNode:
    def __init__(self, id, port, master_port, red_fn, receive_size=2048):
        self.id = id
        self.port = port
        self.master_port = master_port
        self.red_fn = red_fn
        self.input_data = None
        self.receive_size = receive_size

    def start(self):
        try:
            # Reopen socket to listen for input data
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.bind((config.network["SERVER_NAME"], self.port))
            s.listen()
            logging.info(f"Reducer {self.id} started")

            all_data = []
            while True:
                conn, addr = s.accept()
                data = conn.recv(self.receive_size).decode()
                all_data.append(data.split(":")[1])

                if data.endswith(f"{self.port}:DONE"):
                    logging.info(f"Received all data in Reducer {self.id}")
                    break

            all_data.pop()  # Remove the "DONE" message
            all_data = [eval(data) for data in all_data]
            red_result = self.red_fn(all_data)

            s.close()

            self.send_red_output(red_result)
        except Exception as e:
            logging.error(f"An error occurred in start in Reducer {self.id}: {e}")
        finally:
            s.close()

    def stop(self):
        logging.info(f"Stopping Mapper {self.id}")

    def send_red_output(self, output):
        try:
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.connect((config.network["SERVER_NAME"], self.master_port))
            s.sendall(f"{self.port}:{output}".encode())
        except Exception as e:
            logging.error(f"Failed to send reduce output from Reducer {self.id}: {e}")
        finally:
            s.close()

    def handle_msg(self, msg):
        msg_parts = msg.split(":")
        msg_id = msg_parts[0]
        msg_type = msg_parts[1]
        if msg_id == str(self.port):
            match msg_type:
                case "START":
                    self.start()
                case "STOP":
                    self.stop()
                case "WAIT":
                    pass
                case _:
                    logging.warning("Unknown message")

    def run(self):
        try:
            logging.info(f"Reducer {self.id} running")
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.setblocking(False)  # Set socket to non-blocking mode
            s.bind((config.network["SERVER_NAME"], self.port))
            s.listen()

            while True:
                try:
                    conn, addr = s.accept()
                    with conn:
                        data = conn.recv(self.receive_size)
                        logging.info(f"Received data in reducer {self.id}: {data}")
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
